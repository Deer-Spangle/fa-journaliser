import glob
import json
import logging
from collections import Counter

from fa_journaliser.database import Database
from fa_journaliser.journal_info import JournalNotFound, AccountDisabled, PendingDeletion, RegisteredUsersOnly
from fa_journaliser.journal import Journal


logger = logging.getLogger(__name__)


def list_downloaded_journals() -> list[Journal]:
    file_paths = glob.glob("store/**/*.html", recursive=True)
    journals = [
        Journal.from_file_path(file_path) for file_path in file_paths
    ]
    return sorted(journals, key=lambda journal: journal.journal_id)


def check_downloads() -> None:
    all_journals = list_downloaded_journals()
    results = {}
    for journal in all_journals:
        logger.info("Journal ID: %s", journal.journal_id)
        info = journal.info
        try:
            info.check_errors()
        except JournalNotFound:
            logger.info("Journal deleted")
            results[journal.journal_id] = "deleted"
        except AccountDisabled as e:
            logger.info(f"Account disabled: {e}")
            results[journal.journal_id] = "account disabled"
        except PendingDeletion:
            logger.info("Account pending deletion")
            results[journal.journal_id] = "pending deletion"
        except RegisteredUsersOnly:
            # TODO: delete these, redownload
            logger.warning("Registered users only error page")
            results[journal.journal_id] = "registered users only"
        else:
            logger.info("Journal title: %s", info.title)
            results[journal.journal_id] = "Good!"
        if len(results) % 100 == 0:
            counter = Counter(results.values())
            print(f"RESULTS UPDATE: {len(results)}")
            print(counter.most_common())
    logger.info("DONE!")
    counter = Counter(results.values())
    print("RESULTS!")
    for result, count in counter.most_common():
        print(f"Result: {result}, count: {count}")


async def import_downloads(db: Database) -> None:
    all_journals = list_downloaded_journals()
    for journal in all_journals:
        journal_id = journal.journal_id
        is_deleted = False
        archive_date = journal.archive_date
        error = None
        login_used = None
        json_data = None

        logger.info("Journal ID: %s", journal_id)
        info = journal.info
        try:
            info.check_errors()
        except JournalNotFound:
            is_deleted = True
            error = "Journal not found"
            logger.info("Journal not found")
        except AccountDisabled as e:
            is_deleted = True
            error = str(e)
            logger.info(f"Account disabled: {e}")
        except PendingDeletion as e:
            is_deleted = True
            error = str(e)
            logger.info(f"Account pending deletion: {e}")
        except RegisteredUsersOnly:
            # TODO: delete these, redownload
            logger.warning("Registered users only error page")
        else:
            json_data = json.dumps(journal.info.to_json())
            logger.info("Journal title: %s", info.title)
        await db.add_entry(journal_id, is_deleted, archive_date, error, login_used, json_data)
    logger.info("DONE!")
