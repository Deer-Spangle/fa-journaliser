import glob
import logging
from collections import Counter
from typing import Callable, TypeVar, Optional

import aiofiles.os
import prometheus_client

from fa_journaliser.database import Database
from fa_journaliser.journal_info import JournalNotFound, AccountDisabled, PendingDeletion, RegisteredUsersOnly
from fa_journaliser.journal import Journal


logger = logging.getLogger(__name__)


total_journal_files = prometheus_client.Gauge(
    "fajournaliser_archived_journal_files_total",
    "The total number of journal files archived",
)


def list_downloaded_journals() -> list[Journal]:
    file_paths = glob.glob("store/**/*.html", recursive=True)
    journals = [
        Journal.from_file_path(file_path) for file_path in file_paths
    ]
    return sorted(journals, key=lambda journal: journal.journal_id)


def list_journals_truncated(min_id: int, max_id: Optional[int]) -> list[Journal]:
    # List all current journals
    all_journals = list_downloaded_journals()
    total_journal_files.set(len(all_journals))
    # Truncate the set of journals to min and max
    return [
        j for j in all_journals
        if j.journal_id >= min_id and (max_id is None or j.journal_id <= max_id)
    ]



async def check_downloads() -> None:
    all_journals = list_downloaded_journals()
    results = {}
    for journal in all_journals:
        logger.info("Journal ID: %s", journal.journal_id)
        info = await journal.info()
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
            # Any of these should be deleted
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


async def import_downloads(db: Database, repopulate_path: Optional[str]) -> None:
    # List all journal files
    all_journals = list_downloaded_journals()
    logger.info("Total of %s journal files archived", len(all_journals))
    # If a repopulate path is given, filter down that list
    if repopulate_path:
        filter_ids = await db.list_ids_where_path_is_null(repopulate_path)
        all_journals = [j for j in all_journals if j.journal_id in filter_ids]
        logger.info("Filtered down to %s journals to update", len(all_journals))
    # Go through journals, parsing and importing
    for journal in all_journals:
        journal_id = journal.journal_id

        logger.info("Journal ID: %s", journal_id)
        try:
            await journal.save(db)
        except RegisteredUsersOnly:
            logger.warning("Registered users only error page. Deleting")
            await aiofiles.os.remove(journal.journal_html_filename)
            continue
    logger.info("DONE!")


T = TypeVar("T")


def split_list(seq: list[T], condition: Callable[[T], bool]) -> dict[bool, list[T]]:
    result = {True: [], False: []}
    for item in seq:
        result[condition(item)].append(item)
    return result
