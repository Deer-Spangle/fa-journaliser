import asyncio
import dataclasses
import glob
import logging
import os
import pathlib
import sys
from collections import Counter
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

from journal_info import JournalInfo, JournalNotFound, RegisteredUsersOnly, AccountDisabled, PendingDeletion

import aiohttp

logger = logging.getLogger(__name__)

COOKIE_A = ""
COOKIE_B = ""
START_JOURNAL = 10_923_887


@dataclasses.dataclass
class Journal:
    journal_id: int
    _info: Optional[JournalInfo] = dataclasses.field(default=None)

    @property
    def info(self) -> JournalInfo:
        if self._info is None:
            with open(self.journal_html_filename, "r") as f:
                self._info = JournalInfo.from_content(self.journal_id, f.read())
        return self._info

    @property
    def journal_html_filename(self) -> pathlib.Path:
        millions = self.journal_id // 1_000_000
        thousands = (self.journal_id - 1_000_000 * millions) // 1_000
        return pathlib.Path("store") / str(millions).zfill(2) / str(thousands).zfill(3) / f"{self.journal_id}.html"

    def __repr__(self) -> str:
        return f"Journal(id={self.journal_id})"

    @classmethod
    def from_file_path(cls, file_path: str) -> "Journal":
        file_name = pathlib.Path(file_path).name
        if not file_name.endswith(".html"):
            raise ValueError(f"Journal file {file_name} does not end with .html")
        file_id = file_name.removesuffix(".html")
        return Journal(
            int(file_id)
        )


def list_downloaded_journals() -> list[Journal]:
    file_paths = glob.glob("store/**/*.html", recursive=True)
    journals = [
        Journal.from_file_path(file_path) for file_path in file_paths
    ]
    return sorted(journals, key=lambda journal: journal.journal_id)


async def download_journal(journal_id: int) -> Journal:
    journal_url = f"https://www.furaffinity.net/journal/{journal_id}"
    session = aiohttp.ClientSession()
    journal = Journal(journal_id)
    async with session.get(journal_url) as resp:
        resp.raise_for_status()
        filename = journal.journal_html_filename
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "wb") as f:
            async for chunk in resp.content.iter_chunked(8192):
                f.write(chunk)
        return journal


async def work_forwards(start_journal: Journal) -> None:
    logger.info("Working forwards from %s, this is tricky.", start_journal)
    # TODO: needs to actually detect system error pages.
    logger.critical("Aborting work forwards, due to known bug")
    return
    last_known_good = start_journal
    current_journal = start_journal
    while True:
        next_id = current_journal.journal_id + 1
        logger.info("Attempting to download new journal %s", next_id)
        next_journal = await download_journal(next_id)
        logger.info("Downloaded new journal %s", next_journal)
        last_known_good = next_journal
        current_journal = next_journal


async def work_backwards(start_journal: Journal) -> None:
    logger.info("Working backwards from %s. I have the easy job", start_journal)
    current_journal = start_journal
    while True:
        next_id = current_journal.journal_id - 1
        if next_id < 0:
            logger.critical("Working backwards is complete! Wow")
            return
        logger.info("Attempting to download old journal %s", next_id)
        next_journal = await download_journal(next_id)
        logger.info("Downloaded old journal %s", next_journal)
        current_journal = next_journal


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
    logger.info("DONE!")
    counter = Counter(results.values())
    print("RESULTS!")
    for result, count in counter.most_common():
        print(f"Result: {result}, count: {count}")


async def test_journal() -> None:
    start_id = START_JOURNAL
    start_journal = await download_journal(start_id)
    info = start_journal.info
    print(f"Page title: {info.page_title}")
    print(f"System error: {info.is_system_error}")
    print(f"Journal deleted: {info.journal_deleted}")
    print(f"Error message: {info.error_message}")
    print(f"Title: {info.title}")
    print(f"Journal posted: {info.posted_at}")


async def main():
    check_downloads()
    sys.exit(1)


    all_journals = list_downloaded_journals()
    if not all_journals:
        start_id = 10923887
        start_journal = await download_journal(start_id)
        all_journals = [start_journal]
    newest = all_journals[-1]
    oldest = all_journals[0]
    task_fwd = asyncio.create_task(work_forwards(newest))
    task_bkd = asyncio.create_task(work_backwards(oldest))
    await asyncio.gather(task_fwd, task_bkd)

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/fa_search_bot.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)
    # Run the bot
    asyncio.run(main())