import asyncio
import datetime
import os
import logging
from typing import Optional

import aiohttp

from fa_journaliser.journal import Journal
from fa_journaliser.journal_info import RegisteredUsersOnly, JournalInfo
from fa_journaliser.utils import list_downloaded_journals

logger = logging.getLogger(__name__)


async def download_journal(journal_id: int, cookies: Optional[dict] = None) -> Journal:
    journal_url = f"https://www.furaffinity.net/journal/{journal_id}"
    session = aiohttp.ClientSession(cookies=cookies)
    journal = Journal(journal_id, datetime.datetime.now())
    async with session.get(journal_url) as resp:
        resp.raise_for_status()
        filename = journal.journal_html_filename
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        content = b""
        with open(filename, "wb") as f:
            async for chunk in resp.content.iter_chunked(8192):
                content += chunk
                f.write(chunk)
        journal._info = JournalInfo.from_content_bytes(journal_id, content)
        return journal


async def download_journal_with_backup_cookies(journal_id: int, cookies: dict) -> Journal:
    try:
        return await download_journal(journal_id)
    except RegisteredUsersOnly:
        return await download_journal(journal_id, cookies)


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


async def run_download():
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


async def test_download(journal_id: int) -> None:
    start_journal = await download_journal(journal_id)
    info = start_journal.info
    print(f"Page title: {info.page_title}")
    print(f"System error: {info.is_system_error}")
    print(f"Journal deleted: {info.journal_deleted}")
    print(f"Error message: {info.error_message}")
    print(f"Title: {info.title}")
    print(f"Journal posted: {info.posted_at}")
