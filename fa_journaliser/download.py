import asyncio
import datetime
import json
import os
import logging
from typing import Optional

import aiofiles
import aiofiles.os
import aiohttp

from fa_journaliser.database import Database
from fa_journaliser.journal import Journal
from fa_journaliser.journal_info import JournalInfo
from fa_journaliser.utils import list_downloaded_journals, split_list

logger = logging.getLogger(__name__)

BATCH_SIZE = 5


async def download_journal(journal_id: int, cookies: Optional[dict] = None) -> Journal:
    journal_url = f"https://www.furaffinity.net/journal/{journal_id}"
    session = aiohttp.ClientSession(cookies=cookies)
    journal = Journal(journal_id, datetime.datetime.now())
    async with session.get(journal_url) as resp:
        resp.raise_for_status()
        filename = journal.journal_html_filename
        dirname = os.path.dirname(filename)
        await aiofiles.os.makedirs(dirname, exist_ok=True)
        content = b""
        async with aiofiles.open(filename, "wb") as f:
            async for chunk in resp.content.iter_chunked(8192):
                content += chunk
                await f.write(chunk)
        journal._info = JournalInfo.from_content_bytes(journal_id, content)
        return journal


async def download_journal_with_backup_cookies(journal_id: int, cookies: dict) -> Journal:
    journal = await download_journal(journal_id)
    info = await journal.info()
    if info.account_private:
        journal = await download_journal(journal_id, cookies)
    return journal


async def download_and_save(db: Database, journal_id: int, cookies: dict) -> Journal:
    journal = await download_journal_with_backup_cookies(journal_id, cookies)
    await journal.save(db)
    return journal


async def download_if_not_exists(db: Database, journal_id: int, cookies: dict) -> Journal:
    journal = Journal(journal_id)
    if await journal.is_downloaded():
        return journal
    return await download_and_save(db, journal_id, cookies)


async def download_many(journal_ids: list[int], cookies: dict) -> list[Journal]:
    return list(await asyncio.gather(*[
        download_journal_with_backup_cookies(journal_id, cookies) for journal_id in journal_ids
    ]))


async def save_many(journals: list[Journal], db: Database) -> None:
    await asyncio.gather(*[journal.save(db) for journal in journals])


async def delete_many(journals: list[Journal]) -> None:
    await asyncio.gather(*[aiofiles.os.remove(j.journal_html_filename) for j in journals])


async def work_forwards(db: Database, start_journal: Journal, backup_cookies: dict) -> None:
    logger.info("Working forwards from %s, this is tricky.", start_journal)
    last_good_id = start_journal.journal_id
    while True:
        # Figure out next batch of IDs to try
        next_batch = list(range(last_good_id + 1, last_good_id + BATCH_SIZE + 1))
        logger.info("Attempting to download new journals %s", next_batch)
        # Download the next batch
        next_journals = await download_many(next_batch, backup_cookies)
        # Figure out which ones exist
        next_infos = list(await asyncio.gather(*[j.info() for j in next_journals]))
        good_journals = [next_journals[i] for i, info in enumerate(next_infos) if not info.journal_deleted]
        # If none of these journals exist, then wait and try again
        if len(good_journals) == 0:
            logger.warning("Didn't get any good new journals in that batch! Gonna wait and retry")
            await asyncio.sleep(10)
            continue
        # Convert to list of IDs and figure which is the bleeding edge newest journal
        good_ids = [j.journal_id for j in good_journals]
        last_good_id = max(good_ids)
        # Figure out which were before the bleeding edge and which are after
        split_on_last_good = split_list(next_journals, lambda j: j.journal_id <= last_good_id)
        await asyncio.gather(
            save_many(split_on_last_good[True], db),
            delete_many(split_on_last_good[False])
        )
        saved_ids = [j.journal_id for j in split_on_last_good[True]]
        logger.info("Downloaded new journals: (%s) %s", len(saved_ids), saved_ids)


async def work_backwards(db: Database, start_journal: Journal, backup_cookies: dict) -> None:
    logger.info("Working backwards from %s. I have the easy job", start_journal)
    current_journal = start_journal
    while True:
        # Figure out next batch
        next_batch = list(range(max(0, current_journal.journal_id - BATCH_SIZE), current_journal.journal_id))
        # If batch is empty, we're done
        if len(next_batch) == 0:
            logger.critical("Working backwards is complete! Wow")
            return
        # Download next batch
        logger.info("Attempting to download old journal batch %s", next_batch)
        next_journals = await download_many(next_batch, backup_cookies)
        await save_many(next_journals, db)
        logger.info("Downloaded old journals %s", next_batch)
        # Figure out next ID to start from
        current_journal = min(next_journals, key=lambda x: x.journal_id)


async def run_download(db: Database, backup_cookies: dict, start_id: int) -> None:
    all_journals = list_downloaded_journals()
    if not all_journals:
        start_journal = await download_and_save(db, start_id, backup_cookies)
        all_journals = [start_journal]
    newest = all_journals[-1]
    oldest = all_journals[0]
    task_fwd = asyncio.create_task(work_forwards(db, newest, backup_cookies))
    task_bkd = asyncio.create_task(work_backwards(db, oldest, backup_cookies))
    await asyncio.gather(task_fwd, task_bkd)


async def test_download(journal_id: int, db: Database) -> None:
    journal = await download_journal(journal_id)
    info = await journal.info()
    print(f"Page title: {info.page_title}")
    print(f"System error: {info.is_system_error}")
    print(f"Journal deleted: {info.journal_deleted}")
    print(f"Error message: {info.error_message}")
    print(f"Title: {info.title}")
    print(f"Journal posted: {info.posted_at}")
    print("Journal JSON:")
    print(json.dumps(info.to_json(), indent=2))
    await journal.save(db)


async def fill_gaps(db: Database, backup_cookies: dict) -> None:
    all_journals = list_downloaded_journals()
    prev_id: Optional[int] = None
    for journal in all_journals:
        next_id = journal.journal_id
        if prev_id is None:
            prev_id = next_id
            continue
        for missing_id in range(prev_id+1, next_id):
            logger.info("Found missing journal ID: %s, downloading", missing_id)
            await download_and_save(db, missing_id, backup_cookies)
        prev_id = next_id
    logger.info("DONE!")
