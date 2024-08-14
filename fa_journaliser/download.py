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
    while True:
        session = aiohttp.ClientSession(cookies=cookies)
        journal = Journal(journal_id, datetime.datetime.now())
        async with session.get(journal.journal_link) as resp:
            try:
                resp.raise_for_status()
            except aiohttp.ClientError as e:
                logger.warning("Web request failed for journal %s, retrying", journal.journal_link, exc_info=e)
                await asyncio.sleep(5)
                continue
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


async def work_forwards(
        db: Database,
        start_journal: Journal,
        backup_cookies: dict,
        max_id: Optional[int] = None,
        batch_size: int = BATCH_SIZE,
) -> None:
    logger.info("Working forwards from %s, this is tricky.", start_journal)
    last_good_id = start_journal.journal_id
    while True:
        await asyncio.sleep(2)
        # Figure out next batch of IDs to try
        batch_start = last_good_id + 1
        batch_end = last_good_id + batch_size + 1
        if max_id is not None:
            batch_end = min(batch_end, max_id)
        next_batch = list(range(batch_start, batch_end))
        # If batch is empty, we're done
        if not next_batch:
            logger.info("Working forwards complete, reached the maximum journal ID, wow!")
            return
        # Download the next batch
        logger.info("Attempting to download new journals %s", next_batch)
        next_journals = await download_many(next_batch, backup_cookies)
        # Figure out which ones exist
        next_infos = list(await asyncio.gather(*[j.info() for j in next_journals]))
        good_journals = [next_journals[i] for i, info in enumerate(next_infos) if not info.journal_deleted]
        # If none of these journals exist, then wait and try again
        if len(good_journals) == 0:
            logger.warning("Didn't get any good new journals in that batch! Gonna wait and retry")
            await asyncio.sleep(30)
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


async def work_backwards(
        db: Database,
        start_journal: Journal,
        backup_cookies: dict,
        min_id: int = 0,
        batch_size: int = BATCH_SIZE,
) -> None:
    logger.info("Working backwards from %s. I have the easy job", start_journal)
    current_journal = start_journal
    while True:
        # Figure out next batch
        batch_start = max(min_id, current_journal.journal_id - batch_size)
        batch_end = current_journal.journal_id
        next_batch = list(range(batch_start, batch_end))
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


async def run_download(
        db: Database,
        backup_cookies: dict,
        start_id: Optional[int] = None,
        min_id: int = 0,
        max_id: Optional[int] = None,
        forward_batch_size: int = BATCH_SIZE,
        backward_batch_size: int = BATCH_SIZE,
) -> None:
    # List all current journals
    all_journals = list_downloaded_journals()
    # Truncate the set of journals to min and max
    all_journals = [
        j for j in all_journals
        if j.journal_id >= min_id and (max_id is None or j.journal_id <= max_id)
    ]
    # If there are no journals yet, download the start one
    if not all_journals:
        # If start ID isn't set, try and get it from the range
        if start_id is None:
            if max_id is None:
                start_id = min_id
            else:
                start_id = (min_id + max_id) // 2
        else:
            raise ValueError("Start ID or min and max ID, must be set")
        # Download the initial journal
        start_journal = await download_and_save(db, start_id, backup_cookies)
        all_journals = [start_journal]
    # Find newest and oldest in the set
    newest = all_journals[-1]
    oldest = all_journals[0]
    # Work forward and backwards
    task_fwd = asyncio.create_task(work_forwards(db, newest, backup_cookies, max_id, forward_batch_size))
    task_bkd = asyncio.create_task(work_backwards(db, oldest, backup_cookies, min_id, backward_batch_size))
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
