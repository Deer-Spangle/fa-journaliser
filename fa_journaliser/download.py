import asyncio
import datetime
import json
import os
import logging
from typing import Optional

import aiofiles
import aiofiles.os
import aiohttp
import prometheus_client

from fa_journaliser.database import Database
from fa_journaliser.journal import Journal
from fa_journaliser.journal_info import JournalInfo
from fa_journaliser.utils import split_list, total_journal_files, list_journals_truncated, _peak_time_active

logger = logging.getLogger(__name__)

BATCH_SIZE = 5
PEAK_SLEEP = 60
EMPTY_BATCH_SLEEP = 300
PEAK_REGISTERED_CUTOFF = 10_000
USER_AGENT = "FA-Journaliser/1.0.0 (https://github.com/Deer-Spangle/fa-journaliser contact: fa-journals@spangle.org.uk)"


total_web_requests = prometheus_client.Counter(
    "fajournaliser_web_request_total",
    "Total number of web requests which were made",
    labelnames=["has_cookies"],
)
web_request_timing_histogram = prometheus_client.Histogram(
    "fajournaliser_web_request_time_taken_seconds",
    "Histogram of the time taken by each complete web request, from request to content complete. In seconds.",
    buckets=[0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 5, 10],
)
total_downloaded_bytes = prometheus_client.Counter(
    "fajournaliser_total_downloaded_bytes",
    "Total amount of bytes downloaded from FA",
    labelnames=["has_cookies"],
)
total_downloaded_pages = prometheus_client.Counter(
    "fajournaliser_downloaded_pages_total",
    "Total number of pages downloaded from FA",
    labelnames=["has_cookies"],
)
download_attempts_needed = prometheus_client.Histogram(
    "fajournaliser_download_attempts_needed_total",
    "Number of web requests needed to successfully download a page",
    labelnames=["has_cookies"],
    buckets=[1, 2, 3, 4, 5],
)
total_downloaded_journals = prometheus_client.Counter(
    "fajournaliser_downloaded_journals_total",
    "Total number of journals which were archived",
    labelnames=["needed_login"]
)
batch_download_timing_histogram = prometheus_client.Histogram(
    "fajournaliser_batch_download_time_taken_seconds",
    "Histogram of the time taken per batch of downloads, in seconds.",
    labelnames=["batch_size"],
    buckets=[0.1, 0.5, 1, 1.5, 2, 3, 4, 5, 10],
)
batch_save_timing_histogram = prometheus_client.Histogram(
    "fajournaliser_batch_save_time_taken_seconds",
    "Histogram of the time taken to save a batch of journal entries, in seconds.",
    labelnames=["batch_size"],
    buckets=[0.1, 0.5, 1, 1.5, 2, 3, 4, 5, 10],
)
work_forwards_batch_size = prometheus_client.Gauge(
    "fajournaliser_work_forwards_batch_size",
    "Batch size being used for working forwards",
)
work_forwards_last_good_id = prometheus_client.Gauge(
    "fajournaliser_work_forwards_last_good_journal_id",
    "The newest journal ID that has been ingested while working forwards",
)
work_forwards_total_new_journals = prometheus_client.Counter(
    "fajournaliser_work_forwards_total_new_journals",
    "Count of how many new journal pages were archived while working forwards",
)
work_forwards_wasted_downloads = prometheus_client.Counter(
    "fajournaliser_work_forwards_wasted_download_total",
    "The total number of downloads done while working forwards which were then deleted (i.e. downloading journal "
    "pages that don't yet exist)",
)
work_forwards_empty_batch_count = prometheus_client.Counter(
    "fajournaliser_work_forwards_empty_batch_total",
    "The total number of empty batches that were downloaded, where a whole batch of new journals do not exist yet",
)
work_backwards_batch_size = prometheus_client.Gauge(
    "fajournaliser_work_backwards_batch_size",
    "Batch size being used for working backwards",
)
work_backwards_oldest_id = prometheus_client.Gauge(
    "fajournaliser_work_backwards_oldest_journal_id",
    "The oldest journal ID that has been ingested while working backwards",
)
work_backwards_oldest_good_id = prometheus_client.Gauge(
    "fajournaliser_work_backwards_oldest_good_journal_id",
    "The oldest journal ID, which isn't an error page, that has been ingested while working backwards",
)
peak_time_metric = prometheus_client.Gauge(
    "fajournaliser_peak_time_active",
    "Whether peak time is currently active, boolean. Whether there are more than 10k registered users online",
)


async def download_journal(journal_id: int, cookies: Optional[dict] = None) -> Journal:
    # Prepare directory
    journal = Journal(journal_id, datetime.datetime.now())
    filename = journal.journal_html_filename
    dirname = os.path.dirname(filename)
    await aiofiles.os.makedirs(dirname, exist_ok=True)
    # Setup metrics
    req_count = 0
    cookie_label = str(cookies is not None)
    # Keep trying to make the web request until it works
    while True:
        session = aiohttp.ClientSession(cookies=cookies, headers={"User-Agent": USER_AGENT})
        journal._archive_date = datetime.datetime.now()
        try:
            with web_request_timing_histogram.time():
                async with session.get(journal.journal_link) as resp:
                    req_count += 1
                    total_web_requests.labels(has_cookies=cookie_label).inc()
                    # Check web request worked
                    resp.raise_for_status()
                    # Download content
                    content = b""
                    async with aiofiles.open(filename, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            content += chunk
                            await f.write(chunk)
                    journal._info = JournalInfo.from_content_bytes(journal_id, content)
        except aiohttp.ClientError as e:
            logger.warning("Web request failed for journal %s, retrying", journal.journal_link, exc_info=e)
            await asyncio.sleep(5)
            continue
        # Metrics
        download_attempts_needed.labels(has_cookies=cookie_label).observe(req_count)
        total_downloaded_pages.labels(has_cookies=cookie_label).inc()
        total_downloaded_bytes.labels(has_cookies=cookie_label).inc(len(content))
        # Return the journal
        return journal


async def download_journal_with_backup_cookies(journal_id: int, cookies: dict) -> Journal:
    journal = await download_journal(journal_id)
    info = await journal.info()
    if info.account_private:
        journal = await download_journal(journal_id, cookies)
    total_downloaded_journals.labels(needed_login=str(info.account_private)).inc()
    total_journal_files.inc()
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
    with batch_download_timing_histogram.labels(batch_size=str(len(journal_ids))).time():
        return list(await asyncio.gather(*[
            download_journal_with_backup_cookies(journal_id, cookies) for journal_id in journal_ids
        ]))


async def save_many(journals: list[Journal], db: Database) -> None:
    with batch_save_timing_histogram.labels(batch_size=str(len(journals))).time():
        await asyncio.gather(*[journal.save(db) for journal in journals])


async def delete_many(journals: list[Journal]) -> None:
    await asyncio.gather(*[aiofiles.os.remove(j.journal_html_filename) for j in journals])
    total_journal_files.inc(-len(journals))


async def work_forwards(
        db: Database,
        start_journal: Journal,
        backup_cookies: dict,
        max_id: Optional[int] = None,
        batch_size: int = BATCH_SIZE,
        peak_sleep: int = PEAK_SLEEP,
        empty_batch_sleep: int = EMPTY_BATCH_SLEEP,
        peak_users_cutoff: int = PEAK_REGISTERED_CUTOFF,
) -> None:
    work_forwards_batch_size.set(batch_size)
    logger.info("Working forwards from %s, this is tricky.", start_journal)
    last_good_id = start_journal.journal_id
    peak_hours_active = True
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
            work_forwards_empty_batch_count.inc()
            logger.warning(
                "Didn't get any good new journals in that batch! Gonna wait %ss and retry",
                empty_batch_sleep,
            )
            await delete_many(next_journals)
            await asyncio.sleep(empty_batch_sleep)
            continue
        # Convert to list of IDs and figure which is the bleeding edge newest journal
        good_ids = [j.journal_id for j in good_journals]
        last_good_id = max(good_ids)
        work_forwards_last_good_id.set(last_good_id)
        # Figure out which were before the bleeding edge and which are after, save the ones which should exist
        split_on_last_good = split_list(next_journals, lambda j: j.journal_id <= last_good_id)
        await asyncio.gather(
            save_many(split_on_last_good[True], db),
            delete_many(split_on_last_good[False])
        )
        # Metrics and logging
        work_forwards_total_new_journals.inc(len(split_on_last_good[True]))
        work_forwards_wasted_downloads.inc(len(split_on_last_good[False]))
        saved_ids = [j.journal_id for j in split_on_last_good[True]]
        logger.info("Downloaded new journals: (%s) %s", len(saved_ids), saved_ids)
        # Check if it is peak hours
        peak_time_active = _peak_time_active(peak_hours_active, next_infos, peak_users_cutoff)
        peak_time_metric.set(int(peak_hours_active))
        if peak_time_active:
            logger.info("Peak time active, sleeping %s seconds before next batch", peak_sleep)
            await asyncio.sleep(peak_sleep)


async def work_backwards(
        db: Database,
        start_journal: Journal,
        backup_cookies: dict,
        min_id: int = 0,
        batch_size: int = BATCH_SIZE,
        peak_sleep: int = PEAK_SLEEP,
        peak_users_cutoff: int = PEAK_REGISTERED_CUTOFF,
) -> None:
    work_backwards_batch_size.set(batch_size)
    logger.info("Working backwards from %s. I have the easy job", start_journal)
    current_journal = start_journal
    peak_time_active = True
    peak_time_metric.set(peak_time_active)
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
        work_backwards_oldest_id.set(current_journal.journal_id)
        next_infos = list(await asyncio.gather(*[j.info() for j in next_journals]))
        good_ids = [i.journal_id for i in next_infos if not i.journal_deleted]
        if good_ids:
            work_backwards_oldest_good_id.set(min(good_ids))
        # Figure out if peak time is active
        peak_time_active = _peak_time_active(peak_time_active, next_infos, peak_users_cutoff)
        peak_time_metric.set(int(peak_time_active))
        if peak_time_active:
            logger.info("Peak time active, sleeping %s seconds before next batch", peak_sleep)
            await asyncio.sleep(peak_sleep)


async def run_download(
        db: Database,
        backup_cookies: dict,
        start_id: Optional[int] = None,
        min_id: int = 0,
        max_id: Optional[int] = None,
        forward_batch_size: int = BATCH_SIZE,
        backward_batch_size: int = BATCH_SIZE,
        forward_peak_sleep: int = PEAK_SLEEP,
        backward_peak_sleep: int = PEAK_SLEEP,
        forward_empty_batch_sleep: int = EMPTY_BATCH_SLEEP,
        peak_users_cutoff: int = PEAK_REGISTERED_CUTOFF,
) -> None:
    # List relevant journals
    all_journals = list_journals_truncated(min_id, max_id)
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
    task_fwd = asyncio.create_task(work_forwards(
        db,
        newest,
        backup_cookies,
        max_id,
        batch_size=forward_batch_size,
        peak_sleep=forward_peak_sleep,
        empty_batch_sleep=forward_empty_batch_sleep,
        peak_users_cutoff=peak_users_cutoff,
    ))
    task_bkd = asyncio.create_task(work_backwards(
        db,
        oldest,
        backup_cookies,
        min_id,
        batch_size=backward_batch_size,
        peak_sleep=backward_peak_sleep,
        peak_users_cutoff=peak_users_cutoff,
    ))
    await asyncio.gather(task_fwd, task_bkd)


async def test_download(journal_id: int, db: Database, cookies: dict) -> None:
    journal = await download_journal_with_backup_cookies(journal_id, cookies)
    info = await journal.info()
    print(f"Page title: {info.page_title}")
    print(f"System error: {info.is_system_error}")
    print(f"Journal deleted: {info.journal_deleted}")
    print(f"Error message: {info.error_message}")
    info.check_errors()
    print(f"Title: {info.title}")
    print(f"Journal posted: {info.posted_at}")
    print("Journal JSON:")
    print(json.dumps(info.to_json(), indent=2))
    await journal.save(db)


async def fill_gaps(db: Database, backup_cookies: dict, min_id: int, max_id: Optional[int]) -> None:
    # List all archived journal files
    all_journals = list_journals_truncated(min_id, max_id)
    logger.info("There are %s downloaded journal files", len(all_journals))
    # Calculate how many files are missing
    highest_id = max(all_journals, key=lambda x: x.journal_id)
    lowest_id = min(all_journals, key=lambda x: x.journal_id)
    total_expected = highest_id.journal_id - lowest_id.journal_id + 1
    num_missing_files = total_expected - len(all_journals)
    logger.info("There are %s archive files missing", num_missing_files)
    if num_missing_files > 0:
        # Download missing journals
        prev_id: Optional[int] = None
        for journal in all_journals:
            next_id = journal.journal_id
            # Initialise with first ID
            if prev_id is None:
                prev_id = next_id
                continue
            # Fill in missing ones
            for missing_id in range(prev_id+1, next_id):
                logger.info("Found missing journal ID: %s, downloading", missing_id)
                await download_and_save(db, missing_id, backup_cookies)
            prev_id = next_id
        logger.info("Filled in all missing archive files")
    # List which database entries are missing
    all_db_journals = await db.list_journal_ids_truncated(min_id, max_id)
    logger.info("There are %s journal entries", len(all_db_journals))
    num_missing_entries = total_expected - len(all_db_journals)
    logger.info("There are %s database entries missing", num_missing_entries)
    if num_missing_entries > 0:
        # Ingest missing journals
        prev_id: Optional[int] = None
        for journal_id in all_db_journals:
            next_id = journal_id
            # Initialise with first ID
            if prev_id is None:
                prev_id = next_id
                continue
            # Fill in missing entries
            for missing_id in range(prev_id + 1, next_id):
                logger.info("Found missing journal entry ID: %s, refreshing", missing_id)
                journal = Journal(missing_id)
                journal_info = await journal.info()
                # Re-download any that say the journal was deleted or that are incomplete files
                if journal_info.is_data_incomplete or journal_info.journal_deleted or journal_info.account_private:
                    logger.info("This journal page says it was deleted, will re-download")
                    await delete_many([journal])
                    await download_and_save(db, missing_id, backup_cookies)
                else:
                    logger.info("Saving database entry")
                    await journal.save(db)
            # Increment prev id
            prev_id = next_id
    logger.info("DONE!")
