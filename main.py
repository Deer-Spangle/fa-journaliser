import asyncio
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import TypedDict, Optional

import click

from fa_journaliser.database import Database
from fa_journaliser.download import run_download, fill_gaps, test_download, work_forwards, \
    download_if_not_exists, work_backwards
from fa_journaliser.utils import check_downloads, import_downloads

logger = logging.getLogger(__name__)

START_JOURNAL = 10_923_887
DEFAULT_BATCH_SIZE = 5


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/fa_journaliser.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)
    aiohttp_logger = logging.getLogger("aiohttp")
    aiohttp_logger.setLevel(logging.CRITICAL)
    aiohttp_logger.propagate = False


class AppContextObj(TypedDict):
    db: Database
    conf: dict


class AppContext(click.Context):
    obj: AppContextObj


@click.group()
@click.pass_context
def main(ctx: AppContext) -> None:
    ctx.ensure_object(dict)
    # Setup logging
    setup_logging()
    # Load config
    with open("config.json", "r") as f:
        ctx.obj["conf"] = json.load(f)
    # Run the bot
    ctx.obj["db"] = Database()
    asyncio.run(ctx.obj["db"].start())
    ctx.call_on_close(lambda: asyncio.run(ctx.obj["db"].stop()))


@main.command(
    "test-download",
    help="Download and save a single journal, printing information about it, to validate the downloader",
)
@click.option("--journal_id", type=int, required=True, help="ID of the journal to download and test")
@click.pass_context
def cmd_test_download(ctx: AppContext, journal_id: int) -> None:
    ctx.ensure_object(dict)
    asyncio.run(test_download(journal_id, ctx.obj["db"]))


@main.command("check-downloads", help="Checks through all downloaded journals, to ensure they can be correctly parsed")
@click.pass_context
def cmd_check_downloads(ctx: AppContext) -> None:
    ctx.ensure_object(dict)
    # Check downloads
    asyncio.run(check_downloads())


@main.command(
    "import-downloads",
    help="Checks through all downloaded journals, saving or updating them in the database. Any journal snapshots which "
         "are 'registered users only' error pages are deleted",
)
@click.pass_context
def cmd_import_downloads(ctx: AppContext) -> None:
    ctx.ensure_object(dict)
    db = ctx.obj["db"]
    # Import downloads
    asyncio.run(import_downloads(db))


@main.command(
    "run-download",
    help="Starts the archival tool, running both backwards and forwards from a given point, or from the top and bottom "
         "of the current set of downloaded journals. A combination of 'work-forwards' and 'work-backwards' commands.",
)
@click.option(
    "--start_journal",
    type=int,
    help="The ID of the journal to start with, if none exist",
    default=START_JOURNAL,
)
@click.option("--min-journal", "--min", type=int, help="The ID of the oldest journal to download", default=0)
@click.option("--max-journal", "--max", type=int, help="The ID of the newest journal to download", default=None)
@click.option(
    "--batch-size",
    type=int,
    help="How many downloads to do at once in both directions",
    default=DEFAULT_BATCH_SIZE,
)
@click.option(
    "--forward-batch-size",
    type=int,
    help="How many downloads to do at once working forwards",
    default=None,
)
@click.option(
    "--backward-batch-size",
    type=int,
    help="How many downloads to do at once working backwards",
    default=None,
)
@click.pass_context
def cmd_run_download(
        ctx: AppContext,
        start_journal: int,
        max_journal: Optional[int],
        min_journal: int,
        batch_size: int,
        forward_batch_size: Optional[int],
        backward_batch_size: Optional[int],
) -> None:
    ctx.ensure_object(dict)
    db = ctx.obj["db"]
    cookies = ctx.obj["conf"]["fa_cookies"]
    # Setup batch sizes
    if forward_batch_size is None:
        forward_batch_size = batch_size
    if backward_batch_size is None:
        backward_batch_size = batch_size
    # Run downloader
    asyncio.run(run_download(
        db,
        cookies,
        start_id=start_journal,
        min_id=min_journal,
        max_id=max_journal,
        forward_batch_size=forward_batch_size,
        backward_batch_size=backward_batch_size,
    ))


@main.command(
    "work-forwards",
    help="Starts downloading newer and newer journals, starting from the newest it has seen, until it reaches the "
         "newest journals available. Then it keeps up to date with new journals as they are posted",
)
@click.option(
    "--start_journal",
    type=int,
    help="The ID of the journal to start with, if no journals have been downloaded yet",
    default=START_JOURNAL,
)
@click.option("--max-journal", "--max", type=int, help="The ID of the newest journal to download", default=None)
@click.option("--batch-size", type=int, help="How many downloads to do at once", default=DEFAULT_BATCH_SIZE)
@click.pass_context
def cmd_work_forwards(ctx: AppContext, start_journal: int, max_journal: Optional[int], batch_size: int) -> None:
    ctx.ensure_object(dict)
    db = ctx.obj["db"]
    cookies = ctx.obj["conf"]["fa_cookies"]
    # Fetch start journal
    journal = asyncio.run(download_if_not_exists(db, start_journal, cookies))
    # Start working forwards
    asyncio.run(work_forwards(
        db,
        journal,
        cookies,
        max_id=max_journal,
        batch_size=batch_size,
    ))


@main.command(
    "work-backwards",
    help="Starts downloading older and older journals, starting from the oldest it has seen, until it reaches the "
         "first journal on the site.",
)
@click.option(
    "--start_journal",
    type=int,
    help="The ID of the journal to start with, if no journals have been downloaded yet",
    default=START_JOURNAL,
)
@click.option("--min-journal", "--min", type=int, help="The ID of the oldest journal to download", default=0)
@click.option("--batch-size", type=int, help="How many downloads to do at once", default=DEFAULT_BATCH_SIZE)
@click.pass_context
def cmd_work_backwards(ctx: AppContext, start_journal: int, min_journal: int, batch_size: int) -> None:
    ctx.ensure_object(dict)
    db = ctx.obj["db"]
    cookies = ctx.obj["conf"]["fa_cookies"]
    # Fetch start journal
    journal = asyncio.run(download_if_not_exists(db, start_journal, cookies))
    # Start working backwards
    asyncio.run(work_backwards(
        db,
        journal,
        cookies,
        min_id=min_journal,
        batch_size=batch_size,
    ))


@main.command(
    "fill-gaps",
    help="Checks through the list of all downloaded journals, and fills in any missing journals in that dataset which "
         "may have been deleted or lost",
)
@click.pass_context
def cmd_fill_gaps(ctx: AppContext) -> None:
    ctx.ensure_object(dict)
    db = ctx.obj["db"]
    cookies = ctx.obj["conf"]["fa_cookies"]
    # Fill gaps
    asyncio.run(fill_gaps(db, cookies))


if __name__ == "__main__":
    main()
