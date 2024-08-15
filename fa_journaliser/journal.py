import dataclasses
import datetime
import json
import logging
import pathlib
from typing import Optional, Type

import aiofiles
import aiofiles.os
import prometheus_client

from fa_journaliser.database import Database
from fa_journaliser.journal_info import JournalInfo, JournalNotFound, AccountDisabled, PendingDeletion

logger = logging.getLogger(__name__)


total_error_counts = prometheus_client.Counter(
    "fajournaliser_saved_journal_error_type_count_total",
    "Total number of journal error pages seen when saving journals to database, by error type",
    labelnames=["error_class"],
)
# Initialise for all error types that are saved
for exc_class in [None, JournalNotFound, AccountDisabled, PendingDeletion]:
    total_error_counts.labels(error_class=str(exc_class))


@dataclasses.dataclass
class Journal:
    journal_id: int
    _archive_date: Optional[datetime.datetime] = dataclasses.field(default=None)
    _info: Optional[JournalInfo] = dataclasses.field(default=None)

    async def info(self) -> JournalInfo:
        if self._info is None:
            async with aiofiles.open(self.journal_html_filename, "rb") as f:
                content = await f.read()
                self._info = JournalInfo.from_content_bytes(self.journal_id, content)
        return self._info

    async def archive_date(self) -> datetime.datetime:
        if self._archive_date is None:
            unix_mtime = await aiofiles.os.path.getmtime(self.journal_html_filename)
            self._archive_date = datetime.datetime.fromtimestamp(unix_mtime)
        return self._archive_date

    @property
    def journal_html_filename(self) -> pathlib.Path:
        millions = self.journal_id // 1_000_000
        thousands = (self.journal_id - 1_000_000 * millions) // 1_000
        return pathlib.Path("store") / str(millions).zfill(2) / str(thousands).zfill(3) / f"{self.journal_id}.html"

    @property
    def journal_link(self) -> str:
        return f"https://furaffinity.net/journal/{self.journal_id}"

    def __repr__(self) -> str:
        return f"Journal(id={self.journal_id})"

    async def is_downloaded(self) -> bool:
        return await aiofiles.os.path.exists(self.journal_html_filename)

    @classmethod
    def from_file_path(cls, file_path: str) -> "Journal":
        file_name = pathlib.Path(file_path).name
        if not file_name.endswith(".html"):
            raise ValueError(f"Journal file {file_name} does not end with .html")
        file_id = file_name.removesuffix(".html")
        return Journal(
            int(file_id),
            None,
        )

    async def save(
            self,
            db: Database
    ) -> None:
        info = await self.info()
        journal_id = self.journal_id
        is_deleted = False
        archive_date = await self.archive_date()
        error = None
        error_type: Optional[Type] = None
        login_used = info.login_user
        json_data = None

        logger.info("Saving journal link: %s", self.journal_link)
        try:
            info.check_errors()
        except JournalNotFound as e:
            is_deleted = True
            error = "Journal not found"
            error_type = type(e)
            logger.info("Journal not found")
        except AccountDisabled as e:
            is_deleted = True
            error = str(e)
            error_type = type(e)
            logger.info(f"Account disabled: {e}")
        except PendingDeletion as e:
            is_deleted = True
            error = str(e)
            error_type = type(e)
            logger.info(f"Account pending deletion: {e}")
        else:
            json_data = json.dumps(info.to_json())
            logger.info("Journal title: %s", info.title)
        await db.add_entry(journal_id, is_deleted, archive_date, error, login_used, json_data)
        total_error_counts.labels(error_class=str(error_type)).inc()
