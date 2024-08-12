import dataclasses
import datetime
import json
import logging
import pathlib
from typing import Optional

import aiofiles
import aiofiles.os

from fa_journaliser.database import Database
from fa_journaliser.journal_info import JournalInfo, JournalNotFound, AccountDisabled, PendingDeletion

logger = logging.getLogger(__name__)


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

    def __repr__(self) -> str:
        return f"Journal(id={self.journal_id})"

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
        login_used = info.login_user
        json_data = None

        logger.info("Saving journal ID: %s", journal_id)
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
        else:
            json_data = json.dumps(info.to_json())
            logger.info("Journal title: %s", info.title)
        await db.add_entry(journal_id, is_deleted, archive_date, error, login_used, json_data)
