import asyncio
import datetime
import glob
import logging
from typing import Optional

import aiosqlite
import prometheus_client
from aiosqlite import Connection

from fa_journaliser.prom import get_prometheus_port

logger = logging.getLogger(__name__)


total_journal_db_entries = prometheus_client.Gauge(
    "fajournaliser_database_journal_entries_total",
    "Total number of journal entries in the database",
)
database_shards = prometheus_client.Gauge(
    "fajournaliser_database_shard_count_total",
    "Total number of database shards in the sharded database",
)
connected_database_shards = prometheus_client.Gauge(
    "fajournaliser_database_shard_connected_count_total",
    "Total number of currently connected database shards in the sharded database",
)


class Database:

    def __init__(self, filename = "journals.db") -> None:
        self.filename: str = filename
        self.db: Optional[Connection] = None

    async def start(self) -> None:
        self.db = await aiosqlite.connect(self.filename)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("""CREATE TABLE IF NOT EXISTS `journals` (
            `journal_id` INT NOT NULL,
            `is_deleted` BOOLEAN NOT NULL,
            `archive_datetime` DATETIME NOT NULL,
            `error` TEXT,
            `login_used` TEXT,
            `json` TEXT,
            PRIMARY KEY (`journal_id`)
        );""")
        await self.db.commit()
        # If we're not using prometheus, speed up startup by skipping row count
        if get_prometheus_port() is not None:
            entry_count = await self.count_journals()
            total_journal_db_entries.set(entry_count)

    async def stop(self) -> None:
        if self.db is not None:
            await self.db.close()

    async def count_journals(self) -> int:
        async with self.db.execute("SELECT COUNT(*) AS count FROM journals") as cursor:
            async for row in cursor:
                count = row['count']
        return count

    async def add_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str],
    ) -> None:
        await self.db.execute(
            "INSERT INTO journals (journal_id, is_deleted, archive_datetime, error, login_used, json) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(journal_id) DO UPDATE SET "
            "is_deleted = ?, error = ?, login_used = ?, json = ?",
            (
                journal_id, is_deleted, archive_date, error, login_used, json_data,
                is_deleted, error, login_used, json_data
            )
        )
        await self.db.commit()
        total_journal_db_entries.inc(1)

    async def update_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str],
    ) -> None:
        await self.db.execute(
            "UPDATE journals SET "
            "is_deleted = ? AND archive_datetime = ? AND error = ? AND login_used = ? AND json = ? "
            "WHERE journal_id = ?",
            (
                is_deleted, archive_date, error, login_used, json_data, journal_id
            )
        )
        await self.db.commit()

    async def list_journal_ids_truncated(self, min_id: int, max_id: Optional[int]) -> list[int]:
        journal_ids = []
        async with self.db.execute(
                "SELECT journal_id FROM journals WHERE journal_id >= ? AND (? IS NULL OR journal_id <= ?)",
                (min_id, max_id, max_id)
        ) as cursor:
            async for row in cursor:
                journal_ids.append(row['journal_id'])
        return journal_ids

    async def list_ids_where_path_is_null(self, json_path: str) -> list[int]:
        journal_ids = []
        async with self.db.execute(
                "SELECT journal_id FROM journals WHERE error IS NULL AND json_extract(json, ?) IS NULL",
                (json_path,)
        ) as cursor:
            async for row in cursor:
                journal_ids.append(row['journal_id'])
        return journal_ids


class DBShard:
    def __init__(self, db: Database, min_id: int, max_id: int) -> None:
        self.db: Database = db
        self.min_id: int = min_id
        self.max_id: int = max_id # Not inclusive!


class ShardedDatabase:
    SHARD_SIZE = 1_000_000
    def __init__(self) -> None:
        self.shards: list[DBShard] = []
        self.sharding_lock = asyncio.Lock()
        database_shards.set_function(lambda: len(self.shards))

    async def start(self) -> None:
        db_files = glob.glob("journals_*.db")
        async with self.sharding_lock:
            for db_file in db_files:
                db_number = db_file.removeprefix("journals_").removesuffix(".db")
                min_id = int(db_number) * self.SHARD_SIZE
                max_id = min_id + self.SHARD_SIZE
                new_db = Database(db_file)
                logger.info("Connecting to database shard: %s", db_file)
                await new_db.start()
                connected_database_shards.inc(1)
                new_shard = DBShard(new_db, min_id, max_id)
                self.shards.append(new_shard)
        logger.info("Connected to all %s database shards (shard size: %s)", len(self.shards), self.SHARD_SIZE)

    async def stop(self) -> None:
        async with self.sharding_lock:
            for shard in self.shards:
                logger.info("Disconnecting from database shard: %s", shard.db.filename)
                await shard.db.stop()
                connected_database_shards.dec(1)
        logger.info("Disconnected from all database shards")

    def _find_matching_shard(self, journal_id: int) -> Optional[DBShard]:
        for shard in self.shards:
            # Max is not inclusive, but min is.
            if shard.min_id <= journal_id < shard.max_id:
                return shard
        return None

    async def select_shard(self, journal_id: int) -> DBShard:
        shard = self._find_matching_shard(journal_id)
        if shard is not None:
            return shard
        async with self.sharding_lock:
            # Gotta check again, in case the shard was just created
            shard = self._find_matching_shard(journal_id)
            if shard is not None:
                return shard
            # Otherwise, create a new shard
            logger.info("Journal ID %s is not captured by any of the existing Database shards. Creating a new one", journal_id)
            min_id = journal_id - (journal_id % self.SHARD_SIZE)
            max_id = min_id + self.SHARD_SIZE
            filename = f"journals_{min_id//self.SHARD_SIZE}.db"
            new_db = Database(filename)
            await new_db.start()
            connected_database_shards.inc(1)
            shard = DBShard(new_db, min_id, max_id)
            self.shards.append(shard)
            return shard

    async def count_journals(self) -> int:
        total_count = 0
        for shard in self.shards:
            total_count += await shard.db.count_journals()
        return total_count

    async def add_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str],
    ) -> None:
        shard = await self.select_shard(journal_id)
        await shard.db.add_entry(
            journal_id=journal_id,
            is_deleted=is_deleted,
            archive_date=archive_date,
            error=error,
            login_used=login_used,
            json_data=json_data,
        )

    async def update_entry(
            self,
            journal_id: int,
            is_deleted: bool,
            archive_date: datetime.datetime,
            error: Optional[str],
            login_used: Optional[str],
            json_data: Optional[str],
    ) -> None:
        shard = await self.select_shard(journal_id)
        return await shard.db.update_entry(
            journal_id=journal_id,
            is_deleted=is_deleted,
            archive_date=archive_date,
            error=error,
            login_used=login_used,
            json_data=json_data,
        )

    async def list_journal_ids_truncated(self, min_id: int, max_id: Optional[int]) -> list[int]:
        # Figure out which shards need asking
        check_shards: list[DBShard] = []
        min_shard = await self.select_shard(min_id)
        check_shards.append(min_shard)
        if max_id is None:
            # If there's no max ID, add more shards until we don't find anymore
            next_shard = self._find_matching_shard(min_shard.max_id)
            while next_shard is not None:
                check_shards.append(next_shard)
                next_shard = self._find_matching_shard(next_shard.max_id)
        else:
            # If there is a max ID, keep requesting new shards until we reach the max ID
            next_shard = min_shard
            while max_id < next_shard.max_id:
                next_shard = await self.select_shard(next_shard.max_id)
                check_shards.append(next_shard)
        # Now search each shard, and return the collated list
        journal_ids: list[int] = []
        for shard in check_shards:
            shard_min = max(min_id, shard.min_id)
            if max_id is None:
                shard_max = shard.max_id - 1
            else:
                shard_max = min(max_id, shard.max_id-1)
            journal_ids.extend(await shard.db.list_journal_ids_truncated(shard_min, shard_max))
        return journal_ids

    async def list_ids_where_path_is_null(self, json_path: str) -> list[int]:
        journal_ids = []
        for shard in self.shards:
            journal_ids.extend(await shard.db.list_ids_where_path_is_null(json_path))
        return journal_ids
