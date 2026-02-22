"""sqlite database functions"""
import sqlite3 as sql
import sys
import os
import traceback
from datetime import datetime
import logging
from utils.misc import get_photo_metadata

logger = logging.getLogger(__name__)

def adapt_datetime(val):
    """adapt inside sqlite3 to make timestamps without .mmmmmm work"""
    return val.isoformat(" ", "microseconds")

def setup_database(directory):
    """initialize database"""
    DatabaseHandler.db_file = directory + "/icloudpd.db"
    sql.register_adapter(datetime, adapt_datetime)

class DatabaseHandler():
    """DB handler"""
    is_pruned = False

#    def __new__(cls):
#        if not hasattr(cls, 'instance'):
#            cls.instance = super(DatabaseHandler, cls).__new__(cls)
#            cls.instance.db_conn = sql.connect(
#                DatabaseHandler.db_file,
#                detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)
#            cls.instance.db_conn.row_factory = sql.Row
#            cls.instance._create_log_table()
#            cls.instance._create_photo_asset_table()
#            cls.instance._prune_log_table()
#        return cls.instance

    def __init__(self):
        self.db_conn = sql.connect(
                DatabaseHandler.db_file,
                detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)
        self.db_conn.row_factory = sql.Row
        self._create_photo_asset_table()

    def _create_photo_asset_table(self):
        try:
            self.db_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS PhotoAsset (
                    id TEXT,
                    filename TEXT,
                    size TEXT,
                    created TIMESTAMP,
                    asset_date TIMESTAMP,
                    added_date TIMESTAMP,
                    dimensionX INTEGER,
                    dimensionY INTEGER,
                    item_type TEXT,
                    item_type_extension TEXT,
                    path TEXT PRIMARY KEY,
                    md5 TEXT,
                    album
                    )
                """
                )
            self.db_conn.commit()
            self.db_conn.execute("create index if not exists IX_PA_MD5 on PhotoAsset (md5)")
            self.db_conn.execute("create index if not exists IX_PA_FILENAME on PhotoAsset (md5)")
            self.db_conn.commit()
        except sql.Error as er:
            self.print_error(er)

    def print_error(self, er):
        """print error"""
        logger.error("SQLite error: %s", ' '.join(er.args))
        logger.error("Exception class is: %s", er.__class__)
        logger.error("SQLite traceback: ")
        exc_type, exc_value, exc_tb = sys.exc_info()
        logger.error(traceback.format_exception(exc_type, exc_value, exc_tb))

    def newest_asset(self):
        """return the newest asset"""
        try:
            return self.db_conn.execute(
                "SELECT path, created FROM PhotoAsset ORDER BY created DESC LIMIT 1"
                ).fetchone()
        except sql.Error as er:
            self.print_error(er)
        return None

    def asset_exists(self, path):
        """check that asset exists"""
        try:
            row = self.db_conn.execute(
                "select path from PhotoAsset where path = ?", (path,)
                ).fetchone()
            return row is not None
        except sql.Error as er:
            self.print_error(er)
        return False

    def get_asset_md5(self, path):
        """return asset md5"""
        try:
            return self.db_conn.execute(
                "select md5 from PhotoAsset where path = ?", (path,)
                ).fetchone()['md5']
        except sql.Error as er:
            self.print_error(er)
        return 0


    def upsert_asset(self, album, photo, path, md5):
        """insert or update asset"""
        try:
            self.db_conn.execute("INSERT OR REPLACE INTO PhotoAsset VALUES"
                                 " (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                photo.id,
                photo.filename,
                photo.size,
                photo.created,
                photo.asset_date,
                photo.added_date,
                photo.dimensions[0],
                photo.dimensions[1],
                photo.item_type,
                os.path.splitext(photo.filename)[1],
                path,
                md5,
                album
                )
            )
            self.db_conn.commit()
            return get_photo_metadata(photo, album, path, md5)

        except sql.Error as er:
            self.print_error(er)
        return None

    def fetch_duplicates(self):
        """retun set of duplicates identified"""
        try:
            return self.db_conn.execute("select A.md5, A.path, A.size, B.count from PhotoAsset A"
                                        " join (select md5, count(*) as count, size from PhotoAsset"
                                        " group by md5 having count(md5) > 1) B on A.md5 = B.md5"
                                        " order by CAST(A.size as integer), A.path").fetchall()
        except sql.Error as er:
            self.print_error(er)
        return None
