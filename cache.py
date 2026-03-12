"""
工单缓存系统
============
使用 SQLite 缓存工单数据和日报结果，避免重复 API 调用。
"""
import json
import time
import sqlite3
import logging
import threading

from config import CACHE_DB_PATH, CACHE_TTL_SECONDS

logger = logging.getLogger(__name__)


class TicketCache:
    """基于 SQLite 的工单缓存"""

    _local = threading.local()

    def __init__(self, db_path=None, ttl=None):
        self.db_path = db_path or CACHE_DB_PATH
        self.ttl = ttl or CACHE_TTL_SECONDS
        self._init_db()

    def _get_conn(self):
        """每线程独立连接"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
        return self._local.conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ticket_cache (
                ticket_id   TEXT PRIMARY KEY,
                data        TEXT NOT NULL,
                updated_at  INTEGER NOT NULL,
                cached_at   REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS report_cache (
                cache_key   TEXT PRIMARY KEY,
                data        TEXT NOT NULL,
                cached_at   REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_ticket_updated
                ON ticket_cache(updated_at);
            CREATE INDEX IF NOT EXISTS idx_report_cached
                ON report_cache(cached_at);
        """)
        conn.close()

    def get_ticket(self, ticket_id):
        """获取缓存的工单（未过期）"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT data, cached_at FROM ticket_cache WHERE ticket_id = ?",
            (str(ticket_id),),
        ).fetchone()
        if row is None:
            return None
        data, cached_at = row
        if time.time() - cached_at > self.ttl:
            return None
        return json.loads(data)

    def set_ticket(self, ticket_id, data, updated_at=0):
        """缓存工单数据"""
        conn = self._get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO ticket_cache
               (ticket_id, data, updated_at, cached_at)
               VALUES (?, ?, ?, ?)""",
            (str(ticket_id), json.dumps(data, ensure_ascii=False),
             int(updated_at), time.time()),
        )
        conn.commit()

    def get_tickets_batch(self, ticket_ids):
        """批量获取缓存的工单，返回 {id: data} 字典（仅包含命中的）"""
        result = {}
        conn = self._get_conn()
        cutoff = time.time() - self.ttl
        placeholders = ",".join("?" * len(ticket_ids))
        rows = conn.execute(
            f"SELECT ticket_id, data FROM ticket_cache "
            f"WHERE ticket_id IN ({placeholders}) AND cached_at > ?",
            [str(tid) for tid in ticket_ids] + [cutoff],
        ).fetchall()
        for tid, data in rows:
            result[tid] = json.loads(data)
        return result

    def set_tickets_batch(self, tickets):
        """批量缓存工单"""
        conn = self._get_conn()
        now = time.time()
        conn.executemany(
            """INSERT OR REPLACE INTO ticket_cache
               (ticket_id, data, updated_at, cached_at)
               VALUES (?, ?, ?, ?)""",
            [
                (str(t.get("id", "")),
                 json.dumps(t, ensure_ascii=False),
                 int(t.get("updateTime", 0)),
                 now)
                for t in tickets
            ],
        )
        conn.commit()

    def get_report(self, cache_key):
        """获取缓存的日报结果"""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT data, cached_at FROM report_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        data, cached_at = row
        if time.time() - cached_at > self.ttl:
            return None
        return json.loads(data)

    def set_report(self, cache_key, data):
        """缓存日报结果"""
        conn = self._get_conn()
        conn.execute(
            """INSERT OR REPLACE INTO report_cache
               (cache_key, data, cached_at) VALUES (?, ?, ?)""",
            (cache_key, json.dumps(data, ensure_ascii=False), time.time()),
        )
        conn.commit()

    def clear_expired(self):
        """清理过期缓存"""
        cutoff = time.time() - self.ttl
        conn = self._get_conn()
        conn.execute("DELETE FROM ticket_cache WHERE cached_at < ?", (cutoff,))
        conn.execute("DELETE FROM report_cache WHERE cached_at < ?", (cutoff,))
        conn.commit()
        logger.info("已清理过期缓存")

    def clear_all(self):
        """清空所有缓存"""
        conn = self._get_conn()
        conn.execute("DELETE FROM ticket_cache")
        conn.execute("DELETE FROM report_cache")
        conn.commit()
        logger.info("已清空所有缓存")
