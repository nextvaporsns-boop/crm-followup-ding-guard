import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import settings


Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_conn():
    conn = _conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS followup_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                biz_date TEXT NOT NULL,
                snapshot_at TEXT NOT NULL,
                item_id TEXT,
                salesperson TEXT,
                user_id TEXT NOT NULL,
                follow_date TEXT,
                follow_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS reminder_state (
                biz_date TEXT NOT NULL,
                user_id TEXT NOT NULL,
                salesperson TEXT,
                follow_count INTEGER NOT NULL DEFAULT 0,
                first_sent_at TEXT,
                last_sent_at TEXT,
                last_task_id TEXT,
                last_open_ding_id TEXT,
                urge_count INTEGER NOT NULL DEFAULT 0,
                last_read_status TEXT NOT NULL DEFAULT 'unknown',
                resolved_at TEXT,
                PRIMARY KEY (biz_date, user_id)
            );

            CREATE TABLE IF NOT EXISTS run_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT NOT NULL,
                action TEXT NOT NULL,
                source TEXT NOT NULL,
                success INTEGER NOT NULL,
                user_id TEXT,
                salesperson TEXT,
                follow_count INTEGER,
                detail TEXT
            );

            CREATE TABLE IF NOT EXISTS group_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time TEXT NOT NULL,
                event_type TEXT NOT NULL,
                chat_id TEXT,
                open_conversation_id TEXT,
                title TEXT,
                operator_user_id TEXT,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_followup_snapshots_date ON followup_snapshots (biz_date);
            CREATE INDEX IF NOT EXISTS idx_run_logs_run_at ON run_logs (run_at);
            CREATE INDEX IF NOT EXISTS idx_group_events_time ON group_events (event_time);
            """
        )


def add_run_log(
    action: str,
    source: str,
    success: bool,
    user_id: str = "",
    salesperson: str = "",
    follow_count: Optional[int] = None,
    detail: str = "",
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO run_logs(run_at, action, source, success, user_id, salesperson, follow_count, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now_str(), action, source, int(success), user_id, salesperson, follow_count, detail),
        )


def replace_snapshots(biz_date: str, snapshot_at: str, rows: List[Dict[str, Any]]) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM followup_snapshots WHERE biz_date = ?", (biz_date,))
        for row in rows:
            conn.execute(
                """
                INSERT INTO followup_snapshots(
                    biz_date, snapshot_at, item_id, salesperson, user_id, follow_date, follow_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    biz_date,
                    snapshot_at,
                    row.get("item_id", ""),
                    row.get("salesperson", ""),
                    row.get("user_id", ""),
                    row.get("follow_date", ""),
                    int(row.get("follow_count", 0)),
                ),
            )


def get_today_snapshots(biz_date: str, limit: int = 1000) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT snapshot_at, item_id, salesperson, user_id, follow_date, follow_count
            FROM followup_snapshots
            WHERE biz_date = ?
            ORDER BY follow_count ASC, user_id ASC
            LIMIT ?
            """,
            (biz_date, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def get_reminder_state(biz_date: str, user_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT biz_date, user_id, salesperson, follow_count, first_sent_at, last_sent_at, last_task_id,
                   last_open_ding_id, urge_count, last_read_status, resolved_at
            FROM reminder_state
            WHERE biz_date = ? AND user_id = ?
            """,
            (biz_date, user_id),
        ).fetchone()
    return dict(row) if row else None


def upsert_reminder_state(
    biz_date: str,
    user_id: str,
    salesperson: str,
    follow_count: int,
    first_sent_at: Optional[str],
    last_sent_at: Optional[str],
    last_task_id: str,
    last_open_ding_id: str,
    urge_count: int,
    last_read_status: str,
    resolved_at: Optional[str],
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO reminder_state(
                biz_date, user_id, salesperson, follow_count, first_sent_at, last_sent_at, last_task_id,
                last_open_ding_id, urge_count, last_read_status, resolved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(biz_date, user_id) DO UPDATE SET
                salesperson = excluded.salesperson,
                follow_count = excluded.follow_count,
                first_sent_at = COALESCE(reminder_state.first_sent_at, excluded.first_sent_at),
                last_sent_at = excluded.last_sent_at,
                last_task_id = excluded.last_task_id,
                last_open_ding_id = excluded.last_open_ding_id,
                urge_count = excluded.urge_count,
                last_read_status = excluded.last_read_status,
                resolved_at = excluded.resolved_at
            """,
            (
                biz_date,
                user_id,
                salesperson,
                follow_count,
                first_sent_at,
                last_sent_at,
                last_task_id,
                last_open_ding_id,
                urge_count,
                last_read_status,
                resolved_at,
            ),
        )


def list_unresolved_reminders(biz_date: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT biz_date, user_id, salesperson, follow_count, first_sent_at, last_sent_at, last_task_id,
                   last_open_ding_id, urge_count, last_read_status, resolved_at
            FROM reminder_state
            WHERE biz_date = ? AND resolved_at IS NULL
            ORDER BY urge_count ASC, user_id ASC
            """,
            (biz_date,),
        ).fetchall()
    return [dict(row) for row in rows]


def recent_run_logs(limit: int = 200) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT run_at, action, source, success, user_id, salesperson, follow_count, detail
            FROM run_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def add_group_event(
    event_type: str,
    chat_id: str,
    open_conversation_id: str,
    title: str,
    operator_user_id: str,
    payload_json: str,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO group_events(
                event_time, event_type, chat_id, open_conversation_id, title, operator_user_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (now_str(), event_type, chat_id, open_conversation_id, title, operator_user_id, payload_json),
        )


def recent_group_events(limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT event_time, event_type, chat_id, open_conversation_id, title, operator_user_id
            FROM group_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]
