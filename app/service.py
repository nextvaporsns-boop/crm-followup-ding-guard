from datetime import datetime
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

from .config import settings
from .db import (
    add_run_log,
    get_reminder_state,
    get_today_snapshots,
    list_unresolved_reminders,
    recent_group_events,
    replace_snapshots,
    upsert_reminder_state,
)
from .dingtalk_client import DingTalkClient
from .huoban_client import HuobanClient


class FollowupReminderService:
    def __init__(self, dingtalk: DingTalkClient, huoban: HuobanClient) -> None:
        self.dingtalk = dingtalk
        self.huoban = huoban
        self.tz = ZoneInfo(settings.timezone)

    def now(self) -> datetime:
        return datetime.now(self.tz)

    def biz_date(self) -> str:
        return self.now().strftime("%Y-%m-%d")

    def _retry(self, fn, *args, **kwargs):
        last = None
        for _ in range(3):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                last = exc
        raise last

    def refresh_today_snapshot(self, source: str) -> List[Dict[str, object]]:
        rows = self._retry(self.huoban.fetch_today_rows)
        replace_snapshots(self.biz_date(), self.now().isoformat(timespec="seconds"), rows)
        add_run_log("refresh_snapshot", source, True, detail=f"rows={len(rows)}")
        return rows

    def _build_message(self, row: Dict[str, object], urge: bool) -> str:
        salesperson = str(row.get("salesperson") or row.get("user_id"))
        return (
            "伙伴云信息提醒\n"
            f"@{salesperson}\n"
            "请以上人员立即完成今日的线索跟进内容填报：\n"
            "https://app.huoban.com/tables/2100000067280983?viewId=1&permissionId=0"
        )

    def _send_notice_bundle(self, row: Dict[str, object], urge: bool) -> Tuple[str, str]:
        content = self._build_message(row, urge=urge)
        group_resp = self._retry(self.dingtalk.send_group_robot_text, content)
        task_id = str(group_resp.get("errcode", "0"))
        open_ding_id = ""
        return task_id, open_ding_id

    def _latest_session_id(self) -> str:
        for event in recent_group_events(20):
            session_id = str(event.get("chat_id") or "").strip()
            if session_id:
                return session_id
        return ""

    def _build_group_targets(self, rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
        targets: List[Dict[str, object]] = []
        for row in rows:
            user_id = str(row.get("user_id", "")).strip()
            follow_count = int(row.get("follow_count", 0))
            if user_id and follow_count < settings.follow_count_threshold:
                targets.append(row)
        return targets

    def _build_group_message_payload(self, rows: List[Dict[str, object]]) -> Dict[str, object]:
        session_id = self._latest_session_id()
        if not session_id:
            raise ValueError("还没有抓到目标群会话，请先在群里 @机器人 一次")

        targets = self._build_group_targets(rows)
        if not targets:
            raise ValueError("当天伙伴云主表里没有需要提醒的未达标人员，未生成消息")

        preview_names = "、".join(str(row.get("salesperson") or row.get("user_id")) for row in targets[:8])
        if len(targets) > 8:
            preview_names += f" 等{len(targets)}人"

        at_user_ids = [str(row.get("user_id", "")).strip() for row in targets]
        at_line = " ".join(f"@{user_id}" for user_id in at_user_ids)
        lines = [
            "伙伴云信息提醒",
            at_line,
            "请以上人员立即完成今日的线索跟进内容填报：",
            "https://app.huoban.com/tables/2100000067280983?viewId=1&permissionId=0",
        ]
        content = "\n".join(lines)
        return {
            "session_id": session_id,
            "targets": targets,
            "target_count": len(targets),
            "preview_names": preview_names,
            "at_user_ids": at_user_ids,
            "content": content,
            "total_rows": len(rows),
        }

    def preview_group_demo(self, source: str = "web_group_preview") -> Dict[str, object]:
        rows = self.refresh_today_snapshot(source)
        payload = self._build_group_message_payload(rows)
        add_run_log(
            "group_preview",
            source,
            True,
            detail=(
                f"targets={payload['target_count']}, total_rows={payload['total_rows']}, "
                f"preview={payload['preview_names']}, session_id={payload['session_id']}"
            ),
        )
        return payload

    def send_group_demo(self, source: str = "web_group_demo") -> Dict[str, object]:
        rows = self.refresh_today_snapshot(source)
        payload = self._build_group_message_payload(rows)
        resp = self._retry(self.dingtalk.send_group_robot_text, payload["content"], payload["at_user_ids"])
        add_run_log(
            "group_demo",
            source,
            True,
            detail=(
                f"targets={payload['target_count']}, total_rows={payload['total_rows']}, "
                f"preview={payload['preview_names']}, session_id={payload['session_id']}, "
                f"errcode={resp.get('errcode', 0)}"
            ),
        )
        return payload

    def run_initial_check(self, source: str = "scheduler_initial") -> Tuple[int, int]:
        rows = self.refresh_today_snapshot(source)
        sent = 0
        skipped = 0
        now_iso = self.now().isoformat(timespec="seconds")

        for row in rows:
            follow_count = int(row.get("follow_count", 0))
            if follow_count >= settings.follow_count_threshold:
                continue
            state = get_reminder_state(self.biz_date(), str(row["user_id"]))
            if state and state.get("first_sent_at"):
                skipped += 1
                continue
            try:
                task_id, open_ding_id = self._send_notice_bundle(row, urge=False)
                upsert_reminder_state(
                    biz_date=self.biz_date(),
                    user_id=str(row["user_id"]),
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    first_sent_at=now_iso,
                    last_sent_at=now_iso,
                    last_task_id=task_id,
                    last_open_ding_id=open_ding_id,
                    urge_count=0,
                    last_read_status="unknown",
                    resolved_at=None,
                )
                add_run_log(
                    "initial_notice",
                    source,
                    True,
                    user_id=str(row["user_id"]),
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    detail=f"task_id={task_id}",
                )
                sent += 1
            except Exception as exc:
                add_run_log(
                    "initial_notice",
                    source,
                    False,
                    user_id=str(row["user_id"]),
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    detail=str(exc),
                )

        return sent, skipped

    def run_urge_cycle(self, source: str = "scheduler_urge") -> Tuple[int, int, int]:
        latest_rows = {str(row["user_id"]): row for row in self.refresh_today_snapshot(source)}
        reminders = list_unresolved_reminders(self.biz_date())
        urged = 0
        resolved = 0
        read = 0

        for state in reminders:
            user_id = str(state["user_id"])
            row = latest_rows.get(
                user_id,
                {
                    "user_id": user_id,
                    "salesperson": state.get("salesperson", ""),
                    "follow_count": state.get("follow_count", 0),
                },
            )
            follow_count = int(row.get("follow_count", 0))
            if follow_count >= settings.follow_count_threshold:
                upsert_reminder_state(
                    biz_date=self.biz_date(),
                    user_id=user_id,
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    first_sent_at=state.get("first_sent_at"),
                    last_sent_at=state.get("last_sent_at"),
                    last_task_id=str(state.get("last_task_id", "")),
                    last_open_ding_id=str(state.get("last_open_ding_id", "")),
                    urge_count=int(state.get("urge_count", 0)),
                    last_read_status="resolved_by_follow_count",
                    resolved_at=self.now().isoformat(timespec="seconds"),
                )
                resolved += 1
                continue

            last_task_id = str(state.get("last_task_id", ""))
            if last_task_id:
                try:
                    result = self._retry(self.dingtalk.get_work_notice_result, last_task_id)
                    send_result = result.get("send_result", {}) or {}
                    read_users = set(send_result.get("read_user_id_list", []) or [])
                    unread_users = set(send_result.get("unread_user_id_list", []) or [])
                    if user_id in read_users:
                        upsert_reminder_state(
                            biz_date=self.biz_date(),
                            user_id=user_id,
                            salesperson=str(row.get("salesperson", "")),
                            follow_count=follow_count,
                            first_sent_at=state.get("first_sent_at"),
                            last_sent_at=state.get("last_sent_at"),
                            last_task_id=last_task_id,
                            last_open_ding_id=str(state.get("last_open_ding_id", "")),
                            urge_count=int(state.get("urge_count", 0)),
                            last_read_status="read",
                            resolved_at=None,
                        )
                        read += 1
                        continue
                    if unread_users and user_id not in unread_users:
                        add_run_log(
                            "urge_status_skip",
                            source,
                            True,
                            user_id=user_id,
                            salesperson=str(row.get("salesperson", "")),
                            follow_count=follow_count,
                            detail="user not in unread list",
                        )
                        continue
                except Exception as exc:
                    add_run_log(
                        "urge_status_check",
                        source,
                        False,
                        user_id=user_id,
                        salesperson=str(row.get("salesperson", "")),
                        follow_count=follow_count,
                        detail=str(exc),
                    )

            try:
                task_id, open_ding_id = self._send_notice_bundle(row, urge=True)
                upsert_reminder_state(
                    biz_date=self.biz_date(),
                    user_id=user_id,
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    first_sent_at=state.get("first_sent_at"),
                    last_sent_at=self.now().isoformat(timespec="seconds"),
                    last_task_id=task_id,
                    last_open_ding_id=open_ding_id,
                    urge_count=int(state.get("urge_count", 0)) + 1,
                    last_read_status="urged_unread",
                    resolved_at=None,
                )
                add_run_log(
                    "urge_notice",
                    source,
                    True,
                    user_id=user_id,
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    detail=f"task_id={task_id}",
                )
                urged += 1
            except Exception as exc:
                add_run_log(
                    "urge_notice",
                    source,
                    False,
                    user_id=user_id,
                    salesperson=str(row.get("salesperson", "")),
                    follow_count=follow_count,
                    detail=str(exc),
                )

        return urged, resolved, read

    def manual_notify_user(self, user_id: str, source: str = "web_manual_notify") -> Dict[str, object]:
        rows = {str(row["user_id"]): row for row in self.refresh_today_snapshot(source)}
        row = rows.get(user_id)
        if not row:
            raise ValueError(f"未找到 user_id={user_id} 的今日数据")

        follow_count = int(row.get("follow_count", 0))
        state = get_reminder_state(self.biz_date(), user_id) or {}
        task_id, open_ding_id = self._send_notice_bundle(row, urge=True)
        now_iso = self.now().isoformat(timespec="seconds")
        upsert_reminder_state(
            biz_date=self.biz_date(),
            user_id=user_id,
            salesperson=str(row.get("salesperson", "")),
            follow_count=follow_count,
            first_sent_at=state.get("first_sent_at") or now_iso,
            last_sent_at=now_iso,
            last_task_id=task_id,
            last_open_ding_id=open_ding_id,
            urge_count=int(state.get("urge_count", 0)) + 1,
            last_read_status="manual_notice",
            resolved_at=None,
        )
        add_run_log(
            "manual_notice",
            source,
            True,
            user_id=user_id,
            salesperson=str(row.get("salesperson", "")),
            follow_count=follow_count,
            detail=f"task_id={task_id}, open_ding_id={open_ding_id}",
        )
        return {
            "salesperson": str(row.get("salesperson", "")),
            "follow_count": follow_count,
            "task_id": task_id,
            "open_ding_id": open_ding_id,
        }

    def dashboard_rows(self) -> List[Dict[str, object]]:
        snapshots = get_today_snapshots(self.biz_date())
        states = {str(row["user_id"]): row for row in list_unresolved_reminders(self.biz_date())}
        merged: List[Dict[str, object]] = []
        for row in snapshots:
            state = states.get(str(row["user_id"]), {})
            merged.append(
                {
                    **row,
                    "need_remind": int(row.get("follow_count", 0)) < settings.follow_count_threshold,
                    "urge_count": int(state.get("urge_count", 0)),
                    "last_read_status": state.get("last_read_status", ""),
                    "last_sent_at": state.get("last_sent_at", ""),
                }
            )
        return merged

    def preview_rows(self, source: str = "web_preview") -> List[Dict[str, object]]:
        rows = self.refresh_today_snapshot(source)
        states = {str(row["user_id"]): row for row in list_unresolved_reminders(self.biz_date())}
        merged: List[Dict[str, object]] = []
        for row in rows:
            state = states.get(str(row["user_id"]), {})
            merged.append(
                {
                    **row,
                    "need_remind": int(row.get("follow_count", 0)) < settings.follow_count_threshold,
                    "urge_count": int(state.get("urge_count", 0)),
                    "last_read_status": state.get("last_read_status", ""),
                    "last_sent_at": state.get("last_sent_at", ""),
                }
            )
        return merged
