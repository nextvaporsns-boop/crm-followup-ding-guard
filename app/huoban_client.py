from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from .config import settings


def _first_value(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, (int, float)):
        return str(raw)
    if isinstance(raw, list):
        if not raw:
            return None
        first = raw[0]
        if isinstance(first, dict):
            for key in ("title", "text", "value", "id"):
                if key in first and first[key] not in (None, ""):
                    return str(first[key]).strip()
            return str(first).strip()
        return str(first).strip()
    if isinstance(raw, dict):
        for key in ("title", "text", "value", "id"):
            if key in raw and raw[key] not in (None, ""):
                return str(raw[key]).strip()
        return str(raw).strip()
    return str(raw).strip()


def _to_int(raw: Any) -> int:
    value = _first_value(raw)
    if value is None or value == "":
        return 0
    try:
        return int(float(value))
    except ValueError:
        return 0


class HuobanClient:
    def __init__(self) -> None:
        self.url = settings.huoban_api_url
        self.headers = {
            "Open-Authorization": f"Bearer {settings.huoban_bearer_token}",
            "Content-Type": "application/json",
        }

    def _fetch_rows(self, filter_payload: Dict[str, Any], dedup_by_user: bool) -> List[Dict[str, Any]]:
        offset = 0
        limit = settings.huoban_page_limit
        rows: List[Dict[str, Any]] = []

        while True:
            payload = {
                "table_id": settings.huoban_table_id,
                "filter": filter_payload,
                "order": {"field_id": "created_on", "type": "asc"},
                "limit": limit,
                "offset": offset,
            }
            resp = requests.post(self.url, headers=self.headers, json=payload, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            if int(data.get("code", 0)) != 0:
                raise RuntimeError(f"huoban item/list failed: {data}")
            items = data.get("data", {}).get("items", [])

            for item in items:
                fields = item.get("fields", {})
                user_id = _first_value(fields.get(settings.huoban_field_user_id)) or ""
                if not user_id:
                    continue
                rows.append(
                    {
                        "item_id": item.get("item_id", ""),
                        "follow_date": _first_value(fields.get(settings.huoban_field_follow_date))
                        or datetime.now().strftime("%Y-%m-%d"),
                        "salesperson": _first_value(fields.get(settings.huoban_field_sale_name)) or "",
                        "user_id": user_id,
                        "follow_count": _to_int(fields.get(settings.huoban_field_follow_count)),
                    }
                )

            if len(items) < limit:
                break
            offset += limit

        if not dedup_by_user:
            return sorted(rows, key=lambda row: (str(row.get("follow_date", "")), row["user_id"], row["item_id"]))

        dedup: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            dedup[row["user_id"]] = row
        return sorted(dedup.values(), key=lambda row: (int(row.get("follow_count", 0)), row["user_id"]))

    def fetch_today_rows(self) -> List[Dict[str, Any]]:
        return self._fetch_rows(
            {
                "field": settings.huoban_field_follow_date,
                "query": {"eq": "today"},
            },
            dedup_by_user=True,
        )

    def fetch_rows_between(self, start_date: str, end_date_exclusive: str) -> List[Dict[str, Any]]:
        return self._fetch_rows(
            {
                "and": [
                    {
                        "field": settings.huoban_field_follow_date,
                        "query": {"gte": start_date},
                    },
                    {
                        "field": settings.huoban_field_follow_date,
                        "query": {"lt": end_date_exclusive},
                    },
                ]
            },
            dedup_by_user=False,
        )
