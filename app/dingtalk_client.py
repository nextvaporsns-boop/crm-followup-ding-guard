import base64
import hashlib
import hmac
import secrets
import time
import urllib.parse
from typing import Any, Dict, List

import requests

from .config import settings


class DingTalkClient:
    OAPI_BASE = "https://oapi.dingtalk.com"
    V1_BASE = "https://api.dingtalk.com"

    def __init__(self) -> None:
        self._token = ""
        self._token_expire_at = 0.0
        self._jsapi_ticket = ""
        self._jsapi_ticket_expire_at = 0.0

    def _refresh_token(self) -> str:
        resp = requests.get(
            f"{self.OAPI_BASE}/gettoken",
            params={"appkey": settings.dingtalk_app_key, "appsecret": settings.dingtalk_app_secret},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            raise RuntimeError(f"gettoken failed: {data}")
        self._token = data["access_token"]
        expires = int(data.get("expires_in", 7200))
        self._token_expire_at = time.time() + max(expires - 120, 300)
        return self._token

    def access_token(self) -> str:
        if not self._token or time.time() >= self._token_expire_at:
            return self._refresh_token()
        return self._token

    def jsapi_ticket(self) -> str:
        if self._jsapi_ticket and time.time() < self._jsapi_ticket_expire_at:
            return self._jsapi_ticket

        resp = requests.get(
            f"{self.OAPI_BASE}/get_jsapi_ticket",
            params={"access_token": self.access_token()},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            raise RuntimeError(f"get_jsapi_ticket failed: {data}")
        self._jsapi_ticket = str(data.get("ticket", ""))
        expires = int(data.get("expires_in", 7200))
        self._jsapi_ticket_expire_at = time.time() + max(expires - 120, 300)
        return self._jsapi_ticket

    def build_jsapi_config(self, url: str) -> Dict[str, Any]:
        nonce_str = secrets.token_hex(8)
        timestamp = int(time.time())
        plain = (
            f"jsapi_ticket={self.jsapi_ticket()}&noncestr={nonce_str}"
            f"&timestamp={timestamp}&url={url}"
        )
        signature = hashlib.sha1(plain.encode("utf-8")).hexdigest()
        return {
            "corpId": settings.dingtalk_corp_id,
            "timeStamp": timestamp,
            "nonceStr": nonce_str,
            "signature": signature,
        }

    def _oapi_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(
            f"{self.OAPI_BASE}{path}",
            params={"access_token": self.access_token()},
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            raise RuntimeError(f"{path} failed: {data}")
        return data

    def _v1_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(
            f"{self.V1_BASE}{path}",
            headers={
                "x-acs-dingtalk-access-token": self.access_token(),
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json()

    def send_work_notice(self, userid: str, content: str) -> Dict[str, Any]:
        if not settings.dingtalk_notify_agent_id:
            raise RuntimeError("DINGTALK_NOTIFY_AGENT_ID is required")
        return self._oapi_post(
            "/topapi/message/corpconversation/asyncsend_v2",
            {
                "agent_id": int(settings.dingtalk_notify_agent_id),
                "userid_list": userid,
                "msg": {"msgtype": "text", "text": {"content": content}},
            },
        )

    def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        if not chat_id:
            raise RuntimeError("chat_id is required")
        resp = requests.get(
            f"{self.OAPI_BASE}/chat/get",
            params={"access_token": self.access_token(), "chatid": chat_id},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            raise RuntimeError(f"/chat/get failed: {data}")
        return data.get("chat_info", {}) or {}

    def get_work_notice_result(self, task_id: str) -> Dict[str, Any]:
        if not settings.dingtalk_notify_agent_id:
            raise RuntimeError("DINGTALK_NOTIFY_AGENT_ID is required")
        return self._oapi_post(
            "/topapi/message/corpconversation/getsendresult",
            {"agent_id": int(settings.dingtalk_notify_agent_id), "task_id": int(task_id)},
        )

    def send_robot_ding(self, user_ids: List[str], content: str, remind_type: int = 1) -> Dict[str, Any]:
        if not settings.dingtalk_ding_robot_code:
            raise RuntimeError("DINGTALK_DING_ROBOT_CODE is required")
        return self._v1_post(
            "/v1.0/robot/ding/send",
            {
                "robotCode": settings.dingtalk_ding_robot_code,
                "remindType": remind_type,
                "receiverUserIdList": user_ids,
                "content": content,
            },
        )

    def send_group_robot_text(
        self,
        content: str,
        at_user_ids: List[str] | None = None,
        at_mobiles: List[str] | None = None,
        is_at_all: bool = False,
    ) -> Dict[str, Any]:
        if not settings.dingtalk_group_webhook:
            raise RuntimeError("DINGTALK_GROUP_WEBHOOK is required")

        webhook = settings.dingtalk_group_webhook
        if settings.dingtalk_group_secret:
            timestamp = str(int(time.time() * 1000))
            string_to_sign = f"{timestamp}\n{settings.dingtalk_group_secret}"
            signature = hmac.new(
                settings.dingtalk_group_secret.encode("utf-8"),
                string_to_sign.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(signature))
            separator = "&" if "?" in webhook else "?"
            webhook = f"{webhook}{separator}timestamp={timestamp}&sign={sign}"

        payload: Dict[str, Any] = {
            "msgtype": "text",
            "text": {"content": content},
            "at": {
                "atUserIds": at_user_ids or [],
                "atMobiles": at_mobiles or [],
                "isAtAll": is_at_all,
            },
        }

        resp = requests.post(webhook, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") not in (0, "0", None):
            raise RuntimeError(f"group robot send failed: {data}")
        return data

    def convert_chat_to_open_conversation_id(self, chat_id: str) -> str:
        if not chat_id:
            raise RuntimeError("DINGTALK_GROUP_CHAT_ID is required")
        resp = requests.post(
            f"{self.V1_BASE}/v1.0/im/chat/{urllib.parse.quote(chat_id, safe='')}/convertToOpenConversationId",
            headers={"x-acs-dingtalk-access-token": self.access_token()},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        open_conversation_id = data.get("openConversationId", "")
        if not open_conversation_id:
            raise RuntimeError(f"convertToOpenConversationId failed: {data}")
        return str(open_conversation_id)

    def query_group_member_user_ids(self, open_conversation_id: str) -> List[str]:
        if not open_conversation_id:
            raise RuntimeError("openConversationId is required")

        user_ids: List[str] = []
        next_token = ""
        while True:
            payload: Dict[str, Any] = {
                "openConversationId": open_conversation_id,
                "maxResults": 200,
            }
            if next_token:
                payload["nextToken"] = next_token
            data = self._v1_post("/v1.0/im/sceneGroups/members/batchQuery", payload)
            user_ids.extend(data.get("memberUserIds", []) or [])
            if not data.get("hasMore"):
                break
            next_token = str(data.get("nextToken", "") or "")
            if not next_token:
                break
        return user_ids

    def query_group_member_user_ids_by_chat_id(self, chat_id: str) -> List[str]:
        chat_info = self.get_chat_info(chat_id)
        return [str(user_id) for user_id in (chat_info.get("useridlist", []) or []) if str(user_id).strip()]
