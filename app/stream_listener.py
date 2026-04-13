import json
import logging
from typing import Any, Dict

import dingtalk_stream

from .config import settings
from .db import add_group_event, init_db


def _pick(payload: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _record_event(event_type: str, payload: Dict[str, Any]) -> None:
    add_group_event(
        event_type=event_type,
        chat_id=_pick(payload, "chatId", "conversationId", "conversation_id"),
        open_conversation_id=_pick(payload, "openConversationId", "open_conversation_id"),
        title=_pick(payload, "title", "conversationTitle", "sessionTitle"),
        operator_user_id=_pick(payload, "operator", "senderStaffId", "staffId", "userid"),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )


class GroupEventHandler(dingtalk_stream.EventHandler):
    async def process(self, event: dingtalk_stream.EventMessage):
        payload = dict(event.data or {})
        event_type = str(event.headers.event_type or "unknown")
        logging.info("stream event received: %s %s", event_type, payload)
        _record_event(event_type, payload)
        return dingtalk_stream.AckMessage.STATUS_OK, "OK"


class RobotMessageHandler(dingtalk_stream.ChatbotHandler):
    async def process(self, callback: dingtalk_stream.CallbackMessage):
        payload = dict(callback.data or {})
        logging.info("chatbot message received: %s", payload)
        _record_event("chatbot_message", payload)
        return dingtalk_stream.AckMessage.STATUS_OK, "OK"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    init_db()
    credential = dingtalk_stream.Credential(settings.dingtalk_app_key, settings.dingtalk_app_secret)
    client = dingtalk_stream.DingTalkStreamClient(credential)
    client.register_all_event_handler(GroupEventHandler())
    client.register_callback_handler(dingtalk_stream.chatbot.ChatbotMessage.TOPIC, RobotMessageHandler())
    client.start_forever()
