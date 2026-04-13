import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    app_host: str = os.getenv("APP_HOST", "0.0.0.0")
    app_port: int = int(os.getenv("APP_PORT", "8090"))
    timezone: str = os.getenv("TIMEZONE", "Asia/Shanghai")

    dingtalk_app_key: str = os.getenv("DINGTALK_APP_KEY", "")
    dingtalk_app_secret: str = os.getenv("DINGTALK_APP_SECRET", "")
    dingtalk_corp_id: str = os.getenv("DINGTALK_CORP_ID", "")
    dingtalk_notify_agent_id: str = os.getenv("DINGTALK_NOTIFY_AGENT_ID", "")
    dingtalk_ding_robot_code: str = os.getenv("DINGTALK_DING_ROBOT_CODE", "")
    dingtalk_group_webhook: str = os.getenv("DINGTALK_GROUP_WEBHOOK", "")
    dingtalk_group_secret: str = os.getenv("DINGTALK_GROUP_SECRET", "")
    dingtalk_group_chat_id: str = os.getenv("DINGTALK_GROUP_CHAT_ID", "")
    dingtalk_group_open_conversation_id: str = os.getenv("DINGTALK_GROUP_OPEN_CONVERSATION_ID", "")
    dingtalk_callback_token: str = os.getenv("DINGTALK_CALLBACK_TOKEN", "")
    dingtalk_callback_aes_key: str = os.getenv("DINGTALK_CALLBACK_AES_KEY", "")

    huoban_api_url: str = os.getenv("HUOBAN_API_URL", "https://api.huoban.com/openapi/v1/item/list")
    huoban_bearer_token: str = os.getenv("HUOBAN_BEARER_TOKEN", "")
    huoban_table_id: str = os.getenv("HUOBAN_TABLE_ID", "")
    huoban_field_follow_date: str = os.getenv("HUOBAN_FIELD_FOLLOW_DATE", "")
    huoban_field_sale_name: str = os.getenv("HUOBAN_FIELD_SALE_NAME", "")
    huoban_field_user_id: str = os.getenv("HUOBAN_FIELD_USER_ID", "")
    huoban_field_follow_count: str = os.getenv("HUOBAN_FIELD_FOLLOW_COUNT", "")
    huoban_page_limit: int = int(os.getenv("HUOBAN_PAGE_LIMIT", "100"))

    follow_count_threshold: int = int(os.getenv("FOLLOW_COUNT_THRESHOLD", "5"))
    initial_check_hour: int = int(os.getenv("INITIAL_CHECK_HOUR", "18"))
    initial_check_minute: int = int(os.getenv("INITIAL_CHECK_MINUTE", "0"))
    urge_interval_minutes: int = int(os.getenv("URGE_INTERVAL_MINUTES", "15"))
    urge_end_hour: int = int(os.getenv("URGE_END_HOUR", "21"))
    auto_run_enabled: bool = _as_bool(os.getenv("AUTO_RUN_ENABLED", "true"), True)

    db_path: str = str(BASE_DIR / "data" / "app.db")

    @property
    def monitored_user_ids(self) -> list[str]:
        raw = os.getenv("MONITORED_USER_IDS", "")
        return [item.strip() for item in raw.split(",") if item.strip()]


settings = Settings()
