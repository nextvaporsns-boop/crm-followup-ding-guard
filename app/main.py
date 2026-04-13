import json
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import settings
from .callback_crypto import decrypt_callback, encrypt_success
from .db import add_group_event, init_db, recent_group_events, recent_run_logs
from .dingtalk_client import DingTalkClient
from .huoban_client import HuobanClient
from .scheduler_service import SchedulerService
from .service import FollowupReminderService


APP_DIR = Path(__file__).resolve().parent

app = FastAPI(title="CRM Followup Ding Guard")
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

dingtalk_client = DingTalkClient()
huoban_client = HuobanClient()
service = FollowupReminderService(dingtalk_client, huoban_client)
scheduler = SchedulerService(service)


def _redirect_with_msg(ok: bool, msg: str, target: str = "/") -> RedirectResponse:
    return RedirectResponse(
        url=f"{target}?ok={'1' if ok else '0'}&msg={quote_plus(msg)}",
        status_code=303,
    )


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    scheduler.start()


@app.on_event("shutdown")
def shutdown_event() -> None:
    scheduler.shutdown()


@app.get("/")
def index(request: Request):
    try:
        rows = service.preview_rows(source="web_index_preview")
        preview_error = ""
    except Exception as exc:
        rows = service.dashboard_rows()
        preview_error = str(exc)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "rows": rows,
            "logs": recent_run_logs(150),
            "flash_ok": request.query_params.get("ok") == "1",
            "flash_msg": (request.query_params.get("msg") or "").strip(),
            "now_text": datetime.now(ZoneInfo(settings.timezone)).strftime("%Y-%m-%d %H:%M:%S"),
            "threshold": settings.follow_count_threshold,
            "preview_error": preview_error,
        },
    )


@app.get("/group-picker")
def group_picker(request: Request):
    preview_data = None
    preview_error = ""
    if request.query_params.get("preview") == "1":
        try:
            preview_data = service.preview_group_demo(source="web_group_preview")
        except Exception as exc:
            preview_error = str(exc)
    return templates.TemplateResponse(
        "group_picker.html",
        {
            "request": request,
            "corp_id": settings.dingtalk_corp_id,
            "group_events": recent_group_events(20),
            "flash_ok": request.query_params.get("ok") == "1",
            "flash_msg": (request.query_params.get("msg") or "").strip(),
            "preview_data": preview_data,
            "preview_error": preview_error,
        },
    )


@app.post("/api/group/convert")
def convert_group(chat_id: str = Form(...)):
    try:
        open_conversation_id = dingtalk_client.convert_chat_to_open_conversation_id(chat_id.strip())
        return JSONResponse(
            {
                "ok": True,
                "chatId": chat_id.strip(),
                "openConversationId": open_conversation_id,
            }
        )
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/jsapi/config")
def get_jsapi_config(url: str = Form(...)):
    try:
        config = dingtalk_client.build_jsapi_config(url.strip())
        return JSONResponse({"ok": True, "config": config})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.get("/dingtalk/callback")
def dingtalk_callback_check(msg_signature: str, timestamp: str, nonce: str, encrypt: str):
    try:
        decrypt_callback(encrypt, timestamp, nonce, msg_signature)
        return JSONResponse(encrypt_success(timestamp, nonce))
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/dingtalk/callback")
async def dingtalk_callback_post(request: Request, msg_signature: str, timestamp: str, nonce: str):
    body = await request.json()
    encrypt = str(body.get("encrypt", ""))
    try:
        payload = decrypt_callback(encrypt, timestamp, nonce, msg_signature)
        event_type = str(payload.get("EventType") or payload.get("conversationType") or "unknown")
        add_group_event(
            event_type=event_type,
            chat_id=str(payload.get("ChatId", "")),
            open_conversation_id=str(payload.get("OpenConversationId", "")),
            title=str(payload.get("Title", "") or payload.get("conversationTitle", "")),
            operator_user_id=str(payload.get("Operator", "") or payload.get("senderStaffId", "")),
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
        return JSONResponse(encrypt_success(timestamp, nonce))
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/run/initial")
def run_initial_now():
    try:
        sent, skipped = service.run_initial_check(source="web_manual_initial")
        return _redirect_with_msg(True, f"首次检查完成：发送 {sent} 人，跳过 {skipped} 人")
    except Exception as exc:
        return _redirect_with_msg(False, f"首次检查失败：{exc}")


@app.post("/run/urge")
def run_urge_now():
    try:
        urged, resolved, read = service.run_urge_cycle(source="web_manual_urge")
        return _redirect_with_msg(True, f"催办完成：催办 {urged} 人，达标解除 {resolved} 人，已读未催 {read} 人")
    except Exception as exc:
        return _redirect_with_msg(False, f"催办失败：{exc}")


@app.post("/notify/user")
def notify_user_now(user_id: str = Form(...)):
    try:
        result = service.manual_notify_user(user_id=user_id.strip(), source="web_user_button")
        return _redirect_with_msg(
            True,
            f"已发送催办通知：{result['salesperson']}，当前跟进 {result['follow_count']} 条",
        )
    except Exception as exc:
        return _redirect_with_msg(False, f"发送失败：{exc}")


@app.post("/group/demo")
def send_group_demo_now():
    try:
        result = service.send_group_demo(source="web_group_demo")
        return _redirect_with_msg(True, f"已发送群测试消息：{result['preview_names']}", target="/group-picker")
    except Exception as exc:
        return _redirect_with_msg(False, f"群测试消息发送失败：{exc}", target="/group-picker")


@app.get("/group/preview")
def preview_group_demo_now():
    return RedirectResponse(url="/group-picker?preview=1", status_code=303)
