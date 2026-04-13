import base64
import hashlib
import hmac
import json
import os
import struct
from typing import Any, Dict

from Crypto.Cipher import AES

from .config import settings


BLOCK_SIZE = 32


def _sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    parts = [token, timestamp, nonce, encrypt]
    parts.sort()
    return hashlib.sha1("".join(parts).encode("utf-8")).hexdigest()


def _pkcs7_pad(data: bytes) -> bytes:
    pad_len = BLOCK_SIZE - (len(data) % BLOCK_SIZE)
    return data + bytes([pad_len]) * pad_len


def _pkcs7_unpad(data: bytes) -> bytes:
    pad_len = data[-1]
    if pad_len < 1 or pad_len > BLOCK_SIZE:
        raise ValueError("invalid padding")
    return data[:-pad_len]


def _aes_key() -> bytes:
    if not settings.dingtalk_callback_aes_key:
        raise RuntimeError("DINGTALK_CALLBACK_AES_KEY is required")
    return base64.b64decode(settings.dingtalk_callback_aes_key + "=")


def decrypt_callback(encrypt: str, timestamp: str, nonce: str, signature: str) -> Dict[str, Any]:
    token = settings.dingtalk_callback_token
    if not token:
        raise RuntimeError("DINGTALK_CALLBACK_TOKEN is required")
    expected = _sha1_signature(token, timestamp, nonce, encrypt)
    if expected != signature:
        raise ValueError("callback signature mismatch")

    aes_key = _aes_key()
    cipher = AES.new(aes_key, AES.MODE_CBC, aes_key[:16])
    plain = _pkcs7_unpad(cipher.decrypt(base64.b64decode(encrypt)))
    msg_len = struct.unpack(">I", plain[16:20])[0]
    msg = plain[20 : 20 + msg_len]
    receive_id = plain[20 + msg_len :].decode("utf-8")
    # DingTalk callback payloads for internal apps commonly use the appKey
    # as the trailing receiver identifier instead of corpId.
    expected_ids = {settings.dingtalk_app_key, settings.dingtalk_corp_id}
    if receive_id not in expected_ids:
        raise ValueError("callback receiver id mismatch")
    return json.loads(msg.decode("utf-8"))


def encrypt_success(timestamp: str, nonce: str, payload: str = "success") -> Dict[str, str]:
    token = settings.dingtalk_callback_token
    if not token:
        raise RuntimeError("DINGTALK_CALLBACK_TOKEN is required")
    aes_key = _aes_key()
    msg = payload.encode("utf-8")
    raw = (
        os.urandom(16)
        + struct.pack(">I", len(msg))
        + msg
        + settings.dingtalk_app_key.encode("utf-8")
    )
    cipher = AES.new(aes_key, AES.MODE_CBC, aes_key[:16])
    encrypt = base64.b64encode(cipher.encrypt(_pkcs7_pad(raw))).decode("utf-8")
    signature = _sha1_signature(token, timestamp, nonce, encrypt)
    return {
        "msg_signature": signature,
        "timeStamp": timestamp,
        "nonce": nonce,
        "encrypt": encrypt,
    }
