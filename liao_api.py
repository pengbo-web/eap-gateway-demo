# -*- coding: utf-8 -*-
"""Liao-mes API 助手: 复现 RuoYi-Plus 登录加密(AES-ECB body + RSA 包装 AES key)"""
import base64
import json
import os

import requests
from Crypto.Cipher import AES, PKCS1_v1_5
from Crypto.PublicKey import RSA
from Crypto.Util.Padding import pad

PUB = ("MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKoR8mX0rGKLqzcWmOzbfj64K8ZIgOdHnzkXSOVOZbFu"
       "/TJhZ7rFAN+eaGkl3C4buccQd/EjEsj9ir7ijT7h96MCAwEAAQ==")
CLIENT = "e5cd7e4891bf95d1d19206ce24a7b32e"
BASE = os.environ.get("LIAO_BASE", "http://127.0.0.1:8188")


def enc_body(obj: dict):
    aes_key = os.urandom(16).hex()  # 32 字符
    cipher = AES.new(aes_key.encode(), AES.MODE_ECB)
    body = base64.b64encode(cipher.encrypt(pad(json.dumps(obj, separators=(",", ":")).encode(), 16))).decode()
    rsa = PKCS1_v1_5.new(RSA.import_key(base64.b64decode(PUB)))
    header = base64.b64encode(rsa.encrypt(base64.b64encode(aes_key.encode()))).decode()
    return body, header


def login(user="admin", pwd="admin123", tenant="000000"):
    body, header = enc_body({
        "username": user, "password": pwd, "grantType": "password",
        "tenantId": tenant, "clientId": CLIENT,
    })
    r = requests.post(BASE + "/auth/login", data=body, headers={
        "Content-Type": "application/json", "clientid": CLIENT, "encrypt-key": header})
    j = r.json()
    assert j["code"] == 200, j
    return j["data"]["access_token"]


def get(path: str, token: str):
    r = requests.get(BASE + path, headers={"Authorization": "Bearer " + token, "clientid": CLIENT})
    return r.json()


if __name__ == "__main__":
    tok = login()
    print("TOKEN", tok)
