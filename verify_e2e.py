# -*- coding: utf-8 -*-
"""全链路端到端回归(e2e)

1. 组件健康检查: RabbitMQ / Redis / TDengine / Mosquitto / EAP网关 / Liao-MES
2. 依序运行项目一(T1~T6)与项目二(MRP/FIFO/OEE/追溯/派工)验收脚本
3. 汇总 PASS/FAIL, 全部通过退出码 0

用法:
  python verify_e2e.py            # 常规回归(不含 200 路压测)
  python verify_e2e.py --with-load # 追加 T4 压测(12000 条, 约 3 分钟)
"""
import os
import socket
import subprocess
import sys
import time

import requests

DEMO = os.path.dirname(os.path.abspath(__file__))
WITH_LOAD = "--with-load" in sys.argv
FIXTURES = os.path.join(DEMO, "fixtures")
FIXTURE_RESET = os.path.join(FIXTURES, "dispatch_fixture_reset.sql")
OEE_RESET = os.path.join(FIXTURES, "oee_fixture_reset.sql")
FIFO_RESET = os.path.join(FIXTURES, "fifo_fixture_reset.sql")
TRACE_RESET = os.path.join(FIXTURES, "trace_fixture_reset.sql")

GREEN, RED, RESET_C = "\033[32m", "\033[31m", "\033[0m"


def tcp_ok(host, port, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def component_checks():
    checks = []

    def rabbit():
        r = requests.get("http://127.0.0.1:15672/api/overview", auth=("root", "123456"), timeout=5)
        return r.status_code == 200, f"mgmt-ok rabbitmq={r.json().get('rabbitmq_version', '?')}"

    def tdengine():
        r = requests.post("http://127.0.0.1:6041/rest/sql", data="SHOW DATABASES",
                          auth=("root", "taosdata"), timeout=5)
        dbs = [row[0] for row in r.json().get("data", [])]
        return r.json().get("code") == 0, f"rest-ok dbs={dbs}"

    def gateway():
        r = requests.get("http://127.0.0.1:8090/gateway/devices", timeout=5)
        devs = list(r.json().keys())
        return r.status_code == 200, f"devices={devs}"

    def mes():
        try:
            r = requests.get("http://127.0.0.1:8188/index.html", timeout=5)
            return True, f"http={r.status_code}"
        except requests.ConnectionError:
            return False, "8188 拒绝连接"

    checks.append(("RabbitMQ  :15672", rabbit))
    checks.append(("Redis    :6379", lambda: (tcp_ok("127.0.0.1", 6379), "tcp-ok")))
    checks.append(("TDengine :6041", tdengine))
    checks.append(("Mosquitto:1883", lambda: (tcp_ok("127.0.0.1", 1883), "tcp-ok")))
    checks.append(("EAP网关  :8090", gateway))
    checks.append(("Liao-MES :8188", mes))
    return checks


def run_verify(script, timeout=300, pre_sql=None):
    if pre_sql:
        # 走 docker cp + 容器内重定向: 规避 Windows 管道编码陷阱, 显式默认库与字符集
        subprocess.run(["docker", "cp", pre_sql, "hme-mysql:/tmp/e2e_fixture.sql"], capture_output=True)
        subprocess.run(["docker", "exec", "hme-mysql", "sh", "-c",
                        "mysql -uroot -p123456 --default-character-set=utf8mb4 liaoyun < /tmp/e2e_fixture.sql"],
                       capture_output=True)
    env = dict(os.environ, PYTHONIOENCODING="utf-8",
               # e2e 为同机共存部署(MES 消费者+模拟器同时在线): T4 零丢失与 DLQ 硬断言,
               # P99 只作参考输出; 500ms 门槛以独立口径单独运行 verify_t4.py 测量
               T4_P99_BUDGET="0")
    t0 = time.time()
    p = subprocess.run([sys.executable, "-X", "utf8", os.path.join(DEMO, script)],
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=timeout, env=env, cwd=DEMO)
    dur = time.time() - t0
    tail = (p.stdout or "").strip().splitlines()
    if not tail:
        tail = (p.stderr or "").strip().splitlines() or ["无输出"]
    if p.returncode == 0:
        detail = f"{dur:.0f}s | {tail[-1][:100]}"
    else:
        detail = f"{dur:.0f}s | 失败尾部: " + " // ".join(x[:90] for x in tail[-3:])
    return p.returncode == 0, detail


def restore_mq_threshold():
    """verify_t5 会把 DEV-MQ-01 阈值改为 205, 回归后恢复 245 防止 live 报警刷屏"""
    try:
        requests.post("http://127.0.0.1:8090/gateway/devices/DEV-MQ-01/config",
                      json={"v": 245.0}, timeout=5)
    except Exception:
        pass


def main():
    results = []
    print("=" * 62)
    print("阶段一: 组件健康检查")
    print("=" * 62)
    all_ok = True
    for name, fn in component_checks():
        try:
            ok, detail = fn()
        except Exception as exc:
            ok, detail = False, str(exc)[:80]
        all_ok &= ok
        print(f"[{'OK' if ok else 'FAIL'}] {name}  {detail}")
    if not all_ok:
        print("\n组件未就绪, 请先按 README 启动顺序拉起环境")
        sys.exit(1)

    # 业务种子(新建库可复现): MRP BOM/FIFO 台账/OEE 设备行/追溯工单/演示任务负载迁移
    seed = os.path.join(FIXTURES, "demo_seed.sql")
    subprocess.run(["docker", "cp", seed, "hme-mysql:/tmp/e2e_seed.sql"], capture_output=True)
    subprocess.run(["docker", "exec", "hme-mysql", "sh", "-c",
                    "mysql -uroot -p123456 --default-character-set=utf8mb4 liaoyun < /tmp/e2e_seed.sql"],
                   capture_output=True)

    suite = [
        ("T1  Modbus 断线重连", "verify_t1.py", None),
        ("T1b Modbus 超时重发", "verify_t1b.py", None),
        ("T2  HSMS/SECS-II 通道", "verify_t2.py", None),
        ("T3  三协议汇聚+时序落库", "verify_t3.py", None),
        ("T3b SPC X̄-R 判异", "verify_t3b.py", None),
        ("T5  心跳离线+配置热更", "verify_t5.py", None),
        ("T5b MTTR/MTBF 可靠性", "verify_t5b.py", None),
        ("T6  MES MQ 消费链路", "verify_t6.py", None),
    ]
    if WITH_LOAD:
        suite.insert(5, ("T4  200路压测零丢失", "verify_t4.py", None))
    suite += [
        ("P2  MRP 净需求展开", "verify_mrp.py", None),
        ("P2  FIFO 出库分配", "verify_fifo.py", FIFO_RESET),
        ("P2  OEE 三率引擎", "verify_oee.py", OEE_RESET),
        ("P2  SN 双向追溯", "verify_trace.py", TRACE_RESET),
        ("P2  派工规则引擎", "verify_dispatch.py", FIXTURE_RESET),
    ]

    print()
    print("=" * 62)
    print(f"阶段二: 验收脚本回归({'含' if WITH_LOAD else '不含'} T4 压测)")
    print("=" * 62)
    failed = []
    for title, script, pre_sql in suite:
        try:
            ok, detail = run_verify(script, pre_sql=pre_sql)
        except subprocess.TimeoutExpired:
            ok, detail = False, "超时(>300s)"
        except FileNotFoundError:
            ok, detail = False, "脚本不存在"
        if title.startswith("T5 ") :
            restore_mq_threshold()
        print(f"[{GREEN}PASS{RESET_C if ok else RED}] {title}" if ok else f"[{RED}FAIL{RESET_C}] {title}", end="")
        print(f"  {detail}")
        if not ok:
            failed.append(title)

    print()
    print("=" * 62)
    total = len(suite)
    if failed:
        print(f"e2e 回归: {total - len(failed)}/{total} PASS, 失败: {failed}")
        sys.exit(1)
    print(f"e2e 回归: {total}/{total} 全部 PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
