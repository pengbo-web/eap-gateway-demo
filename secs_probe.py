# -*- coding: utf-8 -*-
"""secsgem 主机探针: 验证模拟器行为(选中/收S6F11/发S2F41收S2F42), 为 Java 主站提供对照基准"""
import time

import secsgem.common
import secsgem.gem
import secsgem.hsms
from secsgem.secs.functions.s02f41 import SecsS02F41 as S02F41
from secsgem.secs.functions.s06f12 import SecsS06F12
from secsgem.secs.variables import Binary, List, String


class ProbeHost(secsgem.gem.GemHostHandler):
    def __init__(self, settings):
        super().__init__(settings)
        self.recv_events = []

    def _on_s06f11(self, handler, message):
        self.recv_events.append(message)
        print("[PROBE] S6F11 received:", message.value, flush=True)
        return SecsS06F12(Binary([0]))


def main():
    settings = secsgem.hsms.HsmsSettings(
        device_type=secsgem.common.DeviceType.HOST,
        connect_mode=secsgem.hsms.HsmsConnectMode.ACTIVE,
        address="127.0.0.1",
        port=5020,
    )
    host = ProbeHost(settings)
    host.enable()
    time.sleep(6)
    print("[PROBE] events so far:", len(host.recv_events), flush=True)
    reply = host.send_message(S02F41(List([String, List([])], ["STOP", []])), wait=True)
    print("[PROBE] S2F42 reply:", reply.value if reply else None, flush=True)
    time.sleep(3)
    reply = host.send_message(S02F41(List([String, List([])], ["START", []])), wait=True)
    print("[PROBE] S2F42 reply:", reply.value if reply else None, flush=True)
    time.sleep(3)
    print("[PROBE] total events:", len(host.recv_events), flush=True)


if __name__ == "__main__":
    main()
