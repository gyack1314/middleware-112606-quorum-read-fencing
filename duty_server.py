#!/usr/bin/env python3
"""本地 HTTP 协调服务（标准库）。

    POST /v1/message   单条信封 {"at":..,"kind":..,"record":{}} -> {"events":[...]}
    POST /v1/batch     {"messages":[信封,...]} -> {"results":[[事件,...],...]}
    GET  /v1/board     值班总览
    GET  /v1/state     完整持久化状态快照
    GET  /healthz      健康检查

所有状态变更经同一把锁串行化；状态可落盘，POST kind=restart 即模拟重启恢复。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from coordinator import Coordinator, JsonFileStore, JsonMemoryStore


class DutyApp:
    def __init__(self, state_path: str | None = None) -> None:
        store = JsonFileStore(state_path) if state_path else JsonMemoryStore()
        self.coordinator = Coordinator(store=store)
        self.lock = threading.RLock()

    def handle_message(self, envelope: dict) -> list[dict]:
        with self.lock:
            return self.coordinator.handle(envelope)

    def handle_batch(self, envelopes: list[dict]) -> list[list[dict]]:
        with self.lock:
            return [self.coordinator.handle(envelope) for envelope in envelopes]

    def board(self) -> dict:
        with self.lock:
            return self.coordinator.board()

    def state(self) -> dict:
        with self.lock:
            return self.coordinator.snapshot()


def build_handler(app: DutyApp) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "DutyReadService/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 保持控制台安静
            return

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> Any:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            return json.loads(raw.decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?")[0] == "/healthz":
                self._send_json(200, {"ok": True})
            elif self.path == "/v1/board":
                self._send_json(200, app.board())
            elif self.path == "/v1/state":
                self._send_json(200, app.state())
            else:
                self._send_json(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            path = self.path.split("?")[0]
            try:
                payload = self._read_json()
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                self._send_json(400, {"error": f"bad_json: {error}"})
                return
            try:
                if path == "/v1/message":
                    if not isinstance(payload, dict):
                        self._send_json(400, {"error": "message envelope must be object"})
                        return
                    self._send_json(200, {"events": app.handle_message(payload)})
                elif path == "/v1/batch":
                    messages = payload.get("messages") if isinstance(payload, dict) else None
                    if not isinstance(messages, list):
                        self._send_json(400, {"error": "{'messages': [...]} required"})
                        return
                    self._send_json(200, {"results": app.handle_batch(messages)})
                else:
                    self._send_json(404, {"error": "not_found"})
            except Exception as error:  # 服务边界：错误以 JSON 返回而不是断连
                self._send_json(500, {"error": type(error).__name__, "detail": str(error)})

    return Handler


def build_server(host: str = "127.0.0.1", port: int = 0, state_path: str | None = None) -> tuple[ThreadingHTTPServer, DutyApp]:
    app = DutyApp(state_path)
    server = ThreadingHTTPServer((host, port), build_handler(app))
    return server, app


def serve_http(*, host: str = "127.0.0.1", port: int = 8765, state_path: str | None = None) -> int:
    server, _ = build_server(host, port, state_path)
    actual_host, actual_port = server.server_address[:2]
    print(f"协调读服务已启动：http://{actual_host}:{actual_port}  (state={state_path or 'memory'})", flush=True)
    print("POST /v1/message | POST /v1/batch | GET /v1/board | GET /v1/state", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n停止服务", flush=True)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="本地 HTTP 协调读服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state", default=None)
    args = parser.parse_args()
    raise SystemExit(serve_http(host=args.host, port=args.port, state_path=args.state))
