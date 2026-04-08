#!/usr/bin/env python3
"""server.py — MiniSQL 중계 서버 (지용)

stdlib 만 사용. ./sqlparser 바이너리를 호출해 결과를 HTTP 로 중계한다.

엔드포인트:
  GET  /              → index.html 서빙
  POST /query         → body 의 SQL 을 받아 ./sqlparser ... --json 실행 후 결과 반환
                        request body: {"sql": "..."}
                        response:     {"statements": [{...parsed...}, ...],
                                       "stderr": "..."}

사용:
  python3 server.py            # 0.0.0.0:8000
  python3 server.py 9000       # 포트 지정
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
SQLPARSER_BIN = ROOT / "sqlparser"
INDEX_HTML = ROOT / "index.html"


def run_sqlparser(sql: str) -> dict:
    """SQL 문자열을 파일에 쓰고 ./sqlparser ... --json 실행."""
    if not SQLPARSER_BIN.exists():
        return {
            "error": f"sqlparser binary not found at {SQLPARSER_BIN}. "
                     f"먼저 'make' 로 빌드하세요.",
            "statements": [],
            "stderr": "",
        }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False, encoding="utf-8"
    ) as tf:
        tf.write(sql)
        tmp_path = tf.name

    try:
        proc = subprocess.run(
            [str(SQLPARSER_BIN), tmp_path, "--json"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return {"error": "sqlparser timeout (5s)", "statements": [], "stderr": ""}
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    statements = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            statements.append(json.loads(line))
        except json.JSONDecodeError:
            statements.append({"raw": line})

    return {
        "statements": statements,
        "stderr": proc.stderr,
        "returncode": proc.returncode,
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            if not INDEX_HTML.exists():
                self._send_json(404, {"error": "index.html not found"})
                return
            body = INDEX_HTML.read_bytes()
            self._send(200, body, "text/html; charset=utf-8")
            return

        if self.path == "/health":
            self._send_json(200, {"ok": True, "binary_exists": SQLPARSER_BIN.exists()})
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/query":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            self._send_json(400, {"error": "empty body"})
            return

        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            self._send_json(400, {"error": f"invalid JSON: {e}"})
            return

        sql = data.get("sql", "").strip()
        if not sql:
            self._send_json(400, {"error": "missing 'sql' field"})
            return

        result = run_sqlparser(sql)
        self._send_json(200, result)

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[server] {self.address_string()} {fmt % args}\n")


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[server] MiniSQL viewer listening on http://0.0.0.0:{port}")
    print(f"[server] sqlparser binary: {SQLPARSER_BIN} "
          f"({'found' if SQLPARSER_BIN.exists() else 'NOT FOUND — run make first'})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[server] bye")


if __name__ == "__main__":
    main()
