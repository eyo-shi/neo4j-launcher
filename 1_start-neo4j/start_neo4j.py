import html
import os
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urljoin, urlparse

from utils.neo4j_utils import (
    get_connection_info,
    get_internal_browser_url,
    run_neo4j_supervisor,
)

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def _render_status_page(info: dict) -> str:
    status = html.escape(info.get("status", "starting"))
    rows = [
        ("Status", status),
        ("Username", info.get("username")),
        ("Password", info.get("password")),
        ("Neo4j Browser", info.get("proxied_browser_path")),
        ("HTTP API Connect URL", info.get("http_api_connect_url")),
        ("Internal Bolt URI", info.get("internal_bolt")),
        ("Internal Browser", info.get("internal_browser")),
        ("External Bolt URI", info.get("external_bolt")),
        ("External Browser", info.get("external_browser")),
        ("Service Type", info.get("service_type")),
        ("Port Forward", info.get("port_forward_command")),
        ("Message", info.get("message")),
    ]

    table_rows = []
    for label, value in rows:
        if not value:
            continue
        if label == "Neo4j Browser":
            cell = (
                f'<a href="{html.escape(value)}">Open Neo4j Browser</a> '
                "(recommended)"
            )
        elif label == "External Browser" and str(value).startswith("http"):
            cell = (
                f'<a href="{html.escape(value)}" target="_blank" '
                f'rel="noopener noreferrer">{html.escape(value)}</a> '
                "(may be blocked by network policy)"
            )
        else:
            cell = f"<code>{html.escape(str(value))}</code>"
        table_rows.append(
            f"<tr><th>{html.escape(label)}</th><td>{cell}</td></tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="10">
  <title>Neo4j Launcher</title>
  <style>
    body {{ font-family: sans-serif; margin: 2rem; }}
    table {{ border-collapse: collapse; }}
    th, td {{ border: 1px solid #ccc; padding: 0.5rem 0.75rem; text-align: left; }}
    th {{ background: #f5f5f5; }}
  </style>
</head>
<body>
  <h1>Neo4j Launcher</h1>
  <p>Neo4j deployment status and connection details.</p>
  <p>Use <strong>Open Neo4j Browser</strong> below. On the connect screen, choose <code>https://</code> and enter the <strong>HTTP API Connect URL</strong> shown below. Do not use the external LoadBalancer Bolt URL from your browser.</p>
  <table>
    {''.join(table_rows)}
  </table>
</body>
</html>"""


class Neo4jLauncherHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self._handle_request("GET")

    def do_POST(self) -> None:
        self._handle_request("POST")

    def do_PUT(self) -> None:
        self._handle_request("PUT")

    def do_DELETE(self) -> None:
        self._handle_request("DELETE")

    def do_OPTIONS(self) -> None:
        self._handle_request("OPTIONS")

    def _handle_request(self, method: str) -> None:
        path = urlparse(self.path).path
        if path in ("/health", "/healthz"):
            self._serve_health_check()
            return
        if path == "/":
            self._serve_status_page()
            return
        self._proxy_request(method)

    def _serve_health_check(self) -> None:
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_status_page(self) -> None:
        page = _render_status_page(get_connection_info())
        body = page.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy_request(self, method: str) -> None:
        internal_browser = get_internal_browser_url()
        if not internal_browser:
            self.send_error(503, "Neo4j Browser is not ready yet")
            return

        target_url = urljoin(f"{internal_browser.rstrip('/')}/", self.path.lstrip("/"))
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else None

        request = urllib.request.Request(target_url, data=body, method=method)
        for header, value in self.headers.items():
            header_lower = header.lower()
            if header_lower in HOP_BY_HOP_HEADERS or header_lower == "host":
                continue
            request.add_header(header, value)

        parsed_target = urlparse(target_url)
        request.add_header("Host", parsed_target.netloc)

        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                self.send_response(response.status)
                for header, value in response.headers.items():
                    if header.lower() not in HOP_BY_HOP_HEADERS:
                        self.send_header(header, value)
                self.end_headers()
                while True:
                    chunk = response.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except urllib.error.HTTPError as exc:
            self.send_response(exc.code)
            for header, value in exc.headers.items():
                if header.lower() not in HOP_BY_HOP_HEADERS:
                    self.send_header(header, value)
            self.end_headers()
            self.wfile.write(exc.read())
        except Exception as exc:
            self.send_error(502, f"Failed to proxy Neo4j Browser request: {exc}")

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    port = int(os.getenv("CDSW_APP_PORT") or "8090")
    threading.Thread(target=run_neo4j_supervisor, daemon=True).start()
    print(f"Starting Neo4j Launcher status page on 127.0.0.1:{port}")
    server = ThreadingHTTPServer(("127.0.0.1", port), Neo4jLauncherHandler)
    server.daemon_threads = True
    server.serve_forever()
