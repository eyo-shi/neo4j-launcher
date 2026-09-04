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
    get_proxy_unavailable_reason,
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

FORWARDED_HEADERS = {
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
    "forwarded",
}


def _render_status_page(info: dict) -> str:
    status = html.escape(info.get("status", "starting"))
    rows = [
        ("Status", status),
        ("Supervisor Phase", info.get("supervisor_phase")),
        ("Deployment", info.get("deployment_status")),
        ("Service", info.get("service_status")),
        ("Neo4j Pod", info.get("neo4j_pod_status")),
        ("Pod Logs (tail)", info.get("neo4j_pod_logs")),
        ("K8s Events", info.get("k8s_events")),
        ("Parent Pod", info.get("parent_pod")),
        ("PVC Claim", info.get("pvc_claim")),
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
        elif label == "Pod Logs (tail)" and value:
            cell = (
                f"<pre style='max-height:12rem;overflow:auto'>"
                f"{html.escape(str(value)[-2000:])}</pre>"
            )
        elif label == "K8s Events" and value:
            cell = (
                f"<pre style='max-height:8rem;overflow:auto'>"
                f"{html.escape(str(value)[-1500:])}</pre>"
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
  <p>Deployment status is shown here. Neo4j Browser connects through this application URL using the HTTPS Query API.</p>
  <p>Wait until <strong>Status</strong> becomes <code>running</code>, then open <strong>Open Neo4j Browser</strong>. On the connect screen, use protocol <code>https://</code> and paste the full <strong>HTTP API Connect URL</strong> below.</p>
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
        if path in ("/launcher", "/launcher/"):
            self._serve_status_page()
            return
        if path == "/" and method == "GET" and self._accepts_html():
            self._redirect_to("/launcher")
            return
        self._proxy_request(method)

    def _accepts_html(self) -> bool:
        accept = self.headers.get("Accept", "")
        return "text/html" in accept or accept.startswith("*/*")

    def _redirect_to(self, location: str) -> None:
        body = f"Redirecting to {location}".encode("utf-8")
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
            reason = get_proxy_unavailable_reason()
            self.send_error(
                503,
                f"Neo4j Browser is not ready yet. {reason}",
            )
            return

        target_url = urljoin(f"{internal_browser.rstrip('/')}/", self.path.lstrip("/"))
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else None

        request = urllib.request.Request(target_url, data=body, method=method)
        forwarded_host = None
        forwarded_proto = None
        for header, value in self.headers.items():
            header_lower = header.lower()
            if header_lower in HOP_BY_HOP_HEADERS or header_lower == "host":
                continue
            if header_lower in FORWARDED_HEADERS:
                if header_lower == "x-forwarded-host":
                    forwarded_host = value
                if header_lower == "x-forwarded-proto":
                    forwarded_proto = value
            request.add_header(header, value)

        parsed_target = urlparse(target_url)
        request.add_header("Host", parsed_target.netloc)
        if not forwarded_host:
            forwarded_host = self.headers.get("Host")
        if forwarded_host:
            request.add_header("X-Forwarded-Host", forwarded_host)
        if not forwarded_proto:
            forwarded_proto = "https"
        request.add_header("X-Forwarded-Proto", forwarded_proto)

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
        except urllib.error.URLError as exc:
            self.send_error(
                503,
                "Neo4j Browser is not ready yet. Please wait and retry.",
            )
        except Exception as exc:
            print(f"Proxy error for {method} {self.path}: {exc}")
            self.send_error(502, f"Failed to proxy Neo4j Browser request: {exc}")

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    port = int(os.getenv("CDSW_APP_PORT") or "8090")
    bind_host = os.getenv("NEO4J_LAUNCHER_BIND_HOST", "0.0.0.0")
    threading.Thread(target=run_neo4j_supervisor, daemon=True).start()
    print(f"Starting Neo4j Launcher on {bind_host}:{port}")
    server = ThreadingHTTPServer((bind_host, port), Neo4jLauncherHandler)
    server.daemon_threads = True
    server.serve_forever()
