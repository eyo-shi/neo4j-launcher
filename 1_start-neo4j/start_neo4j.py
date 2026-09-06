import html
import json
import os
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urljoin, urlparse

from utils.neo4j_utils import (
    get_cml_proxy_discovery_json,
    get_connection_info,
    get_internal_browser_url,
    get_proxy_unavailable_reason,
    prepare_neo4j_http_request,
    run_neo4j_supervisor,
    urlopen_neo4j_http,
    is_k8s_proxy_http_url,
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

CORS_ALLOW_HEADERS = (
    "Authorization, Accept, Content-Type, Neo4j-Database, "
    "Neo4j-Transaction-Id, Neo4j-Transaction-Timeout, "
    "X-Forwarded-For, X-Forwarded-Host, X-Forwarded-Proto"
)


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
        ("Password Source", info.get("password_source")),
        ("Deployed NEO4J_AUTH", info.get("deployed_neo4j_auth")),
        ("Auth In Sync", info.get("auth_in_sync")),
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
  <p>Wait until <strong>Status</strong> becomes <code>running</code>, then open <strong>Open Neo4j Browser</strong>. On the connect screen, use protocol <code>https://</code> and paste the full <strong>HTTP API Connect URL</strong> below (must end with <code>/</code>). Keep <strong>Connect with SSO</strong> off.</p>
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
        if method == "OPTIONS":
            self._serve_cors_preflight()
            return
        if path in ("/health", "/healthz"):
            self._serve_health_check()
            return
        if path in ("/launcher", "/launcher/"):
            self._serve_status_page()
            return
        if path == "/launcher/discovery":
            self._serve_discovery_json()
            return
        self._proxy_request(method)

    def _cors_origin(self) -> str:
        origin = self.headers.get("Origin")
        if origin:
            return origin
        host = self.headers.get("Host")
        if host:
            return f"https://{host}"
        return "*"

    def _send_cors_headers(self) -> None:
        origin = self._cors_origin()
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Credentials", "true")
        self.send_header("Vary", "Origin")

    def _serve_cors_preflight(self) -> None:
        requested_headers = self.headers.get(
            "Access-Control-Request-Headers", CORS_ALLOW_HEADERS
        )
        self.send_response(204)
        self._send_cors_headers()
        self.send_header(
            "Access-Control-Allow-Methods",
            "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        )
        self.send_header("Access-Control-Allow-Headers", requested_headers)
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _serve_discovery_json(self) -> None:
        payload = get_cml_proxy_discovery_json()
        if payload is None:
            self.send_error(
                503,
                "Neo4j discovery is not available yet. Please wait and retry.",
            )
            return
        body = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
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

        request = prepare_neo4j_http_request(target_url, data=body, method=method)
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
                continue
            request.add_header(header, value)

        parsed_target = urlparse(target_url)
        if not is_k8s_proxy_http_url(target_url):
            request.add_header("Host", parsed_target.netloc)
        if not forwarded_host:
            forwarded_host = self.headers.get("Host")
        if forwarded_host:
            request.add_header("X-Forwarded-Host", forwarded_host)
        if not forwarded_proto:
            forwarded_proto = "https"
        request.add_header("X-Forwarded-Proto", forwarded_proto)

        try:
            with urlopen_neo4j_http(request, timeout=120) as response:
                body_bytes = response.read()
                path = urlparse(self.path).path
                content_type = response.headers.get("Content-Type", "")
                if (
                    method == "GET"
                    and path in ("", "/")
                    and "application/json" in content_type
                ):
                    rewritten = get_cml_proxy_discovery_json()
                    if rewritten is not None:
                        body_bytes = rewritten.encode("utf-8")

                self.send_response(response.status)
                for header, value in response.headers.items():
                    header_lower = header.lower()
                    if header_lower in HOP_BY_HOP_HEADERS:
                        continue
                    if header_lower == "content-length":
                        continue
                    self.send_header(header, value)
                self._send_cors_headers()
                self.send_header("Content-Length", str(len(body_bytes)))
                self.end_headers()
                self.wfile.write(body_bytes)
        except urllib.error.HTTPError as exc:
            error_body = exc.read()
            self.send_response(exc.code)
            for header, value in exc.headers.items():
                if header.lower() not in HOP_BY_HOP_HEADERS:
                    self.send_header(header, value)
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(error_body)
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


class ReuseAddrHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    port = int(os.getenv("CDSW_APP_PORT") or "8090")
    bind_host = os.getenv("NEO4J_LAUNCHER_BIND_HOST", "127.0.0.1")
    print(f"Starting Neo4j Launcher on {bind_host}:{port}")
    server = ReuseAddrHTTPServer((bind_host, port), Neo4jLauncherHandler)
    server.daemon_threads = True
    threading.Thread(target=run_neo4j_supervisor, daemon=True).start()

    # --- 追記: 定期的に Pod / ロールアウト状態を出力 ---
    import time

    def _log_debugger():
        last_message = None
        while True:
            time.sleep(30)
            info = get_connection_info()
            logs = info.get("neo4j_pod_logs")
            if not logs or logs == last_message:
                continue
            last_message = logs
            print("\n========== [NEO4J POD LOGS TAIL] ==========")
            print(logs)
            print("===========================================\n")

    threading.Thread(target=_log_debugger, daemon=True).start()
    # -----------------------------------------------------------------

    server.serve_forever()
