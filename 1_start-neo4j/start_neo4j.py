import html
import json
import os
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urljoin, urlparse

from utils.neo4j_utils import (
    build_proxied_browser_path,
    get_cml_application_base_url,
    get_cml_proxy_discovery_json,
    get_connection_info,
    get_internal_browser_url,
    get_proxy_unavailable_reason,
    invalidate_neo4j_http_cache,
    is_k8s_proxy_http_url,
    is_neo4j_http_up,
    browser_asset_proxy_paths,
    prepare_neo4j_http_request,
    rewrite_proxy_location_header,
    rewrite_proxy_response_body,
    run_neo4j_supervisor,
    set_request_public_base_url_from_host,
    urlopen_neo4j_http,
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

STRIPPED_RESPONSE_HEADERS = HOP_BY_HOP_HEADERS | {
    "content-encoding",
    "content-length",
    "content-security-policy",
    "content-security-policy-report-only",
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
    def send_error(self, code, message=None, explain=None):
        self.log_error("code %d, message %s", code, message)
        self.send_response(code, message)
        self.send_header('Connection', 'close')
        self._send_cors_headers()
        self.end_headers()
        if explain is None:
            explain = self.responses.get(code, ('', ''))[1]
        body = explain.encode('utf-8')
        self.wfile.write(body)

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

    def _current_request_host(self) -> str | None:
        host = self.headers.get("X-Forwarded-Host") or self.headers.get("Host")
        if not host:
            return None
        return host.split(",")[0].strip()

    def _maybe_redirect_outdated_query(self) -> bool:
        parsed = urlparse(self.path)
        if not parsed.query:
            return False

        from urllib.parse import parse_qsl, urlencode, urlunparse
        q_params = parse_qsl(parsed.query)

        request_host = self._current_request_host()
        if not request_host:
            return False

        current_base = f"https://{request_host}".rstrip("/")
        curr_parsed = urlparse(current_base)
        if curr_parsed.scheme == "https" and not curr_parsed.port:
            curr_parsed = curr_parsed._replace(netloc=f"{curr_parsed.hostname}:443")

        updated = False
        new_params = []
        for key, value in q_params:
            if key in ("connectURL", "discoveryURL") and value.startswith(("http://", "https://")):
                val_parsed = urlparse(value)
                if val_parsed.hostname != curr_parsed.hostname:
                    new_val_parts = val_parsed._replace(
                        scheme=curr_parsed.scheme,
                        netloc=curr_parsed.netloc
                    )
                    new_value = urlunparse(new_val_parts)
                    new_params.append((key, new_value))
                    updated = True
                else:
                    if val_parsed.scheme == "https" and not val_parsed.port:
                        val_parsed = val_parsed._replace(netloc=f"{val_parsed.hostname}:443")
                        new_params.append((key, urlunparse(val_parsed)))
                        updated = True
                    else:
                        new_params.append((key, value))
            else:
                new_params.append((key, value))

        if updated:
            new_query = urlencode(new_params)
            new_path = parsed._replace(query=new_query)
            redirect_target = urlunparse(new_path)
            print(f"Redirecting outdated query params from {self.path} to {redirect_target}")
            self.send_response(302)
            self.send_header("Location", redirect_target)
            self._send_cors_headers()
            self.end_headers()
            return True

        return False

    def _handle_request(self, method: str) -> None:
        request_host = self._current_request_host()
        if request_host:
            set_request_public_base_url_from_host(request_host)
        if method == "GET" and self._maybe_redirect_outdated_query():
            return
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
        if path in ("", "/") and method == "GET":
            self._serve_root()
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
        self.send_header(
            "Access-Control-Allow-Methods",
            "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        )
        self.send_header("Access-Control-Allow-Headers", CORS_ALLOW_HEADERS)
        self.send_header("Vary", "Origin")

    def _serve_cors_preflight(self) -> None:
        requested_headers = self.headers.get(
            "Access-Control-Request-Headers", CORS_ALLOW_HEADERS
        )
        self.send_response(204)
        origin = self._cors_origin()
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Credentials", "true")
        self.send_header(
            "Access-Control-Allow-Methods",
            "GET, POST, PUT, DELETE, PATCH, OPTIONS",
        )
        self.send_header("Access-Control-Allow-Headers", requested_headers)
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Vary", "Origin")
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
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _serve_root(self) -> None:
        accept = self.headers.get("Accept", "")
        if "application/json" in accept:
            self._serve_discovery_json()
            return

        if is_neo4j_http_up():
            browser_path = build_proxied_browser_path()
            self.send_response(302)
            self.send_header("Location", browser_path)
            self._send_cors_headers()
            self.end_headers()
            return

        info = get_connection_info()
        browser_path = info.get("proxied_browser_path")
        if info.get("status") == "running" and browser_path:
            self.send_response(302)
            self.send_header("Location", browser_path)
            self._send_cors_headers()
            self.end_headers()
            return
        self._serve_status_page()

    def _send_proxy_response(
        self,
        status: int,
        headers,
        body_bytes: bytes,
    ) -> None:
        self.send_response(status)
        for header, value in headers.items():
            header_lower = header.lower()
            if header_lower in STRIPPED_RESPONSE_HEADERS:
                continue
            if header_lower == "location":
                value = rewrite_proxy_location_header(value)
            self.send_header(header, value)
        self._send_cors_headers()
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def _proxy_unavailable_response(self, reason: str) -> None:
        if self.command == "GET":
            path = urlparse(self.path).path
            if path.startswith("/browser") or path in ("", "/"):
                self._serve_status_page()
                return
        self.send_error(503, f"Neo4j Browser is not ready yet. {reason}")

    def _proxy_request(self, method: str) -> None:
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else None

        for attempt in range(3):
            internal_browser = get_internal_browser_url()
            if not internal_browser:
                reason = get_proxy_unavailable_reason()
                self._proxy_unavailable_response(reason)
                return

            proxy_paths = browser_asset_proxy_paths(self.path)
            last_http_error: urllib.error.HTTPError | None = None
            last_connection_error: Exception | None = None

            for proxy_index, proxy_path in enumerate(proxy_paths):
                target_url = urljoin(
                    f"{internal_browser.rstrip('/')}/",
                    proxy_path.lstrip("/"),
                )
                request = prepare_neo4j_http_request(
                    target_url, data=body, method=method
                )
                forwarded_host = None
                forwarded_proto = None
                for header, value in self.headers.items():
                    header_lower = header.lower()
                    if (
                        header_lower in HOP_BY_HOP_HEADERS
                        or header_lower == "host"
                        or header_lower == "accept-encoding"
                    ):
                        continue
                    if header_lower in FORWARDED_HEADERS:
                        if header_lower == "x-forwarded-host":
                            forwarded_host = value
                        if header_lower == "x-forwarded-proto":
                            forwarded_proto = value
                        continue
                    request.add_header(header, value)

                request.add_header("Accept-Encoding", "identity")

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
                request.add_header("X-Forwarded-Port", "443")

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
                            else:
                                print(
                                    "WARNING: Failed to rewrite discovery payload "
                                    "because get_cml_proxy_discovery_json() returned None."
                                )

                        body_bytes = rewrite_proxy_response_body(
                            body_bytes, content_type
                        )
                        self._send_proxy_response(
                            response.status, response.headers, body_bytes
                        )
                    return
                except urllib.error.HTTPError as exc:
                    if exc.code == 404 and proxy_index + 1 < len(proxy_paths):
                        print(
                            f"Browser asset not found at {proxy_path}; "
                            f"retrying alternate Neo4j path."
                        )
                        last_http_error = exc
                        continue
                    error_body = exc.read()
                    error_content_type = exc.headers.get("Content-Type", "")
                    error_body = rewrite_proxy_response_body(
                        error_body, error_content_type
                    )
                    self._send_proxy_response(exc.code, exc.headers, error_body)
                    return
                except (urllib.error.URLError, OSError, ConnectionResetError) as exc:
                    last_connection_error = exc
                    if proxy_index + 1 < len(proxy_paths):
                        print(
                            f"Proxy connection failed for {method} {proxy_path}: "
                            f"{exc}. Trying alternate Neo4j path."
                        )
                        continue
                    print(
                        f"Proxy connection failed for {method} {self.path}: "
                        f"{exc}."
                    )
                    if attempt < 2:
                        invalidate_neo4j_http_cache()
                        break
                    self._proxy_unavailable_response(
                        f"Connection failed: {exc}. Please wait and retry."
                    )
                    return
                except Exception as exc:
                    if attempt == 0:
                        print(
                            f"Proxy error for {method} {self.path}: {exc}. "
                            "Refreshing Neo4j HTTP route and retrying."
                        )
                        invalidate_neo4j_http_cache()
                        break
                    print(f"Proxy error for {method} {self.path}: {exc}")
                    self.send_error(
                        502, f"Failed to proxy Neo4j Browser request: {exc}"
                    )
                    return

            if last_http_error is not None:
                error_body = last_http_error.read()
                error_content_type = last_http_error.headers.get("Content-Type", "")
                error_body = rewrite_proxy_response_body(
                    error_body, error_content_type
                )
                self._send_proxy_response(
                    last_http_error.code, last_http_error.headers, error_body
                )
                return
            if last_connection_error is not None and attempt < 2:
                invalidate_neo4j_http_cache()
                continue
            if last_connection_error is not None:
                self._proxy_unavailable_response(
                    f"Connection failed: {last_connection_error}. "
                    "Please wait and retry."
                )
                return
            if attempt < 2:
                invalidate_neo4j_http_cache()
                continue
            return

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
