import html
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils.neo4j_utils import get_connection_info, run_neo4j_supervisor


def _render_status_page(info: dict) -> str:
    status = html.escape(info.get("status", "starting"))
    rows = [
        ("Status", status),
        ("Username", info.get("username")),
        ("Password", info.get("password")),
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
        if label == "External Browser" and value.startswith("http"):
            cell = f'<a href="{html.escape(value)}" target="_blank" rel="noopener noreferrer">{html.escape(value)}</a>'
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
  <table>
    {''.join(table_rows)}
  </table>
</body>
</html>"""


class Neo4jLauncherHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        page = _render_status_page(get_connection_info())
        body = page.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    port = int(os.getenv("CDSW_APP_PORT") or "8090")
    threading.Thread(target=run_neo4j_supervisor, daemon=True).start()
    print(f"Starting Neo4j Launcher status page on 127.0.0.1:{port}")
    HTTPServer(("127.0.0.1", port), Neo4jLauncherHandler).serve_forever()
