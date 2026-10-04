"""
alert_sink.py
Seat Integrity Guard

A tiny webhook receiver that stands in for Slack / Teams / PagerDuty in the demo. Grafana's alerting posts here
(contact point `seat-guard-webhook`) and every notification is printed, so you can see exactly what would have
been sent:    docker compose logs -f alert-sink

Replace the contact point's URL in grafana/provisioning/alerting/contactpoints.yaml to send to a real service.
"""

import json
import os
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.getenv("PORT", "5001"))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        stamp = datetime.now().strftime("%H:%M:%S")
        try:
            payload = json.loads(body)
        except ValueError:
            print(f"[{stamp}] non-JSON notification: {body[:200]!r}", flush=True)
            payload = None
        if payload:
            for alert in payload.get("alerts", []):
                labels, notes = alert.get("labels", {}), alert.get("annotations", {})
                print(f"[{stamp}] {alert.get('status', '?').upper():<9} [{labels.get('severity', '?')}] "
                      f"{labels.get('alertname', '?')}: {notes.get('summary', '')}", flush=True)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):          # silence the default per-request access log
        pass


print(f"Alert sink listening on :{PORT} (Grafana webhook notifications are printed here)", flush=True)
HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
