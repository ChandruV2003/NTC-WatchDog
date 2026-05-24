import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ntc_watchdog_app import (
    EmailAlertConfig,
    ServerHealthResult,
    _load_state,
    _save_state,
    check_client_routes,
    check_server_health,
    evaluate_hosts,
    maybe_remediate_server,
    maybe_send_email_alerts,
    record_audio_level_monitoring,
)
from ntc_store import NTCStore


class _ProbeHandler(BaseHTTPRequestHandler):
    status_active = True
    status_ingesting = False
    status_desired = False
    empty_playlist = False
    secure_redirect = False
    prefixed_segment = False
    playlist_requests = []

    def log_message(self, format, *args):
        return

    def do_GET(self):
        if self.path.startswith("/p/7070"):
            if self.secure_redirect:
                self.send_response(302)
                self.send_header("Location", "/live")
                self.send_header("Set-Cookie", "session=watchdog-session; Secure; HttpOnly; Path=/")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b'<audio src="/listen/live.m3u8?client=watchdog"></audio>')
            return
        if self.path.startswith("/live"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            if self.secure_redirect and "session=watchdog-session" not in (self.headers.get("Cookie") or ""):
                self.wfile.write(b"<html>Join that room with a PIN first.</html>")
            else:
                self.wfile.write(b'<audio src="/listen/live.m3u8?client=watchdog"></audio>')
            return
        if self.path.startswith("/api/live/status"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            active = "true" if self.status_active else "false"
            ingesting = "true" if self.status_ingesting else "false"
            desired = "true" if self.status_desired else "false"
            payload = (
                '{"slug": "room-b", "host_slug": "hp-pavilion-14m-ba1xx", '
                '"room_alias": "Room B", "broadcasting": %s, "is_ingesting": %s, '
                '"desired_active": %s, "listener_count": 1, "current_device": "CQ 1&2", '
                '"stream_transport": "hls", "connection_quality_percent": 100, '
                '"connection_quality_label": "Buffered", "signal_level_db": -24.5, '
                '"signal_peak_db": -6.5, "signal_level_percent": 72, "signal_peak_percent": 91}'
            ) % (active, ingesting, desired)
            self.wfile.write(payload.encode("utf-8"))
            return
        if self.path.startswith("/listen/live.m3u8"):
            self.playlist_requests.append(
                {
                    "path": self.path,
                    "watchdog_header": self.headers.get("X-NTC-Watchdog-Probe"),
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.end_headers()
            if self.empty_playlist:
                self.wfile.write(b"#EXTM3U\n#EXT-X-VERSION:3\n")
            elif self.prefixed_segment:
                self.wfile.write(b"#EXTM3U\n/webcall/listen/hls/room-b/seg.ts?client=watchdog\n")
            else:
                self.wfile.write(b"#EXTM3U\n/listen/hls/room-b/seg.ts?client=watchdog\n")
            return
        if self.path.startswith("/listen/hls/room-b/seg.ts"):
            self.send_response(200)
            self.send_header("Content-Type", "video/mp2t")
            self.end_headers()
            self.wfile.write(b"x" * 1024)
            return
        self.send_response(404)
        self.end_headers()


class _ProbeServer:
    def __init__(
        self,
        *,
        empty_playlist=False,
        secure_redirect=False,
        status_active=True,
        status_ingesting=False,
        status_desired=False,
    ):
        handler = type(
            "ProbeHandler",
            (_ProbeHandler,),
            {
                "empty_playlist": empty_playlist,
                "secure_redirect": secure_redirect,
                "prefixed_segment": False,
                "status_active": status_active,
                "status_ingesting": status_ingesting,
                "status_desired": status_desired,
                "playlist_requests": [],
            },
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class NTCWatchdogTests(unittest.TestCase):
    def test_evaluate_hosts_warns_when_active_room_uses_fallback_device(self):
        class FakeStore:
            def list_hosts(self):
                return [
                    {
                        "slug": "hp-envy-16-ad0xx",
                        "label": "HP Envy 16-ad0xx",
                        "room_slug": "room-a",
                        "desired_active": True,
                        "preferred_audio_pattern": "SQ",
                        "device_order": [],
                        "runtime": {
                            "last_seen_at": "2099-01-01T00:00:00+00:00",
                            "is_ingesting": True,
                            "current_device": "Microphone Array (AMD Audio Device)",
                            "last_error": "",
                        },
                    }
                ]

        issues = evaluate_hosts(FakeStore(), heartbeat_stale_seconds=45, startup_grace_seconds=25)

        self.assertEqual([issue.code for issue in issues], ["preferred-device-missing"])

    def test_save_state_supports_concurrent_writers(self):
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = str(Path(tempdir) / "watchdog-state.json")
            errors = []

            def write_state(index: int):
                try:
                    _save_state(state_path, {"index": index})
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=write_state, args=(index,)) for index in range(12)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)

            self.assertEqual(errors, [])
            self.assertIn("index", _load_state(state_path))
            self.assertEqual(list(Path(tempdir).glob("*.tmp")), [])

    def test_check_server_health_reports_connection_failure(self):
        result = check_server_health("http://127.0.0.1:1/healthz", timeout_seconds=0.5)
        self.assertFalse(result.ok)
        self.assertIsNone(result.status_code)

    def test_maybe_remediate_server_restarts_once_per_cooldown(self):
        calls = []

        def fake_restart(socket_path: str, container_name: str):
            calls.append((socket_path, container_name))

        with tempfile.TemporaryDirectory() as tempdir:
            state_path = str(Path(tempdir) / "watchdog-state.json")
            first = maybe_remediate_server(
                enabled=True,
                state_path=state_path,
                cooldown_seconds=180,
                docker_socket_path="/tmp/docker.sock",
                container_name="ntc-webcall",
                reason="health failed",
                now=1000.0,
                restart_container=fake_restart,
            )
            self.assertTrue(first["attempted"])
            self.assertEqual(first["status"], "restarted")
            self.assertEqual(calls, [("/tmp/docker.sock", "ntc-webcall")])

            second = maybe_remediate_server(
                enabled=True,
                state_path=state_path,
                cooldown_seconds=180,
                docker_socket_path="/tmp/docker.sock",
                container_name="ntc-webcall",
                reason="health failed again",
                now=1060.0,
                restart_container=fake_restart,
            )
            self.assertFalse(second["attempted"])
            self.assertEqual(second["status"], "cooldown")
            self.assertEqual(len(calls), 1)

    def test_maybe_remediate_server_skips_when_disabled(self):
        calls = []

        def fake_restart(socket_path: str, container_name: str):
            calls.append((socket_path, container_name))

        with tempfile.TemporaryDirectory() as tempdir:
            state_path = str(Path(tempdir) / "watchdog-state.json")
            result = maybe_remediate_server(
                enabled=False,
                state_path=state_path,
                cooldown_seconds=180,
                docker_socket_path="/tmp/docker.sock",
                container_name="ntc-webcall",
                reason="health failed",
                now=1000.0,
                restart_container=fake_restart,
            )
            self.assertFalse(result["attempted"])
            self.assertEqual(result["status"], "disabled")
            self.assertEqual(calls, [])

    def test_check_client_routes_exercises_public_hls_path(self):
        with _ProbeServer() as server:
            results = check_client_routes(server.base_url, public_pin="7070", timeout_seconds=1, hls_timeout_seconds=1)

        self.assertTrue(all(result.ok for result in results))
        self.assertEqual([result.name for result in results], ["public-page", "live-status", "hls-playlist", "hls-segment"])
        playlist_requests = server.server.RequestHandlerClass.playlist_requests
        self.assertEqual(len(playlist_requests), 1)
        self.assertIn("watchdog=1", playlist_requests[0]["path"])
        self.assertEqual(playlist_requests[0]["watchdog_header"], "1")
        live_status = next(result for result in results if result.name == "live-status")
        self.assertEqual(live_status.details["room_slug"], "room-b")
        self.assertEqual(live_status.details["signal_level_db"], -24.5)

    def test_check_client_routes_defers_hls_when_source_is_not_broadcasting(self):
        with _ProbeServer(status_active=False, status_ingesting=True, status_desired=True) as server:
            results = check_client_routes(server.base_url, public_pin="7070", timeout_seconds=1, hls_timeout_seconds=1)

        self.assertTrue(all(result.ok for result in results))
        self.assertEqual([result.name for result in results], ["public-page", "live-status", "hls-playlist"])
        self.assertEqual(server.server.RequestHandlerClass.playlist_requests, [])
        self.assertEqual(results[-1].details["reason"], "source-not-broadcasting")

    def test_record_audio_level_monitoring_persists_live_status_samples(self):
        with tempfile.TemporaryDirectory() as tempdir:
            store = NTCStore(str(Path(tempdir) / "ntccast.db"))
            with _ProbeServer() as server:
                results = check_client_routes(server.base_url, public_pin="7070", timeout_seconds=1, hls_timeout_seconds=1)

            monitor = record_audio_level_monitoring(
                store,
                results,
                low_level_db=-42,
                hot_peak_db=-1,
                window_seconds=300,
                min_samples=1,
            )

            self.assertTrue(monitor["recorded"])
            self.assertEqual(monitor["issues"], [])
            summary = store.audio_level_summary("room-b", window_seconds=300)
            self.assertEqual(summary["sample_count"], 1)
            self.assertEqual(summary["max_signal_level_db"], -24.5)

    def test_check_client_routes_handles_secure_session_cookie_over_internal_http(self):
        with _ProbeServer(secure_redirect=True) as server:
            results = check_client_routes(server.base_url, public_pin="7070", timeout_seconds=1, hls_timeout_seconds=1)

        self.assertTrue(all(result.ok for result in results))
        self.assertEqual(results[0].name, "public-page")
        self.assertTrue(results[0].url.endswith("/live"))

    def test_check_client_routes_strips_public_prefix_for_internal_segment_probe(self):
        with _ProbeServer() as server:
            server.server.RequestHandlerClass.prefixed_segment = True
            results = check_client_routes(server.base_url, public_pin="7070", timeout_seconds=1, hls_timeout_seconds=1)

        self.assertTrue(all(result.ok for result in results))
        segment_probe = next(result for result in results if result.name == "hls-segment")
        self.assertIn("/listen/hls/room-b/seg.ts", segment_probe.url)
        self.assertNotIn("/webcall/listen/hls", segment_probe.url)

    def test_check_client_routes_fails_active_hls_without_segments(self):
        with _ProbeServer(empty_playlist=True) as server:
            results = check_client_routes(server.base_url, public_pin="7070", timeout_seconds=1, hls_timeout_seconds=1)

        self.assertFalse(results[-1].ok)
        self.assertEqual(results[-1].name, "hls-playlist")
        self.assertIn("did not contain", results[-1].message)

    def test_maybe_send_email_alerts_deduplicates_until_cooldown(self):
        sent = []

        def fake_send(config, *, subject, text_body, html_body):
            sent.append((subject, text_body, html_body))

        config = EmailAlertConfig(
            enabled=True,
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_username="",
            smtp_password="",
            smtp_starttls=True,
            mail_from="ntc@example.com",
            mail_to=["ops@example.com"],
            subject_prefix="[NTC]",
            cooldown_seconds=900,
            send_resolved=True,
        )
        alert = {
            "severity": "critical",
            "category": "client-route",
            "code": "hls-playlist",
            "message": "HLS playlist timed out",
            "url": "http://ntc-webcall/listen/live.m3u8",
            "details": {"elapsed_ms": 12000},
        }

        with tempfile.TemporaryDirectory() as tempdir:
            state_path = str(Path(tempdir) / "watchdog-state.json")
            first = maybe_send_email_alerts(
                config=config,
                state_path=state_path,
                alerts=[alert],
                remediation={"status": "restarted"},
                now=1000,
                send_email=fake_send,
            )
            second = maybe_send_email_alerts(
                config=config,
                state_path=state_path,
                alerts=[alert],
                remediation={"status": "cooldown"},
                now=1060,
                send_email=fake_send,
            )
            third = maybe_send_email_alerts(
                config=config,
                state_path=state_path,
                alerts=[alert],
                remediation={"status": "cooldown"},
                now=2000,
                send_email=fake_send,
            )

        self.assertEqual(first["status"], "sent")
        self.assertEqual(second["status"], "cooldown")
        self.assertEqual(third["status"], "sent")
        self.assertEqual(len(sent), 2)


if __name__ == "__main__":
    unittest.main()
