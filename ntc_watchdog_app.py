"""Deterministic health watchdog for NTC hosts and rooms."""

from __future__ import annotations

import argparse
import html
import json
import os
import socket
import smtplib
import tempfile
import time
from email.message import EmailMessage
from http.cookiejar import CookieJar
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

from ntc_env import install_legacy_env_aliases
from ntc_store import NTCStore

install_legacy_env_aliases()


def _parse_iso8601(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@dataclass
class WatchdogIssue:
    severity: str
    host_slug: str
    room_slug: str
    code: str
    message: str


@dataclass
class ServerHealthResult:
    ok: bool
    status_code: int | None
    message: str


@dataclass
class EndpointProbeResult:
    ok: bool
    name: str
    url: str
    status_code: int | None
    elapsed_ms: int
    message: str
    details: dict


@dataclass
class EmailAlertConfig:
    enabled: bool
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_starttls: bool
    mail_from: str
    mail_to: list[str]
    subject_prefix: str
    cooldown_seconds: float
    send_resolved: bool


def _load_state(path: str):
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(path: str, state):
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=state_path.parent,
            prefix=f".{state_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            handle.write(json.dumps(state, sort_keys=True, indent=2))
        temp_path.replace(state_path)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _float_or_none(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def check_server_health(health_url: str, *, timeout_seconds: float = 3.0):
    try:
        with urlopen(health_url, timeout=max(0.5, float(timeout_seconds))) as response:
            status_code = getattr(response, "status", None) or response.getcode()
            body = response.read()
    except URLError as exc:
        return ServerHealthResult(ok=False, status_code=None, message=str(exc.reason or exc))
    except Exception as exc:
        return ServerHealthResult(ok=False, status_code=None, message=str(exc))

    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except Exception:
        payload = {}

    ok = 200 <= int(status_code or 0) < 300 and bool(payload.get("ok"))
    message = "healthy" if ok else (payload.get("error") or f"unexpected health response {status_code}")
    return ServerHealthResult(ok=ok, status_code=int(status_code or 0), message=str(message))


def _request(opener, url: str, *, timeout_seconds: float, accept: str = "application/json", headers: dict | None = None):
    request_headers = {
        "Accept": accept,
        "User-Agent": "NTCWatchdog/1.0",
    }
    request_headers.update(headers or {})
    request = Request(
        url,
        headers=request_headers,
    )
    start = time.monotonic()
    try:
        response = opener.open(request, timeout=max(0.5, float(timeout_seconds)))
        body = response.read()
        status_code = getattr(response, "status", None) or response.getcode()
        return int(status_code or 0), body, int((time.monotonic() - start) * 1000), str(response.geturl())
    except HTTPError as exc:
        body = exc.read()
        return int(exc.code), body, int((time.monotonic() - start) * 1000), str(exc.geturl())


def _probe_url(base_url: str, path_or_url: str) -> str:
    """Build a probe URL that works through both public and internal mounts.

    Public playlists are generated behind /webcall, but the watchdog probes the
    container directly at /. Without this normalization, a healthy active stream
    can look broken internally and trigger an unnecessary restart.
    """

    base = base_url.rstrip("/") + "/"
    resolved = urljoin(base, path_or_url)
    public_prefix = os.getenv("NTC_WATCHDOG_PUBLIC_PATH_PREFIX", "/webcall").strip().rstrip("/")
    if not public_prefix:
        return resolved

    parsed_base = urlparse(base)
    parsed_resolved = urlparse(resolved)
    base_path = parsed_base.path.rstrip("/")
    if (
        parsed_resolved.netloc == parsed_base.netloc
        and base_path != public_prefix
        and parsed_resolved.path.startswith(public_prefix + "/")
    ):
        stripped_path = parsed_resolved.path[len(public_prefix):] or "/"
        parsed_resolved = parsed_resolved._replace(path=stripped_path)
        return parsed_resolved.geturl()
    return resolved


def _cookie_header(jar: CookieJar) -> str:
    return "; ".join(f"{cookie.name}={cookie.value}" for cookie in jar)


def _probe_failure(name: str, url: str, status_code: int | None, elapsed_ms: int, message: str, details: dict | None = None):
    return EndpointProbeResult(
        ok=False,
        name=name,
        url=url,
        status_code=status_code,
        elapsed_ms=elapsed_ms,
        message=message,
        details=details or {},
    )


def check_client_routes(
    base_url: str,
    *,
    public_pin: str,
    timeout_seconds: float = 4.0,
    hls_timeout_seconds: float = 12.0,
):
    """Exercise the real public client path that users hit.

    /healthz can be fine while the public/HLS route is wedged, so this keeps
    the failure mode that just happened from hiding behind a green health check.
    """

    base = base_url.rstrip("/") + "/"
    jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    results: list[EndpointProbeResult] = []
    auth_headers: dict[str, str] = {}

    def add_ok(name: str, url: str, status_code: int, elapsed_ms: int, message: str, details: dict | None = None):
        results.append(EndpointProbeResult(True, name, url, status_code, elapsed_ms, message, details or {}))

    try:
        join_url = urljoin(base, f"p/{quote(str(public_pin), safe='')}")
        status_code, body, elapsed_ms, final_url = _request(
            opener,
            join_url,
            timeout_seconds=timeout_seconds,
            accept="text/html,application/xhtml+xml",
        )
        if status_code != 200:
            results.append(
                _probe_failure(
                    "public-page",
                    join_url,
                    status_code,
                    elapsed_ms,
                    f"public page returned HTTP {status_code}",
                    {"final_url": final_url},
                )
            )
            return results

        html_text = body.decode("utf-8", errors="replace")
        if "/listen/live.m3u8" not in html_text and "/listen/live.wav" not in html_text:
            # The production session cookie is Secure. Internal watchdog probes
            # use plain HTTP to the Docker service name, so CookieJar stores the
            # cookie but refuses to resend it. Send it explicitly for this
            # internal-only probe so we still exercise the real listener page.
            manual_cookie = _cookie_header(jar)
            if manual_cookie:
                live_url = urljoin(base, "live")
                status_code, body, elapsed_ms, final_url = _request(
                    opener,
                    live_url,
                    timeout_seconds=timeout_seconds,
                    accept="text/html,application/xhtml+xml",
                    headers={"Cookie": manual_cookie},
                )
                html_text = body.decode("utf-8", errors="replace")
                auth_headers = {"Cookie": manual_cookie}
                if status_code == 200 and ("/listen/live.m3u8" in html_text or "/listen/live.wav" in html_text):
                    add_ok("public-page", live_url, status_code, elapsed_ms, "public page loaded", {"final_url": final_url})
                else:
                    results.append(
                        _probe_failure(
                            "public-page",
                            live_url,
                            status_code,
                            elapsed_ms,
                            "public page loaded but did not include a stream URL",
                            {"final_url": final_url},
                        )
                    )
                    return results
            else:
                results.append(
                    _probe_failure(
                        "public-page",
                        join_url,
                        status_code,
                        elapsed_ms,
                        "public page loaded but did not include a stream URL",
                        {"final_url": final_url},
                    )
                )
                return results
        else:
            add_ok("public-page", join_url, status_code, elapsed_ms, "public page loaded", {"final_url": final_url})

        if not auth_headers:
            manual_cookie = _cookie_header(jar)
            if manual_cookie:
                auth_headers = {"Cookie": manual_cookie}

        status_url = urljoin(base, "api/live/status")
        status_code, body, elapsed_ms, final_url = _request(
            opener,
            status_url,
            timeout_seconds=timeout_seconds,
            accept="application/json",
            headers=auth_headers,
        )
        if status_code != 200:
            results.append(
                _probe_failure(
                    "live-status",
                    status_url,
                    status_code,
                    elapsed_ms,
                    f"live status returned HTTP {status_code}",
                )
            )
            return results
        try:
            live_status = json.loads(body.decode("utf-8") or "{}")
        except Exception as exc:
            results.append(_probe_failure("live-status", status_url, status_code, elapsed_ms, f"invalid live status JSON: {exc}"))
            return results
        add_ok(
            "live-status",
            status_url,
            status_code,
            elapsed_ms,
            "live status loaded",
            {
                "room_slug": live_status.get("slug") or live_status.get("room_slug"),
                "host_slug": live_status.get("host_slug"),
                "room_alias": live_status.get("room_alias") or live_status.get("label"),
                "broadcasting": bool(live_status.get("broadcasting")),
                "is_ingesting": bool(live_status.get("is_ingesting")),
                "desired_active": bool(live_status.get("desired_active")),
                "listener_count": live_status.get("listener_count"),
                "current_device": live_status.get("current_device"),
                "stream_transport": live_status.get("stream_transport"),
                "connection_quality_percent": live_status.get("connection_quality_percent"),
                "connection_quality_label": live_status.get("connection_quality_label"),
                "signal_level_db": live_status.get("signal_level_db"),
                "signal_peak_db": live_status.get("signal_peak_db"),
                "signal_level_percent": live_status.get("signal_level_percent"),
                "signal_peak_percent": live_status.get("signal_peak_percent"),
            },
        )

        active = bool(
            live_status.get("broadcasting")
            or live_status.get("is_ingesting")
            or live_status.get("desired_active")
        )
        if not active:
            return results
        if not live_status.get("broadcasting"):
            add_ok(
                "hls-playlist",
                status_url,
                status_code,
                0,
                "HLS probe skipped until source audio is broadcasting",
                {
                    "reason": "source-not-broadcasting",
                    "is_ingesting": bool(live_status.get("is_ingesting")),
                    "desired_active": bool(live_status.get("desired_active")),
                    "current_device": live_status.get("current_device"),
                    "connection_quality_label": live_status.get("connection_quality_label"),
                },
            )
            return results

        hls_match = None
        marker = "/listen/live.m3u8?client="
        marker_index = html_text.find(marker)
        if marker_index >= 0:
            end_index = marker_index
            while end_index < len(html_text) and html_text[end_index] not in {'"', "'", "<", "&"}:
                end_index += 1
            hls_match = html_text[marker_index:end_index]
        if not hls_match:
            results.append(
                _probe_failure(
                    "hls-playlist",
                    urljoin(base, "listen/live.m3u8"),
                    None,
                    0,
                    "active public page did not expose an HLS playlist URL",
                )
            )
            return results

        playlist_url = _probe_url(base, hls_match)
        if "watchdog=1" not in playlist_url:
            playlist_url = f"{playlist_url}{'&' if '?' in playlist_url else '?'}watchdog=1"
        status_code, body, elapsed_ms, final_url = _request(
            opener,
            playlist_url,
            timeout_seconds=hls_timeout_seconds,
            accept="application/vnd.apple.mpegurl,*/*",
            headers={**auth_headers, "X-NTC-Watchdog-Probe": "1"},
        )
        if status_code != 200:
            results.append(
                _probe_failure(
                    "hls-playlist",
                    playlist_url,
                    status_code,
                    elapsed_ms,
                    f"HLS playlist returned HTTP {status_code} while a meeting is active",
                )
            )
            return results

        playlist_text = body.decode("utf-8", errors="replace")
        segment_path = next(
            (line.strip() for line in playlist_text.splitlines() if line.strip() and not line.startswith("#")),
            "",
        )
        if not segment_path:
            results.append(
                _probe_failure(
                    "hls-playlist",
                    playlist_url,
                    status_code,
                    elapsed_ms,
                    "HLS playlist loaded but did not contain any audio segments",
                )
            )
            return results
        add_ok("hls-playlist", playlist_url, status_code, elapsed_ms, "HLS playlist loaded", {"segment": segment_path})

        segment_url = _probe_url(base, segment_path)
        status_code, body, elapsed_ms, final_url = _request(
            opener,
            segment_url,
            timeout_seconds=timeout_seconds,
            accept="audio/*,video/mp2t,*/*",
            headers={**auth_headers, "X-NTC-Watchdog-Probe": "1"},
        )
        if status_code != 200 or len(body) < 256:
            results.append(
                _probe_failure(
                    "hls-segment",
                    segment_url,
                    status_code,
                    elapsed_ms,
                    f"HLS segment returned HTTP {status_code} with {len(body)} bytes",
                )
            )
            return results
        add_ok("hls-segment", segment_url, status_code, elapsed_ms, "HLS segment loaded", {"bytes": len(body)})
        return results
    except URLError as exc:
        results.append(_probe_failure("client-routes", base, None, 0, str(exc.reason or exc)))
        return results
    except Exception as exc:
        results.append(_probe_failure("client-routes", base, None, 0, str(exc)))
        return results


def record_audio_level_monitoring(
    store: NTCStore,
    client_probes: list[EndpointProbeResult],
    *,
    low_level_db: float = -42.0,
    hot_peak_db: float = -1.0,
    window_seconds: int = 300,
    min_samples: int = 3,
    retain_days: int = 14,
):
    live_probe = next((probe for probe in client_probes if probe.ok and probe.name == "live-status"), None)
    if not live_probe:
        return {"recorded": False, "issues": [], "reason": "live-status-unavailable"}

    details = live_probe.details or {}
    room_slug = (details.get("room_slug") or "").strip()
    if not room_slug:
        return {"recorded": False, "issues": [], "reason": "room-unavailable"}

    host_slug = (details.get("host_slug") or "").strip() or None
    signal_level_db = _float_or_none(details.get("signal_level_db"))
    signal_peak_db = _float_or_none(details.get("signal_peak_db"))
    signal_level_percent = _float_or_none(details.get("signal_level_percent"))
    signal_peak_percent = _float_or_none(details.get("signal_peak_percent"))
    active = bool(details.get("broadcasting") or details.get("is_ingesting") or details.get("desired_active"))

    sample_id = store.record_audio_level_sample(
        room_slug,
        host_slug=host_slug,
        source="watchdog",
        signal_level_db=signal_level_db,
        signal_peak_db=signal_peak_db,
        signal_level_percent=signal_level_percent,
        signal_peak_percent=signal_peak_percent,
        listener_count=int(details.get("listener_count") or 0),
        broadcasting=bool(details.get("broadcasting")),
        is_ingesting=bool(details.get("is_ingesting")),
        desired_active=bool(details.get("desired_active")),
        current_device=str(details.get("current_device") or ""),
        stream_transport=str(details.get("stream_transport") or ""),
        connection_quality_percent=_float_or_none(details.get("connection_quality_percent")),
        connection_quality_label=str(details.get("connection_quality_label") or ""),
    )
    store.prune_audio_level_samples(retain_days=retain_days)

    summary = store.audio_level_summary(room_slug, window_seconds=window_seconds)
    sample_count = int(summary.get("sample_count") or 0)
    max_signal_level_db = _float_or_none(summary.get("max_signal_level_db"))
    max_signal_peak_db = _float_or_none(summary.get("max_signal_peak_db"))
    room_label = details.get("room_alias") or room_slug
    issues: list[WatchdogIssue] = []

    if active and sample_count >= max(1, int(min_samples)) and max_signal_level_db is not None and max_signal_level_db < low_level_db:
        issues.append(
            WatchdogIssue(
                severity="warn",
                host_slug=host_slug or "",
                room_slug=room_slug,
                code="low-program-level",
                message=(
                    f"{room_label} program audio stayed below {low_level_db:.1f} dBFS "
                    f"for {sample_count} watchdog samples."
                ),
            )
        )

    if active and sample_count >= max(1, int(min_samples)) and max_signal_peak_db is not None and max_signal_peak_db > hot_peak_db:
        issues.append(
            WatchdogIssue(
                severity="warn",
                host_slug=host_slug or "",
                room_slug=room_slug,
                code="hot-program-peak",
                message=(
                    f"{room_label} program audio peaked above {hot_peak_db:.1f} dBFS "
                    f"in the recent watchdog window."
                ),
            )
        )

    return {
        "recorded": True,
        "sample_id": sample_id,
        "room_slug": room_slug,
        "host_slug": host_slug,
        "signal_level_db": signal_level_db,
        "signal_peak_db": signal_peak_db,
        "summary": summary,
        "issues": issues,
    }


def _decode_chunked_body(body: bytes) -> bytes:
    decoded = bytearray()
    cursor = 0
    while cursor < len(body):
        line_end = body.find(b"\r\n", cursor)
        if line_end < 0:
            break
        size_text = body[cursor:line_end].split(b";", 1)[0].strip()
        try:
            chunk_size = int(size_text, 16)
        except ValueError:
            return body
        cursor = line_end + 2
        if chunk_size == 0:
            break
        decoded.extend(body[cursor:cursor + chunk_size])
        cursor += chunk_size + 2
    return bytes(decoded)


def _docker_get_json(docker_socket_path: str, request_path: str, *, timeout_seconds: float = 5.0):
    request = (
        f"GET {request_path} HTTP/1.1\r\n"
        "Host: docker\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(max(1.0, float(timeout_seconds)))
        client.connect(docker_socket_path)
        client.sendall(request)
        response = bytearray()
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            response.extend(chunk)

    header_bytes, separator, body = bytes(response).partition(b"\r\n\r\n")
    if not separator:
        raise RuntimeError("Docker response did not include HTTP headers")
    header_text = header_bytes.decode("iso-8859-1", errors="replace")
    status_line = header_text.splitlines()[0] if header_text else ""
    try:
        status_code = int(status_line.split(" ", 2)[1])
    except Exception as exc:
        raise RuntimeError(f"Unparseable Docker response: {status_line}") from exc
    if status_code < 200 or status_code >= 300:
        raise RuntimeError(f"Docker API request failed: {status_line}")
    if "transfer-encoding: chunked" in header_text.casefold():
        body = _decode_chunked_body(body)
    return json.loads(body.decode("utf-8"))


def _int_value(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _docker_cpu_percent(stats: dict) -> float | None:
    cpu_stats = stats.get("cpu_stats") or {}
    precpu_stats = stats.get("precpu_stats") or {}
    cpu_usage = cpu_stats.get("cpu_usage") or {}
    precpu_usage = precpu_stats.get("cpu_usage") or {}
    cpu_delta = _int_value(cpu_usage.get("total_usage")) - _int_value(precpu_usage.get("total_usage"))
    system_delta = _int_value(cpu_stats.get("system_cpu_usage")) - _int_value(precpu_stats.get("system_cpu_usage"))
    if cpu_delta <= 0 or system_delta <= 0:
        return None
    online_cpus = _int_value(cpu_stats.get("online_cpus"))
    if online_cpus <= 0:
        online_cpus = len(cpu_usage.get("percpu_usage") or []) or 1
    return (cpu_delta / system_delta) * online_cpus * 100.0


def _docker_memory_usage(memory_stats: dict) -> int:
    usage = _int_value(memory_stats.get("usage"))
    stat_values = memory_stats.get("stats") or {}
    cache = _int_value(stat_values.get("inactive_file"), _int_value(stat_values.get("cache")))
    return max(0, usage - cache)


def _docker_block_io(stats: dict) -> tuple[int, int]:
    read_bytes = 0
    write_bytes = 0
    entries = ((stats.get("blkio_stats") or {}).get("io_service_bytes_recursive") or [])
    for entry in entries:
        op = str(entry.get("op") or "").casefold()
        value = _int_value(entry.get("value"))
        if op == "read":
            read_bytes += value
        elif op == "write":
            write_bytes += value
    return read_bytes, write_bytes


def collect_container_resource_stats(docker_socket_path: str, container_name: str) -> dict:
    stats = _docker_get_json(
        docker_socket_path,
        f"/containers/{quote(container_name, safe='')}/stats?stream=false&one-shot=true",
    )
    memory_stats = stats.get("memory_stats") or {}
    memory_usage = _docker_memory_usage(memory_stats)
    memory_limit = _int_value(memory_stats.get("limit"))
    networks = stats.get("networks") or {}
    network_rx = sum(_int_value(network.get("rx_bytes")) for network in networks.values())
    network_tx = sum(_int_value(network.get("tx_bytes")) for network in networks.values())
    block_read, block_write = _docker_block_io(stats)
    load_1 = load_5 = load_15 = None
    if hasattr(os, "getloadavg"):
        try:
            load_1, load_5, load_15 = os.getloadavg()
        except OSError:
            load_1 = load_5 = load_15 = None
    return {
        "container_name": container_name,
        "cpu_percent": _docker_cpu_percent(stats),
        "memory_usage_bytes": memory_usage,
        "memory_limit_bytes": memory_limit,
        "memory_percent": (memory_usage / memory_limit * 100.0) if memory_limit > 0 else None,
        "network_rx_bytes": network_rx,
        "network_tx_bytes": network_tx,
        "block_read_bytes": block_read,
        "block_write_bytes": block_write,
        "pids": _int_value((stats.get("pids_stats") or {}).get("current"), 0),
        "system_load_1": load_1,
        "system_load_5": load_5,
        "system_load_15": load_15,
    }


def record_resource_monitoring(
    store: NTCStore,
    *,
    docker_socket_path: str,
    container_names: list[str],
    retain_days: int = 14,
    collect_stats=collect_container_resource_stats,
):
    names = [name for name in _split_csv(",".join(container_names)) if name]
    if not names:
        return {"recorded": False, "samples": [], "errors": [], "reason": "no-containers"}

    samples = []
    errors = []
    for container_name in names:
        try:
            stats = collect_stats(docker_socket_path, container_name)
            sample_id = store.record_system_resource_sample(
                container_name,
                source="watchdog",
                cpu_percent=_float_or_none(stats.get("cpu_percent")),
                memory_usage_bytes=_int_value(stats.get("memory_usage_bytes")),
                memory_limit_bytes=_int_value(stats.get("memory_limit_bytes")),
                memory_percent=_float_or_none(stats.get("memory_percent")),
                network_rx_bytes=_int_value(stats.get("network_rx_bytes")),
                network_tx_bytes=_int_value(stats.get("network_tx_bytes")),
                block_read_bytes=_int_value(stats.get("block_read_bytes")),
                block_write_bytes=_int_value(stats.get("block_write_bytes")),
                pids=_int_value(stats.get("pids")),
                system_load_1=_float_or_none(stats.get("system_load_1")),
                system_load_5=_float_or_none(stats.get("system_load_5")),
                system_load_15=_float_or_none(stats.get("system_load_15")),
            )
            samples.append({"sample_id": sample_id, **stats})
        except Exception as exc:
            errors.append({"container_name": container_name, "error": str(exc)})

    store.prune_system_resource_samples(retain_days=retain_days)
    return {"recorded": bool(samples), "samples": samples, "errors": errors}


def _restart_container(docker_socket_path: str, container_name: str, *, timeout_seconds: int = 10):
    request_path = f"/containers/{quote(container_name, safe='')}/restart?t={max(1, int(timeout_seconds))}"
    request = (
        f"POST {request_path} HTTP/1.1\r\n"
        "Host: docker\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(max(2.0, float(timeout_seconds) + 2.0))
        client.connect(docker_socket_path)
        client.sendall(request)
        response = bytearray()
        while True:
            chunk = client.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
    status_line = response.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
    try:
        status_code = int(status_line.split(" ", 2)[1])
    except Exception as exc:
        raise RuntimeError(f"Unparseable Docker response: {status_line}") from exc
    if status_code not in {200, 204, 304}:
        raise RuntimeError(f"Docker restart failed: {status_line}")


def maybe_remediate_server(
    *,
    enabled: bool,
    state_path: str,
    cooldown_seconds: float,
    docker_socket_path: str,
    container_name: str,
    reason: str,
    now: float | None = None,
    restart_container=_restart_container,
):
    current_time = float(time.time() if now is None else now)
    if not enabled:
        return {"attempted": False, "status": "disabled"}

    state = _load_state(state_path)
    last_attempt_at = float(state.get("last_server_restart_at") or 0.0)
    if last_attempt_at and (current_time - last_attempt_at) < max(1.0, float(cooldown_seconds)):
        return {
            "attempted": False,
            "status": "cooldown",
            "remaining_seconds": max(0.0, float(cooldown_seconds) - (current_time - last_attempt_at)),
        }

    restart_container(docker_socket_path, container_name)
    state["last_server_restart_at"] = current_time
    state["last_server_restart_reason"] = reason
    _save_state(state_path, state)
    return {"attempted": True, "status": "restarted"}


def _load_email_alert_config() -> EmailAlertConfig:
    mail_to = _split_csv(os.getenv("NTC_ALERT_EMAIL_TO"))
    smtp_host = os.getenv("NTC_ALERT_SMTP_HOST", "").strip()
    smtp_username = os.getenv("NTC_ALERT_SMTP_USERNAME", "").strip()
    enabled_default = bool(smtp_host and mail_to)
    return EmailAlertConfig(
        enabled=_bool_env("NTC_ALERT_EMAIL_ENABLED", enabled_default),
        smtp_host=smtp_host,
        smtp_port=int(os.getenv("NTC_ALERT_SMTP_PORT", "587")),
        smtp_username=smtp_username,
        smtp_password=os.getenv("NTC_ALERT_SMTP_PASSWORD", ""),
        smtp_starttls=_bool_env("NTC_ALERT_SMTP_STARTTLS", True),
        mail_from=os.getenv("NTC_ALERT_EMAIL_FROM", smtp_username or "ntc-watchdog@localhost").strip(),
        mail_to=mail_to,
        subject_prefix=os.getenv("NTC_ALERT_SUBJECT_PREFIX", "[NTC]").strip() or "[NTC]",
        cooldown_seconds=float(os.getenv("NTC_ALERT_COOLDOWN_SECONDS", "900")),
        send_resolved=_bool_env("NTC_ALERT_RESOLVED_ENABLED", True),
    )


def _issue_key(alert: dict) -> str:
    return "|".join(
        [
            str(alert.get("category", "")),
            str(alert.get("code", "")),
            str(alert.get("room_slug", "")),
            str(alert.get("host_slug", "")),
            str(alert.get("url", "")),
        ]
    )


def _severity_rank(severity: str) -> int:
    return {"critical": 3, "error": 2, "warn": 1, "info": 0}.get((severity or "").lower(), 0)


def _render_alert_email(
    *,
    subject: str,
    title: str,
    intro: str,
    alerts: list[dict],
    remediation: dict,
    hostname: str,
):
    now = datetime.now(timezone.utc).astimezone()
    rows = []
    for alert in alerts:
        details = alert.get("details") or {}
        details_text = "<br>".join(
            f"<strong>{html.escape(str(key))}:</strong> {html.escape(str(value))}" for key, value in sorted(details.items())
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(alert.get('severity', '')))}</td>"
            f"<td>{html.escape(str(alert.get('category', '')))}</td>"
            f"<td>{html.escape(str(alert.get('code', '')))}</td>"
            f"<td>{html.escape(str(alert.get('message', '')))}</td>"
            f"<td>{details_text}</td>"
            "</tr>"
        )

    remediation_html = ""
    if remediation:
        remediation_html = (
            "<p><strong>Remediation:</strong> "
            f"{html.escape(str(remediation.get('status', 'not-needed')))}"
            f"{' on ' + html.escape(str(remediation.get('target'))) if remediation.get('target') else ''}"
            "</p>"
        )

    html_body = f"""\
<!doctype html>
<html>
  <body style="margin:0;background:#f5f7fb;color:#18202f;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <div style="max-width:760px;margin:0 auto;padding:28px;">
      <div style="background:#101827;color:#ffffff;border-radius:18px 18px 0 0;padding:24px 28px;">
        <div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:#a8b3c7;">NTC Watchdog</div>
        <h1 style="margin:8px 0 0;font-size:26px;line-height:1.2;">{html.escape(title)}</h1>
      </div>
      <div style="background:#ffffff;border:1px solid #dce2ee;border-top:0;border-radius:0 0 18px 18px;padding:24px 28px;">
        <p style="font-size:16px;line-height:1.5;margin-top:0;">{html.escape(intro)}</p>
        <p><strong>Host:</strong> {html.escape(hostname)}<br>
           <strong>Time:</strong> {html.escape(now.strftime('%Y-%m-%d %H:%M:%S %Z'))}</p>
        {remediation_html}
        <table style="border-collapse:collapse;width:100%;font-size:14px;">
          <thead>
            <tr>
              <th align="left" style="border-bottom:1px solid #dce2ee;padding:8px;">Severity</th>
              <th align="left" style="border-bottom:1px solid #dce2ee;padding:8px;">Area</th>
              <th align="left" style="border-bottom:1px solid #dce2ee;padding:8px;">Code</th>
              <th align="left" style="border-bottom:1px solid #dce2ee;padding:8px;">Message</th>
              <th align="left" style="border-bottom:1px solid #dce2ee;padding:8px;">Details</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </div>
    </div>
  </body>
</html>
"""
    text_lines = [title, "", intro, "", f"Host: {hostname}", f"Time: {now.isoformat()}"]
    if remediation:
        text_lines.extend(["", f"Remediation: {remediation.get('status', 'not-needed')}"])
    for alert in alerts:
        text_lines.extend(
            [
                "",
                f"{alert.get('severity', '').upper()} {alert.get('category', '')}/{alert.get('code', '')}",
                str(alert.get("message", "")),
                json.dumps(alert.get("details") or {}, sort_keys=True),
            ]
        )
    return subject, "\n".join(text_lines), html_body


def _send_email(config: EmailAlertConfig, *, subject: str, text_body: str, html_body: str):
    if not config.smtp_host:
        raise RuntimeError("NTC_ALERT_SMTP_HOST is not configured")
    if not config.mail_to:
        raise RuntimeError("NTC_ALERT_EMAIL_TO is not configured")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.mail_from
    message["To"] = ", ".join(config.mail_to)
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=15) as smtp:
        if config.smtp_starttls:
            smtp.starttls()
        if config.smtp_username or config.smtp_password:
            smtp.login(config.smtp_username, config.smtp_password)
        smtp.send_message(message)


def maybe_send_email_alerts(
    *,
    config: EmailAlertConfig,
    state_path: str,
    alerts: list[dict],
    remediation: dict,
    hostname: str | None = None,
    now: float | None = None,
    send_email=_send_email,
):
    if not config.enabled:
        return {"attempted": False, "status": "disabled"}
    if not config.smtp_host or not config.mail_to:
        return {"attempted": False, "status": "not-configured"}

    current_time = float(time.time() if now is None else now)
    hostname = hostname or socket.gethostname()
    state = _load_state(state_path)
    alert_state = state.setdefault("email_alerts", {})
    open_keys = {_issue_key(alert) for alert in alerts}
    previous_open = {key for key, value in alert_state.items() if value.get("open")}

    send_active = []
    for alert in alerts:
        key = _issue_key(alert)
        previous = alert_state.get(key, {})
        last_sent_at = float(previous.get("last_sent_at") or 0.0)
        if not previous.get("open") or (current_time - last_sent_at) >= max(1.0, config.cooldown_seconds):
            send_active.append(alert)

    resolved_keys = sorted(previous_open - open_keys)
    if send_active:
        subject = f"{config.subject_prefix} NTC WebCall needs attention"
        _, text_body, html_body = _render_alert_email(
            subject=subject,
            title="NTC WebCall Needs Attention",
            intro="The watchdog found one or more problems that can affect listeners.",
            alerts=send_active,
            remediation=remediation,
            hostname=hostname,
        )
        send_email(config, subject=subject, text_body=text_body, html_body=html_body)
        for alert in alerts:
            key = _issue_key(alert)
            alert_state[key] = {
                "open": True,
                "last_seen_at": current_time,
                "last_message": alert.get("message", ""),
                "last_sent_at": current_time if alert in send_active else alert_state.get(key, {}).get("last_sent_at"),
            }
        for key in resolved_keys:
            alert_state[key]["open"] = False
            alert_state[key]["resolved_at"] = current_time
        _save_state(state_path, state)
        return {"attempted": True, "status": "sent", "sent_count": len(send_active)}

    if resolved_keys and config.send_resolved:
        resolved_alerts = [
            {
                "severity": "info",
                "category": "resolved",
                "code": key,
                "message": alert_state.get(key, {}).get("last_message") or "Issue resolved.",
                "details": {},
            }
            for key in resolved_keys
        ]
        subject = f"{config.subject_prefix} NTC WebCall recovered"
        _, text_body, html_body = _render_alert_email(
            subject=subject,
            title="NTC WebCall Recovered",
            intro="Previously reported watchdog issues are no longer active.",
            alerts=resolved_alerts,
            remediation={},
            hostname=hostname,
        )
        send_email(config, subject=subject, text_body=text_body, html_body=html_body)
        for key in resolved_keys:
            alert_state[key]["open"] = False
            alert_state[key]["resolved_at"] = current_time
            alert_state[key]["resolved_sent_at"] = current_time
        for alert in alerts:
            key = _issue_key(alert)
            alert_state[key] = {
                "open": True,
                "last_seen_at": current_time,
                "last_message": alert.get("message", ""),
                "last_sent_at": alert_state.get(key, {}).get("last_sent_at"),
            }
        _save_state(state_path, state)
        return {"attempted": True, "status": "resolved-sent", "sent_count": len(resolved_alerts)}

    for alert in alerts:
        key = _issue_key(alert)
        existing = alert_state.setdefault(key, {})
        existing.update(
            {
                "open": True,
                "last_seen_at": current_time,
                "last_message": alert.get("message", ""),
            }
        )
    for key in resolved_keys:
        alert_state[key]["open"] = False
        alert_state[key]["resolved_at"] = current_time
    _save_state(state_path, state)
    return {"attempted": False, "status": "cooldown" if alerts else "ok"}


def evaluate_hosts(store: NTCStore, *, heartbeat_stale_seconds: int, startup_grace_seconds: int):
    now = datetime.now(timezone.utc)
    heartbeat_cutoff = now - timedelta(seconds=max(5, heartbeat_stale_seconds))
    startup_cutoff = now - timedelta(seconds=max(5, startup_grace_seconds))
    issues: list[WatchdogIssue] = []

    for host in store.list_hosts():
        runtime = host.get("runtime") or {}
        room_slug = host["room_slug"]
        host_slug = host["slug"]
        last_seen = _parse_iso8601(runtime.get("last_seen_at"))
        online = bool(last_seen and last_seen >= heartbeat_cutoff)
        recently_seen = bool(last_seen and last_seen >= startup_cutoff)
        desired_active = bool(host.get("desired_active"))
        is_ingesting = bool(runtime.get("is_ingesting"))
        current_device = (runtime.get("current_device") or "").strip()
        last_error = (runtime.get("last_error") or "").strip()
        device_order = host.get("device_order") or []
        preferred_pattern = (host.get("preferred_audio_pattern") or "").strip()

        if desired_active and not online:
            issues.append(
                WatchdogIssue(
                    severity="critical",
                    host_slug=host_slug,
                    room_slug=room_slug,
                    code="host-offline",
                    message=f"{host['label']} should be active but has not heartbeated recently.",
                )
            )
            continue

        if desired_active and online and not is_ingesting and not recently_seen:
            issues.append(
                WatchdogIssue(
                    severity="critical",
                    host_slug=host_slug,
                    room_slug=room_slug,
                    code="ingest-down",
                    message=f"{host['label']} should be active but ingest is not running.",
                )
            )

        if desired_active and online and last_error:
            issues.append(
                WatchdogIssue(
                    severity="warn",
                    host_slug=host_slug,
                    room_slug=room_slug,
                    code="runtime-error",
                    message=f"{host['label']} reported: {last_error}",
                )
            )

        if desired_active and online and not current_device:
            issues.append(
                WatchdogIssue(
                    severity="warn",
                    host_slug=host_slug,
                    room_slug=room_slug,
                    code="missing-device",
                    message=f"{host['label']} is active but has no current input device.",
                )
            )

        if desired_active and online and current_device and preferred_pattern:
            if preferred_pattern.casefold() not in current_device.casefold():
                issues.append(
                    WatchdogIssue(
                        severity="warn",
                        host_slug=host_slug,
                        room_slug=room_slug,
                        code="preferred-device-missing",
                        message=(
                            f"{host['label']} is using {current_device}; "
                            f"expected an input matching {preferred_pattern}."
                        ),
                    )
                )

        if online and current_device and device_order and current_device not in device_order:
            issues.append(
                WatchdogIssue(
                    severity="warn",
                    host_slug=host_slug,
                    room_slug=room_slug,
                    code="unexpected-device",
                    message=f"{host['label']} is using {current_device}, which is not in the saved device order.",
                )
            )

    return issues


def main():
    parser = argparse.ArgumentParser(description="NTC watchdog")
    parser.add_argument("--db-path", default=os.getenv("NTC_DB_PATH"), help="Path to the NTC SQLite database")
    parser.add_argument("--heartbeat-stale-seconds", type=int, default=45, help="Seconds before a host is considered stale")
    parser.add_argument("--startup-grace-seconds", type=int, default=25, help="Grace period before treating a desired-active host as failed")
    parser.add_argument("--record-incidents", action="store_true", help="Write detected issues into meeting_incidents")
    parser.add_argument("--base-url", default=os.getenv("NTC_WATCHDOG_BASE_URL", "http://ntc-webcall:1967"), help="Base URL used for public route probes")
    parser.add_argument("--public-pin", default=os.getenv("NTC_WATCHDOG_PUBLIC_PIN", os.getenv("NTC_DEFAULT_PIN", "7070")), help="PIN used for public client probes")
    parser.add_argument("--skip-client-probe", action="store_true", help="Skip public client/HLS route checks")
    parser.add_argument("--client-timeout-seconds", type=float, default=float(os.getenv("NTC_WATCHDOG_CLIENT_TIMEOUT_SECONDS", "4")), help="Timeout for public route probes")
    parser.add_argument("--hls-timeout-seconds", type=float, default=float(os.getenv("NTC_WATCHDOG_HLS_TIMEOUT_SECONDS", "12")), help="Timeout for active HLS playlist probes")
    parser.add_argument("--record-audio-levels", action=argparse.BooleanOptionalAction, default=_bool_env("NTC_LEVEL_MONITOR_ENABLED", True), help="Persist watchdog audio level samples")
    parser.add_argument("--level-low-db", type=float, default=float(os.getenv("NTC_LEVEL_MONITOR_LOW_DB", "-42")), help="Warn when recent program level remains below this dBFS value")
    parser.add_argument("--level-hot-peak-db", type=float, default=float(os.getenv("NTC_LEVEL_MONITOR_HOT_PEAK_DB", "-1")), help="Warn when recent program peaks exceed this dBFS value")
    parser.add_argument("--level-window-seconds", type=int, default=int(os.getenv("NTC_LEVEL_MONITOR_WINDOW_SECONDS", "300")), help="Rolling level monitor window")
    parser.add_argument("--level-min-samples", type=int, default=int(os.getenv("NTC_LEVEL_MONITOR_MIN_SAMPLES", "3")), help="Minimum samples before level monitor warnings")
    parser.add_argument("--level-retain-days", type=int, default=int(os.getenv("NTC_LEVEL_MONITOR_RETAIN_DAYS", "14")), help="Days to retain level monitor samples")
    parser.add_argument("--record-resource-stats", action=argparse.BooleanOptionalAction, default=_bool_env("NTC_RESOURCE_MONITOR_ENABLED", True), help="Persist Docker CPU/memory/network samples")
    parser.add_argument("--resource-containers", default=os.getenv("NTC_RESOURCE_MONITOR_CONTAINERS", "ntc-webcall,ntc-hls-nginx,ntc-watchdog"), help="Comma-separated Docker containers to sample")
    parser.add_argument("--resource-retain-days", type=int, default=int(os.getenv("NTC_RESOURCE_MONITOR_RETAIN_DAYS", "14")), help="Days to retain resource monitor samples")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()

    store = NTCStore(args.db_path)
    issues = evaluate_hosts(
        store,
        heartbeat_stale_seconds=args.heartbeat_stale_seconds,
        startup_grace_seconds=args.startup_grace_seconds,
    )
    health_url = os.getenv("NTC_WATCHDOG_HEALTH_URL", "http://ntc-webcall:1967/healthz")
    health_timeout_seconds = float(os.getenv("NTC_WATCHDOG_HEALTH_TIMEOUT_SECONDS", "3"))
    remediation_enabled = os.getenv("NTC_WATCHDOG_REMEDIATION_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
    remediation_state_path = os.getenv("NTC_WATCHDOG_STATE_PATH", "/app/data/watchdog-state.json")
    remediation_cooldown_seconds = float(os.getenv("NTC_WATCHDOG_RESTART_COOLDOWN_SECONDS", "180"))
    docker_socket_path = os.getenv("NTC_WATCHDOG_DOCKER_SOCKET", "/var/run/docker.sock")
    restart_target = os.getenv("NTC_WATCHDOG_RESTART_TARGET", "ntc-webcall")
    health = check_server_health(health_url, timeout_seconds=health_timeout_seconds)
    client_probes = [] if args.skip_client_probe else check_client_routes(
        args.base_url,
        public_pin=args.public_pin,
        timeout_seconds=args.client_timeout_seconds,
        hls_timeout_seconds=args.hls_timeout_seconds,
    )
    failed_client_probes = [probe for probe in client_probes if not probe.ok]
    level_monitor = {"recorded": False, "issues": []}
    if args.record_audio_levels and not args.skip_client_probe:
        level_monitor = record_audio_level_monitoring(
            store,
            client_probes,
            low_level_db=args.level_low_db,
            hot_peak_db=args.level_hot_peak_db,
            window_seconds=args.level_window_seconds,
            min_samples=args.level_min_samples,
            retain_days=args.level_retain_days,
        )
        issues.extend(level_monitor.get("issues") or [])
    resource_monitor = {"recorded": False, "samples": [], "errors": []}
    if args.record_resource_stats:
        resource_monitor = record_resource_monitoring(
            store,
            docker_socket_path=docker_socket_path,
            container_names=_split_csv(args.resource_containers),
            retain_days=args.resource_retain_days,
        )
        if args.record_incidents:
            for error in resource_monitor.get("errors", []):
                store.record_event(
                    component="watchdog",
                    event_type="resource-monitor-failed",
                    message=f"NTC resource monitor failed for {error['container_name']}: {error['error']}",
                    level="warn",
                    details=error,
                )
    remediation = {"attempted": False, "status": "not-needed"}

    if not health.ok or failed_client_probes:
        restart_reason = health.message if not health.ok else failed_client_probes[0].message
        if args.record_incidents:
            if not health.ok:
                store.record_event(
                    component="watchdog",
                    event_type="server-health-failed",
                    message=f"NTC WebCall health check failed: {health.message}",
                    level="critical",
                    details={"health_url": health_url, "status_code": health.status_code},
                )
            for probe in failed_client_probes:
                store.record_event(
                    component="watchdog",
                    event_type=f"client-probe-{probe.name}-failed",
                    message=f"NTC WebCall client probe failed: {probe.message}",
                    level="critical",
                    details={
                        "url": probe.url,
                        "status_code": probe.status_code,
                        "elapsed_ms": probe.elapsed_ms,
                        **probe.details,
                    },
                )
        try:
            remediation = maybe_remediate_server(
                enabled=remediation_enabled,
                state_path=remediation_state_path,
                cooldown_seconds=remediation_cooldown_seconds,
                docker_socket_path=docker_socket_path,
                container_name=restart_target,
                reason=restart_reason,
            )
        except Exception as exc:
            remediation = {"attempted": True, "status": "failed", "error": str(exc)}
        remediation["target"] = restart_target

        if args.record_incidents:
            event_type = {
                "restarted": "server-restart-requested",
                "cooldown": "server-restart-skipped",
                "disabled": "server-restart-disabled",
                "failed": "server-restart-failed",
            }.get(remediation.get("status"), "server-restart-status")
            level = "info" if remediation.get("status") == "restarted" else "warn"
            if remediation.get("status") == "failed":
                level = "critical"
            details = {
                "target": restart_target,
                "status": remediation.get("status"),
            }
            if "remaining_seconds" in remediation:
                details["remaining_seconds"] = remediation["remaining_seconds"]
            if "error" in remediation:
                details["error"] = remediation["error"]
            store.record_event(
                component="watchdog",
                event_type=event_type,
                message=f"NTC WebCall remediation status: {remediation.get('status')}",
                level=level,
                details=details,
            )

    if args.record_incidents:
        for issue in issues:
            store.record_incident(
                issue.room_slug,
                host_slug=issue.host_slug,
                severity=issue.severity,
                message=f"[watchdog:{issue.code}] {issue.message}",
            )
            store.record_event(
                component="watchdog",
                event_type=issue.code,
                message=issue.message,
                level=issue.severity,
                room_slug=issue.room_slug,
                host_slug=issue.host_slug,
                details={"severity": issue.severity},
            )

    email_alerts = []
    if not health.ok:
        email_alerts.append(
            {
                "severity": "critical",
                "category": "server",
                "code": "healthz",
                "message": health.message,
                "url": health_url,
                "details": {"status_code": health.status_code},
            }
        )
    for probe in failed_client_probes:
        email_alerts.append(
            {
                "severity": "critical",
                "category": "client-route",
                "code": probe.name,
                "message": probe.message,
                "url": probe.url,
                "details": {
                    "status_code": probe.status_code,
                    "elapsed_ms": probe.elapsed_ms,
                    **probe.details,
                },
            }
        )
    for issue in issues:
        email_alerts.append(
            {
                "severity": issue.severity,
                "category": "host",
                "code": issue.code,
                "message": issue.message,
                "room_slug": issue.room_slug,
                "host_slug": issue.host_slug,
                "details": {},
            }
        )

    email_status = {"attempted": False, "status": "not-needed"}
    try:
        email_status = maybe_send_email_alerts(
            config=_load_email_alert_config(),
            state_path=remediation_state_path,
            alerts=email_alerts,
            remediation=remediation,
        )
    except Exception as exc:
        email_status = {"attempted": True, "status": "failed", "error": str(exc)}
        if args.record_incidents:
            store.record_event(
                component="watchdog",
                event_type="email-alert-failed",
                message=f"NTC WebCall email alert failed: {exc}",
                level="warn",
                details={},
            )

    if args.json:
        payload = {
            "ok": not issues and health.ok and not failed_client_probes,
            "issue_count": len(issues),
            "issues": [asdict(issue) for issue in issues],
            "server_health": asdict(health),
            "client_probes": [asdict(probe) for probe in client_probes],
            "level_monitor": {
                **level_monitor,
                "issues": [asdict(issue) for issue in level_monitor.get("issues", [])],
            },
            "resource_monitor": resource_monitor,
            "remediation": remediation,
            "email_alerts": email_status,
        }
        print(json.dumps(payload, indent=2))
    else:
        if not issues and health.ok and not failed_client_probes:
            print("OK: NTC WebCall hosts are healthy.")
        else:
            for issue in issues:
                print(f"{issue.severity.upper()} {issue.host_slug} {issue.code}: {issue.message}")
            if not health.ok:
                print(f"CRITICAL server-health: {health.message}")
            for probe in failed_client_probes:
                print(f"CRITICAL {probe.name}: {probe.message}")

    if not health.ok or failed_client_probes:
        raise SystemExit(2)
    if any(issue.severity == "critical" for issue in issues):
        raise SystemExit(2)
    if issues:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
