"""Shared NTC visual branding helpers."""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, abort, send_file, url_for

from ntc_env import install_legacy_env_aliases

install_legacy_env_aliases()


BRAND_BACKGROUND_FILENAME = "ntc-embossed-background.jpg"
DEFAULT_BRAND_BACKGROUND_PATH = Path(__file__).resolve().parent / "assets" / BRAND_BACKGROUND_FILENAME


def install_branding(app: Flask) -> None:
    """Serve the shared NTC background and apply it to rendered HTML pages."""

    app.config.setdefault(
        "NTC_BRAND_BACKGROUND_PATH",
        os.getenv("NTC_BRAND_BACKGROUND_PATH", str(DEFAULT_BRAND_BACKGROUND_PATH)),
    )

    @app.get(f"/brand/{BRAND_BACKGROUND_FILENAME}", endpoint="ntc_brand_background")
    def ntc_brand_background():
        path = Path(app.config.get("NTC_BRAND_BACKGROUND_PATH") or DEFAULT_BRAND_BACKGROUND_PATH)
        if not path.exists() or not path.is_file():
            abort(404)
        return send_file(path, mimetype="image/jpeg", conditional=True, max_age=86400)

    @app.after_request
    def apply_ntc_branding(response):
        if response.is_streamed or response.direct_passthrough or response.mimetype != "text/html":
            return response
        body = response.get_data(as_text=True)
        if "</head>" not in body or 'data-ntc-branding="ntc-bg"' in body:
            return response
        style = _brand_style(url_for("ntc_brand_background"))
        response.set_data(body.replace("</head>", f"{style}\n</head>", 1))
        return response


def _brand_style(background_url: str) -> str:
    return f"""
    <style data-ntc-branding="ntc-bg">
      html {{
        min-height: 100%;
        background: #050913;
      }}
      body {{
        background:
          linear-gradient(180deg, rgba(5, 10, 18, 0.50), rgba(5, 10, 18, 0.88)),
          radial-gradient(circle at 12% 0%, rgba(143, 211, 255, 0.18), transparent 30rem),
          radial-gradient(circle at 96% 14%, rgba(116, 221, 180, 0.10), transparent 28rem),
          #050913;
        position: relative;
        isolation: isolate;
        overflow-x: hidden;
      }}
      body::before {{
        content: "";
        position: fixed;
        inset: 0;
        z-index: -1;
        pointer-events: none;
        background: url("{background_url}") center / cover no-repeat;
        opacity: 0.31;
        filter: saturate(1.08) contrast(1.04) brightness(0.9);
      }}
    </style>
    """
