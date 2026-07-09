"""Screenshot the report (light + dark, top + full) for visual verification."""

import pathlib
import sys

from playwright.sync_api import sync_playwright

url = pathlib.Path("mod12930_report.html").resolve().as_uri()
with sync_playwright() as pw:
    b = pw.chromium.launch()
    for scheme in ["light", "dark"]:
        pg = b.new_page(viewport={"width": 1280, "height": 1000}, color_scheme=scheme)
        errors = []
        pg.on("pageerror", lambda e: errors.append(str(e)))
        pg.goto(url)
        pg.wait_for_timeout(600)
        pg.screenshot(path=f"shot_{scheme}_top.png")
        pg.screenshot(path=f"shot_{scheme}_full.png", full_page=True)
        if errors:
            print(f"{scheme}: JS ERRORS:", errors)
            sys.exit(1)
        print(f"{scheme}: ok, no JS errors")
    b.close()
