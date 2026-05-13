"""Generate screenshots and demo GIF for the LiveDataToolView README.

Prerequisites:
    pip install playwright imageio
    playwright install chromium

Usage:
    python docs/generate_screenshots.py
"""

import io
import os
import subprocess
import sys
import time

import imageio.v3 as iio
from playwright.sync_api import sync_playwright

DOCS_DIR = os.path.dirname(os.path.abspath(__file__))
PACKAGE_DIR = os.path.dirname(DOCS_DIR)
EXAMPLE = os.path.join(PACKAGE_DIR, "examples", "live_dashboard.py")
PORT = 5010
URL = f"http://localhost:{PORT}/live_dashboard"
VIEWPORT = {"width": 1400, "height": 900}

WB_JS = """
function findWbShadow(root) {
    const all = root.querySelectorAll('*');
    for (const el of all) {
        if (el.shadowRoot) {
            const rows = el.shadowRoot.querySelectorAll('.wb-row');
            if (rows.length > 0) return el.shadowRoot;
            const deeper = findWbShadow(el.shadowRoot);
            if (deeper) return deeper;
        }
    }
    return null;
}
"""


def click_all_checkboxes(page):
    """Select all Wunderbaum channel checkboxes."""
    page.evaluate(
        f"""() => {{
        {WB_JS}
        const wbRoot = findWbShadow(document);
        if (!wbRoot) return;
        const cbs = wbRoot.querySelectorAll('i.wb-checkbox');
        for (const cb of cbs) cb.click();
    }}"""
    )


def click_live_tab(page):
    """Switch to the Live tab."""
    tabs = page.locator(".bk-tab")
    for i in range(tabs.count()):
        if "Live" in (tabs.nth(i).text_content() or ""):
            tabs.nth(i).click()
            return


def click_live_toggle(page):
    """Click the Live toggle button."""
    toggles = page.locator("button.bk-btn")
    for i in range(toggles.count()):
        text = toggles.nth(i).text_content() or ""
        if "Live" in text:
            toggles.nth(i).click()
            return


def capture(page, frames, delay=500):
    """Capture a screenshot frame."""
    page.wait_for_timeout(delay)
    buf = page.screenshot()
    frames.append(iio.imread(io.BytesIO(buf)))


def start_server():
    """Start the Panel server as a subprocess."""
    # Clean up old DB so example creates fresh data
    db_path = os.path.join(PACKAGE_DIR, "live_demo_archive.sqlite")
    if os.path.exists(db_path):
        os.remove(db_path)

    log_path = os.path.join(DOCS_DIR, "_server.log")
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "panel", "serve", EXAMPLE, "--port", str(PORT)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=PACKAGE_DIR,
    )
    # Wait for Panel server to start
    time.sleep(15)
    return proc


def stop_server(proc):
    """Stop the Panel server subprocess."""
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def main():
    print("Starting Panel server with OPC UA...")
    proc = start_server()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport=VIEWPORT)
            page.goto(URL, timeout=20000)
            # Wait for OPC UA server/client to start and accumulate data
            # (OPC UA starts per-session when page loads)
            print("Waiting 30s for OPC UA data to accumulate...")
            page.wait_for_timeout(30000)

            # Select all channels - auto-fetch finds accumulated data
            click_all_checkboxes(page)
            page.wait_for_timeout(5000)

            # Screenshot: archive tab with accumulated data
            archive_path = os.path.join(DOCS_DIR, "screenshot_archive_tab.png")
            page.screenshot(path=archive_path)
            print("screenshot_archive_tab.png saved")

            # Switch to Live tab and start streaming
            click_live_tab(page)
            page.wait_for_timeout(1000)
            click_live_toggle(page)

            # Wait for live data to accumulate
            page.wait_for_timeout(12000)
            live_path = os.path.join(DOCS_DIR, "screenshot_live_streaming.png")
            page.screenshot(path=live_path)
            print("screenshot_live_streaming.png saved")

            # Create live streaming GIF
            frames = []
            for i in range(10):
                capture(page, frames, 1500)

            gif_path = os.path.join(DOCS_DIR, "live_demo.gif")
            iio.imwrite(gif_path, frames, duration=1000, loop=0)
            print(f"live_demo.gif: {len(frames)} frames")

            browser.close()

    finally:
        print("Stopping server...")
        stop_server(proc)


if __name__ == "__main__":
    main()
