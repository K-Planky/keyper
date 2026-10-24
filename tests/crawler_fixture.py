#!/usr/bin/env python3
"""Keyper-owned, loopback-only HTTP oracle and interactive crawler demo site."""

import argparse
import collections
import contextlib
import http.server
import json
import threading

LEAVES = 24
EXPECTED_STATS = "files: 30   extensions: 4   words: 52\n  html 27\n  jpeg 1\n  jpg 1\n  pdf 1\n"


def html(words, links=()):
    # Empty anchor text contributes no words; comments/attributes are excluded.
    return ("<!doctype html><html><body>" + words + "<!-- ignored words -->" +
            "".join(f'<a href="{link}"></a>' for link in links) + "</body></html>").encode()


def routes():
    """path -> (status, content type, bytes, redirect, gate); no public URLs."""
    result = {}
    for job in ("a", "b"):
        base = f"/{job}/"
        leaves = [f"leaf-{i}.html" for i in range(LEAVES)]
        result[base] = (200, "text/html", html("Alpha beta", [
            *leaves, "photo.JPG", "photo.jpeg", "manual.PDF", "blob", "redirect",
            "missing", "leaf-0.html#duplicate", "/outside.html",
            "/b/" if job == "a" else "/a/",
        ]), None, None)
        for leaf in leaves:
            result[base + leaf] = (200, "text/html", html("Leaf words", [
                "shared.html", "./", "shared.html#duplicate",
            ]), None, "main")
        result[base + "shared.html"] = (200, "text/html", html("Shared end", ["./"]), None, None)
        for name in ("photo.JPG", "photo.jpeg", "manual.PDF", "blob"):
            result[base + name] = (200, "application/octet-stream", b"fixture", None, None)
        result[base + "redirect"] = (302, "text/plain", b"", "leaf-0.html", None)
        result[base + "missing"] = (404, "text/plain", b"missing", None, None)
    for group in ("delay", "abort"):
        result[f"/{group}/"] = (200, "text/html", html("Parent words", ["child.html"]), None, group)
        result[f"/{group}/child.html"] = (200, "text/html", html("Child words"), None, None)
    result["/failure/"] = (503, "text/plain", b"intentional fixture failure", None, None)
    return result


class Fixture(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, port=0, gate_timeout=60):
        super().__init__(("127.0.0.1", port), Handler)
        self.routes = routes()
        self.gate_timeout = gate_timeout
        self.gates = {group: threading.Event() for group in ("main", "delay", "abort")}
        self.changed = threading.Condition()
        self.counts = collections.Counter()
        self.held = collections.Counter()
        self.active = self.peak = 0
        self.errors = []
        self.thread = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_port}"

    def snapshot(self):
        with self.changed:
            return {"get_counts": dict(self.counts), "held": dict(self.held),
                    "active": self.active, "peak": self.peak, "errors": list(self.errors)}

    def release(self, group="all"):
        for name, event in self.gates.items():
            if group in ("all", name):
                event.set()

    def __enter__(self):
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.release()
        self.shutdown()
        self.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive(), "HTTP fixture did not stop"


class Handler(http.server.BaseHTTPRequestHandler):
    # HTTP/1.0 closes each owned connection; no idle keep-alive reader threads.
    def setup(self):
        super().setup()
        self.connection.settimeout(5)

    def log_message(self, *_):
        pass

    def response(self, status, kind, body, location=None):
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        if location:
            self.send_header("Location", location)
        self.end_headers()

    def do_HEAD(self):
        with self.server.changed:
            self.server.errors.append(f"unexpected HEAD {self.path}")
        self.response(405, "text/plain", b"")

    def do_POST(self):
        prefix = "/_fixture/release/"
        group = self.path.removeprefix(prefix)
        if self.path.startswith(prefix) and group in (*self.server.gates, "all"):
            self.server.release(group)
            self.response(200, "text/plain", b"released\n")
            self.wfile.write(b"released\n")
        else:
            with self.server.changed:
                self.server.errors.append(f"unexpected POST {self.path}")
            self.response(404, "text/plain", b"")

    def do_GET(self):
        fixture = self.server
        if self.path == "/_fixture/state":
            body = json.dumps(fixture.snapshot(), sort_keys=True).encode()
            self.response(200, "application/json", body)
            self.wfile.write(body)
            return
        route = fixture.routes.get(self.path)
        with fixture.changed:
            fixture.counts[self.path] += 1
            fixture.active += 1
            fixture.peak = max(fixture.peak, fixture.active)
            if route is None:
                fixture.errors.append(f"unexpected GET {self.path}")
            fixture.changed.notify_all()
        try:
            status, kind, body, location, group = route or (404, "text/plain", b"", None, None)
            self.response(status, kind, body, location)
            if group:
                with fixture.changed:
                    fixture.held[self.path] += 1
                    fixture.changed.notify_all()
                try:
                    if not fixture.gates[group].wait(fixture.gate_timeout):
                        with fixture.changed:
                            fixture.errors.append(f"gate expired: {self.path}")
                        return  # Truncated response fails closed, not a silent release.
                finally:
                    with fixture.changed:
                        fixture.held[self.path] -= 1
                        fixture.changed.notify_all()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError) as error:
            with fixture.changed:
                fixture.errors.append(f"{type(error).__name__}: {self.path}")
        finally:
            with fixture.changed:
                fixture.active -= 1
                fixture.changed.notify_all()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    with Fixture(args.port, gate_timeout=300) as fixture:
        print(f"Fixture: {fixture.url} (gates expire after 300s)", flush=True)
        print("Submit /a/ and /b/; POST /_fixture/release/main; GET /_fixture/state", flush=True)
        with contextlib.suppress(KeyboardInterrupt):
            threading.Event().wait()


if __name__ == "__main__":
    main()
