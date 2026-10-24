"""Self-contained checks for the HTTP oracle, controls and status parser."""

import concurrent.futures
import http.client
import html.parser
import json
import unittest
import urllib.request

from crawler_fixture import EXPECTED_STATS, Fixture, LEAVES, routes
from crawler_process import draining_children, metrics, snapshot


class Text(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.words = 0

    def handle_data(self, data):
        self.words += sum(token[0] in "abcdefghijklmnopqrstuvwxyz" for token in data.lower().split())


class FixtureTests(unittest.TestCase):
    def test_hand_counted_graph(self):
        graph = routes()
        for base in ("/a/", "/b/"):
            pages = {path: reply for path, reply in graph.items() if path.startswith(base)}
            self.assertEqual(len(pages), 32)
            self.assertEqual(sum(reply[0] == 200 for reply in pages.values()), 30)
            self.assertEqual(sum(reply[4] == "main" for reply in pages.values()), LEAVES)
            # Main discovery of shared.html must occur only in held leaf bodies.
            self.assertNotIn(b"shared.html", pages[base][2])
            self.assertEqual(sum(b"shared.html" in reply[2] for reply in pages.values()), LEAVES)
            self.assertEqual(sum(reply[1] == "text/html" for reply in pages.values()), 26)
            # Independent stdlib text extraction checks the actual simple HTML,
            # not just a constant that repeats the expected crawler output.
            parser = Text()
            for reply in pages.values():
                if reply[1] == "text/html":
                    parser.feed(reply[2].decode())
            self.assertEqual(parser.words, 52)  # 2 + 24*2 + 2.
        self.assertIn("words: 52", EXPECTED_STATS)

    def test_body_gate_and_control_requests_are_not_crawl_gets(self):
        with Fixture() as fixture, concurrent.futures.ThreadPoolExecutor() as pool:
            def fetch():
                with urllib.request.urlopen(fixture.url + "/a/leaf-0.html", timeout=5) as response:
                    return response.read()
            future = pool.submit(fetch)
            try:
                with fixture.changed:
                    self.assertTrue(fixture.changed.wait_for(
                        lambda: fixture.held["/a/leaf-0.html"] == 1, timeout=5))
                self.assertFalse(future.done())
                with urllib.request.urlopen(fixture.url + "/_fixture/state", timeout=5) as response:
                    state = json.load(response)
                self.assertEqual(state["get_counts"], {"/a/leaf-0.html": 1})
                self.assertEqual(state["active"], 1)
                request = urllib.request.Request(fixture.url + "/_fixture/release/main", method="POST")
                with urllib.request.urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
                self.assertIn(b"shared.html", future.result(timeout=5))
            finally:
                fixture.release()
            with fixture.changed:
                self.assertTrue(fixture.changed.wait_for(lambda: fixture.active == 0, timeout=5))
            self.assertEqual(fixture.snapshot()["errors"], [])

    def test_expired_gate_truncates_instead_of_succeeding(self):
        with Fixture(gate_timeout=0) as fixture:
            with urllib.request.urlopen(fixture.url + "/delay/", timeout=5) as response:
                with self.assertRaises(http.client.IncompleteRead):
                    response.read()
            self.assertEqual(fixture.snapshot()["errors"], ["gate expired: /delay/"])

    def test_unexpected_get_is_visible(self):
        with Fixture() as fixture:
            with self.assertRaises(urllib.error.HTTPError):
                urllib.request.urlopen(fixture.url + "/outside.html", timeout=5)
            self.assertEqual(fixture.snapshot()["get_counts"], {"/outside.html": 1})
            self.assertEqual(fixture.snapshot()["errors"], ["unexpected GET /outside.html"])

    def test_head_get_pairs_cannot_hide_an_extra_request(self):
        with Fixture() as fixture:
            request = urllib.request.Request(fixture.url + "/a/", method="HEAD")
            with self.assertRaises(urllib.error.HTTPError):
                urllib.request.urlopen(request, timeout=5)
            self.assertEqual(fixture.snapshot()["errors"], ["unexpected HEAD /a/"])

    def test_failure_cleanup_releases_bodies_before_draining_children(self):
        with Fixture() as fixture:
            with self.assertRaisesRegex(RuntimeError, "intentional"):
                with draining_children(fixture) as stack:
                    stack.callback(lambda: self.assertTrue(all(gate.is_set() for gate in fixture.gates.values())))
                    raise RuntimeError("intentional test failure")

    def test_progress_oracle_rejects_incoherent_or_premature_done(self):
        self.assertEqual(snapshot("job 1  crawled 0  frontier 0  in flight 1  files 0  discovered 1  running")["owned"], 1)
        for line in (
            "job 1  crawled 0  frontier 0  in flight 1  files 0  discovered 1  done",
            "job 1  crawled 1  frontier 1  in flight 0  files 1  discovered 1  running",
            "job 1  crawled 0  frontier 0  in flight 0  files 0  discovered 0  running",
        ):
            with self.assertRaises(AssertionError):
                snapshot(line)

    def test_metrics_oracle_rejects_cap_or_drain_violation(self):
        gauges = dict(http=dict(active=0, peak=10), owned=dict(active=0, peak=20),
                      cpu=dict(active=0, peak=4))
        self.assertEqual(metrics("swarmcrawl_metrics " + json.dumps(gauges)), gauges)
        for gauge, field, value in (("http", "peak", 11), ("http", "active", 1),
                                    ("owned", "peak", 21), ("cpu", "peak", 5)):
            bad = {name: dict(value) for name, value in gauges.items()}
            bad[gauge][field] = value
            with self.assertRaises(AssertionError):
                metrics("swarmcrawl_metrics " + json.dumps(bad))


if __name__ == "__main__":
    unittest.main()
