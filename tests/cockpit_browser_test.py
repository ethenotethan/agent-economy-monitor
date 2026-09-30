import json
import pathlib
import subprocess
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from playwright.sync_api import expect, sync_playwright

ROOT = pathlib.Path(__file__).resolve().parents[1]


class CockpitBrowserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        render = subprocess.run(
            ["cargo", "run", "--quiet", "--example", "cockpit_browser_fixture"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if render.returncode != 0:
            raise RuntimeError(f"browser fixture failed to render: {render.stderr}")

        class FixtureHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                path = self.path.split("?", 1)[0]
                if path == "/":
                    body, content_type = render.stdout.encode(), "text/html; charset=utf-8"
                elif path == "/api/v1/pulse":
                    body = json.dumps({"api_version": "v1", "data": [self.fact("pulse:fixture", "pulse_metric", "x402 on base")]}).encode()
                    content_type = "application/json"
                elif path == "/api/v1/buyers":
                    body = json.dumps({"api_version": "v1", "items": [self.fact("buyer:fixture", "buyer", "Buyer fixture")], "next_cursor": None}).encode()
                    content_type = "application/json"
                elif path == "/api/v1/buyers/buyer%3Afixture/dossier":
                    buyer = self.fact("buyer:fixture", "buyer", "Buyer fixture")
                    timeline = self.fact("settlement:fixture", "settlement", "Settled x402 payment")
                    timeline["value"].update({"settled_at": "2000-01-01T00:00:00Z", "service_id": "service:fixture"})
                    service = self.fact("service:fixture", "service", "Service fixture")
                    edge = self.fact("edge:fixture", "attribution", "explicit_requirement")
                    edge["provenance_ids"] = [
                        "11111111-1111-1111-1111-111111111111",
                        "22222222-2222-2222-2222-222222222222",
                    ]
                    edge["value"] = {
                        "source": {"kind": "buyer", "id": "buyer:fixture"},
                        "target": {"kind": "service", "id": "service:fixture"},
                        "predicate": "paid_for",
                        "direction": "outbound",
                        "attribution_method": "explicit_requirement",
                        "confidence": "0.9100",
                    }
                    body = json.dumps({
                        "api_version": "v1",
                        "data": {
                            "buyer": buyer,
                            "classifications": [{
                                "claim_id": "claim:automation",
                                "label": "automated-buyer",
                                "status": "disputed",
                                "confidence": "0.7300",
                                "method": "rules@1",
                                "evidence_window_start": "1999-12-01T00:00:00Z",
                                "evidence_window_end": "2000-01-01T00:00:00Z",
                                "valid_to": "2000-01-02T00:00:00Z",
                                "is_stale": True,
                                "provenance_ids": ["11111111-1111-1111-1111-111111111111"],
                                "supporting_evidence_ids": ["sha256:supporting"],
                                "conflicting_evidence_ids": ["sha256:conflicting"],
                            }],
                            "timeline": {"api_version": "v1", "items": [timeline], "next_cursor": None},
                            "graph": {"root": buyer, "nodes": [service], "edges": [edge]},
                        },
                    }).encode()
                    content_type = "application/json"
                elif path == "/api/v1/provenance/11111111-1111-1111-1111-111111111111":
                    body = json.dumps({
                        "api_version": "v1",
                        "data": {
                            "provenance_id": "11111111-1111-1111-1111-111111111111",
                            "source_id": "runtime-402:fixture",
                            "observed_at": "2000-01-01T00:00:00Z",
                            "parser_version": "x402-adapter@1",
                            "provider": "fixture-rpc",
                            "chain_scope": "base",
                            "block_reference": "12345",
                            "transaction_reference": "0xfixture",
                            "finality": "finalized",
                            "evidence_id": "sha256:fixture",
                            "evidence_sha256": "a" * 64,
                        },
                    }).encode()
                    content_type = "application/json"
                elif path == "/api/v1/graph/service/service%3Afixture":
                    service = self.fact("service:fixture", "service", "Service fixture")
                    body = json.dumps({"api_version": "v1", "data": {"root": service, "nodes": [], "edges": []}}).encode()
                    content_type = "application/json"
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            @staticmethod
            def fact(identifier, kind, label):
                return {"id": identifier, "kind": kind, "label": label, "value": {"amount_atomic": "4200000", "asset": "USDC", "chain_scope": "base", "handle_kind": "wallet"}, "observed_at": "2000-01-01T00:00:00Z", "provenance_ids": ["11111111-1111-1111-1111-111111111111"]}

            def log_message(self, format, *args):
                del format, args
                pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        cls.port = cls.server.server_port
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.playwright = sync_playwright().start()
        cls.browser = cls.playwright.firefox.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=5)

    def test_pulse_loads_and_sse_failure_falls_back_to_polling(self):
        page = self.browser.new_page(viewport={"width": 1440, "height": 900})
        page.goto(f"http://127.0.0.1:{self.port}/")
        expect(page).to_have_title("Pulse · Agent Economy Monitor")
        expect(page.locator(".metric-card")).to_contain_text("x402 on base")
        expect(page.locator("#connection-label")).to_have_text("Live unavailable · polling")
        expect(page.locator("#view-root")).to_have_attribute("data-state", "stale")
        page.close()

    def test_navigation_query_opens_buyers_at_tablet_width(self):
        page = self.browser.new_page(viewport={"width": 900, "height": 1024})
        page.goto(f"http://127.0.0.1:{self.port}/?view=buyers")
        expect(page.locator("html")).to_have_attribute("data-layout", "compact")
        expect(page.locator('[data-view="buyers"]')).to_have_attribute("aria-current", "page")
        expect(page.locator(".fact-card")).to_contain_text("Buyer fixture")
        expect(page.locator("#view-description")).to_contain_text("A handle is not a real-world identity")
        page.close()

    def test_slow_previous_view_cannot_overwrite_current_navigation(self):
        page = self.browser.new_page(viewport={"width": 1440, "height": 900})

        def delayed_pulse(route):
            time.sleep(0.5)
            route.fulfill(
                json={
                    "api_version": "v1",
                    "data": [
                        {
                            "id": "pulse:slow",
                            "kind": "pulse_metric",
                            "label": "Slow Pulse",
                            "value": {"amount_atomic": "1", "asset": "USDC", "chain_scope": "base"},
                            "observed_at": "2000-01-01T00:00:00Z",
                            "provenance_ids": ["11111111-1111-1111-1111-111111111111"],
                        }
                    ],
                }
            )

        page.route("**/api/v1/pulse", delayed_pulse)
        page.goto(f"http://127.0.0.1:{self.port}/")
        page.locator('[data-view="buyers"]').click()
        expect(page.locator("#view-title")).to_have_text("Buyers")
        expect(page.locator(".fact-card")).to_contain_text("Buyer fixture")
        page.wait_for_timeout(700)
        expect(page.locator("#view-title")).to_have_text("Buyers")
        expect(page.locator(".fact-card")).to_contain_text("Buyer fixture")
        expect(page.locator("#view-root")).not_to_contain_text("Slow Pulse")
        page.close()

    def test_buyer_dossier_exposes_classification_graph_and_provenance_lineage(self):
        page = self.browser.new_page(viewport={"width": 1440, "height": 900})
        page.goto(f"http://127.0.0.1:{self.port}/?view=buyers")
        page.locator('[data-buyer-id="buyer:fixture"]').click()

        expect(page.locator(".dossier-title")).to_have_text("Buyer fixture")
        classification = page.locator(".classification-card")
        expect(classification).to_contain_text("automated-buyer")
        expect(classification).to_contain_text("DISPUTED")
        expect(classification).to_contain_text("73.00%")
        expect(classification).to_contain_text("STALE")
        expect(classification).to_contain_text("1 CONFLICT")
        expect(classification).to_contain_text("sha256:conflicting")

        edge = page.locator('[data-source-kind="buyer"][data-target-kind="service"]')
        expect(edge).to_contain_text("paid_for")
        expect(edge).to_contain_text("91.00%")
        expect(edge.locator(".provenance-trigger")).to_have_count(2)

        edge.locator('[data-related-kind="service"]').click()
        expect(page.locator(".graph-title")).to_have_text("Service fixture")
        page.go_back()
        expect(page.locator(".dossier-title")).to_have_text("Buyer fixture")

        classification.locator(".provenance-trigger").click()
        drawer = page.locator("#provenance-drawer")
        expect(drawer).to_have_attribute("aria-hidden", "false")
        expect(drawer).to_contain_text("runtime-402:fixture")
        expect(drawer).to_contain_text("x402-adapter@1")
        expect(drawer).to_contain_text("finalized")
        expect(drawer).to_contain_text("a" * 64)
        expect(drawer).to_contain_text("rules@1")
        expect(drawer).to_contain_text("73.00%")
        page.close()


if __name__ == "__main__":
    unittest.main()
