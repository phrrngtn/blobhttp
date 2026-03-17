"""Integration tests for GCRA rate limiter and proxy routing.

Requires three local servers running before the test:
    python test/flask_ratelimit_server.py   # :8445 — timestamps requests
    python test/flask_proxy_server.py       # :8446 — logging forward proxy

Start them, then:
    uv run pytest test/test_ratelimit.py -v
"""

import json
import subprocess
import time
import urllib.request

import duckdb
import pytest

EXTENSION_PATH = "build/release/bhttp.duckdb_extension"
TARGET_SERVER = "http://localhost:8445"
PROXY_SERVER = "http://localhost:8446"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def con():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.execute(f"LOAD '{EXTENSION_PATH}'")
    return c


@pytest.fixture(autouse=True)
def reset_servers():
    """Reset both servers before each test."""
    for url in [f"{TARGET_SERVER}/reset", f"{PROXY_SERVER}/proxy/reset"]:
        try:
            urllib.request.urlopen(url)
        except Exception:
            pass
    yield


def fetch_json(url):
    return json.loads(urllib.request.urlopen(url).read())


# ---------------------------------------------------------------------------
# Rate limiter pacing tests
# ---------------------------------------------------------------------------

class TestRateLimiterPacing:
    """Verify that the GCRA pacer spaces requests at the configured rate."""

    def test_pacing_at_5_per_second(self, con):
        """Configure 5/s rate limit, fire 10 requests sequentially
        (max_concurrent=1), and verify inter-request gaps ~200ms."""
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'rate_limit', '5/s',
                    'burst', 1,
                    'max_concurrent', 1
                )
            )
        """)

        con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(10) AS t(id)
            )
        """).fetchall()

        stats = fetch_json(f"{TARGET_SERVER}/stats")
        assert stats["total_requests"] == 10

        gaps = stats["inter_request_gaps"]
        assert len(gaps) >= 8, f"Expected >= 8 gaps, got {len(gaps)}"

        # At 5/s with burst=1, the GCRA interval is 200ms.
        # Allow tolerance for scheduling jitter.
        avg_gap = sum(gaps) / len(gaps)
        assert avg_gap >= 0.15, f"Average gap {avg_gap:.3f}s too short for 5/s rate limit"
        # Total wall time should be at least ~1.4s (gaps * 0.2s)
        total_time = stats["request_log"][-1]["timestamp"] - stats["request_log"][0]["timestamp"]
        assert total_time >= 1.4, f"Total time {total_time:.3f}s too short — pacing not working"

    def test_burst_allows_initial_spike(self, con):
        """With burst=5, first 5 requests should fire immediately,
        then pacing kicks in."""
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'rate_limit', '5/s',
                    'burst', 5,
                    'max_concurrent', 1
                )
            )
        """)

        con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(8) AS t(id)
            )
        """).fetchall()

        stats = fetch_json(f"{TARGET_SERVER}/stats")
        gaps = stats["inter_request_gaps"]

        # First few gaps should be very small (burst window)
        burst_gaps = gaps[:4]
        avg_burst_gap = sum(burst_gaps) / len(burst_gaps)
        # After burst, gaps should be ~200ms
        tail_gaps = gaps[5:]
        if tail_gaps:
            avg_tail_gap = sum(tail_gaps) / len(tail_gaps)
            assert avg_tail_gap > avg_burst_gap, (
                f"Tail gaps ({avg_tail_gap:.3f}s) should be larger than burst gaps ({avg_burst_gap:.3f}s)"
            )

    def test_rate_limit_stats_populated(self, con):
        """After rate-limited requests, bh_http_rate_limit_stats() should
        show pacing counters."""
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'rate_limit', '5/s',
                    'burst', 1,
                    'max_concurrent', 1
                )
            )
        """)

        con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(10) AS t(id)
            )
        """).fetchall()

        rows = con.execute("SELECT * FROM bh_http_rate_limit_stats()").fetchall()
        assert len(rows) > 0

        # Find the row for localhost
        localhost_row = [r for r in rows if "localhost" in r[0]]
        assert len(localhost_row) > 0, f"No localhost entry in rate limit stats: {rows}"
        row = localhost_row[0]

        # Column layout: host, rate_limit, rate_rps, burst, requests, paced,
        #                total_wait_seconds, throttled_429, backlog_seconds, ...
        requests = row[4]
        paced = row[5]
        total_wait = row[6]

        # Stats are cumulative across the process, so check >= 10
        assert requests >= 10, f"Expected >= 10 requests, got {requests}"
        assert paced > 0, f"Expected some pacing, got paced={paced}"
        assert total_wait > 0.0, f"Expected positive wait time, got {total_wait}"


# ---------------------------------------------------------------------------
# Server-side 429 + Retry-After backoff
# ---------------------------------------------------------------------------

class TestServerSide429:
    """Verify that the extension records 429 throttle events."""

    def test_429_recorded_in_stats(self, con):
        """Tell the Flask server to enforce 3 req/s, fire a burst, and check
        that the extension's stats reflect 429 responses."""
        # Configure server-side 429 enforcement
        req = urllib.request.Request(
            f"{TARGET_SERVER}/configure",
            data=json.dumps({"max_per_second": 3, "retry_after": 0.5}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req)

        # Disable client-side pacing so all requests hit the server fast
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'rate_limit', '1000/s',
                    'burst', 1000,
                    'max_concurrent', 1
                )
            )
        """)

        results = con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(10) AS t(id)
            )
        """).fetchall()

        status_codes = [r[0] for r in results]
        count_429 = sum(1 for s in status_codes if s == 429)
        count_200 = sum(1 for s in status_codes if s == 200)

        # We should see some 429s and some 200s
        assert count_200 > 0, "Expected some 200 responses"
        assert count_429 > 0, f"Expected some 429 responses, got status codes: {status_codes}"

        # Check extension-side stats
        rows = con.execute("SELECT * FROM bh_http_rate_limit_stats()").fetchall()
        localhost_row = [r for r in rows if "localhost" in r[0]][0]
        throttled = localhost_row[7]  # throttled_429
        assert throttled > 0, f"Expected throttled_429 > 0 in stats, got {throttled}"


# ---------------------------------------------------------------------------
# Proxy routing
# ---------------------------------------------------------------------------

class TestProxyRouting:
    """Verify that requests are routed through the configured proxy."""

    def test_requests_routed_through_proxy(self, con):
        """Configure proxy, fire requests, verify proxy saw them."""
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'proxy', '{PROXY_SERVER}',
                    'rate_limit', '100/s',
                    'burst', 100,
                    'max_concurrent', 1
                )
            )
        """)

        con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(5) AS t(id)
            )
        """).fetchall()

        proxy_stats = fetch_json(f"{PROXY_SERVER}/proxy/stats")
        assert proxy_stats["total_proxied"] == 5, (
            f"Expected 5 proxied requests, got {proxy_stats['total_proxied']}"
        )

        # Every proxied request should target the rate-limit server
        for entry in proxy_stats["proxy_log"]:
            assert TARGET_SERVER in entry["url"], (
                f"Proxied request URL {entry['url']} doesn't target {TARGET_SERVER}"
            )

    def test_proxy_plus_rate_limiting(self, con):
        """Proxy + rate limiting together: requests go through proxy AND are paced."""
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'proxy', '{PROXY_SERVER}',
                    'rate_limit', '5/s',
                    'burst', 1,
                    'max_concurrent', 1
                )
            )
        """)

        con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(6) AS t(id)
            )
        """).fetchall()

        # Verify proxy saw all requests
        proxy_stats = fetch_json(f"{PROXY_SERVER}/proxy/stats")
        assert proxy_stats["total_proxied"] == 6

        # Verify pacing on the target server side
        target_stats = fetch_json(f"{TARGET_SERVER}/stats")
        gaps = target_stats["inter_request_gaps"]
        avg_gap = sum(gaps) / len(gaps) if gaps else 0
        assert avg_gap >= 0.15, (
            f"Average gap {avg_gap:.3f}s too short — rate limiting not working through proxy"
        )


# ---------------------------------------------------------------------------
# Global rate limiter
# ---------------------------------------------------------------------------

class TestGlobalRateLimiter:
    """Verify the session-wide global rate limiter."""

    def test_global_rate_limit(self, con):
        """Set a global rate limit and verify pacing."""
        con.execute(f"""
            SET VARIABLE bh_http_config = bh_http_config_set(
                'default',
                json_object(
                    'global_rate_limit', '5/s',
                    'global_burst', 1,
                    'rate_limit', '1000/s',
                    'burst', 1000,
                    'max_concurrent', 1
                )
            )
        """)

        con.execute(f"""
            SELECT r.response_status_code FROM (
                SELECT bh_http_get('{TARGET_SERVER}/req/' || id::VARCHAR) AS r
                FROM range(8) AS t(id)
            )
        """).fetchall()

        stats = fetch_json(f"{TARGET_SERVER}/stats")
        gaps = stats["inter_request_gaps"]
        avg_gap = sum(gaps) / len(gaps) if gaps else 0
        assert avg_gap >= 0.15, (
            f"Average gap {avg_gap:.3f}s too short — global rate limit not working"
        )

        # Check that (global) row appears in rate limit stats
        rows = con.execute("SELECT * FROM bh_http_rate_limit_stats()").fetchall()
        global_rows = [r for r in rows if r[0] == "(global)"]
        assert len(global_rows) == 1, f"Expected (global) row in stats, got: {[r[0] for r in rows]}"
        assert global_rows[0][4] == 8  # requests
