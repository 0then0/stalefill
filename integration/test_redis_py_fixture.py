"""Exercise expected fixture failures without hiding programming errors."""
import argparse
import io
from pathlib import Path
import runpy
import unittest
from unittest.mock import Mock, patch

import redis


class FixtureErrorsTest(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.acquire = Mock(return_value=1)
        self.release = Mock()
        self.client.register_script.side_effect = [self.acquire, self.release]
        args = argparse.Namespace(
            redis="127.0.0.1:6380", listen="127.0.0.1:8080",
            mode="broken", resp=3, fill="transaction",
        )
        with (
            patch("argparse.ArgumentParser.parse_args", return_value=args),
            patch("redis.Redis", return_value=self.client),
            patch("http.server.ThreadingHTTPServer"),
        ):
            module = runpy.run_path(str(Path(__file__).parent / "redis-py/app.py"))
        self.handler = object.__new__(module["Handler"])
        self.handler.path = "/items/42"
        self.handler.respond = Mock()

    def put_body(self, body):
        self.handler.headers = {"Content-Length": str(len(body))}
        self.handler.rfile = io.BytesIO(body)
        self.handler.do_PUT()

    def test_invalid_write_payloads_do_not_mutate_redis(self):
        for body in (b"invalid", b"{}", b'{"version":null}', b'{"version":"invalid"}'):
            with self.subTest(body=body):
                self.put_body(body)
                self.handler.respond.assert_called_with(502, {"error": "fixture write failed"})
        self.client.delete.assert_not_called()

    def test_redis_errors_have_sanitized_http_responses(self):
        self.client.delete.side_effect = redis.ConnectionError("private upstream detail")
        self.put_body(b'{"version":2}')
        self.handler.respond.assert_called_with(502, {"error": "fixture write failed"})
        self.client.hgetall.side_effect = redis.ResponseError("private cache detail")
        self.handler.do_GET()
        self.handler.respond.assert_called_with(502, {"error": "fixture read failed"})

    def test_lock_contention_does_not_publish_or_release_an_unowned_lock(self):
        self.client.hgetall.return_value = {}
        self.acquire.return_value = 0
        self.handler.do_GET()
        self.handler.respond.assert_called_with(502, {"error": "fixture read failed"})
        self.client.pipeline.assert_not_called()
        self.release.assert_not_called()

    def test_bad_cached_version_is_an_expected_fixture_failure(self):
        self.client.hgetall.return_value = {"version": "invalid"}
        self.handler.do_GET()
        self.handler.respond.assert_called_with(502, {"error": "fixture read failed"})

    def test_programming_errors_are_not_converted_to_http_failures(self):
        self.client.delete.side_effect = AttributeError("fixture bug")
        with self.assertRaises(AttributeError):
            self.put_body(b'{"version":2}')
        self.client.hgetall.side_effect = AttributeError("fixture bug")
        with self.assertRaises(AttributeError):
            self.handler.do_GET()
        self.handler.respond.assert_not_called()


if __name__ == "__main__":
    unittest.main()
