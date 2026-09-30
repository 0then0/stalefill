"""Minimal equivalent of the official hash cache-aside wire pattern."""

import argparse
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis

parser = argparse.ArgumentParser()
parser.add_argument("--redis", required=True)
parser.add_argument("--listen", required=True)
parser.add_argument("--mode", choices=["broken", "fixed"], required=True)
parser.add_argument("--resp", type=int, choices=[2, 3], default=3)
parser.add_argument("--fill", choices=["single", "transaction"], default="transaction")
args = parser.parse_args()
host, port = args.redis.rsplit(":", 1)
client = redis.Redis(
    host=host, port=int(port), protocol=args.resp, decode_responses=True
)
key, lock_key = "product:42", "lock:product:42"
guard = threading.Lock()
version = 1
acquire = client.register_script(
    "return redis.call('SET',KEYS[1],ARGV[1],'NX','PX',ARGV[2]) and 1 or 0"
)
release = client.register_script(
    "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0"
)


def snapshot():
    with guard:
        return version


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_PUT(self):
        global version
        if self.path != "/items/42":
            return self.respond(404, {})
        try:
            value = int(
                json.loads(self.rfile.read(int(self.headers["Content-Length"])))[
                    "version"
                ]
            )
            with guard:
                version = value
            client.delete(key)
            self.respond(200, {"version": snapshot()})
        except (redis.RedisError, ValueError, TypeError, KeyError):
            self.respond(502, {"error": "fixture write failed"})

    def do_GET(self):
        if self.path == "/health":
            return self.respond(200, {"ready": True})
        if self.path == "/authoritative/42":
            return self.respond(200, {"version": snapshot()})
        if self.path != "/items/42":
            return self.respond(404, {})
        try:
            cached = client.hgetall(key)
            hit = bool(cached)
            if hit:
                value = int(cached["version"])
                if args.mode == "fixed" and value != snapshot():
                    hit = False
            if not hit:
                token = uuid.uuid4().hex
                if acquire(keys=[lock_key], args=[token, 30000]) != 1:
                    return self.respond(502, {"error": "fixture read failed"})
                try:
                    value = snapshot()
                    if args.fill == "single":
                        client.hset(key, mapping={"version": value})
                        client.expire(key, 5)
                    else:
                        with client.pipeline(transaction=True) as pipe:
                            pipe.delete(key)
                            pipe.hset(key, mapping={"version": value})
                            pipe.expire(key, 5)
                            pipe.execute()
                finally:
                    release(keys=[lock_key], args=[token])
            self.respond(200, {"version": value})
        except (redis.RedisError, ValueError, TypeError, KeyError):
            self.respond(502, {"error": "fixture read failed"})


host, port = args.listen.rsplit(":", 1)
ThreadingHTTPServer((host, int(port)), Handler).serve_forever()
