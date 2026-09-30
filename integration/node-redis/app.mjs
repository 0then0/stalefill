// Minimal equivalent of the official hash cache-aside wire pattern.
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { createClientPool } from 'redis';

const { values: args } = parseArgs({
  options: {
    redis: { type: 'string' },
    listen: { type: 'string' },
    mode: { type: 'string' },
    resp: { type: 'string', default: '3' },
  },
});
const key = 'product:42',
  lockKey = 'lock:product:42';
const pool = createClientPool({
  url: `redis://${args.redis}`,
  RESP: Number(args.resp),
});
pool.on('error', () => {}); // Reconnects between independent proxy invocations.
let connection;
function connected() {
  connection ??= pool.connect(); // Proxy starts with the first configured probe.
  return connection;
}
const acquire = "return redis.call('SET',KEYS[1],ARGV[1],'NX','PX',ARGV[2]) and 1 or 0";
const release =
  "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0";
let version = 1;
const server = http.createServer(async (req, res) => {
  const respond = (status, body) => {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(body));
  };
  if (req.url === '/health') return respond(200, { ready: true });
  if (req.url === '/authoritative/42' && req.method === 'GET')
    return respond(200, { version });
  if (req.url !== '/items/42') return respond(404, {});
  try {
    await connected();
    if (req.method === 'PUT') {
      let body = '';
      for await (const chunk of req) body += chunk;
      version = JSON.parse(body).version;
      await pool.del(key);
      return respond(200, { version });
    }
    if (req.method !== 'GET') return respond(405, {});
    // Hold a pooled connection for the complete read/fill. A writer obtains
    // another ordinary pooled connection while EXEC is waiting for its reply.
    const value = await pool.execute(async (client) => {
      const cached = await client.hGetAll(key);
      const hit = Object.keys(cached).length > 0;
      if (hit && (args.mode !== 'fixed' || Number(cached.version) === version))
        return Number(cached.version);
      const token = randomUUID();
      if (
        (await client.eval(acquire, {
          keys: [lockKey],
          arguments: [token, '30000'],
        })) !== 1
      )
        throw new Error('fixture lock contention');
      try {
        const old = version;
        await client
          .multi()
          .del(key)
          .hSet(key, { version: String(old) })
          .expire(key, 5)
          .exec();
        return old;
      } finally {
        await client.eval(release, { keys: [lockKey], arguments: [token] });
      }
    });
    respond(200, { version: value });
  } catch {
    respond(502, { error: 'fixture cache operation failed' });
  }
});
const [host, port] = args.listen.split(':');
server.listen(Number(port), host);
