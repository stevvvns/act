import { createServer } from 'node:net';
import { resolve } from 'node:path';
import { fork } from 'node:child_process';
import { encode, decode } from '@msgpack/msgpack';

if (process.argv.length !== 3) {
  console.error(
    'you must provide a path to a configuration file as an argument',
  );
  process.exit(1);
}
const config = (await import(resolve(process.cwd(), process.argv[2]))).default;
const host = config.listen ?? '127.0.0.1';
if (!['127.0.0.1', 'localhost'].includes(host)) {
  console.warn(
    'WARNING: it is dangerous to use this service without firewalling it so that only your application(s) can access it!',
  );
}
const log = config.log ? config.log : () => {};
const actors = {};
for (const [name, path] of Object.entries(config.actors)) {
  log({ evt: 'register', name, path });
  actors[name] = fork(path);
}

createServer((sock) => {
  sock.on('data', async (msg) => {
    const [op, payload] = decode(msg);
    log({ evt: 'rcv', op, payload });
    if (op === 'ensure') {
      sock.write(
        encode([
          'ensure',
          { status: actors[payload] ? 'ok' : 'na', name: payload },
        ]),
      );
      if (actors[payload]) {
        actors[payload].on('message', (msg) => {
          sock.write(encode(['call', decode(msg)]));
        });
      }
    }
    if (op === 'call') {
      const [id, name, mtd, args] = payload;
      if (!actors[name].connected) {
        actors[name] = fork(config.actors[name]);
        actors[name].on('message', (msg) => {
          sock.write(encode(['call', decode(msg)]));
        });
        sock.write(encode(['crash', { name }]));
        return;
      }
      actors[name].send(Object.values(encode([id, mtd, args])));
    }
  });
}).listen(config.port ? parseInt(config.port) : 22327, host);
