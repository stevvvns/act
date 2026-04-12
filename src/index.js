import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { encode, decode } from '@msgpack/msgpack';
import { createConnection } from 'node:net';
import toMs from 'ms';
import { EventEmitter } from 'node:events';

export class DurableSocket extends EventEmitter {
  constructor({
    host,
    port,
    log,
    name,
    initialDelay = 100,
    maxDelay = 10000,
    factor = 2,
    downRetries = 20,
  }) {
    super();
    this.host = host;
    this.port = port;
    this.log = log;
    this.name = name;
    this.initialDelay = this.reconnectDelay = initialDelay;
    this.maxDelay = maxDelay;
    this.factor = factor;
    this.downRetries = downRetries;

    this.connect();
  }

  connect() {
    if (this.destroyed) {
      return;
    }

    this.socket = createConnection({
      host: this.host,
      port: this.port,
    });

    this.socket.on('connect', () => {
      this.retries = 0;
      this.connected = true;
      this.reconnectDelay = this.initialDelay;
      this.log({ evt: 'remote connected', name: this.name });
      this.emit('connect');
    });

    this.socket.on('data', (data) => {
      this.emit('data', data);
    });

    this.socket.on('error', (err) => {
      this.log({ evt: 'remote error', name: this.name, err });
      this.emit('error', err);
    });

    this.socket.on('close', (err) => {
      this.connected = false;
      this.socket = null;

      this.emit('close', err);

      if (!this.destroyed) {
        this.scheduleReconnect();
      }
    });
  }

  write(chunk) {
    if (this.destroyed) {
      throw new Error('socket destroyed');
    }

    if (this.connected && this.socket?.writable) {
      return this.socket.write(chunk);
    }

    throw new Error('not connected');
  }

  destroy() {
    this.destroyed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }

    this.socket?.destroy();
  }

  scheduleReconnect() {
    if (this.reconnectTimer) {
      return;
    }
    if (++this.retries > this.downRetries) {
      this.emit('down');
      this.log({ evt: 'remote down', name: this.name });
    }
    this.log({ evt: 'remote reconnect', name: this.name });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectDelay);
    this.reconnectDelay =
      Math.min(this.reconnectDelay * this.factor, this.maxDelay) +
      (0.8 + Math.random() * 0.4);
  }
}

export default async function act(config) {
  const log = config.log ? config.log : () => {};
  const rv = {};
  const dispatch = {};
  const defaultMaxMailbox = config.maxMailbox ?? 500;
  const defaultPort = config.remotePort ?? 22327;
  const maxMb = {};
  let shuttingDown = false;
  const timeout = (() => {
    if (!config.defaultTimeout) {
      return 1000;
    }
    if (typeof config.defaultTimeout === 'string') {
      return toMs(config.defaultTimeout);
    }
    return config.defaultTimeout;
  })();
  const procs = {};
  const timeouts = {};

  function shutdown() {
    if (!shuttingDown) {
      log({ evt: 'shutdown' });
      shuttingDown = true;
    }
    for (const [name, inFlight] of Object.entries(dispatch)) {
      if (Object.values(inFlight).length === 0) {
        log({ evt: 'exit', name });
        procs[name].send('exit');
      } else {
        log({ evt: 'drain', name });
      }
    }
  }
  function handleResponse(name, payload) {
    const [id, status, resp] = payload;
    if (timeouts[id]) {
      clearTimeout(timeouts[id]);
      delete timeouts[id];
    }
    log({ evt: 'incoming', name, id, status, resp });
    if (!dispatch[name][id]) {
      log({ evt: 'internal error', name, type: 'received unexpected message' });
      return;
    }
    dispatch[name][id][status === 'ok' ? 'resolve' : 'reject'](resp);
    delete dispatch[name][id];
    if (shuttingDown) {
      shutdown();
    }
  }

  log({ evt: 'startup', defaultTimeout: timeout });
  for (const [name, settings] of Object.entries(config.actors)) {
    if (name === 'shutdown') {
      throw new Error('"shutdown" is a reserved name');
    }
    if (!['fork', 'remote'].includes(settings.mode)) {
      throw new Error(`${name}: unhandled mode ${settings.mode}`);
    }
    log({ evt: 'register', name, ...settings });
    maxMb[name] = settings.maxMailbox ?? defaultMaxMailbox;
    dispatch[name] = {};

    function handleRequest({ send, checkConnected }) {
      return new Proxy(
        {},
        {
          get(_, mtd) {
            return (...args) => {
              const id = randomUUID();
              const pr = new Promise((resolve, reject) => {
                if (shuttingDown) {
                  return;
                }
                if (Object.keys(dispatch[name]).length >= maxMb[name]) {
                  log({ evt: 'full mailbox', name, max: maxMb[name] });
                  return reject('mailbox full');
                }
                if (checkConnected && !procs[name].connected) {
                  for (const [id, { reject }] of Object.entries(
                    dispatch[name],
                  )) {
                    if (timeouts[id]) {
                      clearTimeout(timeouts.id);
                    }
                    reject('disconnected');
                  }
                  dispatch[name] = {};
                  procs[name] = fork(settings.module);
                  procs[name].on('message', (msg) =>
                    handleResponse(name, decode(msg)),
                  );
                  return reject('disconnected');
                }
                dispatch[name][id] = { resolve, reject };
                log({ evt: 'outgoing', name, id, mtd, args });
                send(id, mtd, args);
              });
              const reaper = () => {
                if (dispatch[name][id]) {
                  dispatch[name][id].reject('actor timed out');
                }
              };
              timeouts[id] = setTimeout(reaper, timeout);
              pr.timeout = (num) => {
                clearTimeout(timeouts[id]);
                const asMs = typeof num === 'string' ? toMs(num) : num;
                timeouts[id] = setTimeout(reaper, asMs);
                return pr;
              };
              return pr;
            };
          },
        },
      );
    }

    if (settings.mode === 'fork') {
      procs[name] = fork(settings.module);
      procs[name].on('message', (msg) => handleResponse(name, decode(msg)));

      rv[name] = handleRequest({
        send(id, mtd, args) {
          procs[name].send(Object.values(encode([id, mtd, args])));
        },
        checkConnected: true,
      });
      continue;
    }
    // TODO could share a socket for same host/port w/ different actors, but meh
    await new Promise((resolve, reject) => {
      const port = settings.port ?? defaultPort;
      const host = settings.host ?? 'localhost';
      const sock = new DurableSocket({ host, port, log, name });
      sock.on('error', (err) => {
        if (!sock.connected) {
          reject(`failed to connect: ${err}`);
        }
      });
      procs[name] = {
        send(msg) {
          if (msg === 'exit') {
            sock.destroy();
            return;
          }
          sock.write(encode(msg));
        },
      };
      sock.on('data', (msg) => {
        const [op, payload] = decode(msg);
        log({ evt: 'incoming', remote: true, name, op, payload });
        if (op === 'ensure') {
          if (payload.status === 'ok') {
            resolve();
          } else {
            reject(`misconfiguration: ${host}:${port} cannot service ${name}`);
          }
        } else if (op === 'call') {
          handleResponse(name, payload);
        } else if (op === 'crash') {
          for (const [id, { reject }] of Object.entries(dispatch[name])) {
            if (timeouts[id]) {
              clearTimeout(timeouts.id);
            }
            reject('disconnected');
          }
          dispatch[payload] = {};
        }
      });
      sock.on('connect', () => {
        procs[name].send(['ensure', name]);
      });
    });
    // TODO move mailbox restriction to remote-service, dedupe with fork proxy
    rv[name] = handleRequest({
      send(id, mtd, args) {
        procs[name].send(['call', [id, name, mtd, args]]);
      },
    });
  }
  rv.shutdown = shutdown;
  return rv;
}
