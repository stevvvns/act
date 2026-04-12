import { encode, decode } from '@msgpack/msgpack';

export default function actor(callbacks) {
  const reply = (msg) => {
    process.send(Object.values(encode(msg)));
  };

  process.on('message', async (msg) => {
    if (msg === 'exit') {
      process.exit(0);
    }
    const [id, mtd, args] = decode(msg);
    if (!callbacks[mtd]) {
      reply([id, 'error', `no such method ${mtd}`]);
      return;
    }
    try {
      if (Math.random() > 0.8) {
        //throw new Error('crash');
      }
      reply([id, 'ok', await callbacks[mtd](...args)]);
    } catch (ex) {
      reply([id, 'error', ex.toString()]);
      throw ex;
    }
  });
}
