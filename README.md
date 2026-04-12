# act

To define an actor, put it in its own directory and do something like:

**`test/index.js`**
```javascript
import actor from 'act/actor.js';

actor({
  hello(name) {
    return `hey, ${name}`;
  }
});
```

Then, define a config file for your actor(s):

**`config.js`**
```javascript
import { resolve } from 'node:path';

const mod = x => resolve('.', x);
export default {
  defaultTimeout: '10s',
  maxMailbox: 500,
  actors: {
    test: {
      mode: 'fork',
      module: mod('test')
    },
  },
  log: console.log
};
```

Finally, to use the forked service:

**`entry.js`**
```javascript
import act from '../src/index.js';
import config from './config.js';

const sys = await act(config);
console.log(await sys.test.hello('jerks').timeout('30s')); // optional timeout override
sys.shutdown();
```

## Remote Actors

***You MUST firewall your remote actors so that only your app may access them, it is not safe to leave the port open!***

The actor **`test/index.js`** and **`entry.js`** are exactly the same as above.

On the remote server, you need to define a config file for the actors you plan to serve:

**`actors.js`**
```javascript
import { resolve } from 'node:path';

const mod = x => resolve(process.cwd(), x);
export default {
  log: console.log,
  listen: '0.0.0.0',
  actors: {
    remoteTest: mod('test'),
    maxMailbox: 500
  }
}
```

Next, you need to run `remote-service.js` from this package as a service, with the path to the above config as an argument:

`$ node node_modules/act/remote-service.js actors.js`

You should do this with something like supervisord to have some fault tolerance.

After running the service, modify the `config.js` file for the controlling service to specify the host:

**`config.js`**
```javascript
export default {
  defaultTimeout: '10s',
  actors: {
    test: {
      mode: 'remote',
      host: 'localhost' #  or wherever, note the dire warning above about firewalling this
    },
  },
  log: console.log
};
```
