// An in-memory stand-in for a Hibira app: a server-side `pusher` whose
// trigger() reaches the fake pusher-js clients subscribed to the channel
// (a macrotask later, as over a socket), with channel history for
// pusher:rewind. clients.drop() simulates a lost connection.
export function createBus({ history = true } = {}) {
  const log = []; // [channel, event, data, time]
  const clients = new Set();
  const tick = () => new Promise((r) => setTimeout(r, 1));

  const server = {
    triggers: 0,
    async trigger(channel, event, data) {
      server.triggers++;
      if (JSON.stringify(data).length > 4096) throw new Error('message too large');
      log.push([channel, event, data, Date.now()]);
      await tick();
      for (const c of clients) c._deliver(channel, event, data);
    },
  };

  function client() {
    const channels = new Map();
    let online = true;
    const c = {
      channels,
      channel: (name) => channels.get(name),
      subscribe(name) {
        let ch = channels.get(name);
        if (!ch) {
          ch = channelObj(name);
          channels.set(name, ch);
        }
        if (!ch.subscribed) setTimeout(() => {
          if (!online) return;
          ch.subscribed = true;
          ch.emit('pusher:subscription_succeeded', {});
        }, 2);
        return ch;
      },
      unsubscribe(name) {
        channels.delete(name);
      },
      send_event(event, data, name) {
        if (event !== 'pusher:rewind') return;
        setTimeout(() => {
          const ch = channels.get(name);
          if (!ch) return;
          if (!history) return ch.emit('pusher:rewind_error', { error: 'history is off' });
          const since = Date.now() - data.seconds * 1000;
          const list = log.filter(([chName, , , t]) => chName === name && t >= since);
          for (const [, ev, d] of list) ch.emit(ev, d);
          ch.emit('pusher:rewind_succeeded', { count: list.length });
        }, 2);
      },
      drop() {
        online = false;
        for (const ch of channels.values()) ch.subscribed = false;
      },
      restore() {
        online = true;
        for (const name of channels.keys()) c.subscribe(name);
      },
      _deliver(name, event, data) {
        const ch = channels.get(name);
        if (online && ch?.subscribed) ch.emit(event, JSON.parse(JSON.stringify(data)));
      },
    };
    clients.add(c);
    return c;
  }

  return { server, client, log };
}

function channelObj(name) {
  const handlers = new Map();
  return {
    name,
    subscribed: false,
    bind(ev, fn) {
      if (!handlers.has(ev)) handlers.set(ev, new Set());
      handlers.get(ev).add(fn);
    },
    unbind(ev, fn) {
      handlers.get(ev)?.delete(fn);
    },
    emit(ev, data) {
      for (const fn of [...(handlers.get(ev) ?? [])]) fn(data);
    },
    count(ev) {
      return handlers.get(ev)?.size ?? 0;
    },
  };
}
