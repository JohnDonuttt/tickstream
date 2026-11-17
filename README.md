# tickstream

[![ci](https://github.com/JohnDonuttt/tickstream/actions/workflows/ci.yml/badge.svg)](https://github.com/JohnDonuttt/tickstream/actions/workflows/ci.yml)

A small real-time market-data feed in Rust. The server simulates prices for
BTC-USD, ETH-USD and SOL-USD, fans them out over WebSockets with a Tokio
broadcast channel, and disconnects any client that can't keep up. It also
serves a plain HTML/JS page that draws a live chart on a canvas and reconnects
on its own when the connection drops.

The prices are made up (a random walk). The point is the plumbing.

## Running it

```
cargo run
```

Then open http://localhost:3000. Open a few tabs to see the fan-out; untick a
symbol to change that tab's subscription. Kill and restart the server to watch
the page back off and reconnect.

Settings come from environment variables:

| Variable | Default | |
|---|---|---|
| `TICKSTREAM_ADDR` | `127.0.0.1:3000` | listen address |
| `TICKSTREAM_TICK_MS` | `100` | one tick per symbol per interval |
| `TICKSTREAM_BROADCAST_CAPACITY` | `1024` | shared ring buffer size |
| `TICKSTREAM_CLIENT_QUEUE` | `256` | per-client queue size |

`GET /stats` returns counters (published ticks, live connections, slow-client
drops) as JSON.

## Protocol

Server to client, one JSON object per text frame:

```
{"type":"hello","symbols":["BTC-USD","ETH-USD","SOL-USD"]}
{"type":"tick","symbol":"BTC-USD","price":67012.5,"seq":1234,"ts":1731840000000}
{"type":"subscribed","symbols":["BTC-USD"]}
{"type":"error","message":"unknown symbol: DOGE-USD"}
```

Client to server:

```
{"type":"subscribe","symbols":["BTC-USD","ETH-USD"]}
{"type":"subscribe"}
```

A subscribe replaces the current filter; leaving out `symbols` means all of
them. New connections get everything.

## Design

```
                               +-> forwarder -> mpsc (bounded) -> writer -> socket   client A
generator -> broadcast ring ---+-> forwarder -> mpsc (bounded) -> writer -> socket   client B
             (shared, 1024)    +-> ...one per client
```

The generator serialises each tick to JSON once and publishes it into a
`tokio::sync::broadcast` channel. Every client gets its own receiver, so adding
clients costs a refcount bump per tick rather than another serialisation.

Each connection runs three futures in one task:

- the **forwarder** reads the broadcast receiver, applies the symbol filter and
  calls `try_send` on the client's bounded `mpsc` queue;
- the **writer** drains that queue into the socket, and is the only thing that
  waits on the network;
- the **reader** handles subscribe messages and notices when the client leaves.

### Slow consumers

A client that reads slower than the feed produces has to give somewhere. The
options are to block the publisher (one bad client stalls everyone), buffer
without limit (one bad client eats the server's memory), silently skip
messages, or drop the client. tickstream drops the client, for either of two
reasons:

1. **Its queue is full.** The writer is stuck behind a slow socket, so the
   forwarder's `try_send` fails. The forwarder never awaits here, on purpose.
2. **It lagged the broadcast buffer.** If a forwarder falls more than the ring
   size behind, Tokio overwrites the ticks it hasn't read and `recv` returns
   `RecvError::Lagged(n)`. Carrying on would mean a silent gap in the data, so
   that also ends the connection.

Either way the server sends a close frame with code 1008 (policy violation) and
a reason such as `slow consumer: send queue full`, then drops the TCP
connection. A client that has stopped reading may never read that frame, so
delivery gets a two second deadline. The browser client shows the reason and
reconnects with exponential backoff and full jitter (capped at 15 s), so a
server restart doesn't get every tab reconnecting at the same moment. It also
breaks the chart line across the outage instead of joining the two sides.

Dropping and letting the client reconnect is the right trade for a live price
view: a fresh connection gets current prices straight away, while a client
that was allowed to fall behind would show old ones. A feed where every
message matters (order updates, say) would need sequence numbers plus a replay
or snapshot on reconnect instead.

## Tests

```
cargo test
```

Unit tests cover the random walk (determinism with a seed, positive prices on
the tick grid, bounded step sizes, contiguous sequence numbers), the protocol
types, and the forwarding policy in isolation (queue full, lagged, filtered,
client gone). `tests/feed.rs` runs a real server on a random port and checks
that two WebSocket clients both get every tick in order, that subscriptions
filter per client, and that a client which stops reading gets a 1008 close
while a healthy client on the same feed keeps receiving everything.

## License

MIT
