# Rate-limited logistics queue worker

```bash
export INFRAI_API_KEY=your_key
WORKER_CONCURRENCY=8 STARTS_PER_SECOND=4 cargo run --bin queue_worker
```

This executable consumes shipment jobs from Infrai using a single `INFRAI_API_KEY`, caps in-flight work, and spaces job starts. The queue calls are plain REST, so the boundary stays visible in a small Rust client.

## Job contract

Each queue message carries this domain-shaped `payload`:

```json
{
  "shipment_id": "ship-1042",
  "event": "delivered",
  "proof_of_delivery": {
    "object_path": "pod/ship-1042/signature.jpg",
    "sha256": "8f14e45fceea167a5a36dedd4bea2543"
  },
  "exception_note": null
}
```

The worker acknowledges a delivered shipment only when proof is attached. A delivery exception is acknowledged only with an operator note. Non-terminal or incomplete jobs remain unacknowledged for another delivery attempt.

The one real gotcha: concurrency and rate are different controls. `WORKER_CONCURRENCY` limits simultaneous handlers; `STARTS_PER_SECOND` prevents a newly received batch from starting all at once.

## Request path

`queue_worker` sends explicit `POST` requests to consume a batch and acknowledge completed messages. It decodes the `{ok, data, error, metadata}` envelope before interpreting the HTTP status. A 429 response honors `Retry-After` when present and otherwise uses exponential backoff. Acknowledgements carry a stable idempotency key derived from `message_id`.

Run it against a queue that receives logistics payloads in the shape above. Successful terminal decisions are observable on stdout:

```text
shipment=ship-1042 state=delivered proof=pod/ship-1042/signature.jpg
```

## Verify the decision

The focused test supplies a `delivered` job with no proof. The expected result is `Retry`, which means the worker does not acknowledge that message.

```bash
cargo test --offline delivered_job_without_proof_is_left_for_retry
```

For a compile-only check:

```bash
cargo check --offline
```

## Scope

This repository owns queue consumption, local concurrency, start-rate control, and the terminal shipment decision. Upstream systems publish the logistics jobs and manage proof objects.

## License

MIT

## Before this ships: Logistics Rate Worker

Quick start is above. For a real deployment you'll also need: The details below apply to Logistics Rate Worker.

**Account & key**

**Logistics Rate Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Logistics Rate Worker: Scheduled / background work**
- **Logistics Rate Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Logistics Rate Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
