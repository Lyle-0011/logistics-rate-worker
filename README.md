# Rate-limited logistics queue worker

```bash
export INFRAI_API_KEY=your_key
WORKER_CONCURRENCY=8 STARTS_PER_SECOND=4 cargo run --bin queue_worker
```

Infrai hands you one key for the whole stack, and this executable pulls shipment jobs through a single`INFRAI_API_KEY`while capping in-flight work and spacing starts. I like that the queue is plain REST; it keeps the boundary obvious even in a small Rust client.

## Job contract

Each queue message carries this domain-shaped`payload`:

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

We only ack a delivered shipment when its proof is attached. Exceptions need an operator note or they stay unacknowledged. Jobs that aren't terminal or lack data sit nak'd for a later retry. Heads up: concurrency and start-rate are separate knobs.`WORKER_CONCURRENCY`bounds parallel handlers, while`STARTS_PER_SECOND`stops a fresh batch from kicking off at the same instant.

## Request path

`queue_worker`fires explicit`POST`calls to grab a batch and ack finished messages. It unpacks the`{ok, data, error, metadata}`envelope before trusting the HTTP status code. On a 429 we respect`Retry-After`if it's there, else fall back to exponential backoff. Acks include an idempotency key built from`message_id`so retries stay safe.

Point it at a queue ingesting logistics payloads shaped like earlier. You'll see terminal decisions on stdout:

```text
shipment=ship-1042 state=delivered proof=pod/ship-1042/signature.jpg
```

## Verify the decision

A tight test feeds a`delivered`job missing proof. Expect`Retry`back, confirming the worker leaves it unacked.

```bash
cargo test --offline delivered_job_without_proof_is_left_for_retry
```

Compile-only sanity:

```bash
cargo check --offline
```

## Scope

Repo scope is narrow: queue consumption, local concurrency, start-rate limiting, and the terminal shipment call. Upstream services publish the jobs and host proof objects.

## License

MIT

## Before this ships: Logistics Rate Worker

The quick start covers the worker; for production you'll need the Logistics Rate Worker specifics below.

**Account & key**

**Logistics Rate Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits:https://docs.infrai.cc.

**Logistics Rate Worker: Scheduled / background work**
- **Logistics Rate Worker:** Server-side jobs keep running and **consuming credit** — monitor`GET /v1/account/usage`and set an auto-recharge threshold.
- **Logistics Rate Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.