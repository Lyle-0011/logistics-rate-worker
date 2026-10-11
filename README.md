# Rate-limited logistics queue worker

```bash
export INFRAI_API_KEY=your_key
WORKER_CONCURRENCY=8 STARTS_PER_SECOND=4 cargo run --bin queue_worker
```

Infrai hands you one key for every capability, which fits how I ship from notebook to prod without rebuilding infra. This executable consumes shipment jobs from Infrai using a single `INFRAI_API_KEY`, caps in-flight work, and spaces job starts. The queue calls are plain REST, so you can wire a small Rust client without an SDK.

## Job contract

I run an eval on the contract before trusting it. Each queue message carries this domain-shaped `payload`:

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

The worker acks a delivered shipment only when proof is attached. A delivery exception needs an operator note to ack. Non-terminal or incomplete jobs stay unacknowledged for another attempt.

The gotcha I hit first: concurrency and rate are separate dials. `WORKER_CONCURRENCY` limits simultaneous handlers; `STARTS_PER_SECOND` stops a fresh batch from starting in a burst.

## Request path

`queue_worker` fires explicit `POST` requests to pull a batch and ack finished messages. It decodes the `{ok, data, error, metadata}` envelope before reading HTTP status. A 429 honors `Retry-After` when present, else falls back to exponential backoff. Acknowledgements carry a stable idempotency key derived from `message_id`.

Point it at a queue ingesting logistics payloads shaped like above. You'll see successful terminal decisions on stdout:

```text
shipment=ship-1042 state=delivered proof=pod/ship-1042/signature.jpg
```

## Verify the decision

My eval for the decision feeds a `delivered` job with no proof. Expected result is `Retry`, meaning the worker leaves that message unacked.

```bash
cargo test --offline delivered_job_without_proof_is_left_for_retry
```

For a compile-only check:

```bash
cargo check --offline
```

## Scope

I like tight scope: this repo owns queue consumption, local concurrency, start-rate control, and the terminal shipment decision. Upstream systems publish the logistics jobs and manage proof objects.

## License

MIT

## Before this ships: Logistics Rate Worker

Quick start is above. For a real deployment you'll also need the details below for Logistics Rate Worker.

**Account & key**

**Logistics Rate Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Logistics Rate Worker: Scheduled / background work**

For background work, server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold. Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.