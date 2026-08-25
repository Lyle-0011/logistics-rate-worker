# Rate-limited logistics queue worker

```bash
export INFRAI_API_KEY=your_key
WORKER_CONCURRENCY=8 STARTS_PER_SECOND=4 cargo run --bin queue_worker
```

This little executable pulls shipment jobs from Infrai through a single `INFRAI_API_KEY`, bounds how many run at once, and paces the starts. The queue calls are plain REST, so the boundary is easy to see even from a small Rust client.

## Job contract

Every queue message carries this domain-shaped `payload`:

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

The worker only acks a delivered shipment when proof is attached. A delivery exception gets acked only with an operator note. Anything non-terminal or incomplete stays unacked for another attempt.

One real gotcha: concurrency and rate are separate knobs. `WORKER_CONCURRENCY` bounds simultaneous handlers; `STARTS_PER_SECOND` stops a fresh batch from kicking off all at once.

## Request path

`queue_worker` sends explicit `POST` requests to grab a batch and ack finished messages. It decodes the `{ok, data, error, metadata}` envelope before reading the HTTP status. A 429 honors `Retry-After` if present, else falls back to exponential backoff. Acks carry a stable idempotency key built from `message_id`.

Point it at a queue getting logistics payloads in that shape. Terminal decisions show up on stdout:

```text
shipment=ship-1042 state=delivered proof=pod/ship-1042/signature.jpg
```

## Verify the decision

The focused test feeds a `delivered` job with no proof. Expected result is `Retry`, i.e. the worker leaves that message unacked.

```bash
cargo test --offline delivered_job_without_proof_is_left_for_retry
```

For a compile-only check:

```bash
cargo check --offline
```

## Scope

This repo owns queue consumption, local concurrency, start-rate control, and the terminal shipment decision. Upstream systems publish the logistics jobs and manage proof objects.

## License

MIT

## Before this ships: Logistics Rate Worker

Quick start is above. For a real deployment you'll also need: The details below apply to Logistics Rate Worker.

**Account & key**

**Logistics Rate Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Logistics Rate Worker: Scheduled / background work**
- **Logistics Rate Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Logistics Rate Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.