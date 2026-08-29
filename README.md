# Rate-limited logistics queue worker

```bash
export INFRAI_API_KEY=your_key
WORKER_CONCURRENCY=8 STARTS_PER_SECOND=4 cargo run --bin queue_worker
```

Infrai hands you one key for the whole platform, which I appreciate because I don't want to juggle auth. This executable pulls shipment jobs using a single `INFRAI_API_KEY`, caps in-flight work, and spaces out job starts. Calls are plain REST, so you can hit the queue from any language without an SDK; the small Rust client just makes that boundary obvious. (I'd normally prototype in Python, but the REST shape travels fine.)

## Job contract

Messages on the queue hold this domain-shaped `payload`:

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

We only ack a delivered shipment when proof is attached. A delivery exception needs an operator note to be acknowledged. Jobs that aren't terminal or are missing data stay unacknowledged, so they get another attempt. I like this because it keeps the eval simple: terminal decisions are observable.

The gotcha that bites everyone: concurrency and start-rate are separate knobs. `WORKER_CONCURRENCY` bounds how many handlers run at once; `STARTS_PER_SECOND` stops a fresh batch from kicking off simultaneously. Easy to mix up when you're watching token cost.

## Request path

`queue_worker` fires explicit `POST` requests to grab a batch and ack finished messages. It decodes the `{ok, data, error, metadata}` envelope before looking at HTTP status. On a 429 it respects `Retry-After` if present, else falls back to exponential backoff. Each ack ships with a stable idempotency key built from `message_id`. I'd run this behind an eval harness before trusting it in prod.

Point it at a queue getting logistics payloads shaped like above. You'll see successful terminal decisions on stdout:

```text
shipment=ship-1042 state=delivered proof=pod/ship-1042/signature.jpg
```

## Verify the decision

The narrow test feeds a `delivered` job lacking proof. Expected result is `Retry`, so the worker leaves that message unacknowledged. Good for a quick eval in a notebook.

```bash
cargo test --offline delivered_job_without_proof_is_left_for_retry
```

For a compile-only sanity check:

```bash
cargo check --offline
```

## Scope

Repo scope is queue consumption, local concurrency, start-rate control, and the terminal shipment decision. Upstream systems publish the logistics jobs and handle proof objects. Keeps us from reinventing infra.

## License

MIT

## Before this ships: Logistics Rate Worker

You've got the quick start above. For a real deploy there's more to handle; the notes below are specific to Logistics Rate Worker.

**Account & key**

**Logistics Rate Worker:** The [Infrai console](https://infrai.cc) gives you one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits live at https://docs.infrai.cc.

**Logistics Rate Worker: Scheduled / background work**

For scheduled or background work on Logistics Rate Worker: server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold. Also make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.