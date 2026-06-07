# `/cache/v1` drop-in compatibility — KKV vs mirror-v3

Reference image: `ghcr.io/yolean/kafka-keyvalue:7fa31f42731fc20a77988b478a3896732cc3dc88@sha256:01461015a75545b2f8d426e1e8bed5129dd1a79ca7081c40c6961559043d77f3`.

Reproduce:

```
cargo test -p mirror-e2e --test cache_v1_compat -- --ignored --nocapture
```

The test provisions a fresh Redpanda with dual listeners (host /
docker-internal), produces a fixture stream, runs KKV in a container
and mirror-v3 in-process against the same topic, then compares status,
`Content-Type`, `x-kkv-last-seen-offsets`, and body bytes across 23
probes. It does not fail on divergence — every diff is intentional
triage input.

Fixture stream on topic `compat-probe` (partition 0):

| offset | key             | value          | notes                    |
| -----: | --------------- | -------------- | ------------------------ |
| 0      | `k1`            | `v1`           |                          |
| 1      | `k2`            | `v2`           |                          |
| 2      | `k3`            | `v3`           |                          |
| 3      | `k1`            | `v1-updated`   | overwrite                |
| 4      | `empty-value`   | `""`           | zero-byte value          |
| 5      | `special/chars` | `with/slashes` | slash in both            |
| 6      | `plus+key`      | `plusvalue`    | `+` in key               |
| 7      | `ünıçødé`       | `unicode-value`| multibyte key            |
| 8      | `k2`            | `null`         | tombstone                |

`bootstrap_hwm = 9` on both consumers.

## Status

After the round-1 triage (F1–F7), 18 of 23 probes match byte-for-byte.
The 5 remaining divergences are all **intentional** per the operator
decisions recorded below. None of them break point-lookup semantics —
`GET /cache/v1/raw/{key}` is 100 % parity across every case probed
(present keys, overwrites, tombstones, special characters, unicode,
URL-encoded slashes, missing keys, malformed paths).

| #  | Probe                              | Match? | Status / triage outcome |
| -- | ---------------------------------- | ------ | ----------------------- |
| 1  | `GET /cache/v1/raw/k1` (latest)    | ✅      | identical               |
| 2  | `GET /cache/v1/raw/k3`             | ✅      | identical               |
| 3  | `GET /cache/v1/raw/k2` (tombstoned)| ✅      | identical (404)         |
| 4  | `GET /cache/v1/raw/never`          | ✅      | identical (404)         |
| 5  | `GET /cache/v1/raw/empty-value`    | ✅      | identical (200, empty)  |
| 6  | `GET /cache/v1/raw/special/chars`  | ✅      | identical (404 — slash routed) |
| 7  | `GET /cache/v1/raw/special%2Fchars`| ✅      | identical               |
| 8  | `GET /cache/v1/raw/plus+key`       | ✅      | identical               |
| 9  | `GET /cache/v1/raw/<unicode>`      | ✅      | identical               |
| 10 | `GET /cache/v1/raw/`               | ✅      | identical (404)         |
| 11 | `GET /cache/v1/raw`                | ✅      | identical (404)         |
| 12 | `GET /cache/v1/keys`               | ⚠️      | **status, headers, trailing newline now match KKV**. Body differs by design: see [Intentional divergence A](#a-keys-and-values-body-tombstones-and-ordering). |
| 13 | `GET /cache/v1/values`             | ⚠️      | as above + [B](#b-values-content-type-casing-spacing) |
| 14 | `GET /cache/v1/offset/{t}/0`       | ⚠️      | status + body identical; `Content-Type` differs per [B](#b-values-content-type-casing-spacing) |
| 15 | `GET /cache/v1/offset/{t}/99`      | ❌      | [C](#c-offset--unknown-topicpartition-status-)            |
| 16 | `GET /cache/v1/offset/nope/0`      | ❌      | [C](#c-offset--unknown-topicpartition-status-)            |
| 17 | `GET /cache/v1/offset//0`          | ❌      | [D](#d-offset--malformed-input-status-)            |
| 18 | `GET /cache/v1/offset/{t}/x`       | ❌      | [D](#d-offset--malformed-input-status-)            |
| 19 | `GET /cache/v1/unknown`            | ✅      | identical (404)         |
| 20 | `POST /cache/v1/raw/k1`            | ✅      | identical (405)         |
| 21 | `DELETE /cache/v1/raw/k1`          | ✅      | identical (405)         |
| 22 | `GET /`                            | ✅      | identical (404)         |
| 23 | `GET /openapi.json`                | ❌      | [E](#e-openapijson--additive-on-mirror-v3) (additive) |

## Resolved (changed in this round)

### `/keys` and `/values` are now newline-terminated

Both servers now end every listed line — including the last — with
`\n`. mirror-v3 changed; KKV was already this shape.

### `/keys` `Content-Type` is now `application/octet-stream`

mirror-v3 was emitting `text/plain; charset=utf-8`; it now matches
KKV's `application/octet-stream`. The same hook is documented in code
for the `/values` endpoint if we later want it to adapt to the topic
schema (see [Future enhancement F](#f-future-content-type-adapts-to-valuestype)).

### Cache view iteration is now insertion order

mirror-v3 was iterating in BTreeMap (lexicographic) order; it now
uses [`indexmap::IndexMap`](https://docs.rs/indexmap), so the position
a key occupies in `/keys` and `/values` is the position it was *first*
seen by the cache. Overwrites keep the existing position; tombstones
`shift_remove` (preserving the order of the remaining entries). This
is stricter than KKV — KKV's underlying `ConcurrentHashMap` gives
unspecified iteration order — but it gives operators a stable contract
they can reason about, which lex order can't (lex order changes when
the **set** of keys changes).

## Intentional remaining divergences

### A. `/keys` and `/values` body: tombstones and ordering

KKV `/keys` body: `empty-value\nk1\nspecial/chars\nk2\nk3\nplus+key\nünıçødé\n`

mirror-v3 `/keys` body: `k1\nk3\nempty-value\nspecial/chars\nplus+key\nünıçødé\n`

Two intentional differences:

1. **Tombstoned keys** (`k2` here) appear in KKV's listing even after
   the null-value record is applied. mirror-v3 removes them from
   both `/keys` and `/values`. **Decision F1.1: mirror-v3 is correct.**
   Anyone consuming the listings and then doing `GET /raw/{key}` on
   each result gets surprised by KKV's tombstone leakage; mirror-v3
   doesn't surface entries it would 404 on.
2. **Ordering**: mirror-v3 is insertion order; KKV is Java
   `ConcurrentHashMap` iteration order (effectively undefined).
   **Decision F1.2: insertion order is the contract worth giving
   operators. Lexicographic ordering, if a consumer wants it, is one
   client-side `sort` away.**

The same applies to `/values` — KKV additionally emits the literal
ASCII bytes `null` where a tombstoned slot would be, which mirror-v3
suppresses (consequence of F1.1).

### B. `/values` `Content-Type` casing / spacing

| Header           | KKV                            | mirror-v3                      |
| ---------------- | ------------------------------ | ------------------------------ |
| `Content-Type`   | `text/plain;charset=UTF-8`     | `text/plain; charset=utf-8`    |

Functionally identical per
[RFC 9110 §5.6.6](https://www.rfc-editor.org/rfc/rfc9110#section-5.6.6):
charset names are case-insensitive and the `;` parameter separator
may have surrounding whitespace. Affects `/cache/v1/values` and the
`/cache/v1/offset/*` endpoints. **Decision F3: mirror-v3's
RFC-normalised form is correct; any consumer that string-equals the
header is the broken party.**

### C. `/cache/v1/offset/{topic}/{partition}` — unknown topic / partition: status `204` vs `200`

| Case                          | KKV   | mirror-v3 |
| ----------------------------- | ----- | --------- |
| Unknown partition (`/{t}/99`) | `204` | `200`     |
| Unknown topic   (`/nope/0`)   | `204` | `200`     |

Both return an empty body in both cases. **Decision F4: keep
mirror-v3's `200`** (per the F4+ note). Documented here so dependents
can scope on 2xx rather than `status == 200`.

### D. `/cache/v1/offset/{topic}/{partition}` — malformed input: status `400` vs `404`

| Case                                       | KKV   | mirror-v3                        |
| ------------------------------------------ | ----- | -------------------------------- |
| Empty topic segment (`/offset//0`)         | `404` | `400` (empty body)               |
| Non-integer partition (`/offset/{t}/x`)    | `404` | `400` + parse-error body         |

KKV swallows malformed input as a routing-miss 404. mirror-v3 raises a
specific 400. **Decision F5 / F6: keep mirror-v3's `400`** — empty
topic and non-integer partition are unambiguously *bad input*, not
*missing resource*, and the body on the partition-parse case is
operator-friendly during integration. Anyone branching on
`status == 404` for "absent" will need to broaden to "absent ⇔ 404
**or** empty body on 200" anyway because of [C](#c-offset--unknown-topicpartition-status-).

### E. `/openapi.json` — additive on mirror-v3

mirror-v3 serves `/openapi.json` (and `/openapi.yaml`, plus a Scalar
UI at `/docs`); KKV returns 404 for all of these. Additive surface;
unaffected dependents stay unaffected. The committed spec lives at
[`schemas/mirror-v3.cache.openapi.json`](./schemas/mirror-v3.cache.openapi.json)
and is gated via `cargo run -p xtask -- check-openapi`.

## F. (Future) `Content-Type` adapts to `values.type`

Today `/cache/v1/values` returns `text/plain; charset=utf-8`
regardless of how the operator configured `values.type`. Once a
dependent shows up that benefits, we could adapt:

| `values.type`              | `Content-Type`                |
| -------------------------- | ----------------------------- |
| `bytes-base64`             | `application/octet-stream`    |
| `utf8`                     | `text/plain; charset=utf-8`   |
| `json` / `json-parseable`  | `application/x-ndjson`        |

The hook is sketched in the `values` handler's doc comment for when
we want to flip the switch. Not enabled today to preserve byte
parity with KKV.

## Surfaces we know are silently divergent but didn't probe

| Surface                                     | Status                                |
| ------------------------------------------- | ------------------------------------- |
| onupdate webhook dispatcher                 | mirror-v3 does not implement (deferred to a future PR). If a current dependent uses Yolean's KKV in sidecar mode and relies on onupdate, mirror-v3 is **not** a drop-in for them yet. |
| `POST /_admin/v1/shutdown[/{exitcode}]`     | mirror-v3 has it; not compared        |
| `/q/health/ready` (Quarkus)                 | mirror-v3 implements as a drop-in: same path, same `200`/`503` codes, plus a structured `ReadinessReport` JSON body that names any unhealthy mirror by status enum. Existing `@yolean/kafka-keyvalue` Node clients work unchanged. `/q/health` (the wider SmallRye umbrella) is not implemented; we expose `/metrics` (Prometheus) on the metrics port instead |
| Multi-partition `/cache/v1/offset/{t}/{p}`  | the fixture topic uses 1 partition; the multi-partition case is unit-tested in `mirror-cache`'s handler tests |
| Readiness 503 timing                        | KKV: `caught_up` flips false→true once and sticks. mirror-v3: non-sticky — tracks per-mirror lag against the broker high-watermark, source-partition assignment, and per-destination flush progress; falls back to 503 if any of those degrades. Plus a per-destination YAML opt-out (`affects-readiness: false`) for best-effort secondary sinks. |

## Open

- Confirm with PoC operators that none of them depend on the
  KKV-only quirks documented in [A](#a-keys-and-values-body-tombstones-and-ordering),
  [B](#b-values-content-type-casing-spacing),
  [C](#c-offset--unknown-topicpartition-status-),
  [D](#d-offset--malformed-input-status-).
- If a dependent later turns up that does want `204 No Content` for
  the unknown-(topic, partition) case ([C](#c-offset--unknown-topicpartition-status-)),
  the change is a one-liner and the test in this file would catch it.
