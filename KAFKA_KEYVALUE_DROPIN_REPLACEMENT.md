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

## Summary

23 probes total: **16 identical**, **7 divergent**. None of the
divergences invalidate the drop-in story for the dependents we know
about today (point lookups via `/cache/v1/raw/{key}`), but several
warrant a decision on whether we want bug-for-bug parity or a clean
mirror-v3 behavior.

| #  | Probe                              | Match? | Triage                            |
| -- | ---------------------------------- | ------ | --------------------------------- |
| 1  | `GET /cache/v1/raw/k1` (latest)     | ✅      | identical                         |
| 2  | `GET /cache/v1/raw/k3`              | ✅      | identical                         |
| 3  | `GET /cache/v1/raw/k2` (tombstoned) | ✅      | identical (both 404)              |
| 4  | `GET /cache/v1/raw/never`           | ✅      | identical                         |
| 5  | `GET /cache/v1/raw/empty-value`     | ✅      | identical (200, empty body)       |
| 6  | `GET /cache/v1/raw/special/chars`   | ✅      | identical (both 404 — slash routed)|
| 7  | `GET /cache/v1/raw/special%2Fchars` | ✅      | identical                         |
| 8  | `GET /cache/v1/raw/plus+key`        | ✅      | identical                         |
| 9  | `GET /cache/v1/raw/<unicode>`       | ✅      | identical                         |
| 10 | `GET /cache/v1/raw/`                | ✅      | identical (404)                   |
| 11 | `GET /cache/v1/raw`                 | ✅      | identical (404)                   |
| 12 | `GET /cache/v1/keys`                | ❌ ⚠️   | **[F1] tombstoned key listed by KKV; ordering & `Content-Type` differ** |
| 13 | `GET /cache/v1/values`              | ❌ ⚠️   | **[F2] tombstoned value emitted as literal `null` by KKV; ordering, trailing newline, `Content-Type` differ** |
| 14 | `GET /cache/v1/offset/{t}/0`        | ⚠️      | body identical; **[F3] `Content-Type` casing/spacing differs** |
| 15 | `GET /cache/v1/offset/{t}/99`       | ❌      | **[F4] KKV → 204, mirror-v3 → 200** |
| 16 | `GET /cache/v1/offset/nope/0`       | ❌      | **[F4]** same shape                |
| 17 | `GET /cache/v1/offset//0`           | ❌      | **[F5] KKV → 404, mirror-v3 → 400**|
| 18 | `GET /cache/v1/offset/{t}/x`        | ❌      | **[F6] KKV → 404; mirror-v3 → 400 + descriptive body** |
| 19 | `GET /cache/v1/unknown`             | ✅      | identical (404)                   |
| 20 | `POST /cache/v1/raw/k1`             | ✅      | identical (405)                   |
| 21 | `DELETE /cache/v1/raw/k1`           | ✅      | identical (405)                   |
| 22 | `GET /`                             | ✅      | identical (404)                   |
| 23 | `GET /openapi.json`                 | ❌      | **[F7] mirror-v3 200, KKV 404** (additive — intentional) |

## Identical behaviour (no action)

- `GET /cache/v1/raw/{key}` for present keys returns identical status,
  body, `Content-Type: application/octet-stream`, and a JSON
  `x-kkv-last-seen-offsets` header. Tombstoned keys return 404 on both.
  Empty-byte values come back as 200 with a zero-byte body, identical
  on both.
- Slash in the URL path is routed before the key matcher on both
  servers, so `/cache/v1/raw/special/chars` is a 404 on both;
  URL-encoded `%2F` works.
- `+` in the key path is treated literally (not as space) by both —
  the dependent's URL-builder is consistent across the swap.
- Multibyte / non-ASCII keys (`ünıçødé`) work identically once
  percent-encoded.
- Empty trailing segment (`/cache/v1/raw/`) and the bare prefix
  (`/cache/v1/raw`) are 404 on both.
- 405 for `POST` / `DELETE` against `raw/{key}`.
- 404 for unknown paths inside `/cache/v1/` and at `/`.

## Divergences worth a call

### [F1] `GET /cache/v1/keys` — tombstones leak into the key listing on KKV

KKV body (decoded for readability):

```
empty-value
k1
special/chars
k2          ← tombstoned (offset 8) but still listed
k3
plus+key
ünıçødé
<trailing newline>
```

mirror-v3 body:

```
empty-value
k1
k3
plus+key
special/chars
ünıçødé
<no trailing newline>
```

Three sub-issues:

1. **Tombstoned key in the listing.** KKV keeps `k2` after the null-value
   record. `GET /cache/v1/raw/k2` correctly returns 404 — the key is
   "in the map" but with a value the cache reports as absent. This is
   internally inconsistent. mirror-v3 removes the entry from both the
   value lookup and the listing.
2. **Ordering.** KKV returns insertion order (the first time a key
   was seen on the partition). mirror-v3 returns lexicographic order
   (BTreeMap iteration).
3. **`Content-Type`.** KKV emits `application/octet-stream`. mirror-v3
   emits `text/plain; charset=utf-8`. The KKV `CacheResource` source
   omits `@Produces` for this endpoint, so JAX-RS falls back to the
   surrounding default; mirror-v3 picks a content-type that matches
   the byte content.
4. **Trailing newline.** KKV adds one; mirror-v3 doesn't.

**Recommendation:** keep mirror-v3's behaviour. The tombstone bug in
KKV is the kind of thing dependents will eventually trip over; the
ordering difference is harmless (operators using `/keys` to enumerate
should sort anyway); the `Content-Type` is a defensible upgrade.
**Verify** no dependent parses the listing position-sensitively or
expects a trailing newline before shipping. If any do, fix them; we
should not preserve a bug.

### [F2] `GET /cache/v1/values` — KKV emits the literal string `null` for tombstones

KKV body:

```
              ← empty line for the empty-value key
v1-updated
with/slashes
null          ← this is KKV serializing a null-valued slot
v3
plusvalue
unicode-value
<trailing newline>
```

mirror-v3 body:

```

v1-updated
v3
plusvalue
with/slashes
unicode-value
<no trailing newline>
```

Same ordering / trailing-newline / tombstone-listing issues as [F1],
plus KKV silently corrupts the values stream by emitting four bytes of
ASCII `"null"` where a missing value should have been. A binary
consumer of `/values` (which is what the `Content-Type` of
`text/plain;charset=UTF-8` on KKV suggests is the only safe consumer
shape anyway) cannot tell whether the literal bytes `null` are a real
value or a serialization artefact.

**Recommendation:** keep mirror-v3's behaviour. The `null` emission is
unambiguously a KKV bug; nobody should depend on it. Same `Content-Type`
delta as [F1] (mirror-v3 normalises the casing/spacing to
`text/plain; charset=utf-8`).

### [F3] `Content-Type` casing / spacing on `text/plain;charset=UTF-8`

| Header           | KKV                            | mirror-v3                      |
| ---------------- | ------------------------------ | ------------------------------ |
| `Content-Type`   | `text/plain;charset=UTF-8`     | `text/plain; charset=utf-8`    |

Functionally identical per [RFC 9110 §5.6.6](https://www.rfc-editor.org/rfc/rfc9110#section-5.6.6)
(charset names are case-insensitive; the `;` parameter separator can
have surrounding whitespace). Affects `/cache/v1/keys`, `/cache/v1/values`,
`/cache/v1/offset/...`.

**Recommendation:** keep mirror-v3's form. Any compliant consumer
treats them the same; non-compliant string-equality checks would be
the dependent's bug. Not worth changing.

### [F4] `GET /cache/v1/offset/{topic}/{partition}` for unknown partition or topic

| Case                    | KKV   | mirror-v3 |
| ----------------------- | ----- | --------- |
| Unknown partition (`99`)| `204 No Content` | `200 OK` (empty body) |
| Unknown topic           | `204 No Content` | `200 OK` (empty body) |

Both return an empty body. The status diverges. KKV's `204` is
arguably more idiomatic ("no representation to return"); mirror-v3
returns `200` because our handler produces a `text/plain` body and
just lets it be the empty string when the offset is unknown.

**Recommendation:** change mirror-v3 to return `204` for the
unknown-(topic, partition) cases. Cheap to fix, idiomatic, matches
KKV. Any dependent that branches on `r.status_code == 200` to mean
"found" gets a useful upgrade; any that already handles 2xx gets the
same behaviour.

Tracking: should I queue this as a follow-up? — yes.

### [F5] `GET /cache/v1/offset//0` (empty topic) — `400` vs `404`

| KKV   | mirror-v3 |
| ----- | --------- |
| `404` | `400 Bad Request` |

KKV routes empty topic segments as "no such resource" (Quarkus /
JAX-RS path matching default). mirror-v3 explicitly validates
`if topic.is_empty()` and emits `400`. Semantically, empty input is
malformed; `400` is the more honest answer. KKV is incidental
behaviour, not policy.

**Recommendation:** keep mirror-v3's behaviour. Document the
difference for triage. Dependents that distinguish 4xx
sub-classes are unusual.

### [F6] `GET /cache/v1/offset/{topic}/x` (partition is not an integer)

| Header        | KKV               | mirror-v3                                          |
| ------------- | ----------------- | -------------------------------------------------- |
| Status        | `404`             | `400 Bad Request`                                  |
| Body          | empty             | `"Invalid URL: Cannot parse value at index 1 with value \`x\` to a \`u32\`"` |

KKV: JAX-RS couldn't match `x` against `@PathParam Integer partition`,
falls back to 404 (resource not matched). mirror-v3: axum's path
extractor rejects with `400` and the parse error in the body.

**Recommendation:** the mirror-v3 body is operator-friendly during
development and not a security risk (the input is already in the URL
the client sent). Two choices:

- **Match KKV:** silence the body, return `404`. Cheap; matches the
  drop-in promise. But hides the real reason.
- **Keep mirror-v3 as-is:** keep `400` + body. Better DX. But
  programmatically distinguishing "bad partition" from "no such key"
  becomes destination-dependent.

I'd take the middle ground: keep `400` (it's the right status) but
empty the body to match KKV's silence. Small change. If we want to
match KKV byte-for-byte, switch to `404` empty. Wants a decision.

### [F7] `GET /openapi.json` — added by mirror-v3, absent on KKV

mirror-v3 returns `200` + the OpenAPI 3.1 document. KKV returns `404`.
This is additive. The committed spec lives at
[`schemas/mirror-v3.cache.openapi.json`](./schemas/mirror-v3.cache.openapi.json).

**Recommendation:** keep. Dependents that don't hit `/openapi.json`
are unaffected; the new endpoint helps operators discover the surface.
Also: `/openapi.yaml` and the Scalar UI at `/docs` are mirror-v3-only
in the same way. Document in README.

## What we know is silently divergent but didn't probe

| Surface                                     | Status                          |
| ------------------------------------------- | ------------------------------- |
| onupdate webhook dispatcher                 | mirror-v3 does not implement   |
| `POST /_admin/v1/shutdown[/{exitcode}]`     | mirror-v3 has it; not compared  |
| `/q/health` / `/q/health/ready` (Quarkus)   | mirror-v3 does not implement   |
| Multi-partition `/cache/v1/offset/{t}/{p}`  | fixture uses 1 partition only   |
| Readiness 503 timing (window before catch-up) | both serve 503; deeper compare needed |

The onupdate dispatcher was explicitly out of scope for this PR. If a
PoC dependent uses Yolean's KKV in sidecar mode and relies on
onupdate, the swap is **not** a drop-in for them yet — they'd need to
move dispatch elsewhere or wait for the follow-up.

## Triage actions

| ID  | Action                                                                   | Priority |
| --- | ------------------------------------------------------------------------ | -------- |
| F4  | Return `204` from `/cache/v1/offset/{t}/{p}` when (t, p) has no offset    | should-do |
| F6  | Drop the parse-error body from `/cache/v1/offset/{t}/x` (keep `400`)     | optional |
| F1  | Confirm no current dependent reads `/cache/v1/keys` (ordering/trailing nl) | confirm-then-skip |
| F2  | Confirm no current dependent reads `/cache/v1/values` (it's nearly always wrong on KKV anyway) | confirm-then-skip |
| F3  | No change                                                                | n/a |
| F5  | No change; document                                                      | n/a |
| F7  | No change; document                                                      | n/a |

Open per the user note: "It's quite possible that we'll want divergent
behavior for anything that isn't explicitly depended on in current use
cases." Confirm with PoC operators which of these we should preserve
bug-for-bug.
