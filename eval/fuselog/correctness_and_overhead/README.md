# fuselog Correctness Benchmark

This is a benchmark harness for answering two questions about `fuselog` /
`fuselog-apply` when running a real Dockerized, stateful application:

1. **Does fuselog add meaningful latency overhead** to an application's
   requests, compared to running with no state capture at all?
2. **Is the state fuselog captures actually correct** — i.e. can the
   sequence of stateDiffs it produces be replayed with `fuselog-apply` to
   reconstruct a working, non-corrupted copy of the application's state?

It does *not* try to answer question 2 automatically. This harness only
*captures* diffs in a way that makes answering question 2 possible later
(diffs are gap-checked, ordered, and directly consumable by
`fuselog-apply`). Actually reconstructing and byte-comparing state is a
separate, deferred phase — this harness produces the raw material for it and
gives you tools to bisect where corruption creeps in.

This grew out of debugging the MySQL InnoDB corruption issue seen in XDN2's
primary-backup replication (see `XDN2_State.md` for that context) — the
working hypothesis at the time was multi-threaded FUSE write reordering, and
the point of this benchmark is to reproduce/isolate that outside the full
XDN/GigaPaxos stack, against a plain standalone Docker Compose deployment.

---

## Table of contents

- [What each script does](#what-each-script-does)
- [Directory layout](#directory-layout)
- [Prerequisites](#prerequisites)
- [Config files: `apps/*.yaml` and `bench_endpoints.yaml`](#config-files-appsyaml-and-bench_endpointsyaml)
- [Running the capture benchmark](#running-the-capture-benchmark)
- [Understanding the output](#understanding-the-output)
- [Running the replay / bisection tool](#running-the-replay--bisection-tool)
- [The diff filename convention](#the-diff-filename-convention)
- [The fuselog socket protocol](#the-fuselog-socket-protocol-as-implemented-here)
- [Adding a new app](#adding-a-new-app)
- [Known assumptions / things to double-check on your setup](#known-assumptions--things-to-double-check-on-your-setup)
- [Troubleshooting](#troubleshooting)
- [Future work / explicitly out of scope](#future-work--explicitly-out-of-scope)

---

## What each script does

| File | Purpose |
|---|---|
| `capture_bench.py` | **Script A.** Deploys one app (from an XDN descriptor), optionally mounts `fuselog` under its state directory, runs a fixed-count closed-loop read/write HTTP workload against it, and captures a stateDiff after every write. Produces latency stats and a directory of ordered `.diff` files. |
| `replay_checkpoints.py` | **Script B.** Takes the `.diff` files produced by script A, replays them in order via `fuselog-apply` into a scratch directory, snapshots the state at specific checkpoints you choose (e.g. around where you suspect corruption starts), and generates a `docker-compose.yml` with one throwaway container per checkpoint so you can boot each one and manually check whether it's healthy/uncorrupted. |
| `bench_common.py` | Shared library used by both scripts above. Not run directly. Contains descriptor/config parsing, docker-compose generation, and the fuselog Unix-socket capture protocol. |
| `bench_endpoints.yaml` | Config file: for each app variant, points at its XDN descriptor and adds the bench-specific bits the descriptor doesn't have (HTTP endpoints, write payload template, ports). |
| `apps/*.yaml` | XDN app descriptors (the same format XDN itself uses — `components`, `state`, `consistency`, `requests`). These are *not* docker-compose files; both scripts read them and generate real compose files from them on the fly. |
| `compare_summaries.py` | Standalone tool: takes two `summary.json` files from script A (e.g. a `--mode fuselog` run and a `--mode none` run of the same app/workload) and produces a grouped bar chart comparing p50/p95/p99 latency across read, write (excl./incl. capture), and capture-only metrics, plus a plain-text side-by-side table on stdout. |

The split mirrors the two distinct jobs you actually do with this data:
*capture* it once (script A), then *investigate* it as many times as you
want, at whatever checkpoints you choose, without re-running the workload
(script B).

---

## Directory layout

```
fuselog_correctness_bench/
├── README.md                    <- this file
├── bench_common.py               <- shared helpers (library, not run directly)
├── capture_bench.py               <- script A: run the workload, capture diffs
├── replay_checkpoints.py          <- script B: replay diffs, bisect, generate replay compose
├── compare_summaries.py           <- plots two summary.json files side by side
├── bench_endpoints.yaml           <- per-app bench config (endpoints, payloads, ports)
├── apps/
│   ├── bookcatalog-nd.my.yaml     <- XDN descriptor: bookcatalog + MySQL
│   └── bookcatalog-nd.pg.yaml     <- XDN descriptor: bookcatalog + PostgreSQL
├── runs/                          <- output of capture_bench.py (created as you run it)
│   └── <run_id>/
│       ├── live/                  <- fuselog mount point (or plain dir in mode=none)
│       ├── diff/                  <- captured .diff files, in order
│       ├── docker-compose.yml     <- generated compose file for this run
│       ├── fuselog.sock           <- fuselog's Unix control socket
│       └── results/
│           ├── requests.csv       <- per-request latency log
│           ├── summary.json       <- aggregated percentiles + run metadata
│           └── run.log            <- timestamped operational log (mounts, health polls, errors)
└── replays/                       <- output of replay_checkpoints.py (created as you run it)
    └── <name>/
        ├── _work_state/                  <- scratch dir diffs are replayed into (intermediate)
        ├── checkpoint_0000577/           <- snapshot of state after diff #577 applied
        ├── checkpoint_0000980/           <- snapshot of state after diff #980 applied
        ├── apply_latency_summary.json    <- fuselog-apply latency percentiles across the whole replay
        └── docker-compose.yml            <- one throwaway DB service per checkpoint
```

`runs/` and `replays/` aren't checked in anywhere by these scripts — pick
whatever `--out-dir` you want per invocation; nothing hardcodes those paths
except as examples in this README.

---

## Prerequisites

- Docker + Docker Compose v2 (the scripts shell out to `docker compose`, not
  the standalone `docker-compose` v1 binary).
- The `fuselog` and `fuselog-apply` binaries built and either on your `PATH`
  or passed explicitly via `--fuselog-bin` / `--fuselog-apply-bin`.
- FUSE available on the host (`fusermount` / `fusermount3`), and permission
  to mount FUSE filesystems (this generally means being in the right group
  or having `user_allow_other` set in `/etc/fuse.conf`, since we always pass
  `-o allow_other` so Docker's containerized process can access the mount).
- Python 3.9+ with `pyyaml`, `requests`, and (only for `compare_summaries.py`) `matplotlib`:
  ```bash
  pip install pyyaml requests matplotlib --break-system-packages
  ```
- The app images referenced in `apps/*.yaml` already built/pulled
  (`bookcatalog-nd:4`, `mysql:8.0.41-debian`, `postgres:17.4-bookworm`).

---

## Config files: `apps/*.yaml` and `bench_endpoints.yaml`

### `apps/*.yaml` — XDN app descriptors

These are the actual descriptor format XDN uses to define a deployable
service — nothing bench-specific here. Relevant fields both scripts read:

```yaml
name: bookcatalog-nd
components:
  - mysql:
      image: mysql:8.0.41-debian
      stateful: true                     # <- this component holds the data volume
      environments:
        - MYSQL_DATABASE: books
        - MYSQL_ROOT_PASSWORD: root
  - app:
      image: bookcatalog-nd:4
      stateful: false
      entry: true                        # <- this component is the HTTP entrypoint
      port: 80
      environments:
        - DB_TYPE: mysql
        - DB_HOST: mysql                  # <- must match the state component's name
      healthcheck:
        path: /api/books                 # <- used to build the container healthcheck

state: mysql:/var/lib/mysql/             # <- "<component-name>:<container-mount-path>"
```

Both scripts locate the stateful component via the top-level `state:` line,
and the HTTP entrypoint via `entry: true` on a component. Everything else
(`consistency`, `requests:` behavior classification, `deterministic`) is
part of XDN's own semantics and is ignored by this benchmark — it doesn't
need to know about read/write consistency behavior, only where state lives
and how to reach the app over HTTP.

### `bench_endpoints.yaml` — bench-specific config

This is where anything the XDN descriptor doesn't already encode goes:
actual HTTP paths, payload shape for writes, and host ports. One entry per
app variant, keyed by whatever name you want to use with `--app`:

```yaml
bookcatalog-nd-mysql:
  descriptor: apps/bookcatalog-nd.my.yaml   # path relative to this yaml file's own directory
  read_endpoint: "/api/books/{id}"           # {id} is substituted with a book id
  write_endpoint: "/api/books"
  write_method: POST
  write_payload: '{"title":"Book #{counter}","author":"Author #{counter}"}'
  write_headers:
    Content-Type: application/json
  capture_host_port: 18080     # host port capture_bench.py publishes the entry container on
  replay_base_port: 33061      # first host port replay_checkpoints.py uses (auto-incremented per checkpoint)
```

The key (`bookcatalog-nd-mysql`) is what you pass to `--app`, and is also
used verbatim as the `<app_key>` segment in diff filenames (see below) — so
it's how the MySQL and Postgres variants of bookcatalog stay distinguishable
even though both descriptors have the same `name: bookcatalog-nd`.

`{counter}` in `write_payload` is replaced with a monotonically increasing
integer (1, 2, 3, ...) on every write — this makes each write's diff content
deterministic and identifiable later (e.g. you can grep a replayed
checkpoint's DB for `"Book #842"` to confirm write #842 actually landed).

---

## Running the capture benchmark

Basic invocation:

```bash
python3 capture_bench.py \
    --app bookcatalog-nd-mysql \
    --mode fuselog \
    --requests 2000 \
    --read-write-ratio 0.7 \
    --out-dir runs/mysql_run1
```

### Arguments

| Flag | Required | Description |
|---|---|---|
| `--app` | yes | Key from `bench_endpoints.yaml` |
| `--mode` | yes | `fuselog` (mount fuselog on the state dir) or `none` (plain bind mount, no capture — for baseline latency comparison) |
| `--requests` | yes | Total number of closed-loop requests to issue (reads + writes combined) |
| `--read-write-ratio` | no (default `0.7`) | Fraction of requests that are reads. Converted to the smallest integer ratio and used as a **fixed, repeating interleaved pattern** — e.g. `0.7` → 7 reads then 3 writes, repeating, *not* a random per-request draw. This is deliberate: it keeps runs reproducible and comparable across `--mode fuselog` vs `--mode none`. |
| `--out-dir` | yes | Where to put `live/`, `diff/`, `results/`, and the generated compose file. Its basename becomes the `<run_id>` used in container/network names. |
| `--endpoints-config` | no (default `bench_endpoints.yaml` next to the script) | Override if you keep the config elsewhere |
| `--fuselog-bin` | no (default `fuselog` on `PATH`) | Path to the fuselog binary |
| `--health-timeout` | no (default `120`) | Seconds to wait for the entry container to report `healthy` before giving up |

### What happens, in order

1. **fuselog mounts first, always** — before `docker compose up`, never
   after. This was a deliberate decision: the DB container's volume source
   is `live/` from the moment it starts, so fuselog must already be
   intercepting that path before any container touches it. (In `--mode
   none`, this step is skipped entirely — `live/` is just a plain
   directory.) After the mount table shows the mount succeeded, the script
   additionally does a real write→read→delete probe through the mount
   before proceeding — `mountpoint -q` only confirms the mount table entry
   exists, not that fuselog's FUSE handler is actually ready to service I/O
   yet, and a container touching the mount in that gap can fail its own
   init (this is what caused the DB container to fail on first boot when
   the script ran things back-to-back with no gap, even though the exact
   same compose file worked fine run manually with a human-paced delay
   between the two commands).
2. A `docker-compose.yml` is generated from the app's descriptor — one
   service per component, all on a dedicated per-run network
   (`benchnet-<run_id>`), the stateful component's volume pointed at
   `live/`. The stateful component also runs with `user: "${UID}:${GID}"`
   (the script passes your actual host UID/GID as env vars to `docker
   compose`) — this matters because MySQL/Postgres images only attempt to
   `chown` their data directory when running as root, and that chown fails
   with `Operation not permitted` through a fuselog mount that isn't itself
   running as root; running the container as the host user (the same user
   fuselog runs as) means it never needs to chown anything, since
   ownership already lines up. The entry component's `depends_on` is set to
   `condition: service_healthy` against the state component, so Docker
   itself won't start the app container until the DB is healthy.
3. `docker compose up -d`, then poll `docker inspect` on the **stateful**
   container until it reports `healthy` (its healthcheck is the DB's native
   one — `mysqladmin ping` / `pg_isready`) — the time this takes is
   recorded and reported (`state_container_health_time_s` in
   `summary.json`). Then poll the **entry** container's readiness by
   sending real HTTP GETs to `http://localhost:<capture_host_port><healthcheck.path>`
   **from the host**, waiting for an exact `200` — deliberately *not* a
   Docker-level container healthcheck, since that would need `curl` or
   `wget` inside the app image, which can't be assumed (this is what
   caused an app container to sit `unhealthy` forever even though its own
   logs showed it was serving requests fine — the healthcheck command
   itself was failing to execute, not the app).
4. **Diff #0** is captured immediately once healthy — this is the
   baseline, covering whatever the container wrote to its state directory
   during its own startup/schema-init, before any benchmark request has
   been sent. It's saved, not discarded.
5. **Request loop**, `--requests` iterations, single-threaded closed-loop
   (each request fully completes before the next starts), following the
   fixed read/write pattern:
   - **Write**: POST (or whatever `write_method` says) the payload with the
     counter substituted in. The HTTP response is received but **held** —
     the script then immediately captures the stateDiff over the fuselog
     socket and saves it to `diff/`, and *only then* is the request
     considered complete and its latency recorded. This intentionally
     folds the capture step into the measured latency, since that's what a
     synchronous backup pipeline would actually experience. The *capture
     time alone* is also recorded separately, so you can subtract it back
     out when comparing against `--mode none`.
   - **Read**: GET a single book by id, where the id is drawn randomly from
     `[1, requests_written_so_far]`. IDs are **not** checked for existence
     first — hitting an id that was never created (or was deleted, if your
     app supports that) is expected and fine; a 404 is a valid, recorded
     outcome, not an error to avoid.
6. Diff sequence gap check: after the loop, `diff/` is scanned and compared
   against the expected `0..write_count` range. Any gap is logged loudly —
   a gap most likely means a capture failed mid-run (see "aborting" below),
   and a gapped sequence can't be replayed by script B from an empty
   baseline.
7. `results/summary.json` and `results/requests.csv` are written.
8. **Teardown always runs** (in a `finally` block): `docker compose down
   -v`, then unmount fuselog if it was mounted. This happens even if the
   run aborted partway through.

### What "aborting" means

If a stateDiff capture fails mid-run — either the fuselog socket returns an
implausible size header (protocol desync) or the socket closes
unexpectedly — the script **stops the request loop immediately** rather
than reconnecting and continuing. This is deliberate: silently reconnecting
would mask exactly the kind of fuselog bug this benchmark exists to find,
and continuing past a desync would produce a diff sequence with a
silent gap that looks complete but isn't. The failure is logged to
`run.log`, the script proceeds to teardown normally, and it exits with a
non-zero status.

---

## Understanding the output

### `results/requests.csv`

One row per request:

```
idx,type,id_or_counter,http_status,latency_ms,capture_ms
0,read,7,200,4.213,
1,read,3,200,3.987,
...
7,write,1,201,18.442,12.001
```

- `id_or_counter`: for reads, the book id requested; for writes, the
  monotonic write counter (which is also the diff file's count and, per the
  payload template, embedded in the book's title/author).
- `latency_ms`: total request latency. For writes in `--mode fuselog`, this
  **includes** the diff capture time.
- `capture_ms`: only populated for writes in `--mode fuselog` — the time
  spent on the `'g'` round-trip alone, already included in `latency_ms`,
  broken out separately.

### `results/summary.json`

```json
{
  "app": "bookcatalog-nd-mysql",
  "mode": "fuselog",
  "requested_count": 2000,
  "completed_count": 2000,
  "aborted": false,
  "pattern": ["read","read","read","read","read","read","read","write","write","write"],
  "read_write_ratio": 0.7,
  "write_count": 600,
  "state_container_health_time_s": 19.13,
  "read_latency_ms": {"p50": ..., "p95": ..., "p99": ..., "avg": ..., "count": 1400},
  "write_latency_incl_capture_ms": {...},
  "write_latency_excl_capture_ms": {...},
  "capture_latency_ms": {...},
  "diff_sequence_gaps": []
}
```

`state_container_health_time_s` is wall-clock time from just before `docker
compose up -d` is invoked to the stateful component (DB) reporting
`healthy` — useful for seeing whether fuselog itself adds startup latency
to the DB's own init (compare this value between a `--mode fuselog` and
`--mode none` run of the same app). Note this includes ~2s of inherent
polling slop (the health-poll loop checks every 2 seconds), so treat small
differences as noise.

To answer "does fuselog add overhead": run the same app/workload with
`--mode none` and `--mode fuselog`, and compare `write_latency_excl_capture_ms`
between the two runs — if fuselog is transparent, these should be close;
`write_latency_incl_capture_ms` minus `excl_capture_ms` roughly reconstructs
`capture_latency_ms` and tells you fuselog's own added cost on top.
`compare_summaries.py` (below) automates this comparison as a chart.

### Comparing two runs: `compare_summaries.py`

Once you have two `summary.json` files — typically a `--mode fuselog` run
and a `--mode none` run of the same app and workload — plot them side by
side:

```bash
python3 compare_summaries.py \
    --a runs/mysql_fuselog/results/summary.json --label-a fuselog \
    --b runs/mysql_none/results/summary.json --label-b none \
    --out mysql_comparison.png
```

This prints a plain-text p50/p95/p99 table to stdout for every metric group
present in either file (read latency, write latency excl./incl. capture,
capture latency, and stateful-container health time), then writes a grouped
bar chart image with one panel per metric group. If one side has no
capture-latency data (e.g. the `--mode none` side), that panel shows `N/A`
labels rather than a misleading zero-height bar.

`--label-a`/`--label-b` default to each file's own `"mode"` field if
omitted. Both `--a` and `--b` can be summaries from *any* two runs, not just
`fuselog` vs `none` — e.g. two different apps, or two different
`--read-write-ratio` settings, whatever comparison is useful at the time.

### `results/run.log`

Timestamped operational log — mount/unmount events, health poll results,
every capture failure with the raw hex of a bad size header, and the final
gap-check result. This is the first place to look if a run behaved
unexpectedly; it's more detailed than what's printed to stdout.

### `diff/` directory

One file per captured diff, named `p0:<app_key>:<count>.diff` (see next
section), containing the **raw payload bytes exactly as fuselog returned
them** — no added framing, no size header (that's stripped during capture),
directly consumable by `fuselog-apply --statediff=<file>`. This matches
exactly what XDN's own `FuselogStateDiffRecorder.saveStateDiff` writes to
disk in production, so tooling built against real XDN diff directories
works unmodified against these.

---

## Running the replay / bisection tool

Once you have a `diff/` directory from a capture run (especially one where
you suspect the resulting state got corrupted at some point), use
`replay_checkpoints.py` to pinpoint where.

```bash
python3 replay_checkpoints.py \
    --app bookcatalog-nd-mysql \
    --cmtdiff-dir runs/mysql_run1/diff \
    --checkpoints 100,300,577,600 \
    --out-dir replays/mysql_run1_bisect
```

### What it does

1. Discovers all diff files belonging to `--app` in `--cmtdiff-dir`,
   sorted by count.
2. Validates the sequence is **complete and gap-free starting at count=0**
   (unless you pass `--snp-base <dir>` to start from an existing baseline
   snapshot instead of an empty directory — in that case the sequence just
   needs to be gap-free from wherever the baseline left off). If it's not
   complete, the script refuses to proceed rather than silently replaying
   a partial history — you'll get an explicit list of exactly which counts
   are missing.
3. Replays diffs one at a time, in order, via:
   ```
   fuselog-apply <target_dir>/ --statediff=<diff_file>
   ```
   — the same invocation `PrimaryBackupManager`'s `snpDiffApplyThread` uses
   in production, no batching. Each call is individually timed; once the
   whole sequence finishes, p50/p90/p95/p99/avg of `fuselog-apply`'s own
   latency (in ms) are printed to stdout and written to
   `apply_latency_summary.json` under `--out-dir` — this is purely
   `fuselog-apply`'s wall-clock time (the subprocess call itself), not
   including the surrounding file I/O for checkpoint snapshotting.
4. At each count you listed in `--checkpoints`, copies the current state of
   the scratch directory into `checkpoint_<count:07d>/` under `--out-dir`.
5. If `fuselog-apply` itself fails (non-zero exit) on some diff, replay
   stops there and that count is flagged — this may *be* the corruption
   point, distinct from a container failing to boot against otherwise
   successfully-applied state.
6. Generates a `docker-compose.yml` with **one service per checkpoint**,
   containing **only the app's stateful component** (no network, no app
   container — matching the assumption that checking a DB checkpoint for
   corruption doesn't require the app tier running). Image and environment
   variables come straight from the app's XDN descriptor, so the checkpoint
   container matches production configuration exactly.
7. Prints two blocks of ready-to-run commands: one `docker compose ... up
   checkpoint-<N>` line per checkpoint (foreground, not `-d`, so you see
   its logs directly), followed by a single `docker rm -f` line listing
   every checkpoint container, for one-shot cleanup once you're done
   inspecting.

### Manually inspecting a checkpoint

The script's own final output gives you exactly what to run — e.g.:

```
docker compose -f replays/mysql_run1_bisect/docker-compose.yml up checkpoint-577
docker compose -f replays/mysql_run1_bisect/docker-compose.yml up checkpoint-1000

docker rm -f repro-bookcatalog-nd-mysql-checkpoint-577 repro-bookcatalog-nd-mysql-checkpoint-1000
```

Run one `up` line at a time (each blocks in the foreground and streams logs
directly, since `-d` is intentionally omitted) — `Ctrl-C` to stop watching
one before moving to the next. A checkpoint that never reports healthy in
its logs (crash-loops, or its healthcheck — `mysqladmin ping` /
`pg_isready` — never succeeds) is a strong signal that corruption was
introduced at or before that count; you can also check status from another
shell with `docker inspect --format '{{.State.Health.Status}}'
repro-<app_key>-checkpoint-<N>` while it's running. Bring up progressively
earlier/later checkpoints to binary-search the exact diff where things
break, then run the printed `docker rm -f` line once to clean up all of
them in one shot.

### Arguments

| Flag | Required | Description |
|---|---|---|
| `--app` | yes | Same key as used in the capture run — determines image/env/mount-path and which diffs (by `<app_key>` in the filename) get picked up |
| `--cmtdiff-dir` | yes | The `diff/` directory from a capture run |
| `--checkpoints` | yes | Comma-separated counts to snapshot at |
| `--out-dir` | yes | Where checkpoint snapshots + compose file go |
| `--snp-base` | no | Start from an existing snapshot instead of empty; skips the "must start at count=0" requirement |
| `--fuselog-apply-bin` | no (default `fuselog-apply` on `PATH`) | |
| `--stop-after-last-checkpoint` | no (flag) | Stop replaying once the highest requested checkpoint is captured, instead of continuing through the rest of the diffs (useful if you only care about early counts and the diff dir is huge) |
| `--endpoints-config` | no (default `bench_endpoints.yaml` next to the script) | |

---

## The diff filename convention

```
p<pepoch>:<app_key>:<count>.diff
```

- `p0` — placement epoch, always `0` for this benchmark (single-primary,
  no reconfiguration involved). Kept for filename compatibility with real
  XDN `cmtDiff/` directories, which do vary this.
- `<app_key>` — the key from `bench_endpoints.yaml` (e.g.
  `bookcatalog-nd-mysql`), **not** the descriptor's own `name:` field —
  this is what keeps the MySQL and Postgres bookcatalog variants
  distinguishable even though both descriptors say `name: bookcatalog-nd`.
- `<count>` — zero-padded to 7 digits, monotonically increasing, starting
  at `0000000` for the baseline (container-init) diff, then `0000001`,
  `0000002`, ... for each subsequent write, in the order they were
  captured.

Example: `p0:bookcatalog-nd-mysql:0000042.diff` is the 42nd write's diff for
the MySQL variant.

---

## The fuselog socket protocol, as implemented here

`bench_common.capture_state_diff()`:

1. Send the single byte `b'g'` over the connected Unix domain socket.
2. Read exactly 8 bytes, interpreted as a **little-endian signed 64-bit
   size** — this is the payload length that follows.
3. If that size is negative or implausibly large (`> 500 MB`, matching
   `FuselogStateDiffRecorder.MAX_STATEDIFF_BYTES`), raise
   `StateDiffDesyncError` rather than trying to read that many bytes — a
   garbage size header means the socket stream is desynchronized and any
   further reads would just be noise.
4. Otherwise, read exactly that many bytes and return them **verbatim** —
   no size header included, no further parsing. This is deliberately raw:
   correctness verification (a later phase, not part of this benchmark)
   will apply these bytes with `fuselog-apply` directly, so re-encoding
   them here would just be extra surface area for bugs.

`connect_fuselog_socket()` retries the connection for up to ~10 seconds
(100 attempts, 100ms apart), since fuselog daemonizes asynchronously after
the mount command returns — matching the retry loop in
`FuselogStateDiffRecorder.preInitialization`.

---

## Adding a new app

1. Write (or copy) an XDN descriptor into `apps/your-app.yaml`. It needs at
   minimum: `components` (each with `image`, `environments`, and exactly
   one with `stateful: true` and one with `entry: true, port: N`), and a
   top-level `state: <component-name>:<container-path>` line. The entry
   component should have `healthcheck: {path: ...}` pointing at some GET
   endpoint that returns 2xx once the app is actually ready to serve
   traffic (not just "process started").
2. Add an entry to `bench_endpoints.yaml`:
   ```yaml
   your-app-key:
     descriptor: apps/your-app.yaml
     read_endpoint: "/some/{id}"
     write_endpoint: "/some"
     write_method: POST
     write_payload: '{"field":"value #{counter}"}'
     write_headers:
       Content-Type: application/json
     capture_host_port: <pick something unused>
     replay_base_port: <pick something unused>
   ```
3. If your app's state backend isn't MySQL or Postgres, add a branch to
   `db_healthcheck_test()` and `db_default_port()` in `bench_common.py` —
   currently these only recognize `"mysql"` / `"postgres"` substrings in
   the image name.
4. Run `capture_bench.py --app your-app-key --mode none --requests 20`
   first (skipping fuselog) to sanity-check the compose generation and
   HTTP loop against your actual container before adding fuselog into the
   mix.

---

## Known assumptions / things to double-check on your setup

These were flagged as assumptions when the scripts were written, since they
couldn't be verified without your actual environment:

- **Entry container readiness is polled over HTTP from the host**, not via
  a Docker-level container healthcheck — this deliberately avoids assuming
  `curl`/`wget` exist inside the app image (an earlier version used a
  `curl`-based container healthcheck and it silently failed on an image
  without `curl`, leaving the container stuck `unhealthy` even though the
  app itself was serving requests fine). The poll requires an *exact* `200`
  response, not just "not a 5xx" — if your app's healthcheck path can
  legitimately return something other than `200` while ready (e.g. a `204`),
  loosen the check in `wait_for_http_healthy()` in `capture_bench.py`.
- **Stateful container runs as the host UID/GID** (`user: "${UID}:${GID}"`
  in the generated compose file, with `UID`/`GID` injected via
  `compose_env()` in `capture_bench.py`) so its entrypoint never attempts a
  `chown` on the fuselog-mounted directory — MySQL/Postgres only chown when
  running as root, and that chown fails with `Operation not permitted`
  through a non-root fuselog mount. If you add an app whose image *requires*
  running as root for some other reason, this will need revisiting.
- **fuselog mount readiness is confirmed with a real I/O probe** (write,
  read back, delete a marker file) after the mount table shows success, not
  just `mountpoint -q` — a container touching the mount in the gap between
  "mount table entry exists" and "FUSE handler actually ready" can fail its
  own init. This probe file briefly shows up as part of diff #0's captured
  bytes; harmless, but worth knowing it's there if you inspect diff #0
  directly.
- **DB healthcheck / default port chosen by image name substring match**
  (`"mysql"` → `mysqladmin ping`, port 3306; `"postgres"` → `pg_isready`,
  port 5432). Fine for the two current apps; add a branch for anything
  else.
- **`fuselog-apply` invocation**: assumed to be
  `fuselog-apply <target_dir>/ --statediff=<diff_file>`, `target_dir` an
  absolute path ending in `/`. If your actual binary's CLI differs, update
  `run_fuselog_apply()` in `replay_checkpoints.py`.
- **Book IDs are assumed to align with the write counter** (i.e. the
  app assigns auto-incrementing IDs starting at 1, matching the injected
  `{counter}` value) — this is what lets the read step pick a valid-looking
  id without parsing write responses. If the app's ID scheme differs (e.g.
  UUIDs, or IDs don't start at 1), the read step will still send *some* id
  and get whatever response that produces (which is fine per the "arbitrary
  IDs are fine" requirement) — but if you want reads to reliably target
  *known-good* IDs, this assumption needs revisiting.
- **Single fuselog mount, single stateful component.** Only one component
  per app can be `stateful: true` / captured. Multi-container apps where
  two containers each hold independently meaningful state aren't supported
  by this harness (this was an explicit scope decision, not an oversight).

---

## Troubleshooting

**Stateful container exits with `chown: Operation not permitted` in its
logs.** This happens when the container tries to `chown` its data
directory (MySQL/Postgres entrypoints only do this when running as root)
through a fuselog mount that isn't itself running with chown privilege —
the fix already in place is running the container as the host UID/GID (see
"known assumptions" above) so it never attempts the chown at all. If you
still see this, confirm the generated compose file actually has `user:
"${UID}:${GID}"` on the stateful service and that `UID`/`GID` are actually
being injected (`compose_env()` in `capture_bench.py` — `docker compose`
won't substitute `${UID}`/`${GID}` if they aren't in the environment it's
run with, and they won't be set by default in a non-interactive shell).

**Entry container never responds with 200 / `capture_bench.py` times out
at "waiting for `http://localhost:.../...` to respond".** Check
`results/run.log` for the exact URL and timeout, then
`docker compose -f runs/<id>/docker-compose.yml logs app` (or whatever the
entry component is named) — either the app is still starting (bump
`--health-timeout`), the DB took too long, or the app is genuinely
returning something other than exactly `200` at that path (check with
`curl -i` yourself once you know the port).

**Stateful container starts fine manually but fails when the script runs
it (right after mounting fuselog).** This is very likely a timing race
between the FUSE mount finishing and the container's first I/O — running
the two commands manually always has a beat of human delay in between that
the script doesn't have by default. The I/O probe in `mount_fuselog()`
(write→read→delete before returning) exists specifically to close this
race; if you still hit it, the probe itself may need a longer timeout, or
there may be a second, different readiness signal specific to your
`fuselog` build worth probing for instead.

**`fuselog failed to mount on <dir> within 10s`** or **mounted but did not
become I/O-ready within 10s.** Check that the `fuselog` binary is on `PATH`
or that `--fuselog-bin` points at it directly, and that FUSE mounting is
actually permitted for your user (`user_allow_other` in `/etc/fuse.conf`,
or run as a user in the right group). Also check nothing is already
mounted at that path from a previous crashed run — `fusermount -u <dir>`
manually if so.

**`fuselog.sock` never appears / `RuntimeError: failed to connect to
fuselog socket`.** If `--out-dir` was passed as a relative path and
doesn't get resolved to absolute before being handed to the `fuselog`
subprocess (it is resolved automatically as of the current version — check
`args.out_dir = args.out_dir.resolve()` is still the first line after
`parse_args()` in `capture_bench.py` if you've modified it), `fuselog`
daemonizing into the background can end up creating the socket relative to
its own post-fork working directory rather than yours, and the socket
never shows up where the script expects it.

**`garbage stateDiffSize=... raw LE bytes=...` in `run.log`, run aborts.**
This is the protocol-desync detector firing — the socket read something
that isn't a plausible size header. This is itself a signal worth
investigating (it's exactly the kind of bug this benchmark exists to
surface), not just an operational hiccup to retry past. Check what the raw
hex bytes actually decode as, and whether the previous capture's payload
read fully (a short read on the *previous* capture would misalign the
*next* one).

**`diff_sequence_gaps` is non-empty in `summary.json`.** Some write's
capture failed silently in a way the abort logic didn't catch, or the run
was manually interrupted (Ctrl-C) mid-capture. A gapped sequence can't be
replayed from an empty baseline by `replay_checkpoints.py` — you'd need
`--snp-base` pointing at a snapshot from before the gap, if you have one.

**`replay_checkpoints.py` exits immediately with "missing state... cannot
proceed."** This is the gap/completeness validator refusing to replay a
partial history — read the printed missing-count ranges, and either locate
the missing diffs or pass `--snp-base` if you have a valid starting
snapshot from after the gap.

**Port already in use when bringing up a replay checkpoint.**
`replay_base_port` in `bench_endpoints.yaml` is the *first* port; each
subsequent checkpoint in the same `--checkpoints` list increments from
there. If you run multiple bisection sessions concurrently for the same
app, bump `replay_base_port` for one of them or don't run them at the same
time.

**`KeyError: '"title"'` (or similar) when a write request is built.** If
`write_payload` in `bench_endpoints.yaml` was ever substituted with Python's
`.format()`, *any* literal `{...}` in the JSON payload (not just
`{counter}`) gets parsed as a format field — a payload like
`{"title":"Book #{counter}"}` breaks because `{"title":...}` itself looks
like a field named `"title"`. The current code uses `.replace("{counter}",
...)` instead specifically to avoid this — if you see this error, something
is calling `.format()` on the template again.

**`apply_latency_summary.json` percentiles look off / based on very few
samples.** `replay_checkpoints.py` times every `fuselog-apply` call across
the *entire* diff sequence it replays (not just up to your requested
checkpoints, unless `--stop-after-last-checkpoint` is set) — if you only
care about a narrow range, pass `--stop-after-last-checkpoint` so the
percentiles reflect just that range rather than the full history.

---

## Future work / explicitly out of scope

- **Automated state verification.** This harness captures diffs; it
  doesn't yet apply-and-byte-compare against live state automatically.
  That's the natural next phase once this capture tooling is trusted.
- **Multi-container state.** Apps where more than one component holds
  independently meaningful state aren't supported.
- **Concurrent/multi-threaded request generation.** The workload is
  deliberately single-threaded closed-loop, matching the original spec —
  intentionally simpler than `bench_fuse_writer.go`'s multi-threaded
  profiles, since the goal here is deterministic, replayable correctness
  testing rather than throughput stress-testing.
- **Randomized read/write ordering.** Also deliberate — the fixed
  interleaved pattern trades workload realism for reproducibility across
  `--mode fuselog` vs `--mode none` comparisons.
