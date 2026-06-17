# XDN Primary-Backup Prototype

A standalone prototype that runs a single application as three Docker
containers — one primary, two backups — behind a Go reverse proxy, with
pluggable strategies for replicating writes from the primary to the backups.
It exists to experiment with replication/sync mechanisms and backup renewal
strategies for the [XDN](https://github.com/fadhilkurnia/xdn) project,
independent of the main GigaPaxos-based system.

## How it works

```
        ┌──────────────┐
client →│  proxy :2300  │→ primary container (reads + writes)
        └──────────────┘
        ┌──────────────┐
client →│  proxy :2301  │→ backup1 container (reads only; writes → primary)
        └──────────────┘
        ┌──────────────┐
client →│  proxy :2302  │→ backup2 container (reads only; writes → primary)
        └──────────────┘
```

Each of the three ports is fronted by its own `NodeProxy`. Read requests go
straight to that node's own container. Write requests (`POST`/`PUT`/`PATCH`/
`DELETE`) are redirected to the primary; once the primary responds, a sync
step propagates the resulting state diff out to all backup containers before
the response is returned to the client.

The "sync step" is intentionally pluggable (`--sync` flag), so the same proxy
and container-management logic can be benchmarked against three different
replication backends:

- **rsync** — diffs the primary's live data dir against its own snapshot dir
  using `rsync --write-batch`, then replays that batch onto every backup's
  snapshot dir via `--read-batch`.
- **fuse-cpp** — a FUSE filesystem (`fusecpp`) mounted over the primary's
  live dir captures writes as they happen; a companion `fusecpp-apply`
  binary replays the captured diff onto each backup snapshot.
- **fuse-rust** — same idea as fuse-cpp, implemented as a Rust FUSE core
  (`fuserust`) plus one `fuserust-apply` process per backup, communicating
  over Unix sockets instead of files on disk.

Independently of replication, a renewal loop periodically destroys and
re-creates each backup container from a fresh copy of its snapshot dir
(blue/green style, alternating between `live` and `live2` directories) to
bound the divergence between a backup's live state and its snapshot, and to
exercise the container lifecycle code under churn.

## Repository layout

```
.
├── main.go              # wiring: parses flags, builds syncer, proxy, renewer
├── config.go            # Config struct + DefaultConfig()
├── internal/
│   ├── docker/
│   │   ├── manager.go     # Docker container lifecycle (create/start/stop/network)
│   │   └── dbconfig.go    # per-DBType image/env/healthcheck settings
│   ├── proxy/
│   │   └── proxy.go       # reverse proxy: read/write routing, sync hook, switchover log
│   ├── renewer/
│   │   └── renewer.go     # blue/green backup container renewal loop
│   ├── rsync/
│   │   └── syncer.go      # rsync-based sync backend
│   ├── fuse/
│   │   ├── cpp_process.go   # manages the fusecpp subprocess
│   │   ├── cpp_syncer.go    # rsync-based diff capture/apply over the fusecpp socket
│   │   ├── rust_process.go  # manages fuserust + fuserust-apply subprocesses
│   │   └── rust_syncer.go   # diff capture/apply over fuserust Unix sockets
│   └── types/
│       └── types.go       # shared ContainerInfo struct
├── bench.js             # k6 load test script (mixed read/write workload)
└── graph.py             # plots p50/p99 latency from k6 results + switchover markers
```

(Package directories above reflect the module's internal import paths; adjust
if your local layout differs.)

## Requirements

- Go 1.25+
- Docker, with the daemon reachable from your user (the manager pulls
  images, creates containers/networks, and binds ports on `127.0.0.1`)
- `rsync` on `PATH` (used by the rsync backend and by the renewer/seeding
  logic regardless of which sync backend is active)
- For `fuse-cpp`: the `fusecpp` / `fusecpp-apply` binaries, `fusermount3` (or
  `fusermount`), and passwordless `sudo` for the commands in
  `cpp_process.go` (mount requires `allow_other`, which needs root)
- For `fuse-rust`: the `fuserust` / `fuserust-apply` binaries (no `sudo`
  required for these)
- [k6](https://k6.io/) and Python 3 with `matplotlib`/`numpy`, only if you
  want to run `bench.js` / `graph.py`

## Running

```bash
go run . --sync=rsync
```

Useful flags (see `config.go` / `main.go` for the full set and defaults):

| Flag | Default | Purpose |
|---|---|---|
| `--db` | `sqlite` | `sqlite`, `mysql`, or `postgres` |
| `--sync` | `rsync` | `rsync`, `fuse-cpp`, or `fuse-rust` |
| `--refresh-interval` | `30s` | backup renewal cycle period |
| `--log-fuse` | `false` | log fuse-rust capture/apply timing |
| `--log-proxy` | `false` | log proxy-side primary/sync/total timing |
| `--no-redirect` | `false` | disable write redirection (each proxy writes to its own container — breaks consistency, useful as a baseline) |
| `--no-renewer` | `false` | disable the blue/green backup renewal loop |

On startup it pulls/verifies the app image (and DB image, if not SQLite),
starts the three containers, seeds backup snapshots from the primary, brings
up the proxy on `:2300`/`:2301`/`:2302`, and (unless disabled) starts the
renewal loop. `Ctrl-C` triggers graceful shutdown of proxies, renewer
containers, and the main containers.

A `switchovers.log` file is written next to the binary, recording every time
the proxy's target for a role is swapped (timestamp + role), which
`graph.py` uses to annotate the latency plots.

## Benchmarking

With the prototype running:

```bash
k6 run bench.js
python3 graph.py results.json switchovers.log
```

`bench.js` runs a constant-arrival-rate workload (default 100 req/s for
100s) against all three ports simultaneously, with an 80/20 read/write mix
against a `/api/books` endpoint. `graph.py` buckets `http_req_duration` by
second and plots p50 (and optionally p99) per port, overlaying red markers
wherever a switchover occurred during the run, producing `reads.png` and
`writes.png`.

## Notes / known constraints

- The proxy's write-redirect logic assumes a single primary; there's no
  leader election here — `SwapTarget` only repoints a backup's *read*
  target after renewal, not the primary itself.
- `fuse-cpp` requires `sudo` for mount/unmount/chmod; if that's not
  available in your environment, use `fuse-rust` or `rsync` instead.
- This prototype is intentionally separate from XDN's GigaPaxos-based
  replication; it's meant for isolating and measuring the cost of different
  sync mechanisms under a much simpler primary-backup model.
