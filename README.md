# Cluster monitor

A small local web page to watch your Slurm jobs, the resources they hold and use, your storage and your usage on a cluster, and to manage your files — through an SSH connection you have already opened.

Configured for the [Mila](https://mila.quebec) cluster by default (`mila.py`) ; works on any Slurm cluster with `--site generic`.

> Vibe coded with [Claude](https://claude.com/claude-code).

## Features

- **Jobs** : queue and finished jobs (state, exit code, time and memory used against the request, wait), output tail, accounting details, GPU load of a running job ; kill, hold, release, requeue (with `--actions`).
- **Resources** : live graphs of the CPUs, memory and GPUs your jobs hold and use, storage quotas.
- **Storage** : browse, preview, download ; find files unused for 30–90 days ; delete, rename, move (with `--writable`).
- **Cluster** : fair-share, free GPUs, partitions and their limits.

## Requirements

Python ≥ 3.9 (standard library only), Linux or macOS, a recent Firefox, Chromium or Safari, and an SSH alias that keeps its connection open. On Mila, mila init from milatools sets this up.

## Usage

```bash
ssh mila                     # once, in a terminal : log in (one-time code)
python3 server.py            # prints http://127.0.0.1:8765/#t=<token> and opens it
```

Another Slurm cluster : `python3 server.py --site generic --ssh-host <alias>`. Stop with `Ctrl+C`.

| option | effect |
|---|---|
| `--writable` | allow deleting, renaming, moving files (read-only otherwise) |
| `--actions` | allow the job actions : kill, hold, release, requeue (watch only otherwise) |
| `--site NAME` | the cluster description file beside `server.py` (`mila` by default, `generic`) |
| `--ssh-host ALIAS` | the SSH alias of the login node |
| `--root PATH` | an extra directory to browse, always read-only (repeatable) |
| `--sample SECONDS` | sample the Resources view in the background |
| `--keep FILE` | keep the last day of samples across restarts |
| `--port N` (`0` : a free one), `--no-browser`, `--version` | |

On Mila, the GPU occupancy of finished jobs comes from [SARC](https://sarc.mila.quebec) : paste a token in the page, or `export SARC_TOKEN=…`.

## Another cluster

Copy `mila.py` to `<name>.py`, change the storage areas, quota command, links and hints, and run with `--site <name>`. The docstring of `server.Site` lists every attribute.

## Safety

- Listens on `127.0.0.1` only ; every request needs the token printed at start-up.
- **Never opens an SSH connection** : it only reuses yours (`BatchMode=yes`, `ProxyCommand=false`), so it cannot cause failed logins.
- Runs a fixed list of Slurm and shell commands ; values from the page are passed as arguments, never pasted into scripts.
- Jobs are only watched unless `--actions`, storage is read-only unless `--writable` ; every destructive action asks for confirmation.
- The token is never on a command line (`ps`) : the browser is started with a private file (`~/cluster-monitor-opening-…/open.html`, owner only, removed once the page has loaded) that sends it to the address.
- Keys and credential files of the cluster account (`~/.ssh`, `.netrc`, `.aws`, tokens, shell histories… : `SECRETS` in `server.py`) are never previewed nor downloaded, even through a symbolic link, and neither renamed nor moved. A credential kept elsewhere is a file like any other.
- Writes nothing else to disk unless `--keep` is given.

On a workstation shared with other users, loopback is shared too : run it there without `--actions` and without `--writable` (the defaults).
