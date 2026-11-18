# Cluster monitor

A small local web page to watch your Slurm jobs, the resources they hold and use, your storage and your usage on a cluster, and to manage your files — through an SSH connection you have already opened.

Configured for the [Mila](https://mila.quebec) cluster by default (`mila.py`) ; works on any Slurm cluster with `--site generic`.

> Vibe coded with [Claude](https://claude.com/claude-code).

## Features

- **Jobs** : queue and finished jobs (state, exit code, time and memory used against the request, wait), output tail, accounting details, GPU load of a running job ; kill, hold, release, requeue.
- **Resources** : live graphs of the CPUs, memory and GPUs your jobs hold and use, storage quotas.
- **Storage** : browse, preview, download ; find files unused for 30–90 days ; delete, rename, move (with `--writable`).
- **Cluster** : fair-share, free GPUs, partitions and their limits.

## Requirements

Python ≥ 3.9 (standard library only), Linux or macOS, a recent Firefox, Chromium or Safari, and an SSH alias that keeps its connection open :

```
Host mila
    ControlMaster auto
    ControlPersist yes
    ControlPath ~/.ssh/cm-%r@%h:%p
```

(`mila init` from [milatools](https://github.com/mila-iqia/milatools) writes this for you.)

## Usage

```bash
ssh mila                     # once, in a terminal : log in (one-time code)
python3 server.py            # opens http://127.0.0.1:8765/#t=<token>
```

Another Slurm cluster : `python3 server.py --site generic --ssh-host <alias>`. Stop with `Ctrl+C`.

| option | effect |
|---|---|
| `--writable` | allow deleting, renaming, moving files (read-only otherwise) |
| `--no-actions` | disable the job actions : watch only |
| `--site NAME` | the cluster description file beside `server.py` (`mila` by default, `generic`) |
| `--ssh-host ALIAS` | the SSH alias of the login node |
| `--root PATH` | an extra directory to browse, always read-only (repeatable) |
| `--sample SECONDS` | sample the Resources view in the background |
| `--keep FILE` | keep the last day of samples across restarts |
| `--port N`, `--no-browser`, `--version` | |

On Mila, the GPU occupancy of finished jobs comes from [SARC](https://sarc.mila.quebec) : paste a token in the page, or `export SARC_TOKEN=…`.

## Another cluster

Copy `mila.py` to `<name>.py`, change the storage areas, quota command, links and hints, and run with `--site <name>`. The docstring of `server.Site` lists every attribute.

## Safety

- Listens on `127.0.0.1` only ; every request needs the token printed at start-up.
- **Never opens an SSH connection** : it only reuses yours (`BatchMode=yes`, `ProxyCommand=false`), so it cannot cause failed logins.
- Runs a fixed list of Slurm and shell commands ; values from the page are passed as arguments, never pasted into scripts.
- Storage is read-only unless `--writable` ; every destructive action asks for confirmation.
- Writes nothing to disk unless `--keep` is given.

On a shared workstation the loopback token is not a secret : do not use `--writable` there.
