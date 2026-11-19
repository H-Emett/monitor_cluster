"""Local web monitor and file manager for the Slurm jobs and the storage of a cluster reached through an already open SSH connection."""

import argparse
import collections
import importlib.util
import json
import math
import os
import posixpath
import re
import secrets
import shlex
import shutil
import signal
import socket
import stat
import statistics
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

VERSION = "0.1.7"
# The date of what was never read. The clock of `time.monotonic` starts anywhere, near zero in a new process on some
# systems (seen on macOS) : no fixed number is old for it, and a kept answer is used only when there is one.
NEVER = float("-inf")

##### The cluster : what is known of it beyond Slurm #####


class Site:
    """What the monitor knows of one cluster beyond Slurm itself : its storage areas and what they are for, how their quota
    is read, its free GPUs, its links, the usage service that rates the GPU jobs.

    This class is a Slurm cluster of which nothing is known (`--site generic`) : the home directory alone, the size of
    its file system for want of a quota, no link, no usage service. A real cluster is a subclass named `Site` in a
    file beside this one, chosen by its name (`--site mila`, the default, is `mila.py`) : to add a cluster, copy
    `mila.py` and change what differs. Everything that names a cluster is in such a file ; the texts and scripts of
    a site are trusted like the rest of the code, and the page shows them as text.

    - `NAME`, `HOST`, `CONNECT` : the name shown, the SSH alias used without `--ssh-host`, the sentence shown when the
      connection is closed (`{host}` is filled, a `backquoted` part is shown as code).
    - `AREAS` : the environment variables of the cluster that name the personal storage areas, the only places where
      files are changed ; `USES` : what each is for ; `BACKUP` : what a deletion there loses ; `PURGE` : the days after
      which an area deletes an unused file, and `REMIND` what to say of it.
    - `SHARED` : patterns of the paths of shared directories (`--root`) and what each family is.
    - `QUOTA`, `quota` : the script that reads use and quota, and the reading of its output ; `AVAIL`, `free` : the same
      for the free GPUs (an empty `AVAIL` : none).
    - `LINKS` : labels and https addresses ; `HINTS` : a text, a command and a text for the GPU tab of a job (`{node}`
      and `{host}` are filled) ; `NOTES` : sentences of the page that name what the cluster has (`KNOWN`).
    - `USAGE`, `rating` : the usage service that rates the GPU jobs with a personal token. A site that has one gives
      the `REQUIRED` texts and writes `rating` ; the token, its rules, the request and the keeping of the answers are
      here, the same for every site."""

    NAME = "Slurm cluster"
    HOST = ""
    CONNECT = "In a terminal run `ssh {host}`, log in, then press Refresh now."
    AREAS = ("HOME",)
    USES = {"HOME": "The home directory of the account."}
    BACKUP = {}
    PURGE = {}
    REMIND = {}
    SHARED = ()
    LINKS = ()
    HINTS = ()
    # `cluster` : a sentence added under « My account » ; `gpu_hours` : one added to the title of the GPU-hours tile ;
    # `resources` : where the load of the GPUs over a period is found, after « is not recorded by Slurm : » ;
    # `counters` : how often the nodes refresh the counters of `sstat` (`JobAcctGatherFrequency`), in the note of the live graphs.
    NOTES = {}
    KNOWN = ("cluster", "gpu_hours", "resources", "counters")
    QUOTA = 'for root in "$@"; do df -P -B1 -- "$root" 2>/dev/null | tail -n 1; echo \'{sep}\'; done'
    AVAIL = ""
    # The usage service, None when the cluster has none. `service` : its name in a sentence ; `short` : in a few letters ;
    # `measure` : what it measures of a GPU ; `poor`, `good` : the two thresholds of that share ; `page` : the https
    # address where the token is made, and `sign` what it asks ; `field` : the name of the token there ; `host` : where the
    # token is sent ; `variable` : the environment variable of the server that can hold it ('' : none).
    USAGE = None
    REQUIRED = ("service", "short", "measure", "poor", "good", "page", "sign", "field", "host", "variable")
    TOKEN = re.compile(r"[\x21-\x7e]{1,8192}")
    EVERY = 600
    AGAIN = 120
    NAMED = re.compile(r"[a-z][a-z0-9_]*")

    def __init__(self, environ=None):
        """`environ` is the environment of the server : the token of the usage service is taken there, in the variable
        the site names ; one that cannot be a token is refused (ValueError), without being quoted."""
        missing = [key for key in self.REQUIRED if key not in (self.USAGE or dict.fromkeys(self.REQUIRED))]
        if missing:
            raise ValueError(f"the site {self.NAME} describes a usage service without {', '.join(missing)}")
        variable = self.USAGE["variable"] if self.USAGE else ""
        self.token = (environ or {}).get(variable, "").strip() if variable else ""
        if self.token and not self.TOKEN.fullmatch(self.token):
            raise ValueError(f"{variable} holds a space, a line end or a character that is not ASCII : it is not used. Export the token alone.")
        self.given = bool(self.token)
        self.rated = {}
        self.lock = threading.Lock()
        self.rating_lock = threading.Lock()

    @classmethod
    def load(cls, name, folder=None):
        """The class `Site` of the file `name.py` of `folder` (the one of this file) ; this class for `generic`.

        The name is one of a file of the tool : lower-case letters, digits and `_`, no path, and not the name of a
        module of the standard library, which a file of that name beside this one hides from the server (`standard`).
        The file is run as code. Called on this class only (`server.Site.load`)."""
        folder = Path(folder or Path(__file__).resolve().parent).resolve()
        if name == "generic":
            return cls
        if not cls.NAMED.fullmatch(name or "") or name in ("server", "generic") or not (folder / f"{name}.py").is_file():
            raise ValueError(f"no site named {name!r} : --site takes generic, or the name of a file of {folder} that describes a cluster (mila)")
        if cls.standard(name):
            raise ValueError(f"{name}.py has the name of a module of Python, which it hides from the server : rename it")
        # A site imports this module by its name : it must get this one, not a second copy, when this file is the program.
        if getattr(sys.modules.setdefault("server", sys.modules[__name__]), "Site", None) is not Site:
            raise ValueError("another module named server is loaded : a site file cannot find this one")
        spec = importlib.util.spec_from_file_location(f"monitor_site_{name}", folder / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        kind = getattr(module, "Site", None)
        if not (isinstance(kind, type) and issubclass(kind, Site)):
            raise ValueError(f"{name}.py does not describe a cluster : it must define a class Site(server.Site)")
        return kind

    @staticmethod
    def standard(name):
        """True when `name` is a module of the standard library of Python : the names of Python 3.10 and later, or,
        on 3.9, a module found in the folders of the standard library. A module installed elsewhere (site-packages)
        is not one : the server imports none."""
        if name in getattr(sys, "stdlib_module_names", ()) or name in sys.builtin_module_names:
            return True
        folders = {Path(sysconfig.get_paths()[key]).resolve() for key in ("stdlib", "platstdlib")}
        for folder in folders:
            if (folder / f"{name}.py").is_file() or (folder / name / "__init__.py").is_file() or any((folder / "lib-dynload").glob(f"{name}.*")):
                return True
        return False

    def report(self):
        """The lines the site adds to those of the terminal at start-up : here, whether the usage service has a token."""
        if not self.USAGE:
            return []
        variable, page = self.USAGE["variable"], self.USAGE["page"]
        return ["GPU occupancy : " + (f"token of {variable}." if self.given else f"off ; enter a token in the page (Jobs, Finished), from {page}"
                                      + (f", or export {variable}." if variable else "."))]

    def describe(self):
        """What the page shows of the site : texts and addresses, no state."""
        return {"name": self.NAME, "connect": self.CONNECT, "uses": self.USES, "backup": self.BACKUP, "purge": self.PURGE, "remind": self.REMIND,
                "shared": [list(item) for item in self.SHARED], "links": [list(item) for item in self.LINKS],
                "hints": [list(item) for item in self.HINTS], "notes": {key: self.NOTES[key] for key in self.KNOWN if key in self.NOTES},
                "usage": self.USAGE}

    ##### Storage and free GPUs #####

    def quota(self, text, areas, sep):
        """Fills `used`, `limit` (bytes), `files`, `files_limit` of the storage areas from the output of `QUOTA`, and
        returns the text to show as it is under them ('' : none).

        `QUOTA` gets the path of each personal area the cluster has, in the order of `AREAS`, and `{sep}`, a
        separator of the run that no output can forge. Here : one `df` line per area. The figures are those of the
        file system that holds the area — everyone's files, unless the cluster mounts it with a quota of its own —
        and are marked so (`whole`)."""
        for area, line in zip([area for area in areas.values() if area["personal"]], text.split(sep)):
            fields = line.split()
            if len(fields) >= 4 and fields[1].isdigit() and fields[2].isdigit():
                area.update(used=int(fields[2]), limit=int(fields[1]), whole=True)
        return ""

    def free(self, text):
        """The free GPUs by type, from the output of `AVAIL` : `{"name", "free", "total"}` ; none known here."""
        return []

    ##### Usage service : the token and the answers, the same for every site that has one #####

    class Direct(urllib.request.HTTPRedirectHandler):
        """No redirection is followed : a service may answer an unknown token with one, to its sign-in page."""

        def redirect_request(self, *args, **kwargs):
            return None

    @staticmethod
    def share(value):
        """A figure of the usage service as a share between 0 and 1 ; None when absent ; an error for anything else."""
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
            raise ValueError("the usage service gave an answer of an unknown form")
        return value

    def enrolled(self):
        """True when the usage service has a token to be asked with."""
        return bool(self.USAGE and self.token)

    def enrol(self, token):
        """Takes the token of the usage service typed in the page, or forgets the one in use (the empty text, exactly) ; says whether one is set.

        The token is kept in the memory of the server only : never written, logged nor sent back to the page, and
        lost when the server stops. It replaces the one of the environment, and the answers read with the previous
        token are dropped. No message quotes the token."""
        if not self.USAGE:
            raise ValueError("no usage service is known for this cluster")
        if not isinstance(token, str):
            raise ValueError("the token must be a text")
        forget, token = token == "", token.strip(" \t\r\n")
        if not forget and not self.TOKEN.fullmatch(token):
            raise ValueError(f"the token holds a space, a line end or a character that is not ASCII, or is too long : paste the {self.USAGE['field']} alone")
        with self.lock:
            self.token = token
            self.rated = {}
        return bool(token)

    def asked(self, address, token):
        """The JSON answer of the usage service to one request, within 30 seconds and 16 MiB ; a fixed message otherwise.

        Whatever fails in the request, the message names no detail of it : the text of an error of the HTTP
        layer can hold the header that carries the token."""
        deadline, body = time.monotonic() + 30, b""
        try:
            request = urllib.request.Request(address, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
            with urllib.request.build_opener(self.Direct).open(request, timeout=15) as response:
                while chunk := response.read1(65536):
                    body += chunk
                    if len(body) > 16 * 2**20 or time.monotonic() > deadline:
                        raise ValueError
        except urllib.error.HTTPError as error:
            error.close()
            refused = error.code in (301, 302, 303, 307, 308, 401, 403)
            raise ValueError("the usage service refused the token" if refused else f"the usage service answered {error.code}") from None
        except Exception:
            raise ValueError("the usage service did not answer") from None

        def refuse(constant):
            raise ValueError(constant)

        try:
            return json.loads(body, parse_constant=refuse)
        except (ValueError, RecursionError):
            raise ValueError("the usage service gave an answer of an unknown form") from None

    def efficiency(self, days):
        """What the usage service says of the GPU jobs of the last `days` days (`rating`), kept `EVERY` seconds, a failure `AGAIN`.

        The request goes from this computer to the service, not through the login node, with the token of the
        environment of the server or the one typed in the page (`enrol`) : the token is never sent to the page,
        written, nor logged. One request at a time ; an answer read with a token that was replaced meanwhile is
        given to its caller, not kept."""
        if not self.USAGE:
            raise ValueError("no usage service is known for this cluster")
        with self.rating_lock:
            with self.lock:
                token = self.token
                read, value = self.rated.get(days, (NEVER, None))
            if not token:
                variable = self.USAGE["variable"]
                raise ValueError(f"no {self.USAGE['short']} token : create one at {self.USAGE['page']} and enter it in the page (GPU occupancy : set the token)"
                                 + (f", or export it as {variable} before starting the server" if variable else ""))
            failed = isinstance(value, ValueError)
            if value is None or time.monotonic() - read >= (self.AGAIN if failed else self.EVERY):
                try:
                    value = self.rating(days, token)
                except ValueError as error:
                    value = error
                with self.lock:
                    if self.token == token:
                        self.rated[days] = (time.monotonic(), value)
        if isinstance(value, ValueError):
            raise ValueError(str(value))
        return value

    def rating(self, days, token):
        """One reading of the usage service, written by the site that has one : `{"days", "start", "end"}` (what was asked,
        as the tile says it), `"jobs"` : a list of `{"id", "occupancy", "utilization"}` (the Slurm id as a text, two shares
        between 0 and 1 or None : `share`), `"measured"` : how many have an occupancy, `"mean"` : their mean or None,
        `"cut"` : True when the service gave only a part of the jobs. It asks the service with `asked`, and raises
        ValueError with a message that quotes nothing of the token."""
        raise ValueError("no usage service is known for this cluster")


##### Remote side : fixed, read-mostly commands through the SSH master connection #####


class NotConnected(Exception):
    """No open SSH master connection to the cluster."""


class Busy(Exception):
    """The SSH connection refused a command : too many of them at once on the same connection."""


class Cluster:
    """Runs a fixed set of commands on the login node ; never opens a connection by itself.

    Every command is short (Slurm queries, one directory level, a bounded tail) : the login nodes are entry
    points only. The two walks of a directory tree (`measure`, `unused`) are stopped after `WALK` seconds."""

    JOB_ID = re.compile(r"[0-9]+(\+[0-9]+|_([0-9]+|\[[0-9,:%-]+\]))?")
    # The limit of tasks running at once that squeue prints in the identifier of a pending array (`123_[3-10%2]`) : an option of
    # the submission, not a part of the tasks to act on — the commands are given `123_[3-10]`, the form their manual has.
    THROTTLE = re.compile(r"%[0-9]+(?=\])")
    # What is left once the limit is removed : a job, a task, or tasks as numbers, ranges and steps — nothing else is sent to a command.
    TASKS = re.compile(r"[0-9]+(\+[0-9]+|_([0-9]+|\[[0-9]+(-[0-9]+(:[0-9]+)?)?(,[0-9]+(-[0-9]+(:[0-9]+)?)?)*\]))?")
    # The characters of an identifier at most : the whole list of the pending tasks of an array (`SLURM_BITSTR_LEN=0`) may be long.
    LONGEST = 16384
    # The settings of Slurm that the account may export on the cluster (`SQUEUE_STATES`, `SCANCEL_NAME`, `SLURM_CLUSTERS`… : the
    # ENVIRONMENT VARIABLES of each manual) would filter or reshape what the commands list and act on : none is kept, the
    # commands are run as written here.
    PLAIN = ('command unset "${!SQUEUE_@}" "${!SACCT_@}" "${!SCANCEL_@}" "${!SINFO_@}" "${!SCONTROL_@}" "${!SSTAT_@}" SLURM_CLUSTERS SLURM_STEP_GRES SLURM_GRES '
             "SLURM_OVERLAP SLURM_TIME_FORMAT SLURM_BITSTR_LEN 2>/dev/null || :; ")
    ANSI = re.compile(r"\x1b\[[0-9;]*m")
    UNITS = {"": 1, "K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40, "P": 2**50}
    QUEUE = ("id", "partition", "state", "elapsed", "limit", "left", "start", "cpus", "mem", "gres", "priority", "where", "name")
    SQUEUE = "%i|%P|%T|%M|%l|%L|%S|%C|%m|%b|%Q|%R|%j"
    RECORD = ("id", "partition", "state", "exit", "elapsed", "limit", "submit", "start", "end", "nodes", "tres",
              "reqmem", "maxrss", "cpu", "restarts", "ntasks", "name")
    SACCT = "JobID,Partition,State,ExitCode,ElapsedRaw,TimelimitRaw,Submit,Start,End,NodeList,AllocTRES,ReqMem,MaxRSS,TotalCPU,Restarts,NTasks,JobName"
    EPOCH = "SLURM_TIME_FORMAT=%s "
    LISTED_JOBS = 300
    # `--ultra`, for an account of hundreds to thousands of jobs : the finished jobs listed, the seconds given to the accounting to
    # answer, and those a reading of it must be old before a job that left the queue asks for another one (`jobs`).
    ULTRA = {"listed": 5000, "patience": 90, "settle": 60}
    MANY = 1000
    OUTPUT = 400_000
    # The name of the job is the last column : it may hold the separator (`info`).
    STEPS = "JobID,State,ExitCode,Elapsed,TotalCPU,ReqMem,MaxRSS,AllocTRES,ElapsedRaw,TimelimitRaw,JobName"
    RECAP = ("JobID,JobName,State,ExitCode,DerivedExitCode,Reason,Partition,QOS,Account,Submit,Start,End,Elapsed,Timelimit,"
             "Planned,NodeList,ReqTRES,AllocTRES,ReqMem,Restarts,WorkDir,StdOut,StdErr,SubmitLine")
    DAYS = (1, 3, 7, 30)
    WALK = 8
    PARALLEL = 2
    HISTORY = 120
    # The seconds an accounting that failed is left alone before it is asked again (a database that is down answers no faster).
    RETRY = 30
    STATES = "CANCELLED,COMPLETED,FAILED,TIMEOUT,OUT_OF_MEMORY,NODE_FAIL,PREEMPTED,BOOT_FAIL,DEADLINE,RUNNING,REQUEUED,SUSPENDED"
    DOWNLOAD = 100 * 2**20
    PROTECTED = (".ssh", ".bashrc", ".bash_profile", ".bash_login", ".bash_logout", ".bash_aliases", ".profile", ".login", ".logout",
                 ".zshrc", ".zshenv", ".zprofile", ".zlogin", ".zlogout", ".cshrc", ".tcshrc", ".kshrc")
    SECRETS = (".ssh", ".gnupg", ".netrc", ".git-credentials", ".config/git/credentials", ".config/gh", ".aws", ".azure", ".config/gcloud",
               ".kube", ".docker/config.json", ".config/rclone", ".pgpass", ".my.cnf", ".pypirc", ".npmrc", ".boto", ".s3cfg", ".vault-token",
               ".password-store", ".local/share/keyrings", ".cache/huggingface/token", ".cache/huggingface/stored_tokens", ".huggingface/token",
               ".kaggle", ".config/kaggle", ".config/wandb", ".cargo/credentials", ".cargo/credentials.toml", ".config/pypoetry/auth.toml", ".gem/credentials",
               ".oci", ".local/share/jupyter/runtime", ".bash_history", ".zsh_history", ".python_history", ".claude/.credentials.json", ".codex/auth.json",
               ".config/github-copilot", ".comet.config", ".terraform.d/credentials.tfrc.json", ".config/containers/auth.json",
               ".apptainer/remote.yaml", ".singularity/remote.yaml", ".subversion/auth", ".Xauthority", ".local/share/fish/fish_history",
               ".psql_history", ".mysql_history", ".sqlite_history", ".ipython/profile_default/history.sqlite")
    SHIELD = (
        'home=$(realpath -e -- "$HOME" 2>/dev/null) || exit 4; hidden=(); links=(); rooms=(); '
        'trail() { local at=$1 rest=$2 part target hops=0; found=(); '
        'while [ -n "$rest" ]; do part=${rest%%/*}; if [ "$part" = "$rest" ]; then rest=; else rest=${rest#*/}; fi; '
        'case "$part" in ""|.) continue;; ..) [ -n "$at" ] && found+=("$at"); at=${at%/*}; continue;; esac; '
        'if [ -L "$at/$part" ]; then hops=$(( hops + 1 )); [ "$hops" -gt 40 ] && return 1; found+=("$at/$part"); '
        'target=$(readlink -- "$at/$part") || return 1; case "$target" in /*) at=;; esac; rest="$target${rest:+/$rest}"; '
        'else at="$at/$part"; fi; done; end=${at:-/}; }; '
        'for name in "${secrets[@]}"; do hidden+=("$home/$name"); '
        'if trail "$home" "$name"; then hidden+=("$end"); { [ -e "$end" ] || [ -L "$end" ]; } && links+=("${found[@]}"); '
        'if [ -d "$end" ]; then twice=; for item in "${rooms[@]}"; do [ "$item" = "$end" ] && twice=1; done; [ -z "$twice" ] && rooms+=("$end"); fi; '
        'else links+=("${found[@]}"); fi; done; '
        'if [ "${#rooms[@]}" -gt 0 ]; then while IFS= read -r -d "" item; do '
        'if trail "" "$item"; then hidden+=("$end"); fi; links+=("${found[@]}"); '
        'done < <(find -P "${rooms[@]}" -mindepth 1 -maxdepth 2 -type l -print0 2>/dev/null | head -z -n 200); fi; '
        'secret() { local item; for item in "${hidden[@]}"; do [ "$1" = "$item" ] && return 0; case "$1" in "$item"/*) return 0;; esac; done; return 1; }; '
        'holds() { local item; for item in "${hidden[@]}"; do case "$item" in "$1"/*) { [ -e "$item" ] || [ -L "$item" ]; } && return 0;; esac; done; return 1; }; '
        'leads() { local item; for item in "${links[@]}"; do [ "$item" = "$1" ] && return 0; case "$item" in "$1"/*) return 0;; esac; done; return 1; }; '
        'sealed() { secret "$1" || holds "$1" || leads "$1"; }; '
    )
    READ = SHIELD + (
        'real=$(realpath -e -- "$1" 2>/dev/null) || exit 2; secret "$real" && exit 3; '
        'trail "" "$1" || exit 2; for item in "${found[@]}" "$end"; do secret "$item" && exit 3; done; '
        '[ -f "$real" ] && [ -r "$real" ] || exit 2; '
        'exec 3< "$real" || exit 2; '
        'if [ -e /proc/self/fd/3 ]; then held=$(realpath -e -- /proc/self/fd/3 2>/dev/null); '
        '[ "$held" = "$real" ] || { [ -n "$held" ] && secret "$held" && exit 3; }; fi; '
    )
    SEALED = "protected : keys and credential files are not read from here ; read this one in a terminal"
    HOMELESS = "the home directory of the cluster cannot be resolved : nothing is read nor changed from here"
    TOOLS = (
        "say() { printf '%s\\t%s\\t%s\\n' \"$1\" \"$2\" \"${3##*$'\\n'}\"; }; "
        'renice -n 19 -p $$ >/dev/null 2>&1; command -v ionice >/dev/null && ionice -c 3 -p $$ 2>/dev/null; '
        'gentle() { local time=$(( budget - SECONDS )); [ "$time" -lt 1 ] && time=1; timeout "$time" "$@"; }; '
        'gone() { [ ! -e "$1" ] && [ ! -L "$1" ]; }; '
        'areas=(); for root in "${roots[@]}"; do root=$(realpath -e -- "$root" 2>/dev/null) && areas+=("$root"); done; '
        'home=$(realpath -e -- "$HOME"); guard=(); '
        'for name in "${keep[@]}"; do guard+=("$home/$name" "$(realpath -m -- "$home/$name")"); done; '
        'vault=("$home/.ssh" "$(realpath -m -- "$home/.ssh")"); seen=; parent=; '
        'within() { local root deep=; for root in "${areas[@]}"; do case "$1/" in "$root"/*) [ "${#root}" -gt "${#deep}" ] && deep=$root;; esac; done; '
        '[ -n "$deep" ] || return 1; for root in "${shut[@]}"; do case "$1/" in "$root"/*) [ "${#root}" -gt "${#deep}" ] && return 1;; esac; done; return 0; }; '
        'locate() { local dir=${1%/*}; dir=${dir:-/}; [ "$dir" = "$seen" ] && return 0; seen=$dir; '
        'parent=$(realpath -e -- "$dir" 2>/dev/null) || parent=; }; '
        'protected() { local item; for item in "${guard[@]}" "${areas[@]}" "${shut[@]}"; do [ "$1" = "$item" ] && return 0; '
        'case "$item" in "$1"/*) return 0;; esac; done; '
        'for item in "${vault[@]}"; do case "$1" in "$item"/*) return 0;; esac; done; return 1; }; '
        'refuse() { if [ "$SECONDS" -ge "$budget" ]; then '
        'say skipped "$1" "not started : the $budget s allowed on the login node are used, repeat"; return 0; fi; locate "$1"; '
        'if [ -z "$parent" ] || ! within "$parent"; then say refused "$1" "outside the storage areas"; '
        'elif protected "$parent/${1##*/}"; then '
        'say refused "$1" "protected : the storage areas, the SSH keys and the shell start-up files are not changed from here"; '
        'elif [ ! -e "$1" ] && [ ! -L "$1" ]; then say missing "$1" "no such file"; '
        'else return 1; fi; }; '
    )
    REMOVE = (
        'for target in "$@"; do refuse "$target" && continue; '
        'if leads "$parent/${target##*/}"; then say refused "$target" "protected : a link that leads to keys or credentials is not deleted from here"; '
        'elif message=$(gentle rm -rf --one-file-system -- "$target" 2>&1); then say done "$target" ""; '
        'else [ $? = 124 ] && message="stopped after $budget s to spare the login node : partly deleted ; delete it again, or remove a large tree from a job"; '
        'say failed "$target" "$message"; fi; done'
    )
    RENAME = (
        'target=$1; new="${target%/*}/$2"; '
        'if refuse "$target"; then :; '
        'elif sealed "$parent/${target##*/}"; then say refused "$target" "protected : what holds keys or credentials is neither renamed nor moved from here"; '
        'elif protected "$parent/$2" || secret "$parent/$2"; then say refused "$target" "protected name"; '
        'elif [ -e "$new" ] || [ -L "$new" ]; then say exists "$target" "that name is already taken"; '
        'elif message=$(mv -n -T -- "$target" "$new" 2>&1) && gone "$target"; then say done "$new" ""; '
        'else say failed "$target" "${message:-not renamed : that name was taken meanwhile}"; fi'
    )
    MOVE = (
        'dest=$1; shift; to=$(realpath -e -- "$dest" 2>/dev/null) || to=; '
        'if [ -z "$to" ] || ! within "$to" || [ ! -d "$dest" ]; then say refused "$dest" "not a directory of the storage areas"; exit 0; fi; '
        'device=$(stat -L -c %d -- "$dest"); '
        'for target in "$@"; do new="$dest/${target##*/}"; refuse "$target" && continue; '
        'if sealed "$parent/${target##*/}"; then say refused "$target" "protected : what holds keys or credentials is neither renamed nor moved from here"; '
        'elif protected "$to/${target##*/}" || secret "$to/${target##*/}"; then say refused "$target" "protected destination"; '
        'elif [ -e "$new" ] || [ -L "$new" ]; then say exists "$target" "an entry of that name is already there"; '
        'elif [ "$(stat -L -c %d -- "$parent")" != "$device" ]; then '
        'say refused "$target" "another file system : that is a copy, not a move ; use rsync from a terminal or a job"; '
        'elif message=$(gentle mv -n -T -- "$target" "$new" 2>&1) && gone "$target"; then say done "$new" ""; '
        'else [ $? = 124 ] && message="stopped after $budget s : the move is incomplete, check both directories"; '
        'say failed "$target" "${message:-not moved : an entry of that name appeared meanwhile}"; fi; done'
    )
    MKDIR = (
        'new="$1/$2"; to=$(realpath -e -- "$1" 2>/dev/null) || to=; '
        'if [ -z "$to" ] || ! within "$to" || [ ! -d "$1" ]; then say refused "$1" "not a directory of the storage areas"; '
        'elif protected "$to/$2" || secret "$to/$2"; then say refused "$new" "protected name"; '
        'elif [ -e "$new" ] || [ -L "$new" ]; then say exists "$new" "that name is already taken"; '
        'elif message=$(mkdir -- "$new" 2>&1); then say done "$new" ""; '
        'else say failed "$new" "$message"; fi'
    )
    LISTED = 3000
    PREVIEW = 256 * 1024
    ACTIONS = {
        "cancel": 'scancel "$1"',
        # To the steps started by srun only : scancel(1) sends a signal other than KILL neither to the batch script nor to what it
        # runs itself. `--full` would, and a batch shell without a trap dies of it at once, the job with it, before any program saved its state.
        "term": 'scancel --signal=TERM "$1"',
        "requeue": 'scontrol requeue "$1"',
        "hold": 'scontrol hold "$1"',
        "release": 'scontrol release "$1"',
        "cancel_pending": "scancel --me -t PENDING",
        "cancel_all": "scancel --me",
        # One request to the controller for the whole list (`--ctld`, as scancel(1) recommends), a job that ended meanwhile not
        # being an error (`-Q`).
        "cancel_many": 'scancel --ctld -Q "$@"',
    }
    PATTERN = re.compile(r"%(\d*)([jAaxuN%])")
    UNEXPANDED = re.compile(r"%\d*[A-Za-z]")
    BINS = 120
    SPACING = 8
    DAY = 86400
    SAMPLES = DAY // SPACING + 50
    MEASURED = 50
    PROBED = 4
    PROBE_EVERY = 60
    STALE = 90
    WINDOW = 180
    SPAN = 90
    ADVISED = 40
    # What `--ultra` leaves out of a name to group the jobs of a sweep (`run-3-lr0.1` and `run-4-lr0.01` are `run-#-lr#`) : as the page does.
    NUMBERS = re.compile(r"[0-9]+(\.[0-9]+)?")
    PACE = (30, 3600)
    CHECKED = 2
    TURN = 20
    GPU_ROW = re.compile(r"\s*(\d+(?:\.\d+)?)\s*,\s*(\d+(?:\.\d+)?)\s*,\s*(\d+(?:\.\d+)?)\s*")
    # The bytes of the path of a socket (`sun_path`) : 108 on Linux, 104 on macOS and the BSDs. ssh first binds the socket
    # of a shared connection under its `ControlPath` followed by a dot and `SUFFIX` characters, and the name ends with a zero.
    SUN_PATH = 108 if sys.platform.startswith("linux") else 104
    SUFFIX = 16
    # Every ssh of the tool that goes to the cluster — the check of the connection and the commands, not `ssh -G`, which
    # only prints settings — is a client of the connection the user opened, whatever the alias says. With `ControlMaster yes`
    # or `ask`, ssh does not look for an open connection, not even for `-O check` : it starts one of its own (`mux.c`,
    # `muxclient`), which until 0.1.4 was a login sent to the cluster at each refresh. `ControlMaster=no` makes it look ;
    # without a connection it then stops on this computer, and should it still try to connect, it has no way to
    # (`ProxyCommand=false`) and nothing to ask (`BatchMode`).
    CLIENT = ("-o", "ControlMaster=no", "-o", "BatchMode=yes", "-o", "ProxyCommand=false")
    LIVE = (
        "queue=$(squeue --me -h -o '%T|%A') || {{ echo 'squeue failed' >&2; exit 3; }}; echo \"$queue\"; echo '{sep}'; "
        "squeue --me -h -t RUNNING -O 'JobID:20,tres-alloc:200' || exit 3; echo '{sep}'; "
        "ids=$(awk -F'|' '$1 == \"RUNNING\" {{ print $2 }}' <<<\"$queue\" | sort -u | head -n {measured} | paste -sd, -); "
        "date +%s.%N; [ -z \"$ids\" ] || timeout 10 sstat -a -n -P -j \"$ids\" -o JobID,TRESUsageInTot 2>/dev/null; true"
    )
    PROBE = (
        'for job in "$@"; do timeout -k 5 20 srun --jobid="$job" --overlap --ntasks=1 '
        "nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits & done; wait"
    )

    def __init__(self, host, extra_roots=(), writable=False, actions=False, sample=0, keep=None, site=None, ultra=False, files=True):
        self.host = host
        self.ultra = ultra
        self.browse = files
        self.listed, self.patience, self.settle = (self.ULTRA[key] for key in ("listed", "patience", "settle")) if ultra else (self.LISTED_JOBS, 25, 0)
        self.checked = NEVER
        self.muted = False
        self.site = site or Site()
        self.sample = sample
        self.pace = threading.Condition()
        self.keep = keep
        self.writable = writable
        self.actions = actions
        self.extra_roots = tuple(extra_roots)
        self.lock = threading.Lock()
        self.slots = threading.BoundedSemaphore(self.PARALLEL)
        self.long = threading.BoundedSemaphore(self.PARALLEL - 1)
        self.SEP = f"@@@{secrets.token_hex(6)}@@@"
        self.cache = {}
        self.accounts = {}
        self.fault = {}
        self.outdated = set()
        self.departed = NEVER
        self.queued = set()
        self.outputs = {}
        self.known_roots = None
        self.samples = collections.deque(self.restore(), maxlen=self.SAMPLES)
        self.sampled = NEVER
        self.warned = False
        self.counters = {}
        self.gpu_jobs = ()
        self.sampling = threading.Lock()
        self.probing = threading.Lock()
        self.reading = threading.Lock()
        self.probed = NEVER
        self.begun = {}
        self.changes = 0

    ##### Connection #####

    def connected(self):
        """True when the SSH master connection of the alias is alive (no network traffic). The moment of a yes is kept
        (`checked`) : a command sent within `CHECKED` seconds of it does not start a process to ask again (`send`)."""
        try:
            check = subprocess.run(["ssh", *self.CLIENT, "-O", "check", self.host], capture_output=True, timeout=5)
        except (subprocess.TimeoutExpired, OSError):
            return False
        self.checked = time.monotonic() if check.returncode == 0 else NEVER
        return check.returncode == 0

    @classmethod
    def lacking(cls, host, settings):
        """What the SSH settings of the alias (the output of `ssh -G`) lack for a connection to stay open and be reused,
        as a sentence whose `backquoted` parts are code ; '' when nothing is known to lack, or when the text is not
        the settings of ssh (they always name a `hostname`).

        Three cases are told : an alias without `ControlPath` (ssh then has no socket to share a connection through) ;
        one whose `ControlMaster` is off, the default of ssh (`ssh <alias>` then shares its connection only when started
        with `-M`) ; a `ControlPath` too long for a socket of this system. What ssh does with a path too long is said as
        ssh does it : from `SUN_PATH` bytes on it stops before any login, when it looks for an open connection — which
        it does not with `ControlMaster yes` or `ask` ; under that, and with those two, it stops once logged in, when it
        binds the socket — which it only does with `ControlMaster` on, so an alias whose `ControlMaster` is off is told
        that first, and its path with it. Nothing lacks to an alias whose `ControlMaster` is `yes` or `ask` : its
        `ssh` opens the connection, and the tool is a client of it (`CLIENT`) — with `ask` and `autoask`, a connection that
        asks the user to confirm each command it is sent, as they chose. The length is counted in bytes, as the system counts it (the settings are
        given as `os.fsdecode` reads them). A path that still holds a `%` (an old ssh shows it before filling it) has
        an unknown length : nothing is said of it."""
        options = dict(line.split(" ", 1) for line in settings.splitlines() if " " in line)
        if "hostname" not in options:
            return ""
        socket = options.get("controlpath", "none").strip()
        advice = "`ControlMaster auto`, `ControlPath ~/.ssh/cm-%C` and `ControlPersist yes`"
        if socket in ("", "none"):
            return (f"The alias `{host}` keeps no connection open for the monitor to use : its entry of `~/.ssh/config` "
                    f"needs {advice}.")
        longest = cls.SUN_PATH - cls.SUFFIX - 2
        size = 0 if "%" in socket else len(os.fsencode(os.path.expanduser(socket)))
        long = f"too long for a socket of this system ({size} bytes, {longest} at most)"
        shorten = "Shorten it in `~/.ssh/config`, for instance `ControlPath ~/.ssh/cm-%C`."
        master = options.get("controlmaster", "").strip()
        if size >= cls.SUN_PATH and master not in ("true", "yes", "ask"):
            return (f"The `ControlPath` of the alias `{host}` is {long} : `ssh {host}` stops at once with an error (« ControlPath too long »), "
                    f"before any login. {shorten}")
        if master in ("false", "no"):
            return (f"The alias `{host}` has a `ControlPath` but `ControlMaster` is off : `ssh {host}` keeps no connection open for the monitor "
                    f"to use, unless it is started as `ssh -M {host}`. Its entry of `~/.ssh/config` needs `ControlMaster auto` and `ControlPersist yes`."
                    + (f" Its `ControlPath` is also {long}. {shorten}" if size > longest else ""))
        if size > longest:
            return (f"The `ControlPath` of the alias `{host}` is {long} : `ssh {host}` asks you to log in, then stops with an error "
                    f"(« too long for Unix domain socket »), and no connection stays open. {shorten}")
        return ""

    def sharing(self):
        """`lacking` for the alias of this cluster, read with `ssh -G`, which prints the settings and never connects to
        the cluster (it runs what the configuration itself asks to read them : a `Match exec` command, the name
        look-ups of `CanonicalizeHostname`). '' when ssh does not say them within 5 seconds."""
        try:
            shown = subprocess.run(["ssh", "-G", self.host], capture_output=True, timeout=5)
        except (subprocess.TimeoutExpired, OSError):
            return ""
        return "" if shown.returncode else self.lacking(self.host, os.fsdecode(shown.stdout))

    def status(self):
        """What the page is told at each refresh : the connection, the modes of the server, the site. Without a
        connection, what the SSH settings lack to keep one (`sharing`), '' with one."""
        connected = self.connected()
        return {"connected": connected, "host": self.host, "writable": self.writable, "actions": self.actions, "version": VERSION,
                "ultra": self.ultra, "files": self.browse, "sample": self.sample, "enrolled": self.site.enrolled(), "site": self.site.describe(),
                "sharing": "" if connected else self.sharing()}

    def send(self, script, timeout):
        """One attempt to run a script on the login node : exit code, output and error output, as bytes. The connection is
        checked first, unless it just was (`connected`) : the status and the commands of one refresh ask once. A connection
        that closed since makes ssh fail on this computer (255), and `run` then asks again."""
        if time.monotonic() - self.checked >= self.CHECKED and not self.connected():
            raise NotConnected
        proc = subprocess.run(
            ["ssh", *self.CLIENT, "-o", "ConnectTimeout=10", self.host, f"timeout {timeout} bash -s"],
            input=script.encode(), capture_output=True, timeout=timeout + 15,
        )
        return proc.returncode, proc.stdout, proc.stderr

    def run(self, command, timeout=25, args=(), raw=False, once=False):
        """Output of a bash script on the login node. `ProxyCommand=false` forbids any new connection :
        if the master connection died, ssh fails locally instead of sending a failed login.

        A connection carries about ten commands at once and refuses the next ones (ssh then exits with 255
        and an empty output) : at most `PARALLEL` run together, and a refused one is tried again, after a wait
        during which it holds none of these places. ssh also exits
        with 255 when the connection drops in the middle of a command : a command that already answered is
        never sent again, and neither is one that changes something (`once`), which then returns what it
        printed with the code 255 — or, when it printed nothing, is said to have run or not (`Busy`, with that message).

        The script and its `args` (positional parameters `$1`, `$@`) travel on the standard input of a remote
        `bash -s` : the paths and job ids are not in the command line of that shell, and no value coming from the page
        is ever written in the text of a script. The commands the script itself starts (`realpath`, `rm`, `mv`, `scontrol`,
        `srun`) take them as arguments : `ps` shows those to the other users of the node for the instant each one lasts. The remote `timeout` ends the script before
        the local one gives up, so no command is left running on the cluster. `raw` returns the output as bytes."""
        script = "set -- " + " ".join(shlex.quote(arg) for arg in args) + "\n{ " + self.PLAIN + command + "\n} </dev/null\n"
        for attempt in range(3):
            with self.slots:
                code, out, err = self.send(script, timeout)
            if code != 255 or out or once:
                break
            self.checked = NEVER
            if attempt < 2:
                time.sleep(1 + attempt)
        if code == 255 and not (once and out):
            raise Busy("the SSH connection refused the command, or dropped while it ran : it was not sent again, and may have been done or not ; "
                       "look before repeating it" if once else "")
        if code == 124:
            raise subprocess.TimeoutExpired("remote command", timeout)
        err = self.ANSI.sub("", err.decode("utf-8", "replace")).strip()
        return code, out if raw else out.decode("utf-8", "replace"), err

    def heavy(self, *args, **options):
        """`run` for a command that may last (a walk of a directory tree, a download) : one at a time, so that a place stays
        free for the short commands of the page ; one that waited `TURN` seconds for its turn is refused (`Busy`)."""
        if not self.long.acquire(timeout=self.TURN):
            raise Busy("a long command (a download, a measure, a search) is already running on the cluster ; try again when it has ended")
        try:
            return self.run(*args, **options)
        finally:
            self.long.release()

    ##### Parsing #####

    @classmethod
    def job_id(cls, value):
        """A validated job identifier (`123`, `123_4`, `123_[1-9]`, `123+0`) ; the only job input sent to the shell."""
        if not isinstance(value, str) or len(value) > cls.LONGEST or not cls.JOB_ID.fullmatch(value):
            raise ValueError("invalid job id")
        return value

    @classmethod
    def target(cls, value):
        """The identifier a job action is given : the validated one, without the limit of tasks at once that squeue prints with a
        pending array (`123_[3-10%2]` : `123_[3-10]`) ; refused when what is left is not a job, a task or a plain list of tasks."""
        found = cls.THROTTLE.sub("", cls.job_id(value))
        if not cls.TASKS.fullmatch(found):
            raise ValueError("invalid job id")
        return found

    @staticmethod
    def rows(text, columns):
        """Pipe-separated lines as dictionaries ; the last column may contain pipes."""
        out = []
        for line in text.splitlines():
            parts = line.split("|", len(columns) - 1)
            if len(parts) == len(columns):
                out.append(dict(zip(columns, (p.strip() for p in parts))))
        return out

    @staticmethod
    def table(text, separator="|", most=-1):
        """A text table with a header line as `{columns, rows}` ; with `most` columns, the last one may hold the separator."""
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return {"columns": [], "rows": []}
        split = [[cell.strip() for cell in line.split(separator, most - 1)] for line in lines]
        return {"columns": split[0], "rows": split[1:]}

    @classmethod
    def size(cls, text):
        """Bytes of a Slurm or quota size (`420K`, `2G`, `5.00 TiB`, `0 Byte`) ; None when empty."""
        match = re.match(r"\s*(\d+(?:\.\d+)?)\s*([KMGTP]?)", (text or "").upper())
        return int(float(match[1]) * cls.UNITS[match[2]]) if match else None

    @staticmethod
    def seconds(text):
        """Seconds of a Slurm duration (`00:00.552`, `01:02:03`, `1-02:03:04`) ; None when empty."""
        days, _, clock = (text or "").rpartition("-")
        try:
            total = 0.0
            for part in clock.split(":"):
                total = 60 * total + float(part)
            return total + 86400 * int(days or 0)
        except ValueError:
            return None

    @staticmethod
    def integer(value, low, high):
        """An integer argument of the page, clamped to a range."""
        try:
            return max(low, min(int(value), high))
        except (TypeError, ValueError, OverflowError):
            raise ValueError("not a number") from None

    @staticmethod
    def number(text):
        """An integer field, 0 when empty or not a number."""
        return int(text) if (text or "").isdigit() else 0

    @classmethod
    def tres(cls, text):
        """`cpu=1,gres/gpu=1,mem=2G` as the numbers of CPUs and GPUs and the bytes of memory (None when absent)."""
        items = dict(item.split("=", 1) for item in (text or "").split(",") if "=" in item)
        return cls.number(items.get("cpu")), cls.number(items.get("gres/gpu")), cls.size(items.get("mem"))

    @staticmethod
    def epoch(text):
        """A Slurm time stamp asked in seconds since the epoch (`EPOCH`) ; None for `Unknown`, `None`, `N/A`."""
        return int(text) if (text or "").isdigit() else None

    @staticmethod
    def wait(submit, start):
        """Seconds between submission and start ; None while the job has not started."""
        return None if submit is None or start is None else max(0, start - submit)

    @staticmethod
    def first_node(nodes):
        """The first node of a Slurm node list (`cn-a[001-003],cn-b002` gives `cn-a001`)."""
        match = re.match(r"([^\[,]*)(?:\[(\d+))?", nodes or "")
        return match[1] + (match[2] or "")

    @classmethod
    def usage(cls, state, elapsed, limit, tres, reqmem, cpu, peaks):
        """What a job used against what it asked for : memory peak / request, CPU time / allocated, time / limit.

        The memory asked is the allocated one (`mem=` of `AllocTRES`, a total whatever the option that asked for
        it : `--mem`, `--mem-per-cpu`, `--mem-per-gpu`), `ReqMem` only without it. The CPU time of a running job
        is not recorded before a step ends : no figure rather than a zero."""
        cpus, gpus, memory = cls.tres(tres)
        elapsed, cpu = cls.number(elapsed), cls.seconds(cpu)
        peaks = [peak for peak in map(cls.size, peaks) if peak]
        known = cpu is not None and elapsed and cpus and state != "RUNNING"
        return {
            "elapsed": elapsed, "limit": 60 * cls.number(limit), "cpus": cpus, "gpus": gpus,
            "mem_req": memory or cls.size(reqmem), "mem_peak": max(peaks) if peaks else None,
            "cpu_eff": round(cpu / (elapsed * cpus), 4) if known else None,
        }

    ##### Jobs #####

    def jobs(self, days, fresh=False, quiet=False):
        """The queue, and the finished jobs and totals of the last `days` days.

        `squeue` runs at every call ; `sacct`, a query of the accounting database, only when asked (`fresh`),
        when a job has left the queue, after an action, or when its last answer is older than `HISTORY` seconds —
        this last case not for a `quiet` caller (a hidden tab, which only keeps the mark of its name up to date).
        A job that left the queue outdates the accounting of every period, not only the one asked. With `--ultra`
        (`settle`), where a job leaves at almost every refresh, it asks for a new reading only once the last one is
        `settle` seconds old, for each period. A queue that could not be read says nothing of the jobs that left, and an accounting
        that failed — an error of `sacct`, or no answer in its time — is not kept : its error is given with the queue and with the finished jobs kept
        before, if any, which it outdates, and the accounting is asked again after `RETRY` seconds, or at once by a `fresh` call."""
        days = days if days in self.DAYS else 1
        with self.lock:
            changes = self.changes
            stamp, value = self.cache.get(days, (NEVER, None))
            if not fresh and value is not None and time.monotonic() - stamp < 4:
                return value
        # `SLURM_BITSTR_LEN=0` : the list of the pending tasks of an array whole, where squeue cuts it at 64 bytes by default
        # (squeue(1)) — a cut identifier is one the job actions would refuse.
        code, out, err = self.run(f"SLURM_BITSTR_LEN=0 {self.EPOCH}squeue --me -h -o '{self.SQUEUE}'")
        queue = self.rows(out, self.QUEUE)
        ids = {job["id"] for job in queue}
        with self.lock:
            read, account = self.accounts.get(days, (NEVER, None))
            failed, failure = self.fault.get(days, (NEVER, None))
            if code == 0:
                # A job that left outdates the reading kept for every period ; each is read again when it is next asked for,
                # and old enough (`settle`).
                if self.queued - ids:
                    self.outdated.update(self.accounts, (days,))
                    self.departed = time.monotonic()
                self.queued = ids
            left = days in self.outdated and time.monotonic() - read >= self.settle
            if left:
                self.outdated.discard(days)
            # A hidden tab leaves an accounting that fails alone for as long as it would a good one.
            rest = self.HISTORY if quiet else self.RETRY
        if not fresh and time.monotonic() - failed < rest:
            # The accounting just failed : what was kept before, if anything, with what it said.
            account = {**(account or failure), "error": failure["error"]}
        elif fresh or left or account is None or read == NEVER or (not quiet and time.monotonic() - read > self.HISTORY):
            # One reading of the accounting at a time. A call that waited for another one takes its answer when it
            # only wanted a recent one ; a call that needs a new one (`fresh`, a job that left the queue) takes it
            # only if that reading began after the call. An answer is not kept once an action changed the jobs.
            asked = time.monotonic()
            with self.reading:
                with self.lock:
                    latest, known = self.accounts.get(days, (NEVER, None))
                    begun = self.begun.get(days, NEVER)
                    failed, failure = self.fault.get(days, (NEVER, None))
                if not fresh and time.monotonic() - failed < rest:
                    # It failed for the call this one waited for : not asked again at once.
                    account = {**(known or failure), "error": failure["error"]}
                elif known is not None and (begun >= asked if fresh or left else latest > read):
                    account = known
                else:
                    begin, new = time.monotonic(), None
                    try:
                        new = account = self.account(days)
                    finally:
                        with self.lock:
                            if new is not None and not new["error"]:
                                self.fault.pop(days, None)
                                if self.changes == changes:
                                    self.accounts[days], self.begun[days] = (time.monotonic(), new), begin
                                    # A job that left while this reading was made, seen by any call, leaves it out of date ; one
                                    # begun after the last job left is up to date.
                                    (self.outdated.add if self.departed > begin else self.outdated.discard)(days)
                            else:
                                # What was kept before is out of date (the job that left is not in it), and still the best there is to show.
                                if days in self.accounts:
                                    self.accounts[days] = (NEVER, self.accounts[days][1])
                                if new is not None:
                                    self.fault[days] = (time.monotonic(), new)
                    if new["error"] and known is not None:
                        account = {**known, "error": new["error"]}
        value = {"queue": queue, "listed": code == 0, **account, "error": err if code else account["error"]}
        with self.lock:
            if self.changes == changes:
                self.cache[days] = (time.monotonic(), value)
        return value

    @staticmethod
    def brief(data, since):
        """The answer of `jobs` for a page that holds the reading of the accounting made at `since` : without the finished jobs
        and what is drawn from them when that reading is still the one in use — they are sent when they change, not at each refresh."""
        return {**data, "history": None, "advice": None, "timeline": None} if since is not None and since == data["read"] else data

    def account(self, days):
        """The finished jobs of the period with what they used, and the totals, from the accounting database.

        The jobs are selected by state over the period : without `--state`, `sacct` keeps only the jobs that
        became eligible, and a job cancelled while held would be missing. The dates are asked in seconds since
        the epoch, so the page shows them in its own time zone. `sacct` gives the whole run of a job that started
        before the period : the GPU-hours and CPU-hours count only the part of each run inside it."""
        try:
            code, out, err = self.run(f"{self.EPOCH}sacct -P -n --start=now-{days}days --end=now --state={self.STATES} -o {self.SACCT}", timeout=self.patience)
        except subprocess.TimeoutExpired:
            # The queue is still worth showing (`jobs`).
            code, out, err = 124, "", "the accounting database did not answer in time"
        records = self.rows(out, self.RECORD)
        now = time.time()
        peaks, tasks = {}, {}
        for row in records:
            base, dot, _ = row["id"].partition(".")
            if dot:
                peaks.setdefault(base, []).append(row["maxrss"])
                tasks[base] = max(tasks.get(base, 0), self.number(row["ntasks"]))
        jobs = []
        for row in records:
            if "." in row["id"]:
                continue
            state = row["state"].split(" ")[0]
            job = {key: row[key] for key in ("id", "partition", "exit", "nodes", "name")}
            job.update({key: self.epoch(row[key]) for key in ("submit", "start", "end")})
            job.update(self.usage(state, row["elapsed"], row["limit"], row["tres"], row["reqmem"], row["cpu"], peaks.get(row["id"], [])))
            job.update(state=state, restarts=self.number(row["restarts"]), wait=self.wait(job["submit"], job["start"]), tasks=tasks.get(row["id"], 0))
            inside = job["elapsed"] if job["start"] is None else min(job["end"] or now, now) - max(job["start"], now - 86400 * days)
            job["counted"] = max(0, min(job["elapsed"], inside))
            jobs.append(job)
        history = sorted((job for job in jobs if job["state"] not in ("RUNNING", "PENDING", "REQUEUED", "SUSPENDED", "RESIZING")),
                         key=lambda job: job["end"] or job["start"] or job["submit"] or 0, reverse=True)
        waits = [job["wait"] for job in jobs if job["wait"] is not None]
        states = {}
        for job in history:
            states[job["state"]] = states.get(job["state"], 0) + 1
        summary = {
            "days": days, "jobs": len(jobs), "states": states,
            "gpu_hours": sum(job["gpus"] * job["counted"] for job in jobs) / 3600,
            "cpu_hours": sum(job["cpus"] * job["counted"] for job in jobs) / 3600,
            "wait_median": statistics.median(waits) if waits else None,
            "restarts": sum(job["restarts"] for job in jobs),
        }
        return {"history": history[:self.listed], "finished": len(history), "summary": summary, "read": now,
                "timeline": self.timeline(jobs, days, now), "advice": self.advice(history, self.ultra), "waits": self.waits(jobs),
                "error": (err or f"sacct ended with the code {code}") if code else ""}

    def info(self, job):
        """The accounting record of a job, its steps, its use of the request, and `scontrol show job` while known."""
        job = self.job_id(job).split("_[")[0]
        _, out, _ = self.run(
            f"scontrol show job \"$1\" 2>&1; echo '{self.SEP}'; sacct -P -j \"$1\" -o {self.STEPS}; echo '{self.SEP}'; "
            # sstat does not know the tasks of an array by `123_4` (job_array.html) : it is given the identifier of the job itself.
            "raw=$1; case \"$1\" in *_*) raw=$(squeue -h -j \"$1\" -o %A 2>/dev/null | head -n 1); case \"$raw\" in ''|*[!0-9]*) raw=$1;; esac;; esac; "
            f"sstat -a -P -j \"$raw\" -o JobID,MaxRSS,AveCPU,NTasks 2>/dev/null; echo '{self.SEP}'; "
            f"sacct --expand-patterns -X -P -j \"$1\" -o {self.RECAP} 2>/dev/null || sacct -X -P -j \"$1\" -o {self.RECAP}",
            args=(job,),
        )
        control, steps, live, recap = (part.strip() for part in (out.split(self.SEP) + [""] * 3)[:4])
        steps, live = self.table(steps, most=11), self.table(live)
        live["rows"] = [row for row in live["rows"] if not row[0].endswith(".extern")]
        usage = None
        main = next((row for row in steps["rows"] if "." not in row[0] and len(row) == 11), None)
        if main:
            peaks = [row[6] for row in steps["rows"] if len(row) == 11] + [row[1] for row in live["rows"] if len(row) > 1]
            usage = self.usage(main[1].split(" ")[0], main[8], main[9], main[7], main[5], main[4], peaks)
        # Shown as before : the name after the identifier, without the two raw columns.
        steps = {"columns": steps["columns"][:1] + steps["columns"][10:11] + steps["columns"][1:8],
                 "rows": [row[:1] + row[10:11] + row[1:8] for row in steps["rows"]]}
        lines = recap.splitlines()
        names = lines[0].split("|") if lines else []
        values = lines[1].split("|", len(names) - 1) if len(lines) > 1 else []
        record = [[name, value] for name, value in zip(names, values) if value.strip()]
        known = "Invalid job id" not in control and "JobId=" in control
        return {"record": record, "scontrol": control if known else "", "accounting": steps, "live": live, "usage": usage}

    @classmethod
    def expand(cls, pattern, values):
        """A Slurm file name pattern (`%j`, `%A`, `%a`, `%x`, `%u`, `%N`, `%%`, padded as `%4a`) with its values."""
        def value(match):
            width, letter = match.groups()
            text = "%" if letter == "%" else values.get(letter, "")
            return text.zfill(min(int(width), 10)) if width and text.isdigit() else text
        return cls.PATTERN.sub(value, pattern)

    def output_path(self, job, stream):
        """The output (or error) file of a job, from `scontrol` while it knows the job, else from the accounting.

        Returns the path and, when the recorded name still holds a pattern that cannot be expanded here
        (`%n`, `%t`, `%s`, `%J`), that name : a wrong path is not shown as if it were the file."""
        key = "StdErr" if stream == "err" else "StdOut"
        fields = "StdOut,StdErr,WorkDir,JobIDRaw,NodeList,JobName"
        _, out, _ = self.run(
            f"scontrol show job \"$1\" 2>/dev/null | sed -n 's/^ *{key}=//p' | head -n 1; echo '{self.SEP}'; "
            f"(sacct --expand-patterns -X -n -P -j \"$1\" -o {fields} 2>/dev/null || sacct -X -n -P -j \"$1\" -o {fields}) | head -n 1; "
            f"echo '{self.SEP}'; echo \"$USER\"",
            args=(job,),
        )
        path, recorded, user = (part.strip() for part in (out.split(self.SEP) + ["", ""])[:3])
        if path:
            return path, ""
        stdout, stderr, workdir, raw, nodes, name = (recorded.split("|", 5) + [""] * 5)[:6]
        pattern = (stderr if stream == "err" else "") or stdout
        if not pattern:
            return "", ""
        if self.UNEXPANDED.search(self.PATTERN.sub("", pattern)):
            return "", pattern
        base, _, index = job.partition("_")
        path = self.expand(pattern, {"j": raw or job, "A": base, "a": index, "x": name, "u": user, "N": self.first_node(nodes)})
        return (path if path.startswith("/") else f"{workdir}/{path}"), ""

    def output(self, job, stream, lines):
        """The last lines of the output (or error) file of a job, within `OUTPUT` bytes.

        The byte bound is applied on the cluster : a log made of one endless line (a progress bar that writes
        carriage returns) does not cross the connection whole at every refresh."""
        job = self.job_id(job).split("_[")[0]
        lines = self.integer(lines, 10, 2000)
        stream = "err" if stream == "err" else "out"
        with self.lock:
            if len(self.outputs) > 2000:
                self.outputs.clear()
            path = self.outputs.get((job, stream))
        if not path:
            path, pattern = self.output_path(job, stream)
            if pattern:
                return {"path": pattern, "text": "", "missing": "The file name recorded for this job holds a pattern that is not "
                        "expanded here (%n, %t, %s or %J) : open the file from the Storage view."}
            if not path:
                return {"path": "", "text": "", "missing": "No output file is recorded for this job (interactive job, or not started yet)."}
            with self.lock:
                self.outputs[(job, stream)] = path
        code, text, _ = self.run(self.sealing() + self.READ + f'tail -n {lines} <&3 | tail -c {self.OUTPUT}', args=(path,))
        if code in (3, 4):
            return {"path": path, "text": "", "missing": self.SEALED if code == 3 else self.HOMELESS}
        if code:
            return {"path": path, "text": "", "missing": "This file does not exist or is not readable : not written yet, deleted, or purged."}
        return {"path": path, "text": text, "missing": ""}

    def gpu(self, job):
        """`nvidia-smi` inside a running job (an overlapping step : it takes no resource from the job)."""
        job = self.target(job)
        probe = (
            "nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw "
            f"--format=csv; echo '{self.SEP}'; "
            "nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv"
        )
        code, out, err = self.run(f'srun --jobid="$1" --overlap --ntasks=1 bash -c {shlex.quote(probe)}', timeout=40, args=(job,))
        gpus, _, apps = out.partition(self.SEP)
        return {"gpus": self.table(gpus, ","), "apps": self.table(apps, ","), "error": err if code else ""}

    def act(self, action, job, jobs=None):
        """One of `ACTIONS` ; the per-job ones take a validated identifier, `cancel_many` a list of them (`jobs`, `MANY` at most :
        one `scancel` for a whole selection). Sent once, never repeated ; whatever the outcome, what was read of the jobs is dropped."""
        if not self.actions:
            raise PermissionError("job actions are disabled : start the server with --actions")
        template = self.ACTIONS[action if isinstance(action, str) else ""]
        if '"$@"' in template:
            if not isinstance(jobs, list) or not 1 <= len(jobs) <= self.MANY:
                raise ValueError(f"between 1 and {self.MANY} jobs are expected")
            args = tuple(self.target(item) for item in jobs)
            command = template.replace('"$@"', f"{args[0]} … ({len(args)} jobs)" if len(args) > 1 else args[0])
        else:
            args = (self.target(job),) if '"$1"' in template else ()
            command = template.replace('"$1"', args[0]) if args else template
        try:
            code, out, err = self.run(template, args=args, once=True)
        finally:
            with self.lock:
                self.cache.clear()
                # Out of date, not forgotten : still the finished jobs to show if the accounting fails next (`jobs`).
                self.accounts = {days: (NEVER, account) for days, (_, account) in self.accounts.items()}
                self.changes += 1
        return {"ok": code == 0, "command": command, "output": (out + "\n" + err).strip()}

    ##### Sizing and waits #####

    @staticmethod
    def waits(jobs):
        """The median wait before start of the jobs of the period, by partition, with the number of jobs it is taken on."""
        found = {}
        for job in jobs:
            if job["wait"] is not None:
                found.setdefault(job["partition"], []).append(job["wait"])
        return {partition: {"median": statistics.median(waits), "jobs": len(waits)} for partition, waits in found.items()}

    @classmethod
    def advice(cls, history, pattern=False):
        """What the finished jobs of a same name used against what they asked for, and a smaller request when one fits.
        With `pattern` (`--ultra`, where a sweep is thousands of names), of a same name once its numbers are left out.

        Only the completed jobs give the figures (a failed job may have stopped early). A request is suggested when
        it would be at most 80 % of the present one : the memory peak plus 20 %, in whole GiB ; the longest run plus
        25 %, by steps of 5 minutes ; the CPUs in use (CPU time / elapsed, largest of the jobs) plus 20 %. None is
        suggested for the memory of a name that ran out of memory, nor for the time of a name that timed out :
        those need more, and the counts say so. The memory peak the accounting records is the one of the largest
        task of a step, not of the job : a name with a step of several tasks gets no advice on memory nor on
        CPUs. The `ADVISED` names with the most jobs."""
        names = {}
        for job in history:
            names.setdefault(cls.NUMBERS.sub("#", job["name"]) if pattern else job["name"], []).append(job)
        rows = []
        for name, jobs in names.items():
            done = [job for job in jobs if job["state"] == "COMPLETED"]
            if not done:
                continue
            count = {state: sum(job["state"] == state for job in jobs) for state in ("TIMEOUT", "OUT_OF_MEMORY")}
            peak = max((job["mem_peak"] for job in done if job["mem_peak"]), default=None)
            asked = max((job["mem_req"] for job in done if job["mem_req"]), default=None)
            used = max((job["cpus"] * job["cpu_eff"] for job in done if job["cpu_eff"] is not None), default=None)
            row = {
                "name": name, "jobs": len(jobs), "completed": len(done), "timeout": count["TIMEOUT"], "oom": count["OUT_OF_MEMORY"],
                "mem_peak": peak, "mem_req": asked, "elapsed": max(job["elapsed"] for job in done),
                "limit": max(job["limit"] for job in done), "cpus": max(job["cpus"] for job in done), "cpu_used": used,
                "tasks": max(job.get("tasks", 0) for job in jobs), "mem": None, "time": None, "cores": None,
                "cpu_back": 0.0, "mem_back": 0.0,
            }
            single = row["tasks"] <= 1
            if peak and asked and not row["oom"] and single:
                wanted = max(1, math.ceil(1.2 * peak / 2**30))
                row["mem"] = wanted if wanted * 2**30 <= 0.8 * asked else None
            if row["elapsed"] and row["limit"] and not row["timeout"]:
                wanted = 5 * math.ceil(1.25 * row["elapsed"] / 300)
                row["time"] = wanted if 60 * wanted <= 0.8 * row["limit"] else None
            if used is not None and row["cpus"] > 1 and single:
                wanted = max(1, math.ceil(1.2 * used))
                row["cores"] = wanted if wanted <= 0.8 * row["cpus"] else None
            # What the smaller requests would have given back : for each job of the name, whatever its end, what it held above the
            # suggestion, over the part of its run inside the period (`counted`, as the totals count it) — CPU-hours, GiB-hours.
            for job in jobs:
                held = (job.get("counted", job["elapsed"]) or 0) / 3600
                if row["cores"]:
                    row["cpu_back"] += max(0, job["cpus"] - row["cores"]) * held
                if row["mem"] and job["mem_req"]:
                    row["mem_back"] += max(0.0, job["mem_req"] / 2**30 - row["mem"]) * held
            row["cpu_back"], row["mem_back"] = round(row["cpu_back"], 2), round(row["mem_back"], 2)
            rows.append(row)
        rows.sort(key=lambda row: (-row["jobs"], row["name"]))
        return rows[:cls.ADVISED]

    ##### Usage service of the cluster : the business of its site #####

    def enrol(self, token):
        """Hands the token of the usage service typed in the page to the site ; says only whether one is set."""
        return {"enrolled": self.site.enrol(token)}

    def efficiency(self, days):
        """What the usage service of the site says of the GPU jobs of the last `days` days (one of `DAYS`)."""
        return self.site.efficiency(days if days in self.DAYS else 1)

    ##### Resources #####

    def restore(self):
        """The samples of the last day read from the file of `--keep`, which is then replaced by a file of them only.

        A file that holds something else than samples is refused, not emptied : the option may have been given
        a wrong path. So is a symbolic link. The new file is written beside and renamed over it, readable by
        its owner only."""
        if not self.keep:
            return []
        path = Path(self.keep)
        if path.is_symlink():
            raise ValueError("it is a symbolic link")
        now, found, foreign, known = time.time(), {}, 0, 0
        for line in (path.read_text(errors="replace").splitlines() if path.exists() else []):
            try:
                sample = json.loads(line)
            except ValueError:
                sample = None
            if isinstance(sample, dict) and isinstance(sample.get("t"), (int, float)) and "running" in sample:
                known += 1
                if now - self.DAY < sample["t"] <= now + 60:
                    found[sample["t"]] = sample
            elif line.strip():
                foreign += 1
        if foreign > 1 or (foreign and not known):
            raise ValueError("it holds something else than samples of this tool")
        samples = [found[key] for key in sorted(found)]
        fresh = path.with_name(path.name + ".new")
        with os.fdopen(os.open(fresh, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600), "w") as file:
            os.fchmod(file.fileno(), 0o600)
            file.writelines(json.dumps(sample) + "\n" for sample in samples)
        os.replace(fresh, path)
        return samples

    def record(self, sample):
        """Appends a sample to the file of `--keep` ; a sample written twice (after a reading of the GPUs) is read once, the last.

        Best effort : a file that cannot be written (disk full, directory removed) is said once in the terminal,
        and the sampling goes on. Written outside the lock of the cluster : a slow disk does not hold the page up
        (a sample is one short line added at the end of the file)."""
        if not self.keep:
            return
        try:
            with os.fdopen(os.open(self.keep, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600), "a") as file:
                file.write(json.dumps(sample) + "\n")
        except OSError as error:
            if not self.warned:
                self.warned = True
                print(f"The samples are no longer kept ({error.strerror}) : the file of --keep cannot be written.", flush=True)

    def background(self, seconds):
        """Turns the background sampling on (a sample every `seconds`, within `PACE`) or off (0), until the server stops ; says the pace in use.

        Asked by the page (Resources view) ; `--sample` only gives the pace the server starts with. A value that is
        not an integer of that range is refused and changes nothing. On a change the thread of `sample_forever`
        is woken up and starts a whole new wait : no sample is taken at the moment of the change ; the pace
        already in use, asked again, changes nothing and delays no sample. Each change is said in the terminal :
        the sampling goes on after the page that asked for it is closed."""
        low, high = self.PACE
        if isinstance(seconds, bool) or not isinstance(seconds, int) or not (seconds == 0 or low <= seconds <= high):
            raise ValueError(f"the background sampling takes 0 (off) or {low} to {high} seconds")
        with self.pace:
            changed = seconds != self.sample
            if changed:
                self.sample = seconds
                self.pace.notify_all()
        if changed:
            print("Resources : background sampling " + (f"every {seconds} s, view open or not" if seconds else "off") + " (set from the page).", flush=True)
        return {"sample": seconds}

    def sample_forever(self):
        """Takes a sample about every `sample` seconds, view open or not, while the connection is open ; sleeps while `sample` is 0.

        Off unless `--sample` or the page (`background`) turned it on. A change of pace restarts the wait : a sample
        is taken only at the end of a whole wait at the pace still in use. The wait starts when the previous
        sample ends, so the period is the pace plus the time of a sample ; a sample asked less than `SPACING`
        seconds after one of the open view is not taken (`live`)."""
        while True:
            with self.pace:
                pace = self.sample
                if self.pace.wait(pace or None) or not pace or pace != self.sample:
                    continue
            try:
                if self.connected():
                    self.live()
                self.muted = False
            except Exception as error:
                # Said once per run of failures, not at each sample.
                if not self.muted:
                    self.muted = True
                    print(f"Resources : a background sample could not be taken ({type(error).__name__}) ; the sampling goes on.", flush=True)

    @classmethod
    def timeline(cls, jobs, days, now):
        """What the jobs of the period held over time, as `BINS` time-weighted means : running jobs, allocated CPUs,
        GPUs and memory, and what the finished jobs used of them.

        The accounting records one figure per job, not a curve : the CPU use of a job is its mean over its run
        (CPU time / elapsed) and its memory is its peak, both spread over the whole run. A running job has
        neither before a step ends : it counts in the allocated series only."""
        first, width = now - 86400 * days, 86400 * days / cls.BINS
        names = ("running", "cpus", "gpus", "mem", "cpu_used", "mem_used")
        series = {name: [0.0] * cls.BINS for name in names}
        for job in jobs:
            if job["start"] is None:
                continue
            start, end = max(job["start"], first), min(job["end"] or now, now)
            if end <= start:
                continue
            values = (1, job["cpus"], job["gpus"], job["mem_req"] or 0,
                      job["cpus"] * (job["cpu_eff"] or 0), job["mem_peak"] or 0)
            for index in range(int((start - first) // width), min(cls.BINS - 1, int((end - first) // width)) + 1):
                low = first + index * width
                share = (min(end, low + width) - max(start, low)) / width
                if share > 0:
                    for name, value in zip(names, values):
                        series[name][index] += share * value
        return {"start": first, "step": width, **{name: [round(value, 3) for value in values] for name, values in series.items()}}

    @classmethod
    def rates(cls, known, steps, stamp, alive=(), pause=600):
        """The CPUs each running step uses, from its cumulated CPU time at successive readings : the new state, the total
        over the jobs whose steps all have a rate, and the number of jobs left out of it (a job with a step not
        measured yet is not counted for a part of its use, which would read as idle).

        `known` and the state returned map a step to `[marks, rate, seen]` : `seen` is the `[time, CPU seconds]` of
        its counter when it last moved (or was first read), and the marks are those at which it was seen to
        change, on the clock of the cluster. The nodes refresh these counters at their own pace (every 30 s or
        so), which is not the pace of the samples : the rate is taken from the oldest mark kept, `WINDOW` seconds
        back at least once there are enough of them, so that it spans several refreshes ; between two of them
        a step keeps its rate. The first reading of a step is not a mark (its counter may be up to one refresh
        old), and a rate is given only over `SPAN` seconds at least, since a change is dated by the sample that
        saw it : a step has a rate after two minutes or so. A counter that has not moved for `STALE` seconds
        gives 0, and no rate from the moment it moves again until `SPAN` seconds are measured. A new step, a counter that went back (a requeue) or a pause of the sampling of more than ten
        minutes (`pause` : more when the background sampling is slower than that) start over, without a rate. When no step of an `alive` job is in this reading (its node did
        not answer), its steps keep their state for ten minutes ; a step that ended beside others is dropped."""
        state = {}
        for step, spent in steps.items():
            marks, rate, (moment, before) = known.get(step, ([], None, [stamp, spent]))
            if spent > before and 0 < stamp - moment <= pause:
                marks = marks + [[stamp, spent]]
                while len(marks) > 2 and stamp - marks[1][0] >= cls.WINDOW:
                    marks = marks[1:]
                if stamp - marks[0][0] >= cls.SPAN:
                    rate = (spent - marks[0][1]) / (stamp - marks[0][0])
                elif rate == 0:
                    rate = None
                state[step] = [marks, rate, [stamp, spent]]
            elif spent != before:
                state[step] = [[], None, [stamp, spent]]
            elif stamp - moment > cls.STALE:
                state[step] = [[[stamp, spent]], 0.0, [stamp, spent]]
            else:
                state[step] = [marks, rate, [moment, before]]
        answered = {step.partition(".")[0] for step in steps}
        for step, entry in known.items():
            job = step.partition(".")[0]
            if job in alive and job not in answered and stamp - entry[2][0] <= pause:
                state[step] = entry
        jobs = {}
        for step, entry in state.items():
            jobs.setdefault(step.partition(".")[0], []).append(entry[1])
        found = [sum(rates) for rates in jobs.values() if None not in rates]
        return state, (sum(found) if found else None), len(jobs) - len(found)

    def live(self):
        """One sample of what the running jobs hold and use now, added to the samples kept in memory.

        Allocated : the CPUs, GPUs and memory of the running jobs (`tres-alloc` of `squeue`). Used : `sstat` gives,
        for each running step, its cumulated CPU time and its resident memory ; the CPUs in use come from two
        readings (`rates`), so the first sample has none, and never exceed the CPUs allocated. Slurm does not
        know the load of a GPU : `probe` reads it on request. At most `MEASURED` jobs are asked to `sstat`.
        One sample is taken at a time, `SPACING` seconds at least after the previous one : another call returns
        at once. A sample carries the pace of the background sampling when it was taken (`pace`, 0 when off) :
        the page breaks the line of a graph from it. A `squeue` that fails raises, it does not give a sample of zeros ; an `sstat` that does not
        answer while jobs run gives a sample without a use, and leaves the counters as they were ; jobs whose
        steps have no CPU time at all (an interactive allocation without a step) use 0."""
        if not self.sampling.acquire(blocking=False):
            return
        try:
            if time.monotonic() - self.sampled < self.SPACING:
                return
            code, out, err = self.run(self.LIVE.format(sep=self.SEP, measured=self.MEASURED))
            parts = out.split(self.SEP)
            if code or len(parts) != 3:
                raise ValueError("the queue could not be read" + (" : " + err.splitlines()[-1] if err else ""))
            queue, allocated, steps = parts
            listed = [line.split("|") for line in queue.splitlines() if "|" in line]
            states = [line[0] for line in listed]
            alive = {line[1].strip() for line in listed if line[0] == "RUNNING"}
            cpus = gpus = memory = 0
            holders = []
            for line in allocated.splitlines():
                job, _, tres = line.strip().partition(" ")
                held = self.tres(tres.strip())
                cpus, gpus, memory = cpus + held[0], gpus + held[1], memory + (held[2] or 0)
                if held[1] and self.JOB_ID.fullmatch(job):
                    holders.append(job)
            clock, _, steps = steps.strip().partition("\n")
            try:
                stamp = float(clock)
            except ValueError:
                stamp = time.time()
            spent, resident, measured = {}, 0, set()
            for row in self.rows(steps, ("id", "usage")):
                items = dict(item.split("=", 1) for item in row["usage"].split(",") if "=" in item)
                seconds = self.seconds(items.get("cpu"))
                if seconds is not None:
                    spent[row["id"]] = seconds
                resident += self.size(items.get("mem")) or 0
                measured.add(row["id"].partition(".")[0])
            running = states.count("RUNNING")
            with self.lock:
                rate, missed = None, 0
                if spent or not running:
                    self.counters, rate, missed = self.rates(self.counters, spent, stamp, alive, max(600, 2 * self.sample + 30))
                elif measured:
                    rate = 0.0
                self.gpu_jobs = tuple(holders)
                sample = {
                    "t": time.time(), "running": running, "pending": states.count("PENDING"),
                    "cpus": cpus, "gpus": gpus, "mem": memory,
                    "cpu_used": None if rate is None else round(min(rate, cpus), 3), "mem_used": resident if measured else None,
                    "gpu_used": None, "gpu_mem": None, "gpu_read": None, "measured": len(measured), "unrated": missed,
                    "pace": self.sample,
                }
                self.samples.append(sample)
                self.sampled = time.monotonic()
            self.record(dict(sample))
        finally:
            self.sampling.release()

    def probe(self, hours=1):
        """The load of the GPUs of the running jobs, read once with `nvidia-smi` inside each job, on request only :
        like the GPU tab, it adds a step to each job asked. At most `PROBED` jobs, on the first node of each ;
        one reading at a time and `PROBE_EVERY` seconds at least after one that was sent. The result is written in the sample
        the reading started from : the GPUs in use (the sum of the utilisations), their memory, and how many
        GPUs were read."""
        if not self.probing.acquire(blocking=False):
            raise ValueError("a reading of the GPUs is already running")
        try:
            wait = self.PROBE_EVERY - (time.monotonic() - self.probed)
            if wait > 0:
                raise ValueError(f"the GPUs were read less than {self.PROBE_EVERY} s ago, and each reading adds a step to the jobs : "
                                 f"wait {int(wait) + 1} s")
            self.live()
            with self.lock:
                sample = self.samples[-1] if self.samples else None
                jobs, total = self.gpu_jobs[:self.PROBED], len(self.gpu_jobs)
            if sample is None or not jobs:
                raise ValueError("no running job holds a GPU")
            try:
                _, out, err = self.run(self.PROBE, timeout=40, args=jobs, once=True)
            except (Busy, NotConnected):
                raise
            except Exception:
                self.probed = time.monotonic()
                raise
            self.probed = time.monotonic()
            rows = [[float(cell) for cell in match.groups()] for match in map(self.GPU_ROW.fullmatch, out.splitlines()) if match]
            if not rows:
                raise ValueError("nvidia-smi did not answer in the jobs" + (" : " + err.splitlines()[-1] if err else ""))
            with self.lock:
                sample.update(gpu_used=round(sum(row[0] for row in rows) / 100, 3), gpu_mem=int(sum(row[1] for row in rows) * 2**20),
                              gpu_read=len(rows))
            self.record(dict(sample))
            gpus = [{"load": row[0] / 100, "memory": int(row[1] * 2**20), "total": int(row[2] * 2**20)} for row in rows]
            return {"samples": self.recent(hours), "limit": self.MEASURED, "probed": len(jobs), "holders": total, "gpus": gpus}
        finally:
            self.probing.release()

    def recent(self, hours):
        """The samples of the last `hours` hours : the page is sent the range it draws, not the day the server keeps."""
        since = time.time() - 3600 * hours
        with self.lock:
            return [sample for sample in self.samples if sample["t"] >= since]

    def resources(self, days, fresh=False, hours=1):
        """The samples of the last `hours` hours (`days` = 0, a new one taken first) or the timeline of the last `days` days."""
        if not days:
            self.live()
            return {"samples": self.recent(hours), "limit": self.MEASURED}
        data = self.jobs(days, fresh)
        return {"timeline": data["timeline"], "read": data["read"], "days": days if days in self.DAYS else 1, "error": data["error"]}

    ##### Cluster #####

    def overview(self):
        """Free GPUs (the command and the reading of the site), the partitions with their per-user limits, and the fair-share of the account."""
        _, out, _ = self.run(
            f"{{ {self.site.AVAIL or 'true'}; }} 2>&1; echo '{self.SEP}'; sinfo -s -h -o '%P|%a|%l|%F'; echo '{self.SEP}'; "
            f"scontrol show partition -o; echo '{self.SEP}'; sacctmgr -nP show qos format=Name,MaxTRESPU; echo '{self.SEP}'; "
            f"sshare -U -n -P -o Account,RawShares,RawUsage,EffectvUsage,FairShare; echo '{self.SEP}'; "
            "sacctmgr -nP show assoc user=\"$USER\" format=Account,MaxJobs,MaxSubmit"
        )
        avail, summary, described, qos, share, assoc = (part.strip() for part in (out.split(self.SEP) + [""] * 5)[:6])
        gpus = self.site.free(avail)
        limits = {row["name"]: row["limit"] for row in self.rows(qos, ("name", "limit"))}
        details = {}
        for line in described.splitlines():
            fields = dict(item.split("=", 1) for item in line.split() if "=" in item)
            details[fields.get("PartitionName", "")] = fields
        partitions = []
        for row in self.rows(summary, ("name", "state", "time", "nodes")):
            name = row["name"].rstrip("*")
            fields = details.get(name, {})
            counts = (row["nodes"].split("/") + ["0"] * 4)[:4]
            limit = ",".join(item for item in limits.get(fields.get("QoS", ""), "").split(",") if item and not item.startswith("license"))
            partitions.append({
                "name": name, "default": row["name"].endswith("*"), "state": row["state"], "time": row["time"],
                "limit": limit, "tier": fields.get("PriorityTier", ""), "preempt": fields.get("PreemptMode", ""),
                "idle": self.number(counts[1]), "total": self.number(counts[3]),
            })
        share = self.rows(share, ("account", "shares", "usage", "effective", "fairshare"))
        assoc = self.rows(assoc, ("account", "max_jobs", "max_submit"))
        return {"gpus": gpus, "partitions": partitions, "share": share[0] if share else None, "assoc": assoc[0] if assoc else None}

    ##### Storage : reading #####

    def areas(self):
        """The storage areas as two dictionaries, their paths and their real paths (symbolic links resolved).

        The personal areas of the site (`AREAS`, the names of environment variables of the cluster : `HOME`…) are under these names ; a
        directory given at start-up is under its own path. An absolute path is never one of these names : whatever
        it is called, an extra directory cannot pass for a personal area."""
        with self.lock:
            if self.known_roots is not None:
                return self.known_roots
        _, out, _ = self.run(
            "for root in " + " ".join(f'"${name}"' for name in self.site.AREAS) + ' "$@"; do '
            'printf \'%s\\t%s\\n\' "$root" "$(realpath -e -- "$root" 2>/dev/null)"; done',
            args=self.extra_roots,
        )
        found, reals = {}, {}
        for name, line in zip(self.site.AREAS + self.extra_roots, out.splitlines()):
            path, real = (line.split("\t") + [""])[:2]
            if path.startswith("/") and len(path) > 1:
                found[name] = posixpath.normpath(path)
                reals[name] = real or found[name]
        # Kept once the cluster named one of them : an area it does not have is asked for once, not at each request.
        if any(name in found for name in self.site.AREAS):
            with self.lock:
                self.known_roots = (found, reals)
        return found, reals

    def roots(self):
        """The path of each storage area, personal and extra."""
        return self.areas()[0]

    def personal(self):
        """The storage areas of the account itself, the only ones where files are changed."""
        return {name: root for name, root in self.roots().items() if name in self.site.AREAS}

    @staticmethod
    def holder(path, roots):
        """The name of the deepest of `roots` that contains a path ; '' when none does."""
        return max((name for name, root in roots.items() if path == root or path.startswith(root + "/")),
                   key=lambda name: len(roots[name]), default="")

    def path(self, value, change=False):
        """A normalised absolute path inside one of the storage roots ; for a `change`, inside a personal area only.

        Like a job id, it reaches a script as a positional argument (`"$1"`), never inside its text. With `--no-files`
        no path is taken : every reading and change of the files goes through here."""
        if not self.browse:
            raise PermissionError("the files are not shown : the server was started with --no-files")
        if not isinstance(value, str) or not value.startswith("/") or value.startswith("//") or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("invalid path")
        value = posixpath.normpath(value)
        area = self.holder(value, self.roots())
        if not area:
            raise ValueError("path outside the storage areas")
        if change and area not in self.site.AREAS:
            raise ValueError("a directory added with --root is read-only : nothing is changed there from here")
        return value

    def storage(self):
        """Use and quota of each storage area, read and understood by the site (`QUOTA`, `quota`)."""
        roots = self.roots()
        _, out, _ = self.run(self.site.QUOTA.replace("{sep}", self.SEP), args=[roots[name] for name in self.site.AREAS if name in roots])
        areas = {name: {"name": name, "path": path, "personal": name in self.site.AREAS, "used": None, "limit": None,
                        "files": None, "files_limit": None, "whole": False}
                 for name, path in roots.items()}
        raw = self.site.quota(self.ANSI.sub("", out), areas, self.SEP)
        return {"areas": list(areas.values()), "raw": raw.strip()}

    def files(self, path):
        """One level of a directory : type, size, modification and access times, link target of each entry.

        `area` is the storage area the path was reached from ; `place` the one that really holds the directory,
        which differs behind a symbolic link (of the home directory into another area) : the backup and deletion policy shown is that of
        `place`, and files are changed only where `place` is a personal area. An entry whose name cannot be
        sent back (control character, invalid UTF-8) is marked `odd`."""
        path = self.path(path)
        code, out, err = self.run(
            "[ -d \"$1\" ] || { echo 'not a directory' >&2; exit 2; }; printf '%s\\0' \"$(realpath -e -- \"$1\")\"; "
            f"find -H \"$1\" -mindepth 1 -maxdepth 1 -printf '%y|%Y|%s|%T@|%A@|%f\\0%l\\0' | head -z -n {2 * self.LISTED}",
            args=(path,),
        )
        if code == 2:
            raise ValueError("not a directory, or not readable")
        real, *cells = out.split("\0")
        entries = []
        for record, target in zip(cells[0::2], cells[1::2]):
            kind, kept, size, modified, accessed, name = (record.split("|", 5) + [""] * 5)[:6]
            entries.append({
                "name": name, "dir": kept == "d", "link": target if kind == "l" else "", "broken": kept in ("N", "L"),
                "size": self.number(size), "modified": float(modified or 0), "accessed": float(accessed or 0),
                "odd": any(ord(char) < 32 or ord(char) == 127 or char == "\ufffd" for char in name),
            })
        entries.sort(key=lambda entry: (not entry["dir"], entry["name"].lower()))
        roots, reals = self.areas()
        area, place = self.holder(path, roots), self.holder(real, reals)
        return {"path": path, "area": area, "root": roots[area], "place": place, "entries": entries,
                "truncated": len(entries) >= self.LISTED,
                "changes": self.writable and area in self.site.AREAS and place in self.site.AREAS, "error": err if code else ""}

    def sealing(self):
        """The start of a script that tells what holds keys or credentials (`SECRETS`, under the home directory of the cluster).

        `SHIELD` resolves each name one component at a time (`trail`) : `hidden` holds the name and the real path it leads to, `links`
        every symbolic link met on the way to one that exists, and every directory left by a `..` of a link ; the links found in a
        directory of the list (two levels, 200 at most : a key of `~/.ssh` that is a link to a file kept elsewhere) are resolved the same
        way, so that what they lead to is hidden too. `READ` then resolves the path asked and stops with the code 3 when the real
        file, or any place the path goes through, is one of `hidden` or inside one, whatever the link that led to it (4 : the home directory cannot be resolved, and nothing is
        read) ; the file is then opened once, and what was opened is checked again where the system tells it (`/proc`), so that a path
        changed between the check and the reading gives no credential : the readers take the open file, not the path. A change script refuses to rename or move one, what is inside one and a directory that holds one — under another name it
        would be read — and to delete, rename or move a link of `links` or a directory that holds one : without the link, what it led to
        would be a file like any other."""
        return "secrets=(" + " ".join(shlex.quote(name) for name in self.SECRETS) + "); "

    def preview(self, path, end):
        """The first or last 256 KiB of a file, as text ; a binary file is reported, not shown ; a key or a credential file is refused."""
        path = self.path(path)
        tool = "head" if end == "head" else "tail"
        code, out, _ = self.run(self.sealing() + self.READ + f'stat -L -c %s -- /dev/fd/3 && {tool} -c {self.PREVIEW} <&3', args=(path,))
        if code:
            raise ValueError({3: self.SEALED, 4: self.HOMELESS}.get(code, "not a readable file"))
        size, _, text = out.partition("\n")
        binary = "\0" in text
        return {"path": path, "size": self.number(size), "binary": binary, "text": "" if binary else text,
                "cut": self.number(size) > self.PREVIEW, "end": tool}

    def measure(self, path):
        """Number and total size of the files under a directory ; a lower bound when the walk is stopped."""
        path = self.path(path)
        code, out, _ = self.heavy(
            f"timeout {self.WALK} nice -n 19 find -H \"$1\" -xdev -type f -printf '%s\\n' 2>/dev/null "
            "| awk '{ n += 1; s += $1 } END { printf \"%.0f %.0f\\n\", n, s }'; echo \"${PIPESTATUS[0]}\"",
            timeout=self.WALK + 20, args=(path,),
        )
        lines = out.split()
        if len(lines) < 3:
            raise ValueError("could not measure this directory")
        return {"path": path, "files": int(lines[0]), "bytes": int(lines[1]), "partial": lines[2] == "124", "seconds": self.WALK}

    def unused(self, path, days):
        """Files under a directory neither read nor modified for `days` days (an area may delete them after some : `Site.PURGE`).

        `find` counts whole days and `+N` means more than N : `+(days - 1)` is « at least `days` days »."""
        path = self.path(path)
        days = self.integer(days, 1, 3650)
        code, out, _ = self.heavy(
            f"timeout {self.WALK} nice -n 19 find -H \"$1\" -xdev -type f -atime +{days - 1} -mtime +{days - 1} "
            f"-printf '%A@|%T@|%s|%P\\0' 2>/dev/null | head -z -n 500; printf '%s' \"${{PIPESTATUS[0]}}\"",
            timeout=self.WALK + 20, args=(path,),
        )
        *records, status = out.split("\0")
        entries = []
        for record in records:
            accessed, modified, size, name = (record.split("|", 3) + [""] * 3)[:4]
            entries.append({"name": name, "size": self.number(size), "used": max(float(accessed or 0), float(modified or 0))})
        entries.sort(key=lambda entry: entry["used"])
        return {"path": path, "days": days, "entries": entries, "truncated": len(entries) >= 500, "partial": status.strip() == "124",
                "seconds": self.WALK}

    def download(self, path):
        """The content of a file of at most 100 MiB ; a larger one is for `rsync`.

        One download at a time, so that a slot stays free for the short commands of the page. The size is read
        first and exactly that many bytes are sent : a file that grows meanwhile (the log of a running job) is
        cut at the size it had."""
        path = self.path(path)
        code, out, _ = self.heavy(
            self.sealing() + self.READ + 'size=$(stat -L -c %s -- /dev/fd/3) || exit 2; echo "$size"; '
            f'[ "$size" -le {self.DOWNLOAD} ] && head -c "$size" <&3',
            timeout=300, args=(path,), raw=True,
        )
        cut = out.find(b"\n")
        size, data = out[:max(cut, 0)], memoryview(out)[cut + 1:]
        if code in (3, 4):
            raise ValueError(self.SEALED if code == 3 else self.HOMELESS)
        if code == 2 or not size.isdigit():
            raise ValueError("not a readable file")
        if int(size) > self.DOWNLOAD:
            raise ValueError(f"this file is larger than 100 MiB : copy it from a terminal with  rsync -avP {self.host}:{shlex.quote(path)} .")
        if len(data) != int(size):
            raise ValueError("the transfer was interrupted")
        return data

    ##### Storage : changes (delete, rename, move, new directory) #####

    def entries(self, paths):
        """Validated paths of entries to change : inside a storage area, never an area itself."""
        if not isinstance(paths, list) or not 1 <= len(paths) <= 500:
            raise ValueError("between 1 and 500 paths are expected")
        paths = [self.path(path, change=True) for path in paths]
        if any(path in self.roots().values() for path in paths):
            raise ValueError("a storage area itself cannot be changed")
        return paths

    @staticmethod
    def name(value):
        """A validated entry name : one path component, without control characters."""
        if (not isinstance(value, str) or not value.strip() or value in (".", "..") or "/" in value
                or len(value.encode()) > 255 or any(ord(char) < 32 or ord(char) == 127 for char in value)):
            raise ValueError("invalid name")
        return value

    def permit(self):
        """Refuses a change in read-only mode, before any validation that would query the cluster."""
        if not self.writable:
            raise PermissionError("read-only : start the server with --writable to change files")

    def change(self, script, args):
        """Runs one of the change scripts ; each line of its output is the outcome for one entry.

        The script checks again, on the cluster, that the real directory of each entry (symbolic links resolved)
        is inside a personal area, and that the entry is not one of `PROTECTED` : a link cannot lead a change
        outside. The whole call has `WALK` seconds, at low priority : what is not done is reported, not left running.
        The areas and the protected paths are resolved once, and the time left is tested before anything else for
        each entry : the checks cost one process per directory, not per entry. The script is sent once (a rename
        sent again would answer « no such file ») ; if the connection drops meanwhile, the outcomes received are
        returned with a warning. A directory given with `--root` stays read-only under any name : its real path is
        handed to the script (`shut`), which refuses what really is inside it, reached through a link of a personal area too, and the directory itself or one that holds it (`protected`)."""
        self.permit()
        closed = [real for name, real in self.areas()[1].items() if name not in self.site.AREAS]
        roots, keep, shut = (" ".join(shlex.quote(item) for item in items) for items in (self.personal().values(), self.PROTECTED, closed))
        try:
            code, out, err = self.run(f"roots=({roots}); keep=({keep}); shut=({shut}); budget={self.WALK}; " + self.sealing() + self.SHIELD + self.TOOLS + script,
                                      timeout=self.WALK + 20, args=args, once=True)
        finally:
            with self.lock:
                self.outputs.clear()
        # One outcome per line of the script : a name may hold a line separator of Unicode, which is not the end of a line here.
        results = [dict(zip(("status", "path", "message"), (line.split("\t", 2) + ["", ""])[:3])) for line in out.split("\n") if line]
        ok = code == 0 and bool(results) and all(result["status"] == "done" for result in results)
        error = "" if results else (err or "no answer from the cluster")
        if code == 4 and not results:
            error = self.HOMELESS
        if code == 255:
            error = "the SSH connection dropped during the change : the outcomes listed may be incomplete, check the directory"
        return {"ok": ok, "results": results, "error": error}

    def remove(self, paths):
        """Deletes files and whole directories ; a symbolic link is removed, not what it points to."""
        self.permit()
        return self.change(self.REMOVE, self.entries(paths))

    def rename(self, path, name):
        """Renames an entry inside its directory ; never overwrites."""
        self.permit()
        return self.change(self.RENAME, (*self.entries([path]), self.name(name)))

    def move(self, paths, directory):
        """Moves entries into another directory of the same file system ; never overwrites."""
        self.permit()
        return self.change(self.MOVE, (self.path(directory, change=True), *self.entries(paths)))

    def mkdir(self, path, name):
        """Creates a directory."""
        self.permit()
        return self.change(self.MKDIR, (self.path(path, change=True), self.name(name)))


##### Local side : a loopback-only HTTP server with a per-launch token #####


class Handler(BaseHTTPRequestHandler):
    """Serves the page and a small JSON API ; every API call needs the token printed at start-up.

    The arguments travel in the request body and the API paths avoid the words that browser content blockers
    filter : EasyPrivacy blocks `/log?id=`, which made the output of a job unreachable from a browser with uBlock Origin."""

    cluster = None
    token = ""
    page = None
    hosts = ()
    door = None
    OPENING = 120
    # The seconds a connection has to send its request : one that stays silent does not keep a thread (`reply` lifts the limit,
    # the answer of the cluster and the sending of a large file take the time they need).
    timeout = 15

    def setup(self):
        """The whole request has twice the time given to a silence : a connection that sends a byte now and then keeps no thread either."""
        super().setup()
        self.limit = threading.Timer(2 * self.timeout, self.cut)
        self.limit.daemon = True
        self.limit.start()

    def cut(self):
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def finish(self):
        self.limit.cancel()
        super().finish()

    def log_message(self, *args):
        """No request log (job names and paths stay out of the terminal)."""

    def handle(self):
        """A browser that left before its answer (a reload, a closed tab : Chromium drops its pending requests) is not an error of the server,
        nor is a connection that did not send its request in time."""
        try:
            super().handle()
        except OSError:
            pass

    def reply(self, status, payload, content="application/json"):
        body = payload if isinstance(payload, (bytes, memoryview)) else json.dumps(payload).encode()
        self.connection.settimeout(None)
        self.limit.cancel()
        self.send_response(status)
        self.send_header("Content-Type", content if content.endswith("octet-stream") else content + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; connect-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; worker-src blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
        )
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(body)

    def allowed(self):
        """Loopback `Host` header (DNS rebinding) and the token in a custom header (cross-site requests).

        The comparison is on bytes : `compare_digest` raises on a non-ASCII string."""
        origin = self.headers.get("Origin")
        if self.headers.get("Host") not in self.hosts or (origin is not None and origin not in ["http://" + host for host in self.hosts]):
            return False
        known = secrets.compare_digest(self.headers.get("X-Auth", "").encode(), self.token.encode())
        if known and self.door:
            self.forget()
        return known

    @classmethod
    def usher(cls, address):
        """Opens the address in the default browser without writing its token on a command line.

        An address given to the browser as an argument is shown by `ps` to every user of the computer, for as long as the
        browser it started runs. The browser is given a private file instead (a directory of its owner alone, a file of
        mode 600), which sends it to the address : `ps` shows a path. The directory is made in the home directory, where the
        browsers that live in a sandbox (snap, Flatpak) can read a file, which they cannot under `/tmp` ; in the directory of
        temporary files when that fails. It is removed at the first request that carries the token, after two minutes, and
        when the server stops. False when no file could be written or no browser started : the address printed is then opened by hand."""
        home = os.path.expanduser("~")
        parents = ([home] if os.path.isabs(home) else []) + [None]
        for parent in parents:
            cls.sweep(parent)
        for parent in parents:
            try:
                cls.door = door = Path(tempfile.mkdtemp(prefix="cluster-monitor-opening-", dir=parent))
                with open(os.open(door / "open.html", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as file:
                    file.write('<!doctype html><meta charset="utf-8"><title>Cluster monitor</title><meta name="referrer" content="no-referrer">'
                               f'<meta http-equiv="refresh" content="0; url={address}"><a href="{address}">Open the cluster monitor</a>\n')
                break
            except (OSError, RuntimeError):
                cls.forget()
        else:
            print("The file that hands the address to the browser could not be written : open the address above.", flush=True)
            return False
        timer = threading.Timer(cls.OPENING, cls.forget)
        timer.daemon = True
        timer.start()
        try:
            started = webbrowser.open((door / "open.html").as_uri())
        except Exception:
            started = False
        if not started:
            cls.forget()
            print("The browser could not be started : open the address above.", flush=True)
        return bool(started)

    @classmethod
    def sweep(cls, parent):
        """Removes the files of the address left there by servers that were killed : the directories of that name of this user, older
        than the time a server keeps one, that hold nothing else."""
        try:
            for door in Path(parent or tempfile.gettempdir()).glob("cluster-monitor-opening-*"):
                state = door.lstat()
                if (stat.S_ISDIR(state.st_mode) and state.st_uid == os.getuid() and time.time() - state.st_mtime > cls.OPENING + 60
                        and all(entry.name == "open.html" and stat.S_ISREG(entry.lstat().st_mode) for entry in door.iterdir())):
                    shutil.rmtree(door, ignore_errors=True)
        except OSError:
            pass

    @classmethod
    def forget(cls):
        """Removes the file that handed the address to the browser ; safe to call twice, and from several threads."""
        door, cls.door = cls.door, None
        if door:
            shutil.rmtree(door, ignore_errors=True)

    @staticmethod
    def port(text):
        """The `--port` argument : 0 lets the system choose a free one."""
        if not (text.isascii() and text.isdigit()) or int(text) > 65535:
            raise argparse.ArgumentTypeError("a port is a number from 0 to 65535 (0 : a free one, chosen by the system)")
        return int(text)

    def guarded(self, call):
        """Runs an API call and maps its failures to HTTP statuses : the page always gets a JSON answer.

        The call and the encoding of its result are guarded, not the sending : an answer that cannot be sent (the browser
        left) is the business of `handle`, and an error of connection raised by a call itself is an internal error like any other."""
        if not self.allowed():
            return self.reply(403, {"error": "forbidden"})
        try:
            result = call()
            binary = isinstance(result, (bytes, memoryview))
            body = result if binary else json.dumps(result).encode()
        except PermissionError as error:
            return self.reply(405, {"error": str(error)})
        except NotConnected:
            return self.reply(503, {"error": "not_connected"})
        except Busy as error:
            return self.reply(429, {"error": str(error) or "the SSH connection refused the command (too many sessions at once, or it dropped) ; try again"})
        except subprocess.TimeoutExpired:
            return self.reply(504, {"error": "the cluster did not answer in time"})
        except KeyError:
            return self.reply(400, {"error": "unknown action or missing argument"})
        except (ValueError, TypeError) as error:
            return self.reply(400, {"error": str(error) or "bad request"})
        except Exception as error:
            print(f"internal error : {type(error).__name__}", flush=True)
            return self.reply(500, {"error": f"internal error ({type(error).__name__})"})
        return self.reply(200, body, "application/octet-stream" if binary else "application/json")

    ROUTES = {
        "/api/status": lambda cluster, args: cluster.status(),
        "/api/jobs": lambda cluster, args: Cluster.brief(cluster.jobs(Cluster.integer(args.get("days", 1), 1, 30), args.get("fresh") is True, args.get("quiet") is True),
                                                          args.get("since")),
        "/api/job": lambda cluster, args: cluster.info(args.get("id")),
        "/api/output": lambda cluster, args: cluster.output(args.get("id"), args.get("stream", "out"), args.get("lines", 200)),
        "/api/gpu": lambda cluster, args: cluster.gpu(args.get("id")),
        "/api/resources": lambda cluster, args: cluster.resources(Cluster.integer(args.get("days", 0), 0, 30), args.get("fresh") is True,
                                                                  Cluster.integer(args.get("hours", 1), 1, 24)),
        "/api/gpus": lambda cluster, args: cluster.probe(Cluster.integer(args.get("hours", 1), 1, 24)),
        "/api/efficiency": lambda cluster, args: cluster.efficiency(Cluster.integer(args.get("days", 1), 1, 30)),
        "/api/enrol": lambda cluster, args: cluster.enrol(args["token"]),
        "/api/background": lambda cluster, args: cluster.background(args["seconds"]),
        "/api/overview": lambda cluster, args: cluster.overview(),
        "/api/storage": lambda cluster, args: cluster.storage(),
        "/api/files": lambda cluster, args: cluster.files(args.get("path")),
        "/api/preview": lambda cluster, args: cluster.preview(args.get("path"), args.get("end", "tail")),
        "/api/measure": lambda cluster, args: cluster.measure(args.get("path")),
        "/api/unused": lambda cluster, args: cluster.unused(args.get("path"), args.get("days", 60)),
        "/api/action": lambda cluster, args: cluster.act(args["action"], args.get("id"), args.get("ids")),
        "/api/download": lambda cluster, args: cluster.download(args.get("path")),
        "/api/remove": lambda cluster, args: cluster.remove(args.get("paths")),
        "/api/rename": lambda cluster, args: cluster.rename(args.get("path"), args.get("name")),
        "/api/move": lambda cluster, args: cluster.move(args.get("paths"), args.get("directory")),
        "/api/mkdir": lambda cluster, args: cluster.mkdir(args.get("path"), args.get("name")),
    }

    def do_GET(self):
        """The page only ; the API is POST."""
        if urlparse(self.path).path != "/":
            return self.reply(404, {"error": "not found"})
        if self.headers.get("Host") not in self.hosts:
            return self.reply(403, b"forbidden", "text/plain")
        return self.reply(200, self.page.read_bytes(), "text/html")

    def do_POST(self):
        """One API call : a fixed path and its arguments as a JSON object in the body, never in the address."""
        route = self.ROUTES.get(urlparse(self.path).path)
        if route is None:
            return self.reply(404, {"error": "not found"})
        if not self.allowed():
            return self.reply(403, {"error": "forbidden"})
        length = self.headers.get("Content-Length") or ""
        if not (length.isascii() and length.isdigit()) or int(length) > 512 * 1024 or self.headers.get_content_type() != "application/json":
            return self.reply(400, {"error": "bad request"})
        length = int(length)
        raw = self.rfile.read(length)
        self.limit.cancel()
        if len(raw) != length:
            # Cut before its end (`cut`, or a client that left) : nothing is done with half a request.
            return None

        def call():
            # A fixed message : the text of a decoding error quotes bytes of the body, which can hold a token.
            try:
                args = json.loads(raw)
            except (ValueError, RecursionError):
                raise ValueError("bad request") from None
            if not isinstance(args, dict):
                raise ValueError("bad request")
            return route(self.cluster, args)

        return self.guarded(call)


class Server(ThreadingHTTPServer):
    """The HTTP server : a thread per request, `ROOM` of them at most. A connection beyond is closed at once, so that connections
    opened from this computer and left silent use up neither the threads nor the files of the process ; each is closed anyway
    when it has not sent its request in time (`Handler.timeout`)."""

    daemon_threads = True
    ROOM = 64
    request_queue_size = ROOM

    def __init__(self, *args):
        super().__init__(*args)
        self.room = threading.BoundedSemaphore(self.ROOM)

    def process_request(self, request, client_address):
        if not self.room.acquire(blocking=False):
            return self.shutdown_request(request)
        try:
            super().process_request(request, client_address)
        except Exception:
            self.room.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.room.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", default="mila", metavar="NAME",
                        help="the cluster : the name of the file beside server.py that describes it (mila, the default), or generic for a Slurm cluster of which nothing is known")
    parser.add_argument("--ssh-host", metavar="ALIAS", help="alias of the login node in the SSH configuration (the one of the site without it : mila)")
    parser.add_argument("--port", type=Handler.port, default=8765, metavar="N", help="local port (8765 without it ; 0 : a free one, chosen by the system)")
    parser.add_argument("--no-browser", action="store_true", help="print the address without opening it")
    parser.add_argument("--root", action="append", default=[], metavar="PATH",
                        help="one more storage area to browse, e.g. the shared directory of a group (repeatable)")
    parser.add_argument("--writable", action="store_true",
                        help="allow the storage view to delete, rename, move entries and create directories (read-only without it)")
    parser.add_argument("--actions", action="store_true",
                        help="allow the job actions (kill, kill pending, kill all, SIGTERM, hold, release, requeue) : the page only watches without it")
    parser.add_argument("--no-actions", action="store_true",
                        help="the page only watches : the default since 0.1.6, kept so that a command written for an earlier version still starts")
    parser.add_argument("--no-files", action="store_true",
                        help="no Storage view : the files of the account are neither listed, previewed, downloaded nor changed from the page "
                             "(the quotas stay in Resources, and the panel of a job still reads its output file)")
    parser.add_argument("--ultra", action="store_true",
                        help="for hundreds to thousands of jobs : the queue and the finished jobs drawn as mosaics (one square per job, grouped by name, filtered by state), "
                             "up to 5000 finished jobs listed, "
                             "the accounting database read once a minute at most when jobs keep leaving the queue")
    parser.add_argument("--sample", type=int, default=0, metavar="SECONDS",
                        help="start with the background sampling on : a sample of the resources of the running jobs every SECONDS (30 to 3600), "
                             "view open or not ; off without it, and turned on, off or changed from the Resources view")
    parser.add_argument("--keep", metavar="FILE",
                        help="keep the samples of the Resources view in FILE, so that the last day survives a restart ; nothing is written without it")
    parser.add_argument("--version", action="version", version=f"cluster monitor {VERSION}")
    args = parser.parse_args()
    roots = [posixpath.normpath(path) for path in args.root]
    if any(not path.startswith("/") or path.startswith("//") or path == "/" or any(ord(char) < 32 or ord(char) == 127 for char in path)
           for path in roots):
        parser.error("--root takes an absolute path")
    if args.sample and not Cluster.PACE[0] <= args.sample <= Cluster.PACE[1]:
        parser.error("--sample takes {} to {} seconds".format(*Cluster.PACE))
    if args.actions and args.no_actions:
        parser.error("--actions and --no-actions cannot be given together")
    if args.no_files and (args.writable or roots):
        parser.error("--no-files shows no file : it cannot be given with --writable nor --root")

    try:
        site = Site.load(args.site)(os.environ)
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    host = args.ssh_host or site.HOST
    if not host:
        parser.error(f"--ssh-host is needed with --site {args.site} : the alias of the login node in the SSH configuration")
    if host.startswith("-"):
        parser.error("--ssh-host takes the alias of the login node, not an option of ssh")
    try:
        Handler.cluster = Cluster(host, roots, writable=args.writable, actions=args.actions, sample=args.sample, keep=args.keep, site=site,
                                  ultra=args.ultra, files=not args.no_files)
    except (OSError, ValueError) as error:
        parser.exit(1, f"Cannot use the file of --keep : {getattr(error, 'strerror', None) or error}. Nothing was changed.\n")
    threading.Thread(target=Handler.cluster.sample_forever, daemon=True).start()
    Handler.token = secrets.token_urlsafe(24)
    Handler.page = Path(__file__).resolve().parent / "index.html"
    try:
        server = Server(("127.0.0.1", args.port), Handler)
    except OSError as error:
        parser.exit(1, f"Cannot listen on port {args.port} ({error.strerror}) : another monitor may be running ; stop it or use --port.\n")
    port = server.server_address[1]
    # A browser leaves the port out of `Host` when it is the one of http.
    Handler.hosts = (f"127.0.0.1:{port}", f"localhost:{port}") + (("127.0.0.1", "localhost") if port == 80 else ())
    address = f"http://127.0.0.1:{port}/#t={Handler.token}"
    print(f"Cluster monitor {VERSION}, {site.NAME} (ssh {host}) : {address}\n(loopback only ; the token changes at every launch ; Ctrl+C to stop)", flush=True)
    print("Storage : " + ("not shown (--no-files)." if args.no_files else "files can be deleted, renamed and moved (--writable)." if args.writable
                          else "read-only (add --writable to manage files)."), flush=True)
    print("Jobs : " + ("can be killed, held and requeued from the page (--actions)." if args.actions else "watched only (add --actions to kill, hold and requeue).")
          + (" Many jobs (--ultra) : drawn as mosaics, grouped by name, 5000 finished jobs listed." if args.ultra else ""), flush=True)
    print("Resources : " + (f"sampled every {args.sample} s in the background, view open or not (--sample ; changed in the Resources view)" if args.sample
                            else "sampled while the view is open (background sampling off : turned on in the Resources view, or with --sample)")
          + (f" ; kept in {args.keep} (--keep)." if args.keep else " ; kept in memory only (--keep to keep a day over a restart)."), flush=True)
    for line in site.report():
        print(line, flush=True)
    if not Handler.cluster.connected():
        print("No open SSH connection. " + site.CONNECT.replace("{host}", host), flush=True)
        lack = Handler.cluster.sharing()
        if lack:
            print(lack.replace("`", ""), flush=True)
    for name in ("SIGTERM", "SIGHUP"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: sys.exit(0))
    try:
        if not args.no_browser:
            # In a thread : `webbrowser` waits for the end of a program named in `BROWSER`, and the page is served meanwhile.
            threading.Thread(target=Handler.usher, args=(address,), daemon=True).start()
        server.serve_forever()
    except KeyboardInterrupt:
        print()
    finally:
        Handler.forget()


if __name__ == "__main__":
    main()
