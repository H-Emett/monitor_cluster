"""The Mila cluster for the cluster monitor : its storage areas, `disk-quota`, `savail`, its links and its usage service (SARC)."""

import datetime
import re
from urllib.parse import urlencode

import server


class Site(server.Site):
    """The Mila cluster (docs.mila.quebec). Everything the monitor shows or does that is not plain Slurm is here :
    `server.py` knows no name, address nor command of this cluster. Another cluster is another file like this one."""

    NAME = "Mila cluster"
    HOST = "mila"
    CONNECT = "In a terminal run `ssh {host}`, enter the one-time code, then press Refresh now."
    AREAS = ("HOME", "SCRATCH", "ARCHIVE")
    USES = {
        "HOME": "Code, libraries, small results. Backed up every day.",
        "SCRATCH": "Datasets, checkpoints, temporary results. No backup ; a file unused for 90 days is deleted.",
        "ARCHIVE": "Long-term storage, as archives of large files. No backup ; visible from login and CPU nodes only.",
    }
    BACKUP = {
        "HOME": "Home is backed up once a day : what is older than a day can be restored by IT support, the rest is lost.",
        "SCRATCH": "These files are in scratch, which has no backup : a deletion cannot be undone.",
        "ARCHIVE": "These files are in the archive, which has no backup : a deletion cannot be undone.",
    }
    PURGE = {"SCRATCH": 90}
    REMIND = {"SCRATCH": "Scratch deletes a file after 90 unused days : copy what matters to $ARCHIVE or $HOME."}
    SHARED = (
        ("^/network/projects/", "Shared project directory, backed up every day : it holds the files of colleagues. Read-only from here."),
        ("^/network/(datasets|weights)(/|$)", "Curated datasets and weights : read-only for everyone."),
    )
    LINKS = (
        ("Compute utilization dashboard (SM occupancy of past jobs)", "https://sarc.mila.quebec/dash/metrics"),
        ("Cluster dashboard (Grafana)", "https://dashboard.server.mila.quebec"),
        ("Documentation", "https://docs.mila.quebec"),
        ("IT support", "https://it-support.mila.quebec"),
        ("JupyterHub", "https://jupyterhub.server.mila.quebec/"),
        ("MFA portal", "https://mfa.mila.quebec"),
    )
    HINTS = (
        ("The measure the cluster watches is the SM occupancy (a notification when it stays under 15 % for two weeks ; above 30 % is good) : inside the job, run",
         "uvx milalib monitor -i 10 -m sm_occupancy", "."),
        ("Node graphs (Netdata) : run", "ssh -L 19999:{node}.server.mila.quebec:19999 {host}", "then open http://localhost:19999"),
    )
    NOTES = {
        "cluster": "Efficiency of past GPU jobs (SM occupancy) is not in Slurm : see the compute utilization dashboard below.",
        "gpu_hours": "Not comparable with the compute utilization dashboard, which counts RGU-hours (GPUs weighted by type) over UTC days",
        "resources": "see the compute utilization dashboard (Cluster view)",
        "counters": "The nodes refresh these counters every 30 s or so : the CPUs in use are a mean over the last three minutes.",
    }
    USAGE = {
        "service": "the compute utilization service of Mila (SARC)", "short": "SARC", "measure": "SM occupancy", "poor": 0.15, "good": 0.30,
        "page": "https://sarc.mila.quebec/token", "sign": "Mila sign-in", "field": "refresh_token", "host": "sarc.mila.quebec", "variable": "SARC_TOKEN",
    }
    QUOTA = "disk-quota 2>&1; echo '{sep}'; df -P -B1 -- \"$ARCHIVE\" 2>/dev/null | tail -n 1"
    AVAIL = "savail"
    # A figure over its quota is followed by `*`, a grace period may be two words (`6 days`), a count may hold commas ; a row is one line.
    HOME_ROW = re.compile(r"^[ \t]*/\S+[ \t]+(\S+?)\*?[ \t]+\S+[ \t]+(\S+)[ \t]+\S+(?:[ \t]+[a-z]+)?[ \t]+([\d,]+)\*?[ \t]+\S+[ \t]+([\d,]+)[ \t]+\S+(?:[ \t]+[a-z]+)?[ \t\r]*$", re.M)
    SCRATCH_ROW = re.compile(r"\|\|\s*([\d.,]+\s*\w+)\*?\s*\|\s*([\d.,]+\s*\w+|unlimited)\s*\|\|\s*([\d,]+)\*?\s*\|\s*([\d,]+|unlimited)")
    AVAIL_ROW = re.compile(r"^(\S+)\s+(\d+)\s*/\s*(\d+)\s*$", re.M)
    SARC = "https://sarc.mila.quebec"
    SARC_JOBS = 500

    ##### Storage and free GPUs : the commands of the cluster #####

    def quota(self, text, areas, sep):
        """Home and scratch from `disk-quota` (space and number of files against their quota), the archive from `df` ; the text of `disk-quota` is shown as it is."""
        size, number = (lambda text: server.Cluster.size(text.replace(",", ""))), (lambda text: server.Cluster.number(text.replace(",", "")))
        quota, _, archive = text.partition(sep)
        home = self.HOME_ROW.search(quota)
        if home and "HOME" in areas:
            areas["HOME"].update(used=size(home[1]), limit=size(home[2]), files=number(home[3]), files_limit=number(home[4]))
        scratch = self.SCRATCH_ROW.search(quota)
        if scratch and "SCRATCH" in areas:
            areas["SCRATCH"].update(used=size(scratch[1]), limit=size(scratch[2]), files=number(scratch[3]), files_limit=number(scratch[4]) or None)
        fields = archive.split()
        if len(fields) >= 4 and fields[1].isdigit() and "ARCHIVE" in areas:
            areas["ARCHIVE"].update(used=int(fields[2]), limit=int(fields[1]))
        return quota

    def free(self, text):
        """The free GPUs by type, from `savail`."""
        return [{"name": row[1], "free": int(row[2]), "total": int(row[3])} for row in self.AVAIL_ROW.finditer(text)]

    ##### GPU efficiency : the usage service (SARC) #####

    def rating(self, days, token):
        """The SM occupancy of the GPU jobs of the last `days` days, which Slurm does not record : the jobs with their
        occupancy, and the mean over the time they ran.

        One fixed address ; the `SARC_JOBS` jobs that used the most are asked, over whole days of UTC time (the
        answer says which) ; the mean is weighted by their time. A figure that is not a share between 0 and 1
        makes the whole answer « of an unknown form » : no figure is shown rather than a wrong one."""
        today = datetime.datetime.now(datetime.timezone.utc).date()
        start, end = today - datetime.timedelta(days=days), today + datetime.timedelta(days=1)
        data = self.asked(f"{self.SARC}/dash/metrics/jobs?{urlencode({'start': start, 'end': end, 'limit': self.SARC_JOBS})}", token)
        rows = data.get("jobs") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise ValueError("the usage service gave an answer of an unknown form")
        jobs, total, weight = [], 0.0, 0.0
        for row in rows[:self.SARC_JOBS]:
            if not isinstance(row, dict) or isinstance(row.get("job_id"), bool) or not isinstance(row.get("job_id"), (int, str)):
                continue
            occupancy, utilization = self.share(row.get("gpu_sm_occupancy_mean")), self.share(row.get("gpu_utilization_mean"))
            elapsed = row.get("elapsed")
            elapsed = elapsed if isinstance(elapsed, (int, float)) and not isinstance(elapsed, bool) and 0 < elapsed < 10**9 else 0
            jobs.append({"id": str(row["job_id"])[:40], "occupancy": occupancy, "utilization": utilization})
            if occupancy is not None:
                total, weight = total + elapsed * occupancy, weight + elapsed
        return {"days": days, "start": str(start), "end": str(end), "jobs": jobs, "measured": sum(job["occupancy"] is not None for job in jobs),
                "mean": total / weight if weight else None, "cut": len(rows) >= self.SARC_JOBS}
