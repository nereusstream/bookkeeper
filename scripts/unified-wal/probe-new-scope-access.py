#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Probe an explicitly supplied, isolated new-scope access matrix using two actual Unix identities.

This administrative experiment never grants Bookie authority or verifies an old service's drained state.
It only creates/removes its own nonce files; it never chmods, renames, or changes existing storage files.
"""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import pwd
import stat
import subprocess
import sys
import time
import uuid

KINDS = {"JOURNAL", "LEDGER", "INDEX", "ARENA"}
MAX_INPUT = 16384
MAX_WORK_INPUT = 262144
MAX_OUTPUT = 262144
MAX_INVENTORY = 128


def identity():
    return {"uid": os.geteuid(), "gid": os.getegid(), "groups": sorted(os.getgroups())}


def root_identity(path):
    info = os.stat(path, follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("scope root is not a directory")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid,
            "gid": info.st_gid, "mode": stat.S_IMODE(info.st_mode)}


def roots(values):
    if not isinstance(values, list) or len(values) != 4 or {v["kind"] for v in values} != KINDS:
        raise ValueError("exactly four declared root kinds are required")
    result = []
    for value in values:
        path = value["path"]
        if not isinstance(path, str) or len(path.encode("utf-8")) > 1024 or not os.path.isabs(path):
            raise ValueError("invalid scope path")
        canonical = str(Path(path).resolve(strict=True))
        if canonical != path:
            raise ValueError("scope path must already be canonical")
        result.append({"kind": value["kind"], "path": path, "identity": root_identity(path)})
    if len({v["path"] for v in result}) != 4:
        raise ValueError("duplicate scope path")
    return sorted(result, key=lambda v: v["path"])


def inventory(scope_roots):
    entries = {}
    pending = [value["path"] for value in scope_roots]
    directories = set()
    while pending:
        directory = pending.pop()
        if directory in directories:
            continue
        directories.add(directory)
        if len(directories) > MAX_INVENTORY:
            raise ValueError("too many scope directories")
        with os.scandir(directory) as children:
            for child in children:
                info = child.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise ValueError("scope contains a symbolic alias")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(child.path)
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    entries[child.path] = {"path": child.path, "device": info.st_dev, "inode": info.st_ino,
                                           "uid": info.st_uid, "gid": info.st_gid,
                                           "mode": stat.S_IMODE(info.st_mode), "size": info.st_size}
                    if len(entries) > MAX_INVENTORY:
                        raise ValueError("too many existing scope files")
                else:
                    raise ValueError("scope contains an unsupported file or hard link")
    return [entries[path] for path in sorted(entries)]


def readable_marker(path, nonce):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    own = os.fstat(fd)
    complete = False
    try:
        os.fchmod(fd, 0o666)
        content = nonce.encode("ascii")
        done = 0
        while done < len(content):
            count = os.write(fd, content[done:])
            if count <= 0:
                raise OSError(errno.EIO, "short marker write")
            done += count
        os.fsync(fd)
        complete = True
        return (own.st_dev, own.st_ino)
    finally:
        os.close(fd)
        if not complete:
            current = os.stat(path, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (own.st_dev, own.st_ino):
                raise RuntimeError("partial probe marker identity changed")
            os.unlink(path)


def operation(action):
    try:
        action()
        return {"status": "ALLOWED"}
    except OSError as failure:
        return {"status": "DENIED" if failure.errno in (errno.EACCES, errno.EPERM) else "ERROR",
                "errno": failure.errno}


def opened(path, flags):
    fd = os.open(path, flags | os.O_NOFOLLOW)
    os.close(fd)


def created(path, nonce):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    own = os.fstat(fd)
    try:
        content = nonce.encode("ascii")
        done = 0
        while done < len(content):
            count = os.write(fd, content[done:])
            if count <= 0:
                raise OSError(errno.EIO, "short probe write")
            done += count
        os.fsync(fd)
    finally:
        os.close(fd)
        current = os.stat(path, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (own.st_dev, own.st_ino):
            raise OSError(errno.EIO, "probe file identity changed")
        os.unlink(path)


def worker(request):
    actor = identity()
    nonce = request["nonce"]
    if not isinstance(nonce, str) or len(nonce) != 32 or any(c not in "0123456789abcdef" for c in nonce):
        raise ValueError("invalid probe nonce")
    if not isinstance(request["roots"], list) or len(request["roots"]) != 4:
        raise ValueError("invalid worker scope")
    if {root["kind"] for root in request["roots"]} != KINDS:
        raise ValueError("invalid worker root kinds")
    for root in request["roots"]:
        path = root["path"]
        if not isinstance(path, str) or len(path.encode("utf-8")) > 1024 or not os.path.isabs(path):
            raise ValueError("invalid worker path")
    files = request["files"]
    if not isinstance(files, list) or len(files) > MAX_INVENTORY + 4:
        raise ValueError("invalid worker file inventory")
    for path in files:
        if not isinstance(path, str) or len(path.encode("utf-8")) > 1024 or not os.path.isabs(path):
            raise ValueError("invalid worker file path")
        if not any(os.path.commonpath([root["path"], path]) == root["path"] for root in request["roots"]):
            raise ValueError("worker file escaped its scope")
    file_observations = [{"path": path,
                          "readOpen": operation(lambda: opened(path, os.O_RDONLY)),
                          "writeOpen": operation(lambda: opened(path, os.O_RDWR))} for path in files]
    observations = []
    for root in request["roots"]:
        path = root["path"]
        read = operation(lambda: opened(path, os.O_RDONLY | os.O_DIRECTORY))
        write_path = os.path.join(path, ".uw-scope-access-" + nonce + "-" + str(actor["uid"]))
        write = operation(lambda: created(write_path, nonce))
        observations.append({"kind": root["kind"], "path": path, "directoryRead": read, "createWriteSync": write})
    return {"nonce": nonce, "actor": actor, "roots": observations, "files": file_observations}


def read_input(max_bytes):
    value = sys.stdin.buffer.read(max_bytes + 1)
    if len(value) > max_bytes:
        raise ValueError("oversized scope request")
    return json.loads(value.decode("utf-8"))


def probe(request, timeout):
    old_roots = roots(request["oldRoots"])
    new_roots = roots(request["newRoots"])
    for old in old_roots:
        for new in new_roots:
            shared = os.path.commonpath([old["path"], new["path"]])
            if shared in (old["path"], new["path"]) or old["identity"] == new["identity"]:
                raise ValueError("old and new roots overlap")
    old_user = request["oldUser"]
    old_uid = pwd.getpwnam(old_user).pw_uid
    current = identity()
    if old_uid == 0 or old_uid == current["uid"]:
        raise ValueError("old service must have a distinct non-root Unix identity")
    nonce = uuid.uuid4().hex
    before_files = inventory(new_roots)
    markers = {}
    script = str(Path(__file__).resolve())
    command = ["/usr/bin/sudo", "-n", "-u", old_user, "--", sys.executable, "-I", "-B", script, "--worker"]
    started = time.monotonic_ns()
    try:
        # World-readable probes detect searchable-but-not-listable directories even when initially empty.
        for root in new_roots:
            path = os.path.join(root["path"], ".uw-scope-readable-" + nonce)
            markers[path] = readable_marker(path, nonce)
        work = {"nonce": nonce, "roots": [{"kind": v["kind"], "path": v["path"]} for v in new_roots],
                "files": sorted([value["path"] for value in before_files] + list(markers))}
        encoded = json.dumps(work).encode()
        if len(encoded) > MAX_WORK_INPUT:
            raise ValueError("oversized worker inventory")
        new = worker(work)
        child = subprocess.run(command, input=encoded, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=timeout, cwd="/", close_fds=True, env={"PATH": "/usr/bin:/bin"})
        if child.returncode != 0 or len(child.stdout) > MAX_OUTPUT:
            raise RuntimeError("actual old-credential child did not complete successfully")
        old = json.loads(child.stdout.decode("utf-8"))
        if old.get("nonce") != nonce or old.get("actor", {}).get("uid") != old_uid:
            raise RuntimeError("old-credential child identity or nonce mismatch")
        if [{k: v[k] for k in ("kind", "path")} for v in old.get("roots", [])] != work["roots"]:
            raise RuntimeError("old-credential child scope mismatch")
        if [value["path"] for value in old.get("files", [])] != work["files"]:
            raise RuntimeError("old-credential child file inventory mismatch")
    finally:
        for path, own in markers.items():
            current_marker = os.stat(path, follow_symlinks=False)
            if (current_marker.st_dev, current_marker.st_ino) != own:
                raise RuntimeError("private probe marker identity changed")
            os.unlink(path)
    after = roots(request["newRoots"])
    if after != new_roots or inventory(new_roots) != before_files:
        raise RuntimeError("scope root/file identity/mode/owner changed during the probe")
    new_ok = all(v[operation]["status"] == "ALLOWED" for v in new["roots"]
                 for operation in ("directoryRead", "createWriteSync"))
    old_denied = all(v[operation]["status"] == "DENIED" for v in old["roots"]
                     for operation in ("directoryRead", "createWriteSync"))
    new_ok = new_ok and all(value["readOpen"]["status"] == "ALLOWED" for value in new["files"])
    old_denied = old_denied and all(value[operation]["status"] == "DENIED" for value in old["files"]
                                    for operation in ("readOpen", "writeOpen"))
    file_unknown = any(value[operation]["status"] == "ERROR" for actor in (old, new) for value in actor["files"]
                       for operation in ("readOpen", "writeOpen"))
    unknown = file_unknown or any(v[operation]["status"] == "ERROR" for actor in (old, new) for v in actor["roots"]
                  for operation in ("directoryRead", "createWriteSync"))
    status = "UNAVAILABLE" if unknown else "VERIFIED" if new_ok and old_denied else "REJECTED"
    return {"schema": "UWNS1", "status": status, "nonce": nonce,
            "scopeSha256": hashlib.sha256(json.dumps(request, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "scriptSha256": hashlib.sha256(Path(script).read_bytes()).hexdigest(),
            "interpreter": sys.executable, "oldCommand": command, "oldChildExitCode": child.returncode,
            "oldChildElapsedNanos": time.monotonic_ns() - started,
            "newActor": new, "oldActor": old, "oldRoots": old_roots, "newRoots": new_roots,
            "existingFiles": before_files, "maxExistingFiles": MAX_INVENTORY,
            "oldCredentialAccessToNewScope": "NONE" if old_denied else "UNPROVEN",
            "newCredentialAccessToNewScope": "READ_WRITE" if new_ok else "UNPROVEN",
            "authority": "ACCESS_SNAPSHOT_ONLY",
            "oldServiceDisposition": "NOT_VERIFIED", "stockBinaryMatrix": "NOT_EXECUTED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    try:
        request = read_input(MAX_WORK_INPUT if args.worker else MAX_INPUT)
        if args.worker:
            output = worker(request)
        else:
            if args.timeout <= 0 or args.timeout > 60:
                raise ValueError("probe timeout must be between zero and sixty seconds")
            output = probe(request, args.timeout)
        encoded = json.dumps(output, sort_keys=True)
        if len(encoded.encode()) > MAX_OUTPUT:
            raise RuntimeError("oversized probe response")
        print(encoded)
        return 0 if args.worker or output["status"] == "VERIFIED" else 1
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, KeyError, TypeError):
        print(json.dumps({"schema": "UWNS1", "status": "UNAVAILABLE", "authority": "ACCESS_SNAPSHOT_ONLY"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
