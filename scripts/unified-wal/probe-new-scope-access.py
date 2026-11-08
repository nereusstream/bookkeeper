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
MANIFEST_SCHEMA = "UWNS2"
MAX_MANIFEST_ROOTS = 1 + 2 * 64
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


def manifest_roots(values):
    if not isinstance(values, list) or not 3 <= len(values) <= MAX_MANIFEST_ROOTS:
        raise ValueError("invalid manifest root count")
    members = {}
    paths = set()
    for row in values:
        if set(row) != {"kind", "arena", "path"}:
            raise ValueError("invalid manifest root fields")
        kind, arena, path = row["kind"], row["arena"], row["path"]
        if not isinstance(path, str) or len(path.encode("utf-8")) > 1024 or not os.path.isabs(path):
            raise ValueError("invalid manifest root path")
        if kind == "CONTROL":
            if arena != "":
                raise ValueError("control root cannot belong to an Arena")
        else:
            member = uuid.UUID(arena)
            if kind not in {"ARENA", "INDEX"} or str(member) != arena or member.int == 0:
                raise ValueError("invalid manifest Arena root role")
        if path in paths or (kind, arena) in members:
            raise ValueError("duplicate manifest root path or role")
        paths.add(path)
        members[kind, arena] = path
    arenas = {arena for kind, arena in members if kind == "ARENA"}
    if ("CONTROL", "") not in members or not arenas or arenas != {arena for kind, arena in members if kind == "INDEX"}:
        raise ValueError("manifest control/Arena/index coverage is incomplete")
    independent = [members["CONTROL", ""]] + [members["ARENA", arena] for arena in arenas]
    for index, left in enumerate(independent):
        for right in independent[index + 1:]:
            if os.path.commonpath([left, right]) in (left, right):
                raise ValueError("independent manifest roots overlap")
    for arena in arenas:
        if os.path.dirname(members["INDEX", arena]) != members["ARENA", arena]:
            raise ValueError("index root is outside its Arena")
    return sorted(values, key=lambda v: v["path"])


def observed_manifest_roots(values):
    result = []
    identities = set()
    for value in manifest_roots(values):
        path = value["path"]
        if str(Path(path).resolve(strict=True)) != path:
            raise ValueError("manifest root path must already be canonical")
        observed = root_identity(path)
        key = observed["device"], observed["inode"]
        if key in identities:
            raise ValueError("manifest root aliases another required root")
        identities.add(key)
        result.append(dict(value, identity=observed))
    return result


def manifest_scope(request):
    scope = request["scope"]
    if set(scope) != {"oldBookieId", "bookieId", "incarnation", "manifestIdentity"}:
        raise ValueError("invalid manifest access scope fields")
    for key in ("oldBookieId", "bookieId"):
        value = scope[key]
        if not isinstance(value, str) or not 1 <= len(value.encode("utf-8")) <= 255 or "\0" in value:
            raise ValueError("invalid manifest Bookie scope")
    if scope["oldBookieId"] == scope["bookieId"]:
        raise ValueError("manifest access requires a distinct Bookie")
    for key, length in (("incarnation", 32), ("manifestIdentity", 64)):
        value = scope[key]
        if not isinstance(value, str) or len(value) != length or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("invalid manifest scope identity")
    return scope


def require_nonce(nonce):
    if not isinstance(nonce, str) or len(nonce) != 32 or any(c not in "0123456789abcdef" for c in nonce):
        raise ValueError("invalid probe nonce")


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
    require_nonce(nonce)
    manifest = request.get("schema") == MANIFEST_SCHEMA
    if manifest:
        manifest_roots(request["roots"])
    else:
        if not isinstance(request["roots"], list) or len(request["roots"]) != 4:
            raise ValueError("invalid worker scope")
        if {root["kind"] for root in request["roots"]} != KINDS:
            raise ValueError("invalid worker root kinds")
    for root in request["roots"]:
        path = root["path"]
        if not isinstance(path, str) or len(path.encode("utf-8")) > 1024 or not os.path.isabs(path):
            raise ValueError("invalid worker path")
    files = request["files"]
    if not isinstance(files, list) or len(files) > MAX_INVENTORY + len(request["roots"]):
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
    fields = ("kind", "arena", "path") if manifest else ("kind", "path")
    for root in request["roots"]:
        path = root["path"]
        read = operation(lambda: opened(path, os.O_RDONLY | os.O_DIRECTORY))
        write_path = os.path.join(path, ".uw-scope-access-" + nonce + "-" + str(actor["uid"]))
        write = operation(lambda: created(write_path, nonce))
        observations.append(dict({key: root[key] for key in fields}, directoryRead=read, createWriteSync=write))
    return {"nonce": nonce, "actor": actor, "roots": observations, "files": file_observations}


def read_input(max_bytes):
    value = sys.stdin.buffer.read(max_bytes + 1)
    if len(value) > max_bytes:
        raise ValueError("oversized scope request")
    return json.loads(value.decode("utf-8"))


def probe(request, timeout):
    manifest = request.get("schema") == MANIFEST_SCHEMA
    scope = manifest_scope(request) if manifest else None
    fields = ("kind", "arena", "path") if manifest else ("kind", "path")
    observe_new = observed_manifest_roots if manifest else roots
    old_roots = roots(request["oldRoots"])
    new_roots = observe_new(request["newRoots"])
    for old in old_roots:
        for new in new_roots:
            shared = os.path.commonpath([old["path"], new["path"]])
            alias = all(old["identity"][key] == new["identity"][key] for key in ("device", "inode"))
            if shared in (old["path"], new["path"]) or (alias if manifest else old["identity"] == new["identity"]):
                raise ValueError("old and new roots overlap")
    old_user = request["oldUser"]
    old_uid = pwd.getpwnam(old_user).pw_uid
    current = identity()
    if old_uid == 0 or old_uid == current["uid"]:
        raise ValueError("old service must have a distinct non-root Unix identity")
    nonce = request["nonce"] if manifest else uuid.uuid4().hex
    require_nonce(nonce)
    before_files = inventory(new_roots)
    required_writes = request["requiredWrites"] if manifest else []
    if manifest and (not isinstance(required_writes, list) or required_writes != sorted(set(required_writes))
                     or not set(required_writes).issubset({row["path"] for row in before_files})):
        raise ValueError("required writable files are absent from the manifest inventory")
    markers = {}
    script = str(Path(__file__).resolve())
    command = ["/usr/bin/sudo", "-n", "-u", old_user, "--", sys.executable, "-I", "-B", script, "--worker"]
    started = time.monotonic_ns()
    try:
        # World-readable probes detect searchable-but-not-listable directories even when initially empty.
        for root in new_roots:
            path = os.path.join(root["path"], ".uw-scope-readable-" + nonce)
            markers[path] = readable_marker(path, nonce)
        work = {"nonce": nonce, "roots": [{k: v[k] for k in fields} for v in new_roots],
                "files": sorted([value["path"] for value in before_files] + list(markers))}
        if manifest:
            work["schema"] = MANIFEST_SCHEMA
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
        if [{k: v[k] for k in fields} for v in old.get("roots", [])] != work["roots"]:
            raise RuntimeError("old-credential child scope mismatch")
        if [value["path"] for value in old.get("files", [])] != work["files"]:
            raise RuntimeError("old-credential child file inventory mismatch")
    finally:
        for path, own in markers.items():
            current_marker = os.stat(path, follow_symlinks=False)
            if (current_marker.st_dev, current_marker.st_ino) != own:
                raise RuntimeError("private probe marker identity changed")
            os.unlink(path)
    after = observe_new(request["newRoots"])
    if after != new_roots or inventory(new_roots) != before_files or manifest and roots(request["oldRoots"]) != old_roots:
        raise RuntimeError("scope root/file identity/mode/owner changed during the probe")
    new_ok = all(v[operation]["status"] == "ALLOWED" for v in new["roots"]
                 for operation in ("directoryRead", "createWriteSync"))
    old_denied = all(v[operation]["status"] == "DENIED" for v in old["roots"]
                     for operation in ("directoryRead", "createWriteSync"))
    new_ok = new_ok and all(value["readOpen"]["status"] == "ALLOWED" for value in new["files"])
    if manifest:
        writable = {value["path"] for value in new["files"] if value["writeOpen"]["status"] == "ALLOWED"}
        new_ok = new_ok and set(required_writes).issubset(writable)
    old_denied = old_denied and all(value[operation]["status"] == "DENIED" for value in old["files"]
                                    for operation in ("readOpen", "writeOpen"))
    file_unknown = any(value[operation]["status"] == "ERROR" for actor in (old, new) for value in actor["files"]
                       for operation in ("readOpen", "writeOpen"))
    unknown = file_unknown or any(v[operation]["status"] == "ERROR" for actor in (old, new) for v in actor["roots"]
                  for operation in ("directoryRead", "createWriteSync"))
    status = "UNAVAILABLE" if unknown else "VERIFIED" if new_ok and old_denied else "REJECTED"
    receipt = {"schema": MANIFEST_SCHEMA if manifest else "UWNS1", "status": status, "nonce": nonce,
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
    if manifest:
        receipt["scope"] = scope
        receipt["requiredWrites"] = required_writes
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    schema = "UWNS1"
    try:
        request = read_input(MAX_WORK_INPUT if args.worker else MAX_INPUT)
        if request.get("schema") == MANIFEST_SCHEMA:
            schema = MANIFEST_SCHEMA
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
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, KeyError, TypeError, AttributeError):
        print(json.dumps({"schema": schema, "status": "UNAVAILABLE", "authority": "ACCESS_SNAPSHOT_ONLY"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
