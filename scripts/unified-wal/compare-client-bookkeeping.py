#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
"""Compare only CLIENT-1 bookkeeping against a frozen Git source baseline; never change checkout."""
import hashlib
import json
import math
import platform
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

root = Path(tempfile.mkdtemp(prefix="uw-client1-benchmark-"))
base = "30215501bb"
base_commit = subprocess.check_output(["git", "rev-parse", base], text=True).strip()
source_prefix = "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/"
source = root / "baseline-source"
source.mkdir()
for name in ("PendingAddOp.java", "LedgerHandle.java"):
    (source / name).write_bytes(subprocess.check_output(["git", "show", base_commit + ":" + source_prefix + name]))
shutil.copyfile("lombok.config", source / "lombok.config")
report = ET.parse("bookkeeper-server/target/surefire-reports/TEST-org.apache.bookkeeper.client.PendingAddOpTest.xml")
classpath = next(p.get("value") for p in report.getroot().find("properties") if p.get("name") == "java.class.path")
lombok = next(p for p in classpath.split(":") if "/lombok/" in p)
classes = root / "baseline-classes"
classes.mkdir()
compile_command = ["javac", "--release", "17", "-cp", classpath, "-processorpath", lombok, "-d", str(classes)]
compile_command += [str(p) for p in sorted(source.glob("*.java"))]
with (root / "compile.log").open("w") as log:
    subprocess.run(compile_command, check=True, stdout=log, stderr=subprocess.STDOUT)
jar = Path("microbenchmarks/target/benchmarks.jar").resolve()
paths = [Path(source_prefix + n) for n in ("PendingAddOp.java", "LedgerHandle.java")]
paths += [Path("microbenchmarks/src/main/java/org/apache/bookkeeper/client/PendingAddOpBenchmark.java"), Path(__file__)]
manifest = {
    "baselineCommit": base_commit,
    "currentParent": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "platform": platform.platform(),
    "java": subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT, text=True),
    "scope": "Normal PendingAddOp bookkeeping only; 3/3/2, reusable 1KiB payload; no digest/transport/storage",
    "sourceSha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
    "baselineSha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(source.glob("*.java"))},
    "jarSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
    "compileCommand": compile_command,
    "commands": [],
}
print("Benchmark directory:", root, flush=True)
for name, cp in (("baseline", str(classes) + ":" + str(jar)), ("current", str(jar))):
    command = ["java", "-cp", cp, "org.openjdk.jmh.Main", "PendingAddOpBenchmark.currentResponses",
               "-f", "2", "-wi", "3", "-i", "5", "-w", "1s", "-r", "1s", "-prof", "gc",
               "-rf", "json", "-rff", str(root / (name + ".json"))]
    manifest["commands"].append(command)
    (root / "inputs.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with (root / (name + ".log")).open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
    results = json.loads((root / (name + ".json")).read_text())
    assert len(results) == 2, "Both fault-domain policy modes must produce results"
    for result in results:
        assert math.isfinite(result["primaryMetric"]["score"])
        counters = result["secondaryMetrics"]
        cpu = counters["cpuNanos"]["score"] / counters["operations"]["score"]
        print(name, result["params"], "ns/op", result["primaryMetric"]["score"],
              "thread CPU ns/op", cpu, "B/op", counters["gc.alloc.rate.norm"]["score"], flush=True)
