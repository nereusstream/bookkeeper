#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
"""Run the declared Model C slice and sensitivity cases with immutable inputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import tempfile
import time


MATRIX = {
    'a-profile-metadata-create': None,
    'ce-profile-metadata-create-instance': 'SuccessMatchesInstance',
    'ce-profile-metadata-create-sidecar': 'SuccessMatchesSidecar',
    'ce-profile-metadata-create-payload': 'SuccessMatchesInitialPayload',
    'ce-profile-metadata-create-absence': 'AbsenceDoesNotProveNoEffect',
    'ce-profile-metadata-create-ack': 'SuccessHasActualMetadataSnapshot',
    'a-profile-backing-budget': None,
    'ce-profile-backing-budget-allocation': 'BackingsKeepFullCredit',
    'ce-profile-backing-budget-resize': 'ResizeKeepsOldAndNewCredit',
    'ce-profile-backing-budget-view': 'BackingsKeepFullCredit',
    'ce-profile-backing-budget-close': 'BackingsKeepFullCredit',
    'ce-profile-backing-budget-response': 'ResponseSplitConservesReservation',
    'ce-profile-backing-budget-lane': 'ReadBackingKeepsAllocationLane',
    'a-profile-recovery-dispatch': None,
    'ce-profile-recovery-dispatch-auth': 'SubmittedRequiresExactOpenGrant',
    'ce-profile-recovery-dispatch-source': 'CopyRequiresSourceCredit',
    'ce-profile-recovery-dispatch-close': 'SuccessRequiresCurrentGrantAtDecision',
    'ce-profile-recovery-dispatch-activate': 'RecoveryDoesNotActivateNormal',
    'ce-profile-recovery-dispatch-unknown': 'NoneIsNotSubmittedData',
    'ce-profile-recovery-dispatch-io': 'IoProtectionWaitsForActualTerminals',
    'a-profile-fence-dispatch': None,
    'ce-profile-fence-dispatch-auth': 'ControlRequiresExactAuthorization',
    'ce-profile-fence-dispatch-workspace': 'ControlWorkspaceMovesAtomically',
    'ce-profile-fence-dispatch-early': 'SuccessRequiresFenceForceAndOwner',
    'ce-profile-fence-dispatch-terminal': 'ControlCopiesWaitForActualTerminal',
    'ce-profile-fence-dispatch-unknown': 'UnknownIsNotPreIoNone',
    'ce-profile-fence-dispatch-unclassified': 'ExistingControlSurvivesUnclassifiedFull',
    "a-profile-normal-dispatch": None,
    "ce-profile-normal-dispatch-header": "HeaderBoundPrecedesBody",
    "ce-profile-normal-dispatch-early": "SuccessRequiresActualDurability",
    "ce-profile-normal-dispatch-unknown": "NoneIsNotUnknownData",
    "ce-profile-normal-dispatch-io": "IoProtectionWaitsForActualTerminals",
    "ce-profile-normal-dispatch-response": "ResponseProtectionWaitsForWriteTerminal",
    "a-startup-data-exclusion": None,
    "ce-startup-data-exclusion-order": "RecoveryStartsWithOwnExclusion",
    "ce-startup-data-exclusion-handoff": "FileHandoffKeepsExclusion",
    "ce-startup-data-exclusion-cleanup": "ClosedAttemptHasNoOwnedScope",
    "ce-startup-data-exclusion-foreign": "ForeignWriterKeepsItsScope",
    "a-registration-admission": None,
    "ce-registration-admission-session": "AdmissionUsesOriginalConnectedSession",
    "ce-registration-admission-reconnect": "RetiredAttemptNeverReopens",
    "ce-registration-admission-late": "RetiredAttemptNeverReopens",
    "ce-registration-admission-release": "OutstandingOwnershipWaitsForActualTerminals",
    "a-authority-read": None,
    "ce-authority-read-key": "FoundMatchesRequestedKey",
    "ce-authority-read-invalid": "FoundHasValidPersistentCompleteRecord",
    "ce-authority-read-error": "AbsentOnlyFromActualNoNode",
    "ce-authority-read-version": "FoundKeepsOneAtomicSnapshot",
    "a-local-readiness-publication": None,
    "ce-local-readiness-publication-file": "AppliedHasFileAndDirectoryBarriers",
    "ce-local-readiness-publication-directory": "AppliedHasFileAndDirectoryBarriers",
    "ce-local-readiness-publication-unknown": "UncertainInstanceNeverReturnsReceipt",
    "ce-local-readiness-publication-pending": "UnpublishedTempNeverBecomesReceipt",
    "ce-local-readiness-publication-generation": "OneGenerationBindsOneReceipt",
    "a-registration-publication": None,
    "ce-registration-publication-authority": "PublishedOnlyAgainstMatchingAuthority",
    "ce-registration-publication-session": "OldDemotionCannotRemoveReplacement",
    "ce-registration-publication-service": "WritableProjectionMatchesService",
    "ce-registration-publication-unknown": "UnresolvedPublicationNeverReportsApplied",
    "a-readiness-cas": None,
    "ce-readiness-cas-birth": "ReadinessCasPreservesBirthAndExpectedGeneration",
    "ce-readiness-cas-generation": "ReadinessCasPreservesBirthAndExpectedGeneration",
    "ce-readiness-cas-unknown": "UnresolvedLostReplyNeverReportsApplied",
    "c-current-copy-facts": None,
    "ce-current-copy-facts-superseded": "CurrentCopyProofIsExact",
    "ce-current-copy-facts-rejected": "CurrentCopyProofIsExact",
    "ce-current-copy-facts-retired": "CurrentCopyProofIsExact",
    "ce-current-copy-facts-identity": "QueriesBindCurrentIdentityAndDigest",
    "c-arena-state-indices": None,
    "ce-arena-state-indices-free": "IssuedRangesAreExact",
    "ce-arena-state-indices-hole": "FeasibleUnissuedIdentityIsAccepted",
    "ce-arena-state-indices-high": "HighWatermarkTracksOnlySuccessfulIdentity",
    "ce-arena-state-indices-order": "CurrentGrantKeepsOriginalOrder",
    "ce-retired-sequence-ranges-empty-barrier": "EmptyBarrierRemovalIsFolded",
    "c-retired-sequence-ranges": None,
    "ce-retired-sequence-ranges-gap": "RetiredCoverageIsExact",
    "ce-retired-sequence-ranges-live": "CurrentAllocationOrderIsNotBridged",
    "ce-retired-sequence-ranges-cut": "RequiredBookieCutIsRetained",
    "ce-retired-sequence-ranges-empty": "EmptyRetirementKeepsRequiredBookieCut",
    "c-internal-checkpoint-history": None,
    "ce-internal-checkpoint-history-unbounded": "ImageHasBoundedInternalHistory",
    "ce-internal-checkpoint-history-root": "SnapshotRetainsCurrentRootIdentities",
    "ce-internal-checkpoint-history-legacy": "LegacyReadyHasCompleteCheckpointHistory",
    "ce-internal-checkpoint-history-business": "SnapshotKeepsBusinessResults",
    "ce-internal-checkpoint-history-original": "OriginalRestoresValidCompactedImage",
    "c-arena-space-maintenance": None,
    "ce-arena-space-maintenance-resources": "MaintenanceStartsWithActualResources",
    "ce-arena-space-maintenance-repeat": "UnchangedFullLiveWindowsVisitedOnce",
    "ce-arena-space-maintenance-refusal": "KnownRefusalKeepsFreshProtectedWork",
    "ce-arena-space-maintenance-unknown": "UnknownStopsDataAdmission",
    "c-cold-control-startup": None,
    "ce-cold-control-startup-guard": "RotationHasValidatedProtectedClosedData",
    "ce-cold-control-startup-open": "OpenHasRecoveredCapacity",
    "ce-cold-control-startup-failure": "ReadyHasDurableOpenAndNoFailure",
    "c-control-capacity-recovery": None,
    "ce-control-capacity-oversize": "OversizeNeverStartsRotation",
    "ce-control-capacity-pause": "PressureRefusesNewEntries",
    "ce-control-capacity-demand": "BlockedDemandRemains",
    "c-runtime-control-rotation": None,
    "ce-runtime-control-rotation-cancel": "RotationKeepsReservations",
    "ce-runtime-control-rotation-owner": "GuardWaitsForOriginalOwner",
    "ce-runtime-control-rotation-session": "OpenSessionAndCursorRemain",
    "ce-runtime-control-rotation-failure": "UnknownStopsGraph",
    "c-maintenance-reserve": None,
    "ce-maintenance-reserve-foreground": "PlannedForegroundKeepsReserve",
    "ce-maintenance-reserve-merged": "PlannedForegroundKeepsReserve",
    "ce-maintenance-reserve-workset": "DestinationFitsDeclaredWorkset",
    "ce-maintenance-reserve-restart": "WholeBlocksAccountOnce",
    "c-compaction-tail": None,
    "ce-compaction-tail-boundary": "TailWritesUseUnusedRange",
    "ce-compaction-tail-retained": "RetainedCurrentCopyNeverFreed",
    "c-compaction-capture": None,
    "ce-compaction-capture-cancel": "CaptureAndCopyKeepOwnedBuffers",
    "ce-compaction-capture-measurement": "CopyMatchesMeasurement",
    "c-compaction-sweep": None,
    "ce-compaction-sweep-reset": "ThreeAdmittedCallsCoverStableRound",
    "ce-compaction-sweep-expanding": "FourthAdmittedCallRevisitsOldestGrant",
    "c-compaction-selection": None,
    "ce-compaction-selection-input": "DeadCandidatesAvoidInputRefusal",
    "ce-compaction-selection-index": "SelectedSourcesUseActualData",
    "c-compaction-group": None,
    "ce-compaction-allocation-cost": "SelectionHasAllocationNetGain",
    "ce-compaction-early-free": "FreedSourcesHaveDurableSuccessor",
    "ce-compaction-shared-cut": "AppliedFreeUsesOwnCut",
    "ce-compaction-cancel": "PendingMembersKeepProtection",
    "ce-compaction-incomplete-group": "FreedSourcesHaveDurableSuccessor",
    "c-move-index": None,
    "ce-move-index-old-put": "CapturedOwnersHadNoOldPut",
    "ce-move-index-cancel": "MaintenanceKeepsProtection",
    "ce-move-index-stale-row": "RecoveredLocatorUsesSelector",
    "ce-move-index-missing-bytes": "CoveredReadyValidatesCurrentBytes",
    "c-movefree-reuse": None,
    "ce-movefree-current": "SelectedAllocationRemainsLive",
    "ce-movefree-late-target": "SelectedAllocationRemainsLive",
    "ce-movefree-stale-cut": "AppliedFreeHadExactSelectorCut",
    "ce-movefree-old-bytes": "RestartKeepsControlSelector",
    "c-conditional-move": None,
    "ce-move-before-force": "SelectorHasBarrier",
    "ce-move-ignore-predecessor": "SelectorMatchesReplay",
    "ce-move-cancel": "PendingMoveKeepsProtection",
    "c-relocation-completion": None,
    "ce-relocation-early-facts": "CopyFactsHaveBarrierAndPrefix",
    "ce-relocation-ordinary": "CopyDoesNotPublishOrdinary",
    "ce-relocation-cancel": "BuffersWaitForBothTerminals",
    "c-terminal-index": None,
    "ce-terminal-index-late-put": "ReleasedCoordinatesHaveNoNativeRows",
    "ce-terminal-index-before-delete": "ReleasedCoordinatesHaveNoNativeRows",
    "ce-terminal-index-new-tombstone": "OnlyCapturedTerminalCoordinatesRetire",
    "ce-terminal-index-cancel": "MaintenanceGateHeldThroughActualDelete",
    "c-whole-reuse": None,
    "ce-whole-reuse-before-free": "ReuseHasTerminalOldIoAndFree",
    "ce-whole-reuse-before-io": "ClosedTailHasActualDurableIo",
    "ce-whole-reuse-old-bytes": "OldBytesNeverBecomeCurrent",
    "c-whole-free": None,
    "ce-whole-free-partial-death": "EveryOccupantWasTerminal",
    "ce-whole-free-early-release": "SubmittedFreeRetainsProtection",
    "ce-whole-free-reboot-pins": "RebootCountsDoNotProveOldIoTerminal",
    "ce-whole-free-before-force": "FreeAuthorityHasBarrier",
    "c-allocation-order": None,
    "ce-allocation-file-order": "NewerAllocationIsNotBlockedByFileOffset",
    "ce-allocation-old-tail": "OldAllocationsCannotBeReentered",
    "c-tail-recovery": None,
    "ce-tail-recovery-invalid-index": "SkippedBodyHasDurableMatchingIndex",
    "ce-tail-recovery-before-sync": "ReadyHasRequiredDataSync",
    "ce-tail-recovery-ignore-gap": "ReadyHasContiguousPhysicalPrefix",
    "c-runtime-coverage": None,
    "ce-runtime-before-flush": "RuntimeCoverageWasFlushed",
    "ce-runtime-clock-reset": "PublicationClockAdvances",
    "ce-runtime-late-cut": "RuntimeCoverageStopsAtCapturedData",
    "c-index-maintenance": None,
    "ce-index-control-credit": "FenceCanUseReservedLane",
    "ce-index-early-eviction": "LocalSuccessKeepsLocatorAuthority",
    "c-derived-coverage": None,
    "ce-derived-before-flush": "CoverageWasPersisted",
    "c-managed-shared": None,
    "c-managed-split": None,
    "ce-data-before-barrier": "DataSuccessHasBarrier",
    "ce-control-before-sync": "ControlHasBarrier",
    "ce-open-replay-after-error": "KnownErrorRemainsIsolated",
    "c-checkpoint": None,
    "c-arena-rotation": None,
    "ce-arena-rotation-early-reclaim": "SurvivingAuthority",
    "a-bookie-checkpoint": None,
    "a-bookie-rotation": None,
    "ce-bookie-rotation-terminal": "RetiredBookieTerminalPermissionsRemainDenied",
    "ce-bookie-checkpoint-terminal": "BookieTerminalPermissionsRemainDenied",
    "ce-checkpoint-durable-cut": "SnapshotCutWasApplied",
    "ce-checkpoint-early-publication": "PublishedImageHasDurableCommit",
}
TOOL_SHA1 = "bee4a54f3ee3d4afc347c3240ec2d9e93b075104"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tool", required=True, type=Path, help="Official tla2tools.jar from release 1.7.4")
    args = parser.parse_args()
    tool = args.tool.resolve()
    if hashlib.sha1(tool.read_bytes()).hexdigest() != TOOL_SHA1:
        parser.error("TLA+ tools must match the official 1.7.4 release SHA-1")
    root = Path(__file__).resolve().parents[2]
    model = root / "docs/rfcs/unified-wal/implementation/models"
    run = Path(tempfile.mkdtemp(prefix="unified-wal-model-"))
    print(f"Run directory: {run}", flush=True)
    shutil.copytree(model / "modules", run / "modules")
    shutil.copytree(model / "configs", run / "configs")
    (run / "checker-output").mkdir()
    (run / "counterexamples").mkdir()
    inputs = [*model.glob("modules/*.tla"), *model.glob("configs/*.cfg"), Path(__file__).resolve()]
    sources = root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment"
    inputs.append(sources / "SegmentProfileAddHandler.java")
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/proto/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/wire").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/wire").glob("*.java"))
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/tls/TLSContextFactory.java")
    inputs.extend(sources / name for name in ["IsolatedSegmentStorage.java", "IsolatedSegmentFiles.java",
                  "SegmentArenaControlLog.java", "SegmentBookieControlLog.java", "SegmentControlCapacityException.java",
                  "SegmentDataPhysicalDriver.java", "SegmentPhysicalPrefix.java", "SegmentHotDirectory.java", "SegmentControlCheckpoint.java", "SegmentControlFormat.java",
                  "SegmentControlRotation.java", "SegmentDataFileLock.java", "SegmentDerivedIndex.java",
                  "SegmentDataRecovery.java", "SegmentOwnerLoop.java", "SegmentIoExecutor.java", "SegmentPointRead.java",
                  "SegmentArenaControlDriver.java", "SegmentBookieControlDriver.java",
                  "SegmentArenaMove.java", "SegmentDuplicateCheck.java",
                  "SegmentEntryInputs.java", "SegmentBufferPool.java", "SegmentBatchFormat.java",
                  "SegmentLocalReadinessStore.java", "SegmentLocalReadinessCodec.java", "SegmentFileIo.java"])
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentLocalReadinessStoreTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentBookieControlLogTest.java")
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/zookeeper/ZooKeeperWatcherBase.java")
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/startup").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/startup").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/control").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/control").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/discover/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/meta").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/versioning").glob("*.java"))
    inputs.extend(root / name for name in [
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataBuilder.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataImpl.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataUtils.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/BKException.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/LedgerMetadata.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/DigestType.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/conf/AbstractConfiguration.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/conf/ClientConfiguration.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/meta/zk/ZKMetadataDriverBase.java",
        "bookkeeper-server/src/test/java/org/apache/bookkeeper/meta/TestLedgerMetadataSerDe.java"])
    inputs.extend(root / name for name in ["pom.xml", "bookkeeper-common/pom.xml", "bookkeeper-server/pom.xml",
                  "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/EngineProfile.java",
                  "bookkeeper-server/src/main/java/org/apache/bookkeeper/zookeeper/ZooKeeperClient.java",
                  "bookkeeper-server/src/test/java/org/apache/bookkeeper/test/ZooKeeperUtil.java"])
    design_paths = ["docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md",
                    "docs/rfcs/unified-wal/implementation/startup-readiness/README.md",
                    "docs/rfcs/unified-wal/RFC-0003-segment-storage-allocator.md",
                    "docs/rfcs/unified-wal/RFC-0005-segment-bookie-state.md",
                    "docs/rfcs/unified-wal/spikes/SPIKE-C-no-object-tla.md"]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    manifest = {
        "sourceCommit": head,
        "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=root, text=True).strip(),
        "sourceSha256": {str(p.relative_to(root)): sha256(p) for p in inputs},
        "designInputsAtSourceCommit": {p: hashlib.sha256(subprocess.check_output(
            ["git", "show", f"{head}:{p}"], cwd=root)).hexdigest() for p in design_paths},
        "toolRelease": "1.7.4", "toolSha1": TOOL_SHA1, "toolSha256": sha256(tool),
        "java": subprocess.run(["java", "-version"], capture_output=True, text=True, check=True).stderr,
        "platform": platform.platform(), "workers": 1, "heapMiB": 512, "directMiB": 64,
        "fingerprintPolynomial": 0, "seed": 1, "timeoutSecondsPerConfig": 120,
        "scope": "Finite Model A standard-metadata-create and Bookie applied metadata checkpoint and Model C session/prefix/barriers/publication/derived-coverage/index-maintenance/runtime-coverage/tail-recovery/allocation-order/whole-allocation-FREE/healthy-CLOSED-reuse/terminal-index-cleanup/relocation-completion/conditional-MOVE/MOVE-FREE-reuse/native-MOVE-coverage/compaction-group/compaction-selection/compaction-sweep/pooled-compaction-capture/retained-compaction-tail/maintenance-reserve/runtime-control-rotation/control-capacity-recovery/cold-control-startup/arena-space-maintenance/internal-checkpoint-history/retired-sequence-ranges/arena-state-indices/current-copy-facts/readiness-cas/registration-publication/local-readiness-publication/authority-read/registration-admission/startup-data-exclusion/profile-normal-dispatch/profile-fence-dispatch/profile-recovery-dispatch/profile-backing-budget slices",
        "stateConstraint": "None; finite constants in copied configs; no symmetry reduction",
        "matrix": MATRIX, "productionAuthority": False,
        "deferred": ["full Model A", "Model D", "full A+C", "C+D", "full MOVE/FREE native coverage and reused DATA recovery", "fresh Get/read pin model and full MOVE/FREE coverage recovery", "runtime worker freeze/credits",
                     "full online control rotation", "partial/corrupt framing", "quorum", "liveness"],
    }
    (run / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    results = {}
    for name, expected in MATRIX.items():
        states = run / (name + "-states")
        command = ["java", "-Xmx512m", "-XX:MaxDirectMemorySize=64m", "-XX:+UseParallelGC",
                   "-cp", str(tool), "tlc2.TLC", "-workers", "1", "-fp", "0", "-seed", "1",
                   "-coverage", "1", "-config", str(run / "configs" / (name + ".cfg")),
                   "-metadir", str(states), "ProfileMetadataCreate.tla" if "profile-metadata-create" in name
                   else "ProfileBackingBudget.tla" if "profile-backing-budget" in name
                   else "ProfileRecoveryDispatch.tla" if "profile-recovery-dispatch" in name
                   else "ProfileFenceDispatch.tla" if "profile-fence-dispatch" in name
                   else "ProfileNormalDispatch.tla" if "profile-normal-dispatch" in name
                   else "StartupDataExclusion.tla" if "startup-data-exclusion" in name
                   else "RegistrationAdmission.tla" if "registration-admission" in name
                   else "AuthorityRead.tla" if "authority-read" in name
                   else "LocalReadinessPublication.tla" if "local-readiness-publication" in name
                   else "RegistrationPublication.tla" if "registration-publication" in name
                   else "ReadinessCas.tla" if "readiness-cas" in name
                   else "CurrentCopyFacts.tla" if "current-copy-facts" in name
                   else "ArenaStateIndices.tla" if "arena-state-indices" in name
                   else "RetiredSequenceRanges.tla" if "retired-sequence-ranges" in name
                   else "InternalCheckpointHistory.tla" if "internal-checkpoint-history" in name
                   else "ArenaSpaceMaintenance.tla" if "arena-space-maintenance" in name
                   else "ColdControlStartup.tla" if "cold-control-startup" in name
                   else "ControlCapacityRecovery.tla" if "control-capacity" in name
                   else "RuntimeControlRotation.tla" if "runtime-control-rotation" in name
                   else "MaintenanceReserve.tla" if "maintenance-reserve" in name
                   else "CompactionSweep.tla" if "compaction-sweep" in name
                   else "CompactionSelection.tla" if "compaction-selection" in name
                   else "RetainedCompactionTail.tla" if "compaction-tail" in name
                   else "PooledCompactionCapture.tla" if "compaction-capture" in name
                   else "CompactionGroup.tla" if "compaction" in name else (
                       "NativeMoveCoverage.tla" if "move-index" in name else (
                       "MoveFreeReuse.tla" if "movefree" in name else (
                       "ConditionalMove.tla" if "move" in name else (
                       "RelocationCompletion.tla" if "relocation" in name else (
                       "TerminalIndexCleanup.tla" if "terminal-index" in name else (
                       "WholeAllocationReuse.tla" if "whole-reuse" in name else (
                       "WholeAllocationFree.tla" if "whole-free" in name else (
                       "AllocationOrder.tla" if "allocation" in name else (
                       "TailRecovery.tla" if "tail-recovery" in name else (
                       "RuntimeCoverage.tla" if "runtime-" in name else (
                       "IndexMaintenance.tla" if "index-" in name else (
                       "DerivedCoverage.tla" if "derived" in name else (
                       "ArenaRotation.tla" if "arena-rotation" in name else (
                       "BookieCheckpoint.tla" if "bookie-checkpoint" in name or "bookie-rotation" in name else (
                       "ArenaCheckpoint.tla" if "checkpoint" in name else "SegmentAllocator.tla")))))))))))))))]
        wrapper = ["/usr/bin/time", "-l" if platform.system() == "Darwin" else "-v"]
        output = run / "checker-output" / (name + ".log")
        start = time.monotonic()
        with output.open("w") as log:
            process = subprocess.Popen(wrapper + command, cwd=run / "modules", stdout=log,
                                       stderr=subprocess.STDOUT, env={**os.environ, "LC_ALL": "C"},
                                       start_new_session=True)
            try:
                code = process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                # Stop this exact checker before declaring an incomplete result or releasing its state files.
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                code = None
        text = output.read_text()
        complete = code == 0 and "Model checking completed. No error has been found." in text
        violation = re.search(r"Invariant (\w+) is violated\.", text)
        has_trace = violation is not None and "State 1:" in text
        counterexample = expected is not None and code not in (None, 0) and (
            f"Invariant {expected} is violated." in text and "State 1:" in text)
        status = "PASS" if complete and expected is None else (
            "EXPECTED_COUNTEREXAMPLE" if counterexample else (
                "FAIL" if has_trace or complete else "INCONCLUSIVE"))
        if has_trace:
            shutil.copy2(output, run / "counterexamples" / (name + ".txt"))
        stats = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found, "
                           r"([\d,]+) states left on queue", text)
        counts = [int(v.replace(",", "")) for v in stats[-1]] if stats else [None, None, None]
        depth = re.search(r"The depth of the complete state graph search is (\d+)", text)
        rss = re.search(r"(\d+)\s+maximum resident set size", text)
        linux_rss = re.search(r"Maximum resident set size \(kbytes\): (\d+)", text)
        results[name] = {"status": status, "complete": complete, "exitCode": code,
                         "expectedInvariant": expected, "command": command,
                         "violatedInvariant": violation[1] if violation else None,
                         "generatedStates": counts[0], "distinctStates": counts[1], "queueDepth": counts[2],
                         "searchDepth": int(depth[1]) if depth else None,
                         "diameter": int(depth[1]) if complete and depth else None,
                         "elapsedSeconds": time.monotonic() - start,
                         "peakRssBytes": int(rss[1]) if rss else (int(linux_rss[1]) * 1024 if linux_rss else None)}
        (run / "coverage-summary.json").write_text(json.dumps(results, indent=2) + "\n")
        if states.exists():
            shutil.rmtree(states)
        print(name, status, counts, flush=True)
    good = all(r["status"] == ("PASS" if MATRIX[name] is None else "EXPECTED_COUNTEREXAMPLE")
               for name, r in results.items())
    (run / "exit-code").write_text("0\n" if good else "1\n")
    files = [p for p in run.rglob("*") if p.is_file()]
    (run / "checksums.txt").write_text("".join(f"{sha256(p)}  {p.relative_to(run)}\n" for p in sorted(files)))
    return 0 if good else 1


if __name__ == "__main__":
    raise SystemExit(main())
