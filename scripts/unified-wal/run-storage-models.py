#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
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
    'c-closed-moved-image': None,
    'ce-closed-moved-image-current': 'AdmittedHasCurrentCopy',
    'ce-closed-moved-image-orphan': 'AdmittedExplainsAllCopies',
    'c-closed-reconstruction': None,
    'c-closed-reconstruction-mixed': None,
    'witness-closed-reconstruction-complete': 'NoCompleteRecovery',
    'witness-closed-reconstruction-late-ready': 'NoRejectedLateReady',
    'witness-closed-reconstruction-delete': 'NoDeleteRecovery',
    'witness-closed-reconstruction-retry': 'NoGatedHealthyRetry',
    'ce-closed-reconstruction-second-member': 'ReadyHasAllMembers',
    'ce-closed-reconstruction-shadow': 'ReadyUsesOriginalClaim',
    'ce-closed-reconstruction-candidate': 'ReadyHasClosedCandidates',
    'ce-closed-reconstruction-partial': 'ResolutionAfterPublication',
    'ce-closed-reconstruction-cleanup': 'ResolutionAfterPublication',
    'ce-closed-reconstruction-file0': 'FirstOpenChecksEveryFile',
    'ce-closed-reconstruction-retry-gate': 'StartupWireWaitsForAdmission',
    'ce-closed-reconstruction-cutoff': 'OldGrantStaysReadOnly',
    'witness-acd-profile-sdk-all-delete-free-discard-first': 'AllNoCompletedFreeAfterFirstDiscard',
    'witness-acd-profile-sdk-all-delete-free-discard-second': 'AllNoCompletedFreeAfterSecondDiscard',
    'ce-acd-profile-sdk-all-delete-free-discard-early': 'AllDiscardWaitsForActualReturn',
    'ce-acd-profile-sdk-all-delete-free-discard-omitted': 'AllDiscardCanSettle',
    'acd-profile-sdk-all-delete-free': None,
    'witness-acd-profile-sdk-all-delete-free': 'AllNoCompletedFree',
    'ce-acd-profile-sdk-all-delete-free-early': 'AllThirdAckHasOwnPublication',
    'ce-acd-profile-sdk-all-delete-free-peer': 'AllThirdAckHasOwnPublication',
    'ce-acd-profile-sdk-all-delete-free-drain': 'AllClosedHasEverySourceAndIoDrain',
    'ce-acd-profile-sdk-all-delete-free-source': 'AllClosedHasEverySourceAndIoDrain',
    'acd-profile-sdk-delete-free': None,
    'witness-acd-profile-sdk-delete-free': 'ACDNoCompletedFree',
    'ce-acd-profile-sdk-delete-free-drain': 'ACDClosedHasOriginalDrain',
    'ce-acd-profile-sdk-delete-free-io': 'ACDActualIoMatchesSdk',
    'ce-acd-profile-sdk-delete-free-quorum': 'ACDLocalDeleteHasFencedClose',
    'ac-coordinate-profile-sdk-bridge': None,
    'ce-ac-coordinate-profile-sdk-bridge-cancel': 'ACCoordinateRejectsConflictingAdmission',
    'ce-ac-coordinate-profile-sdk-bridge-payload': 'ACCoordinateRejectsConflictingSuccess',
    'ce-ac-coordinate-profile-sdk-bridge-length': 'ACCoordinateRejectsConflictingSuccess',
    'ce-ac-coordinate-profile-sdk-bridge-lac': 'ACCoordinateIgnoresLacOnlyChange',
    'ac-prefix-profile-sdk-bridge': None,
    'ce-ac-prefix-profile-sdk-bridge-suffix': 'ACSuccessHasReturnedQuorum',
    'ce-prefix-before-earlier-io-return': 'PhysicalSuccessWaitsIoReturn',
    'ac-profile-sdk-bridge': None,
    'ce-ac-profile-sdk-bridge-late-submit': 'ACSentCanReachStorageAfterWait',
    'ce-ac-profile-sdk-bridge-early': 'ACAcknowledgmentHasOwnLocalPublication',
    'ce-ac-profile-sdk-bridge-peer': 'ACAcknowledgmentHasOwnLocalPublication',
    'ce-ac-profile-sdk-bridge-recovery': 'ACAcknowledgmentCameFromLiveEpisode',
    'ce-ac-profile-sdk-bridge-cancel': 'ACCancellationKeepsStorageProtection',
    'cd-whole-free': None,
    'ce-cd-whole-free-delivery': 'CDDeliveryHasLogicalObligation',
    'ce-cd-whole-free-force': 'CDLocalEffectHasForce',
    'ce-cd-whole-free-neighbour': 'CDRemainingLedgerStaysAllocated',
    'ce-cd-whole-free-quorum': 'CDFreeHasEveryLogicalAndLocalEffect',
    'a-profile-journal-compaction': None,
    'ce-profile-journal-compaction-snapshot': 'PrunedHasCompleteSnapshot',
    'ce-profile-journal-compaction-effective': 'PrunedHasEffectiveTakeover',
    'ce-profile-journal-compaction-effects': 'PrunedKeepsEveryEffect',
    'ce-profile-journal-compaction-cas': 'PrunedHasNativeSelectionCAS',
    'ce-profile-journal-compaction-physical': 'PhysicalReceiptKeepsOriginalBirth',
    'ce-profile-journal-compaction-custody': 'CancellationKeepsActualCustody',
    'a-profile-restore-dependencies': None,
    'witness-profile-restore-dependencies-mixed': 'NoObservedMixedOutcome',
    'ce-profile-restore-dependencies-atomic': 'AtomicAdmission',
    'ce-profile-restore-dependencies-floor': 'AdmittedAboveFloor',
    'ce-profile-restore-dependencies-retry': 'RetryKeepsOriginalImage',
    'ce-profile-restore-dependencies-mode': 'CompleteHasCoverage',
    'ce-profile-restore-dependencies-stale': 'CompleteHasCurrentCut',
    'ce-profile-restore-dependencies-scope': 'CompleteHasCoverage',
    'ce-profile-restore-dependencies-budget': 'CompleteWithinBudget',
    'ce-profile-restore-dependencies-close': 'SettledHasExactReceipt',
    'ce-profile-restore-dependencies-delete': 'SettledHasExactReceipt',
    'ce-profile-restore-dependencies-empty': 'SettledHasExactReceipt',
    'a-profile-delete-retention': None,
    'ce-profile-delete-retention-cas': 'RetentionHasCurrentPolicyCAS',
    'ce-profile-delete-retention-atomic': 'RetentionHasAtomicTombstone',
    'ce-profile-delete-retention-window': 'RetentionKeepsOriginalWindows',
    'ce-profile-delete-retention-birth': 'RetentionRejectsRecreatedBirth',
    'ce-profile-delete-retention-legacy': 'RetentionDoesNotBackfillLegacy',
    'ce-profile-delete-retention-terminal': 'WindowHasAllOriginalTerminals',
    'ce-profile-delete-retention-restart': 'WindowHasLocalObservation',
    'ce-profile-delete-retention-fresh': 'WindowHasFreshNativeCut',
    'a-profile-delete-effective': None,
    'ce-profile-delete-effective-cut': 'EffectiveHasCompleteLocalCut',
    'ce-profile-delete-effective-ack': 'EffectiveHasOriginalAck',
    'ce-profile-delete-effective-registry': 'EffectiveRetiresOldRegistry',
    'ce-profile-delete-effective-gate': 'EffectiveRetiresOldGate',
    'ce-profile-delete-effective-force': 'EffectiveLocalProgressHasForce',
    'ce-profile-delete-effective-event': 'EffectiveNewEventKeepsGeneration',
    'ce-profile-delete-effective-history': 'EffectiveOldEventKeepsLineage',
    'ce-profile-delete-effective-checkpoint': 'EffectiveCheckpointKeepsProgress',

    'a-profile-handoff-ack': None,
    'ce-profile-handoff-ack-local': 'AckHasActualLocalAdoption',
    'ce-profile-handoff-ack-source': 'AckConfirmsOriginalPreparation',
    'ce-profile-handoff-ack-birth': 'AckConfirmsOriginalRowBirth',
    'ce-profile-handoff-ack-retry': 'AckMatchingRetryRetainsFirstRow',
    'ce-profile-handoff-ack-release': 'AckReleaseWaitsActualColdReturn',

    'a-profile-delete-adoption': None,
    'ce-profile-delete-adoption-cut': 'AdoptionHasCompleteCut',
    'ce-profile-delete-adoption-force': 'AdoptionHasForce',
    'ce-profile-delete-adoption-checkpoint': 'AdoptionReplaysCheckpoint',
    'ce-profile-delete-adoption-release': 'AdoptionReleaseWaitsOwnerTerminal',

    'a-profile-delete-handoff-prepare': None,
    'ce-profile-delete-handoff-admission': 'HandoffPreparedCutIsStable',
    'ce-profile-delete-handoff-head': 'HandoffPreparedCutIsStable',
    'ce-profile-delete-handoff-snapshot': 'HandoffPreparedHasSnapshot',
    'ce-profile-delete-handoff-commit': 'HandoffPreparedHasCommittedEffects',
    'ce-profile-delete-handoff-stop': 'HandoffStopStartsNoPreparation',
    'ce-profile-delete-handoff-release': 'HandoffReleaseWaitsActual',

    'a-profile-delete-snapshot': None,
    'ce-profile-delete-snapshot-chunk': 'SnapshotPublicationHasEveryChunk',
    'ce-profile-delete-snapshot-scope': 'SnapshotPublicationHasScope',
    'ce-profile-delete-snapshot-birth': 'SnapshotBootstrapHasScopeAndBirth',
    'ce-profile-delete-snapshot-effect': 'SnapshotCursorHasEveryEffect',
    'ce-profile-delete-snapshot-suffix': 'SnapshotBootstrapHasCompleteSuffix',
    'ce-profile-delete-snapshot-stop': 'SnapshotStopStartsNoPublication',
    'ce-profile-delete-snapshot-release': 'SnapshotReleaseWaitsProcedure',

    'a-profile-physical-receipt': None,
    'ce-profile-physical-receipt-scope': 'ReceiptWritesNeedScope',
    'ce-profile-physical-receipt-commit': 'ReceiptObservationsNeedCommit',
    'ce-profile-physical-receipt-birth': 'ReceiptObservationsNeedBirth',
    'ce-profile-physical-receipt-quorum': 'ReceiptCompletionNeedsEveryStoredTarget',
    'ce-profile-physical-receipt-stop': 'ReceiptStopStartsNoPublication',
    'ce-profile-physical-receipt-release': 'ReceiptReleaseWaitsProcedure',

    'a-profile-physical-wire': None,
    'ce-profile-physical-wire-event': 'PhysicalWireNeedsEvent',
    'ce-profile-physical-wire-peer': 'PhysicalWireNeedsPeer',
    'ce-profile-physical-wire-cursor': 'PhysicalWireNeedsAppliedEvent',
    'ce-profile-physical-wire-fresh': 'PhysicalWireReportNeedsFreshEvent',
    'ce-profile-physical-wire-quorum': 'PhysicalWireAggregateNeedsEveryTarget',
    'ce-profile-physical-wire-drain': 'PhysicalWireReleaseNeedsActual',

    'a-profile-physical-delete': None,
    'ce-profile-physical-delete-scope': 'PhysicalHasScope',
    'ce-profile-physical-delete-unknown': 'PhysicalHasKnownBytes',
    'ce-profile-physical-delete-index': 'PhysicalScanIncludesAllOrigins',
    'ce-profile-physical-delete-copy': 'PhysicalScanIncludesAllOrigins',
    'ce-profile-physical-delete-force': 'PhysicalFreeNeedsForce',
    'ce-profile-physical-delete-live': 'PhysicalKeepsOtherLedger',
    'ce-profile-physical-delete-io': 'PhysicalWaitsOldIo',
    'ce-profile-physical-delete-owner': 'PhysicalCreditWaitsActualOwner',

    'a-profile-write-lac-a2': None,
    'a-profile-write-lac-a3': None,
    'ce-profile-write-lac-scope': 'WriteLacHasScope',
    'ce-profile-write-lac-digest': 'WriteLacHasDigest',
    'ce-profile-write-lac-confirmed': 'WriteLacUsesConfirmedCut',
    'ce-profile-write-lac-force': 'WriteLacAppliedNeedsForce',
    'ce-profile-write-lac-quorum': 'WriteLacQuorumIsDistinct',
    'ce-profile-write-lac-unknown': 'WriteLacAckIsDurable',
    'ce-profile-write-lac-drain': 'WriteLacReleaseWaitsActual',
    'ce-profile-write-lac-fence': 'WriteLacFenceNeverAdvances',
    'ce-profile-write-lac-replay': 'WriteLacReplayPreservesAuthority',

    'a-profile-delete-publication': None,
    'ce-profile-delete-publication-prefix': 'LogicalHasEveryDelivery',
    'ce-profile-delete-publication-activation': 'LogicalHasEveryDelivery',
    'ce-profile-delete-publication-inert': 'AppliedNeedsLogicalActivation',
    'ce-profile-delete-publication-birth': 'ReportedPublicationHasBirth',
    'ce-profile-delete-publication-stop': 'StoppedStartsNoProbe',
    'ce-profile-delete-publication-release': 'FollowerDrainWaitsActual',
    'ce-profile-delete-publication-overlap': 'OneOwnedProbe',

    'a-profile-delete-registration': None,
    'ce-profile-delete-registration-cursor': 'WritableCoveredPublicationCut',
    'ce-profile-delete-registration-cut': 'WritableCoveredPublicationCut',
    'ce-profile-delete-registration-missing': 'WritableHasAssignment',
    'ce-profile-delete-registration-generation': 'WritableHasEffectiveGeneration',
    'ce-profile-delete-registration-incarnation': 'WritableUsesOriginalIncarnation',
    'ce-profile-delete-registration-growth': 'OrdinaryHeadGrowthKeepsRegistration',

    'a-profile-delete-applied': None,
    'ce-profile-delete-applied-hole': 'CursorHasNoHole',
    'ce-profile-delete-applied-effect': 'CursorHasNoHole',
    'ce-profile-delete-applied-force': 'CursorNeedsDurableEffect',
    'ce-profile-delete-applied-checkpoint': 'CursorHasNoHole',
    'ce-profile-delete-applied-custody': 'OldCustodyWaitsPhysical',
    'ce-profile-delete-applied-assignment': 'ApplyUsesInitialAssignment',
    'ce-profile-delete-applied-incarnation': 'CursorUsesActualIncarnation',

    'a-profile-delete-delivery': None,
    'ce-profile-delete-delivery-head': 'HeadCommitHasNoHole',
    'ce-profile-delete-delivery-duplicate': 'SameRequestUsesOnePosition',
    'ce-profile-delete-delivery-assignment': 'ReturnedHasExactAuthority',
    'ce-profile-delete-delivery-birth': 'ReturnedHasExactAuthority',
    'ce-profile-delete-delivery-missing': 'ReturnedHasExactAuthority',
    'ce-profile-delete-delivery-streams': 'OneSharedStream',

    'a-profile-logical-delete': None,
    'ce-profile-logical-delete-close': 'DeleteNeedsFencedClosed',
    'ce-profile-logical-delete-seal': 'TargetsFollowStandardSeal',
    'ce-profile-logical-delete-history': 'LogicalHasCompleteObligation',
    'ce-profile-logical-delete-logical': 'LogicalHasCompleteObligation',
    'ce-profile-logical-delete-birth': 'ReportRequiresManifestBirth',
    'ce-profile-logical-delete-cancel': 'ColdOwnsGate',
    'ce-profile-logical-delete-offline': 'OfflineDoesNotPreventLogical',

    'a-profile-writer-close': None,
    'ce-profile-writer-close-enter': 'EnterWaitsActualData',
    'ce-profile-writer-close-atomic': 'RecoveryAtomicallyFreezesCut',
    'ce-profile-writer-close-tail': 'ClosedMatchesWriterPrefix',
    'ce-profile-writer-close-digest': 'AdoptionNeedsBoundCut',
    'ce-profile-writer-close-cancel': 'CompletionWaitsData',
    'ce-profile-writer-close-length': 'ClosedMatchesWriterPrefix',
    'ce-profile-writer-close-kind': 'AdoptionNeedsSupportedKind',
    'a-profile-writer-close-owner': None,
    'ce-profile-writer-close-owner-result': 'OwnerActualNeedsColdTerminal',
    'ce-profile-writer-close-owner-queued': 'OwnerAcceptedClosePinsActual',
    'ce-profile-writer-close-owner-callback': 'OwnerSuccessWaitsLease',

    'a-profile-replica-audit': None,
    'a-open-replica-audit': None,
    'ce-open-replica-audit-scope': 'OpenAuditHasScopeAndKnownCut',
    'ce-open-replica-audit-cut': 'OpenAuditHasScopeAndKnownCut',
    'ce-open-replica-audit-tail': 'OpenAuditNeverScansGrowingTail',
    'ce-open-replica-audit-fence': 'OpenAuditNeverFencesWriter',
    'ce-open-replica-audit-unknown': 'OpenAuditMarkerHasActualSource',
    'ce-open-replica-audit-fresh': 'OpenAuditMarkerUsesFinalFresh',
    'ce-open-replica-audit-clear': 'OpenAuditNeverClearsMarker',
    'ce-open-replica-audit-health': 'OpenAuditWaitsActualHealth',
    'ce-open-replica-audit-lease': 'OpenAuditWaitsActualLease',
    'ce-profile-replica-audit-open': 'AuditOnlyClosed',
    'ce-profile-replica-audit-scope': 'AuditOnlyScoped',
    'ce-profile-replica-audit-unknown': 'UnknownNeverSchedulesMissing',
    'ce-profile-replica-audit-fresh': 'MarkerNeedsFinalFreshCut',
    'ce-profile-replica-audit-clear': 'ObserverNeverClearsMarker',
    'ce-profile-replica-audit-health': 'DrainWaitsActualHealth',
    'ce-profile-replica-audit-lease': 'CapacityWaitsActualLease',
    'ce-profile-replica-audit-sdk': 'SDKWaitsActualHealthAndLease',
    'ce-profile-replica-audit-budget': 'SharedPoolReservationIsBounded',

    'a-profile-membership-audit': None,
    'ce-profile-membership-audit-admin-index': 'AdminIndexPreservesReferences',
    'ce-profile-membership-audit-admin-return': 'LegacyDoesNotRetireProfile',
    'ce-profile-membership-audit-classic-mask': 'KnownLossSchedulesCheck',
    'ce-profile-membership-audit-classic-route': 'MarksBelongToProfile',
    'ce-profile-membership-audit-scope': 'MarksHaveValidScope',
    'ce-profile-membership-audit-cut': 'MarksUseStableCut',
    'ce-profile-membership-audit-fresh': 'MarksUseFreshPeers',
    'ce-profile-membership-audit-unknown': 'UnknownNeverMeansMissing',
    'ce-profile-membership-audit-incarnation': 'KnownLossSchedulesCheck',
    'ce-profile-membership-audit-grant': 'MarkerIsSchedulingOnly',
    'ce-profile-membership-audit-queue': 'NotificationsAreCoalesced',

    'a-profile-service-bootstrap': None,
    'ce-profile-service-bootstrap-default': 'DefaultIsOff',
    'ce-profile-service-bootstrap-context': 'EnabledUsesContext',
    'ce-profile-service-bootstrap-audit': 'EnabledUsesSupportedAuditScope',
    'ce-profile-service-bootstrap-ready': 'StartNeedsReady',
    'ce-profile-service-bootstrap-sdk': 'SDKWaitsForPool',
    'ce-profile-service-bootstrap-owners': 'OwnersWaitForSDK',
    'ce-profile-service-bootstrap-failure': 'FailureReturnKeepsNoOwners',
    'ce-profile-service-bootstrap-interrupt': 'SDKCompletionNeedsActualReturn',
    'ce-profile-service-bootstrap-signal': 'ReturnPreservesSignal',

    'a-profile-recovery-peer-pool': None,
    'ce-profile-recovery-peer-pool-capacity': 'ConnectionReservationIsBounded',
    'ce-profile-recovery-peer-pool-cold': 'ColdBeforeCapacityReturn',
    'ce-profile-recovery-peer-pool-transport': 'TransportBeforeCapacityReturn',
    'ce-profile-recovery-peer-pool-task': 'OriginalTaskUntilDrained',
    'ce-profile-recovery-peer-pool-borrowed': 'BorrowedLeasePinsTransport',
    'ce-profile-recovery-peer-pool-verified': 'LeaseHasVerifiedPeers',
    'ce-profile-recovery-peer-pool-incarnation': 'NoSilentIncarnationRejoin',
    'ce-profile-recovery-peer-pool-child': 'ActualChildPinsLease',

    'a-profile-service-shutdown': None,
    'a-profile-service-shutdown-self': None,
    'ce-profile-service-shutdown-join': 'ManagerNeedsActualDrain',
    'ce-profile-service-shutdown-repeated': 'RepeatedCallerWaitsForCleanup',
    'ce-profile-service-shutdown-manager': 'ManagerNeedsActualDrain',
    'ce-profile-service-shutdown-sdk': 'SDKNeedsActualDrain',
    'ce-profile-service-shutdown-signal': 'CallerRestoresInterrupt',
    'ce-profile-service-shutdown-metadata': 'SDKCloseKeepsMetadataCleanup',
    'ce-profile-service-shutdown-self': 'SelfStopNeverJoinsItself',
    'ce-profile-service-shutdown-restart': 'ShutdownNeverRestarts',
    'ce-profile-service-shutdown-auditor': 'AuditorNeedsActualDrain',

    'a-profile-registered-connection': None,
    'ce-profile-registered-connection-pre': 'ReadyNeedsPre',
    'ce-profile-registered-connection-hello': 'ReadyNeedsHello',
    'ce-profile-registered-connection-post': 'ReadyNeedsPost',
    'ce-profile-registered-connection-cold': 'ColdDrainsBeforeCapacity',
    'ce-profile-registered-connection-write': 'OutputDrainsBeforeCapacity',
    'ce-profile-registered-connection-hello-write': 'HelloWriteDrainsBeforeCapacity',
    'ce-profile-registered-connection-transport': 'TransportDrainsBeforeCapacity',
    'ce-profile-registered-connection-retire': 'NoAcquisitionAfterDrain',

    'a-profile-peer-discovery': None,
    'ce-profile-peer-discovery-ready': 'ReadyRequired',
    'ce-profile-peer-discovery-owner': 'OwnerRequired',
    'ce-profile-peer-discovery-pair': 'PairRequired',
    'ce-profile-peer-discovery-marker': 'MarkerRequired',
    'ce-profile-peer-discovery-generation': 'GenerationUnchangedAtCut',
    'ce-profile-peer-discovery-birth': 'BirthUnchangedAtCut',

    'a-profile-empty-repair': None,
    'ce-profile-empty-repair-admission': 'BoundaryRequiresAdmission',
    'ce-profile-empty-repair-boundary': 'PublicationRequiresBoundary',
    'ce-profile-empty-repair-data': 'NoFakeData',
    'ce-profile-empty-repair-grant': 'NoDataGrant',
    'ce-profile-empty-repair-normal': 'NoNormalActivation',
    'ce-profile-empty-repair-cut': 'OriginalCutIsPreserved',
    'ce-profile-empty-repair-delete': 'DeleteBlocksPublication',
    'ce-profile-empty-repair-birth': 'OriginalBirthAtPublication',
    'ce-profile-empty-repair-receipt': 'PublicationRetainsReceipt',
    'ce-profile-empty-repair-retry': 'RetryRetainsOperation',
    'ce-profile-empty-repair-permit': 'ActualBoundaryOwnsPermit',
    'a-profile-replication-worker': None,
    'a-profile-replication-worker-threeofthree': None,
    'ce-profile-replication-worker-classic': 'NoClassicForProfile',
    'ce-profile-replication-worker-quorum': 'ClearRequiresAllCopies',
    'ce-profile-replication-worker-data': 'PublicationRequiresDurableData',
    'ce-profile-replication-worker-normal': 'NoNormalActivation',
    'ce-profile-replication-worker-intent': 'SameIntentOnResume',
    'ce-profile-replication-worker-recopy': 'PublishedResumeDoesNotRecopy',
    'ce-profile-replication-worker-drain': 'ActualChildRetainsTask',
    'ce-profile-replication-worker-task': 'OriginalTaskAtClear',
    'ce-profile-replication-worker-delete': 'DeletePreventsCompletion',
    'ce-profile-replication-worker-descriptor': 'DescriptorRequired',
    'ce-profile-replication-worker-unknown': 'UnknownDoesNotPublish',
    'ce-profile-replication-worker-empty': 'EmptyMissingRemainsPending',
    'ce-profile-replication-worker-bound': 'WorkIsBounded',
    'ce-profile-replication-worker-prefix': 'PrefixIsFixed',
    'a-profile-marker-cleanup': None,
    'ce-profile-marker-cleanup-birth': 'OriginalMarkerBirth',
    'ce-profile-marker-cleanup-version': 'OriginalMarkerVersion',
    'ce-profile-marker-cleanup-metadata': 'MatchingMetadata',
    'ce-profile-marker-cleanup-delete': 'DeleteKeepsTask',
    'ce-profile-marker-cleanup-lock': 'OriginalWorkerLock',
    'ce-profile-marker-cleanup-task': 'OriginalAcquiredTask',
    'ce-profile-marker-cleanup-issuer': 'OriginalIssuer',
    'ce-profile-marker-cleanup-expiry': 'LiveProofAtClaim',
    'ce-profile-marker-cleanup-reuse': 'ProofUsedOnce',
    'ce-profile-marker-cleanup-stop': 'StopBeforeSubmissionKeepsTask',
    'ce-profile-marker-cleanup-gate': 'ActualCommandOwnsGate',
    'ce-profile-marker-cleanup-release': 'UnknownReleaseKeepsForeignLock',
    'a-profile-replica-health': None,
    'ce-profile-replica-health-all': 'AllRequiredCopiesFound',
    'ce-profile-replica-health-unknown': 'UnknownNeverHealthy',
    'ce-profile-replica-health-length': 'MatchingLength',
    'ce-profile-replica-health-empty': 'EmptyRequiresAllTargets',
    'ce-profile-replica-health-birth': 'FreshBirth',
    'ce-profile-replica-health-version': 'FreshVersion',
    'ce-profile-replica-health-delete': 'DeletePreventsProof',
    'ce-profile-replica-health-stop': 'StoppedNeverGetsProof',
    'ce-profile-replica-health-gate': 'ActualReadOwnsGate',
    'c-profile-terminal-cut': None,
    'ce-profile-terminal-cut-force': 'CutHasForce',
    'ce-profile-terminal-cut-precut': 'CutWaitsForPrecut',
    'ce-profile-terminal-cut-admission': 'NoNewAdmission',
    'ce-profile-terminal-cut-read': 'NoExcludedRead',
    'ce-profile-terminal-cut-checkpoint': 'CheckpointRetainsUpgrade',
    'ce-profile-terminal-cut-replay': 'ReplayKeepsCut',
    'ce-profile-terminal-cut-pending-free': 'SuffixFreeHasDurableCut',
    'ce-profile-terminal-cut-prefix-free': 'RequiredPrefixIsNotDead',
    'a-profile-closed-repair': None,
    'ce-profile-closed-repair-birth': 'PublicationHasSameBirth',
    'ce-profile-closed-repair-delete': 'PublicationBeforeDelete',
    'ce-profile-closed-repair-reopen': 'ClosedNeverReopens',
    'ce-profile-closed-repair-boundary': 'ClosedBoundaryIsPreserved',
    'ce-profile-closed-repair-cut': 'OriginalCloseCutIsPreserved',
    'ce-profile-closed-repair-version': 'RepairVersionIncludesCloseOffset',
    'ce-profile-closed-repair-copy': 'PublicationHasDurableCopy',
    'ce-profile-closed-repair-readable': 'TargetReadableAfterGrantClose',
    'ce-profile-closed-repair-recopy': 'PublishedResumeDoesNotRecopy',
    'a-profile-whole-recovery': None,
    'a-profile-whole-recovery-threeofthree': None,
    'ce-profile-whole-recovery-proof': 'SelectionHasVerifiedProof',
    'ce-profile-whole-recovery-selection': 'SelectionIsFixed',
    'ce-profile-whole-recovery-context': 'ContextIsAdopted',
    'ce-profile-whole-recovery-intent': 'TargetIdentityIsStable',
    'ce-profile-whole-recovery-stop': 'StoppedDoesNotStartWork',
    'ce-profile-whole-recovery-gate': 'ActualChildOwnsGate',
    'ce-profile-whole-recovery-close': 'SuccessHasMatchingClosed',
    'ce-profile-whole-recovery-unknown': 'QuorumCountsActualDurable',
    'ce-profile-whole-recovery-history': 'ClosedKeepsCleanupHistory',
    'a-profile-readonly-open': None,
    'ce-profile-readonly-delete': 'OpenHadLiveAdoption',
    'ce-profile-readonly-birth': 'OpenHadSameBirth',
    'ce-profile-readonly-incarnation': 'OpenHadExpectedIncarnation',
    'ce-profile-readonly-gate': 'OpenHadFinalGate',
    'ce-profile-readonly-cancel-open': 'CancelledOpenNotReturned',
    'ce-profile-readonly-cancel-read': 'ActualReadOwnsPermit',
    'ce-profile-readonly-drain': 'DrainNeedsActualOpenTerminal',
    'ce-profile-readonly-boundary': 'ReadonlyHasFixedBoundary',
    'ce-profile-readonly-suffix': 'ReadonlyDoesNotExposeSuffix',
    'ce-profile-readonly-mutation': 'ReadonlyDoesNotMutate',
    'a-profile-readonly-sdk-owner': None,
    'ce-profile-readonly-sdk-cold': 'SDKColdOpenPinsLease',
    'ce-profile-readonly-sdk-read': 'SDKActualReadPinsLease',
    'ce-profile-readonly-sdk-pool': 'SDKDependenciesWaitForPool',
    'a-profile-readonly-shared-sdk': None,
    'ce-profile-readonly-shared-sdk-control': 'SharedSDKControlActualPinsChild',
    'ce-profile-readonly-shared-sdk-read-view': 'SharedSDKReadViewsPinChild',
    'ce-profile-readonly-shared-sdk-lease': 'SharedSDKSurvivorPinsAcquisition',
    'ce-profile-readonly-shared-sdk-dependencies': 'SharedSDKDependenciesWaitForActualChildren',
    'ce-profile-readonly-shared-sdk-observation': 'SharedSDKQueuedObservationPinsAdmission',
    'a-profile-recovery-completion': None,
    'a-profile-recovery-completion-empty': None,
    'a-profile-recovery-completion-empty-threeofthree': None,
    'a-profile-recovery-completion-historical-empty': None,
    'a-profile-recovery-completion-historical-empty-threeofthree': None,
    'ce-profile-recovery-completion-empty-length': 'ClosedHasMatchingLength',
    'ce-profile-recovery-completion-empty-prefix': 'ClosedHasMatchingPrefix',
    'ce-profile-recovery-completion-empty-grant': 'EmptyCloseCreatesNoGrant',
    'ce-profile-recovery-completion-historical-floor': 'ClosedContainsFrozenHistory',
    'a-profile-recovery-completion-threeofthree': None,
    'ce-profile-recovery-completion-unknown': 'ClosedHasActualTerminal',
    'ce-profile-recovery-completion-duplicate': 'ClosedHasDistinctQuorum',
    'ce-profile-recovery-completion-capability': 'ClosedHasCapability',
    'ce-profile-recovery-completion-birth': 'ClosedHasSameBirth',
    'ce-profile-recovery-completion-length': 'ClosedHasMatchingLength',
    'ce-profile-recovery-completion-context': 'ClosedHasMatchingContext',
    'ce-profile-recovery-completion-reopen': 'MatchingClosedNeverReopens',
    'ce-profile-recovery-completion-cancel': 'ActualAttemptOwnsPermit',
    'ce-profile-recovery-completion-terminal': 'ClosedHasActualTerminal',
    'a-profile-recovery-selection': None,
    'ce-profile-recovery-selection-meta': 'PublicationHasMetaBirth',
    'ce-profile-recovery-selection-marker': 'PublicationHasMarkerGuard',
    'ce-profile-recovery-selection-origin': 'PublicationHasVerifiedOrigin',
    'ce-profile-recovery-selection-birth': 'AdoptionHasPhysicalBirth',
    'ce-profile-recovery-selection-reference': 'SelectionHasReference',
    'ce-profile-recovery-selection-offset': 'VersionHasSingleSelectionOffset',
    'ce-profile-recovery-selection-range': 'CopyUsesSelectedRange',
    'ce-profile-recovery-selection-fixed': 'SelectionIsFixed',
    'ce-profile-recovery-selection-close': 'SelectionNeverCloses',
    'ce-profile-repair-membership-selection': 'LastFragmentHasSelection',
    'a-profile-recovery-prefix': None,
    'a-profile-recovery-prefix-threeofthree': None,
    'ce-profile-recovery-prefix-early': 'AllSourcesEnded',
    'ce-profile-recovery-prefix-unknown-missing': 'TailHasDefinitiveCoverage',
    'ce-profile-recovery-prefix-unknown-loss': 'LossHasRequiredFiniteSet',
    'ce-profile-recovery-prefix-required': 'TailIsAfterRequired',
    'ce-profile-recovery-prefix-conflict': 'ConflictsQuarantine',
    'ce-profile-recovery-prefix-prior': 'ProgressHasContiguousPrefix',
    'ce-profile-recovery-prefix-length': 'CandidateHasMatchingLength',
    'ce-profile-recovery-prefix-cancel': 'InFlightOwnsReservation',
    'ce-profile-recovery-prefix-context': 'EvidenceHasCurrentContext',
    'ce-profile-recovery-prefix-source': 'EvidenceHasBoundFencedSources',
    'ce-profile-recovery-prefix-close': 'NoClosedPublication',
    'a-profile-repair-copy': None,
    'a-profile-repair-copy-output': None,
    'ce-profile-repair-copy-output-return': 'ActualCopyRetainsOutputCustody',
    'ce-profile-repair-copy-sources': 'CopyHasSources',
    'ce-profile-repair-copy-grant': 'CopyHasGrant',
    'ce-profile-repair-copy-copies': 'PublicationHasAllCopies',
    'ce-profile-repair-copy-unknown': 'OnlyDurableCounts',
    'ce-profile-repair-copy-cancel': 'InFlightHasPermit',
    'ce-profile-repair-copy-resume': 'PublishedResumeNeverCopies',
    'ce-profile-repair-copy-parallel': 'OnlyOneEntryInFlight',
    'ce-profile-repair-copy-stop': 'CancelStopsFutureEntries',
    'ce-profile-repair-copy-normal': 'NormalNeverActivates',
    'a-profile-repair-membership': None,
    'ce-profile-repair-membership-meta-birth': 'PublicationHasMetaBirth',
    'ce-profile-repair-membership-intent-birth': 'PublicationHasIntentBirth',
    'ce-profile-repair-membership-delete': 'DeletePreventsPublication',
    'ce-profile-repair-membership-admission': 'PublicationHasAdmission',
    'ce-profile-repair-membership-atomic': 'PublicationIsAtomic',
    'ce-profile-repair-membership-slot': 'PublicationUsesPlannedSlot',
    'ce-profile-repair-membership-context': 'ContextIsPreserved',
    'ce-profile-repair-membership-retention': 'HistoryIsRetained',
    'ce-profile-repair-membership-proof': 'AdoptionRequiresProof',
    'ce-profile-repair-membership-boundary': 'PublicationHasBoundedFragment',
    'a-profile-recovery-close': None,
    'ce-profile-recovery-close-capability': 'CutHasCapability',
    'ce-profile-recovery-close-authority': 'CutHasAuthority',
    'ce-profile-recovery-close-binding': 'CutHasExactBinding',
    'ce-profile-recovery-close-precut': 'CloseWaitsForPrecut',
    'ce-profile-recovery-close-force': 'ReadableHasForce',
    'ce-profile-recovery-close-cancel': 'CustodyUntilTerminal',
    'ce-profile-recovery-close-normal': 'NormalNeverActivates',
    'ce-profile-recovery-close-late': 'CloseStopsRecoveryAdmission',
    'ce-profile-recovery-close-terminal': 'DurableReplyHasTerminal',
    'a-profile-repair-grant': None,
    'ce-profile-repair-grant-admission': 'GrantHasAdmissionAndInstall',
    'ce-profile-repair-grant-install': 'AdmissionHasInstallAndBirth',
    'ce-profile-repair-grant-delete': 'DeletePreventsFreshGrant',
    'ce-profile-repair-grant-birth': 'AdmissionHasInstallAndBirth',
    'ce-profile-repair-grant-auth': 'GrantHasAuthAndExactTuple',
    'ce-profile-repair-grant-tuple': 'GrantHasAuthAndExactTuple',
    'ce-profile-repair-grant-force': 'GrantHasActualForce',
    'ce-profile-repair-grant-retention': 'AdmittedTargetIsRetained',
    'ce-profile-repair-grant-normal': 'RecoveryDoesNotActivateNormal',
    'a-profile-recovery-observation': None,
    'a-profile-recovery-observation-threeofthree': None,
    'ce-profile-recovery-observation-fence': 'AbsenceHasFencedBoundCoverage',
    'ce-profile-recovery-observation-unknown': 'AbsenceHasFencedBoundCoverage',
    'ce-profile-recovery-observation-context': 'EvidenceHasCurrentContext',
    'ce-profile-recovery-observation-budget': 'BudgetNeverMeansAbsence',
    'ce-profile-recovery-observation-add': 'ReadonlyNeverAdds',
    'ce-profile-recovery-observation-cancel': 'CallerCancelKeepsReservation',
    'ce-profile-recovery-observation-close': 'ObservationNeverCloses',
    'a-profile-recovery-prelude': None,
    'a-profile-recovery-prelude-threeofthree': None,
    'ce-profile-recovery-prelude-meta-birth': 'WrittenContextHasObservedBirth',
    'ce-profile-recovery-prelude-root-birth': 'WrittenContextHasObservedBirth',
    'ce-profile-recovery-prelude-authority': 'WrittenContextHasPublishedAuthority',
    'ce-profile-recovery-prelude-creator': 'CreatorFieldsArePreserved',
    'ce-profile-recovery-prelude-operation': 'OperationIsFrozen',
    'ce-profile-recovery-prelude-unknown': 'FenceCoverageHasDurableVerifiedSources',
    'ce-profile-recovery-prelude-peer': 'FenceCoverageHasDurableVerifiedSources',
    'ce-profile-recovery-prelude-scope': 'FenceScopeIsCurrent',
    'ce-profile-recovery-prelude-close': 'PreludeNeverPublishesClosed',
    'a-profile-lac-evidence': None,
    'ce-profile-lac-evidence-explicit-digest': 'CoverageCountsOnlyVerified',
    'ce-profile-lac-evidence-explicit-frontier': 'SuccessHasMatchingLength',
    'a-profile-lac-evidence-threeofthree': None,
    'ce-profile-lac-evidence-digest': 'CoverageCountsOnlyVerified',
    'ce-profile-lac-evidence-length': 'SuccessHasMatchingLength',
    'ce-profile-lac-evidence-coordinate': 'SuccessHasCorrectAnchor',
    'ce-profile-lac-evidence-conflict': 'SuccessHasNoLengthConflict',
    'ce-profile-lac-evidence-cancel': 'InFlightOwnsReservation',
    'ce-profile-lac-evidence-metadata': 'SuccessKeepsMetadata',
    'a-profile-lac-source': None,
    'ce-profile-lac-source-auth': 'ReplyRequiresPermission',
    'ce-profile-lac-source-empty': 'EmptyRequiresWholeLedgerProof',
    'ce-profile-lac-source-explicit': 'EmptyRequiresWholeLedgerProof',
    'ce-profile-lac-source-length': 'SourceLengthIsNeverPrefixLength',
    'ce-profile-lac-source-binding': 'ReplyRequiresSourceBinding',
    'ce-profile-lac-source-validation': 'SourceHasValidatedData',
    'a-profile-point-read': None,
    'ce-profile-point-read-auth': 'AdmissionRequiresPermission',
    'ce-profile-point-read-missing': 'MissingRequiresCompleteStableFence',
    'ce-profile-point-read-cancel': 'InFlightKeepsCredit',
    'ce-profile-point-read-terminal': 'ObserverWaitsForActualTerminal',
    'ce-profile-point-read-loan': 'QueuedConsumerOwnsLoan',
    'ce-profile-point-read-output': 'OutputWaitsForWrite',
    'ce-profile-point-read-binding': 'ReplyRequiresBinding',
    'a-profile-sdk-replacement': None,
    'a-profile-sdk-replacement-threeofthree': None,
    'a-profile-sdk-replacement-sequential': None,
    'ce-profile-sdk-replacement-activation': 'AvailabilityIncludesActivation',
    'ce-profile-sdk-replacement-gate': 'GateRequiresWholeTransition',
    'ce-profile-sdk-replacement-old': 'RetiredReplicaCannotAck',
    'ce-profile-sdk-replacement-source': 'ResendKeepsSource',
    'ce-profile-sdk-replacement-coordinate': 'ResendKeepsCoordinate',
    'ce-profile-sdk-replacement-resend': 'OnlyAffectedSlotsResent',
    'ce-profile-sdk-replacement-callback': 'OneOriginalCallback',
    'a-profile-sdk-replacement-owner': None,
    'ce-profile-sdk-replacement-owner-queued': 'OwnerActualWaitsForQueuedEpisode',
    'ce-profile-sdk-replacement-owner-actual': 'OwnerActualWaitsForReplacementEpisode',
    'ce-profile-sdk-replacement-owner-stopped': 'OwnerStoppedCallbackDoesNotAdopt',
    'a-profile-replacement-install': None,
    'ce-profile-replacement-install-prepare': 'InstallHasBoundLivePreparation',
    'ce-profile-replacement-install-delete': 'InstallHasBoundLivePreparation',
    'ce-profile-replacement-install-metadata': 'InstallHasBoundLivePreparation',
    'ce-profile-replacement-install-inactive': 'InstallNeverActivatesNormal',
    'ce-profile-replacement-install-capability': 'ActivationHasNegotiatedBoundSchema',
    'ce-profile-replacement-install-schema': 'ActivationHasNegotiatedBoundSchema',
    'ce-profile-replacement-install-force': 'AckHasActualForceAndOwnerApply',
    'a-profile-replacement-lifecycle': None,
    'ce-profile-replacement-lifecycle-candidate': 'GrantRequiresCurrentPublishedAuthority',
    'ce-profile-replacement-lifecycle-delete': 'DeleteCutNeverReopens',
    'ce-profile-replacement-lifecycle-metadata': 'PublicationHasSameMetadataAndPageBirth',
    'ce-profile-replacement-lifecycle-page': 'PublicationHasSameMetadataAndPageBirth',
    'ce-profile-replacement-lifecycle-root': 'MutationUsesObservedRootBirth',
    'ce-profile-replacement-lifecycle-history': 'GrantedTargetsAreRetained',
    'ce-profile-replacement-lifecycle-discovery': 'DiscoveryIncludesPublishedTargets',
    'ce-profile-replacement-lifecycle-old': 'GrantRequiresCurrentPublishedAuthority',
    'a-profile-membership-cas': None,
    'ce-profile-membership-cas-marker': 'NewPhysicalBirthIsNeverOverwritten',
    'ce-profile-membership-cas-birth': 'CapturedBirthIsFrozen',
    'ce-profile-membership-cas-freeze': 'FrozenMembershipIsNeverWritten',
    'ce-profile-membership-cas-creator': 'MetadataFieldsArePreserved',
    'ce-profile-membership-cas-custom': 'MetadataFieldsArePreserved',
    'ce-profile-membership-cas-unknown': 'SuccessHasExactMappingAndVersion',
    'ce-profile-membership-cas-version': 'SuccessHasExactMappingAndVersion',
    'a-profile-sdk-bridge': None,
    'ce-profile-sdk-bridge-unknown': 'KnownNoneNeverClearsUnknown',
    'ce-profile-sdk-bridge-source': 'RetryKeepsSource',
    'ce-profile-sdk-bridge-coordinate': 'RetryKeepsCoordinate',
    'ce-profile-sdk-bridge-callback': 'OneOriginalCallback',
    'ce-profile-sdk-bridge-close': 'LocalStopNeverPublishesClosed',
    'a-profile-normal-client-source': None,
    'ce-profile-normal-client-source-quota': 'SourceSnapshotRequiresQuota',
    'ce-profile-normal-client-source-cancel': 'QueuedSourceWaitsForTaskDecision',
    'ce-profile-normal-client-source-bound': 'SubmittedEntryFitsInstalledBound',
    'ce-profile-normal-client-source-status': 'SuccessRequiresDurableStatus',
    'ce-profile-normal-client-source-write': 'OutputWaitsForActualWrite',
    'ce-profile-normal-client-source-sdk-stop': 'SdkStopWaitsActualCustody',
    'ce-profile-normal-client-source-sdk-cancel': 'SdkStopWaitsActualCustody',
    'a-profile-initial-client': None,
    'ce-profile-initial-client-peer': 'RequestRequiresVerifiedPeer',
    'ce-profile-initial-client-binding': 'SuccessHasBoundReply',
    'ce-profile-initial-client-cancel': 'SlotsWaitForBothTerminals',
    'ce-profile-initial-client-write': 'SlotsWaitForBothTerminals',
    'ce-profile-initial-client-id': 'FrameIdsNeverAlias',
    'ce-profile-initial-client-unknown': 'NetworkUncertaintyNeverMeansNone',
    'ce-profile-initial-client-revive': 'CancelledFutureIsNotRevived',
    'a-profile-initial-socket': None,
    'ce-profile-initial-socket-capability': 'ControlRequiresNegotiation',
    'ce-profile-initial-socket-authority': 'ControlRequiresExactAuthority',
    'ce-profile-initial-socket-workspace': 'WorkspaceMovesWithControl',
    'ce-profile-initial-socket-terminal': 'ColdAndIoCopiesRemainCharged',
    'ce-profile-initial-socket-barrier': 'SuccessRequiresActualForceAndOwner',
    'ce-profile-initial-socket-unknown': 'SubmittedUnknownIsNeverNone',
    'ce-profile-initial-socket-late': 'TimedOutAdmissionNeverSubmits',
    'a-profile-initial-availability': None,
    'ce-profile-initial-availability-ack': 'AvailabilityWaitsForAllE',
    'ce-profile-initial-availability-binding': 'AvailabilityHasCurrentBoundResults',
    'ce-profile-initial-availability-unknown': 'AvailabilityDoesNotCountUnknown',
    'ce-profile-initial-availability-history': 'AvailabilityDoesNotCountHistoricalFence',
    'ce-profile-initial-availability-rebuild': 'RequiredMetadataIsNeverRebuilt',
    'ce-profile-initial-availability-delete': 'AvailabilityObservesReadyNotDelete',
    'ce-profile-initial-availability-metadata': 'AvailabilityHasMatchingMetadataFact',
    'a-profile-initial-availability-sdk-create': None,
    'ce-profile-initial-availability-sdk-origin': 'CreateWriterHasNativeBirth',
    'ce-profile-initial-availability-sdk-claim': 'CreateHandoffConsumesClaim',
    'ce-profile-initial-availability-sdk-work': 'CreatePrivateWorkPinsLease',
    'ce-profile-initial-availability-sdk-writer': 'CreateUndeliveredWriterPinsLease',
    'a-profile-ready-lifecycle': None,
    'ce-profile-ready-lifecycle-inert': 'ActivationHasPublishedReady',
    'ce-profile-ready-lifecycle-head-birth': 'BirthChecksProtectTheCurrentRoot',
    'ce-profile-ready-lifecycle-page-birth': 'ReadyReferencesActualPageBirth',
    'ce-profile-ready-lifecycle-metadata-birth': 'ReadyValidatedMetadataBirth',
    'ce-profile-ready-lifecycle-delete': 'DeleteCutNeverRollsBack',
    'ce-profile-ready-lifecycle-retention': 'PublishedTargetsSurviveDelete',
    'ce-profile-ready-lifecycle-all-e': 'ReadyRequiresAllEReceipts',
    'a-profile-initial-metadata': None,
    'ce-profile-initial-metadata-ack': 'MetadataWaitsForAllE',
    'ce-profile-initial-metadata-binding': 'MetadataHasExactDurableResults',
    'ce-profile-initial-metadata-root': 'MetadataHasSameRootSnapshot',
    'ce-profile-initial-metadata-absence': 'MetadataHasCommittedReservation',
    'ce-profile-initial-metadata-plan': 'InvalidPlanHasNoReservation',
    'ce-profile-initial-metadata-writer': 'MetadataAloneNeverReturnsWriter',
    'ce-profile-install-dispatch-generation': 'InstallReceiptKeepsBirthGeneration',
    'a-profile-install-dispatch': None,
    'ce-profile-install-dispatch-exact': 'AdmissionHasExactAuthorityAndTarget',
    'ce-profile-install-dispatch-barrier': 'InstallSuccessHasBarrier',
    'ce-profile-install-dispatch-owner': 'InstallSuccessHasOwnerApply',
    'ce-profile-install-dispatch-stop': 'SubmittedOwnershipWaitsForBothTerminals',
    'ce-profile-install-dispatch-normal': 'InstallPreservesNormalState',
    'ce-profile-install-dispatch-unknown': 'SubmittedUnknownNeverBecomesNone',
    'a-profile-reservation-authority': None,
    'ce-profile-reservation-authority-key': 'GrantedMatchesRequestedKey',
    'ce-profile-reservation-authority-target': 'GrantedTargetIsPlanned',
    'ce-profile-reservation-authority-corrupt': 'GrantedHasValidCommittedRoot',
    'ce-profile-reservation-authority-normal': 'PreparingOnlyAuthorizesInstall',
    'ce-profile-reservation-authority-version': 'ProjectionKeepsReadVersion',
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
    'ce-profile-backing-budget-base': 'BaseHeldThroughActualDrain',
    'ce-profile-backing-budget-slot': 'SlotHeldThroughActualDrain',
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
    "a-startup-requirements": None,
    "a-profile-listener-drain": None,
    "ce-profile-listener-drain-early": "ListenerDoneHasNoTrackedCustody",
    "ce-profile-listener-drain-handoff": "ListenerDoneWaitsWorkerHandoff",
    "ce-profile-listener-drain-admission": "RetiredListenerCannotAdmit",
    "ce-profile-listener-drain-credit": "DisconnectKeepsActualCredit",
    "ce-profile-listener-drain-caller": "CallerCannotCompleteActualListenerDrain",
    "ce-startup-requirements-open": "StorageRequiresSupportedPreOpenSource",
    "ce-startup-requirements-birth": "WritableUsesFrozenSourceAtPublication",
    "ce-startup-requirements-substitution": "CallerCannotSubstituteBoundRequirements",
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
    parser.add_argument("--config-prefix", action="append", default=[],
                        help="Run declared configurations matching this prefix; repeat for related slices")
    parser.add_argument("--timeout-seconds", type=int, default=120,
                        help="Finite checker wall-time budget per configuration (1..3600, default 120)")
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 3600:
        parser.error("Checker timeout must be within 1..3600 seconds")
    for prefix in args.config_prefix:
        if not any(name.startswith(prefix) for name in MATRIX):
            parser.error(f"No declared configuration matches {prefix!r}")
    selected = {name: expected for name, expected in MATRIX.items()
                if not args.config_prefix or any(name.startswith(p) for p in args.config_prefix)}
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
                  "SegmentBackupImage.java", "SegmentStorageInspector.java", "SegmentOwnedFiles.java",
                  "SegmentClosedReconstruction.java", "SegmentProfileService.java",
                  "SegmentArenaControlLog.java", "SegmentBookieControlLog.java", "SegmentControlCapacityException.java",
                  "SegmentDataPhysicalDriver.java", "SegmentPhysicalPrefix.java", "SegmentHotDirectory.java", "SegmentControlCheckpoint.java", "SegmentControlFormat.java",
                  "SegmentControlRotation.java", "SegmentDataFileLock.java", "SegmentDerivedIndex.java",
                  "SegmentDataRecovery.java", "SegmentOwnerLoop.java", "SegmentIoExecutor.java", "SegmentPointRead.java",
                  "SegmentArenaControlDriver.java", "SegmentBookieControlDriver.java", "SegmentProfileDeleteAdoption.java", "SegmentProfileInstallStore.java", "SegmentProfileControlConfig.java", "SegmentProfileReadConfig.java",
                  "SegmentArenaMove.java", "SegmentDuplicateCheck.java",
                  "SegmentEntryInputs.java", "SegmentBufferPool.java", "SegmentBatchFormat.java",
                  "SegmentLocalReadinessStore.java", "SegmentLocalReadinessCodec.java", "SegmentFileIo.java",
                  "SegmentProfileListener.java"])
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentLocalReadinessStoreTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentBookieControlLogTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileInstallStoreTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileInitialSocketClientTest.java")
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/zookeeper/ZooKeeperWatcherBase.java")
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/startup").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/startup").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/control").glob("*.java"))
    inputs.extend((root / "bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/control").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/discover/profile").glob("*.java"))
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/meta").glob("*.java"))
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/TestLedgerUnderreplicationManager.java")
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/replication/ReplicationException.java")
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/replication/ReplicationWorker.java")
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/replication/AutoRecoveryMain.java")
    inputs.extend(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/replication" / name for name in
                  ("Auditor.java", "AuditorElector.java", "AuditorTask.java", "AuditorBookieCheckTask.java", "AuditorReplicasCheckTask.java",
                   "BookieLedgerIndexer.java"))
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/server/service/AutoRecoveryService.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/AutoRecoveryMainTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/AuditorBookieTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/AuditorReplicasCheckTaskTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/BookieLedgerIndexTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/client/BookieDecommissionTest.java")
    inputs.append(root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/BookKeeperAdmin.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/AuditorPeriodicBookieCheckTest.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/AuditorBookieCheckTaskTest.java")
    inputs.append(root / "bookkeeper-common/src/test/java/org/apache/bookkeeper/test/TestStatsProvider.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/replication/TestReplicationWorker.java")
    inputs.append(root / "bookkeeper-server/src/test/java/org/apache/bookkeeper/test/ZooKeeperUtil.java")
    inputs.extend((root / "bookkeeper-server/src/main/java/org/apache/bookkeeper/versioning").glob("*.java"))
    inputs.extend(root / name for name in [
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ProfileBookieClient.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ProfileCreateRequest.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ProfileLedgerClient.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ProfileLedgerHandle.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ProfileLacEvidenceRead.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ProfileRecoveryReadHandle.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingAddOp.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingWriteLacOp.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ClientContext.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ClientInternalConf.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/BookKeeperClientStats.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/BookKeeper.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataBuilder.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataImpl.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataUtils.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/BKException.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/RoundRobinDistributionSchedule.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/DistributionSchedule.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/util/ByteBufList.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/LedgerMetadata.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/DigestType.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/conf/AbstractConfiguration.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/conf/ClientConfiguration.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/meta/zk/ZKMetadataDriverBase.java",
        "bookkeeper-server/src/test/java/org/apache/bookkeeper/meta/TestLedgerMetadataSerDe.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/checksum/DigestManager.java",
        "bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/checksum/MacDigestManager.java"])
    inputs.extend(root / name for name in ["pom.xml", "bookkeeper-common/pom.xml", "bookkeeper-server/pom.xml",
                  "bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/EngineProfile.java",
                  "bookkeeper-server/src/main/java/org/apache/bookkeeper/zookeeper/ZooKeeperClient.java",
                  "bookkeeper-server/src/test/java/org/apache/bookkeeper/test/ZooKeeperUtil.java"])
    design_paths = ["docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md",
                    "docs/rfcs/unified-wal/implementation/startup-readiness/README.md",
                    "docs/rfcs/unified-wal/RFC-0003-segment-storage-allocator.md",
                    "docs/rfcs/unified-wal/RFC-0004-range-recovery-delete.md",
                    "docs/rfcs/unified-wal/implementation/profile-wire/README.md",
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
        "fingerprintPolynomial": 0, "seed": 1, "timeoutSecondsPerConfig": args.timeout_seconds,
        "scope": "Finite Model A historical-repair-copy-two-entry-source-grant-one-inflight-full-copy-before-publication-sticky-unknown-caller-cancel-permit-published-close-resume-no-normal and bounded-historical-repair-membership-atomic-CAS-birth-admission-head-inverse-context-retention-close-authority and independent-close-authority-UIC4-precut-cut-force-terminal-custody-no-normal and fixed-repair-slot-inert-installed-admitted-retained-delete-birth-exact-auth-local-force-recovery-data-no-normal and readonly-fresh-context-recovery-observation-per-source-fenced-bound-missing-budget-reservation-no-add-close and recovery-same-record-context-physical-birth-guards-published-authority-frozen-operation-and-verified-durable-fence-coverage-not-CLOSED and LAC-evidence-A2-A3-URL2-independent-explicit-digest-entire-endpoint-exclusion-max-frontier-verified-coverage-matching-boundary-length-conflict-metadata-snapshot-and-caller-cancel-reservation and LAC-source-entryId-piggyback-LAC-source-length-provenance-URL1-URL2-explicit-separate-whole-ledger-empty-not-prefix-anchor and point-read-permission-complete-stable-fenced-missing-native-get-io-owner-terminal-queued-loan-output-write-reply-binding and original-sdk-two-entry-two-slot-A2-A3-one-two-same-start-completion-gate-replacement-activation-retired-replica-contiguous-callback and replacement-preparation-inactive-install-UIC2-schema-force-owner and replacement-lifecycle-page-birth-metadata-atomic-cut-retained-prefix-current-authority and guarded-standard-metadata-physical-birth-CAS-exact-version-immutable-fields and fixed-initial-sdk-bounded-retry-sticky-unknown-source-coordinate-original-A2-callback-local-stop and normal-client-source-quota-cancel-encoding-write-wire and initial-client-correlation-slot-custody and initial-socket-dispatch and resumable-initial-all-E-availability and initial-READY-lifecycle-birth-CAS-retention and initial-metadata-all-E-gate physical-INSTALL-dispatch PREPARING-reservation-authority and standard-metadata-create and G1 restore dependency atomic admission/complete observed cut/exact CLOSE receipt and closed-reconstruction two-file native claim/READY, ordered publication, action-6 replay, and owned startup and Bookie applied metadata checkpoint and Model C session/prefix/barriers/publication/derived-coverage/index-maintenance/runtime-coverage/tail-recovery/allocation-order/whole-allocation-FREE/healthy-CLOSED-reuse/terminal-index-cleanup/relocation-completion/conditional-MOVE/MOVE-FREE-reuse/native-MOVE-coverage/compaction-group/compaction-selection/compaction-sweep/pooled-compaction-capture/retained-compaction-tail/maintenance-reserve/runtime-control-rotation/control-capacity-recovery/cold-control-startup/arena-space-maintenance/internal-checkpoint-history/retired-sequence-ranges/arena-state-indices/current-copy-facts/readiness-cas/registration-publication/registration-consumer-peer-discovery/registered-connection-actual-drain/worker-service-shutdown-actual-child-join-interrupt/registered-peer-pool-task-lease-reservations-drain/owned-service-bootstrap/local-readiness-publication/authority-read/registration-admission/startup-data-exclusion/profile-normal-dispatch/profile-fence-dispatch/profile-recovery-dispatch/profile-backing-budget slices and composed two-logical-ledger-one-retained-target-tombstone-force-apply-old-I/O-reader-drain-whole-allocation-FREE C+D and composed two-independent-storage-episodes-original-SDK-A2-ACK-live-response-no-recovery-mint A+C and original-SDK-live-storage-actual-drain-then-fenced-CLOSED-logical-delete-shared-FREE A+C+D with all-three original storage episodes and discarded-request actual-return settlement and explicit post-drain-frozen-SDK VIEW",
        "stateConstraint": "None; finite constants in copied configs; no symmetry reduction",
        "tlcViews": {name: match.group(1) for name in selected
                     if (match := re.search(r"(?m)^VIEW\s+(\w+)",
                                            (run / "configs" / (name + ".cfg")).read_text()))},
        "matrix": selected, "selectedConfigPrefixes": args.config_prefix, "productionAuthority": False,
        "deferred": ["full Model A", "Model D", "full A+C beyond bounded two-storage-episode SDK ACK composition", "full C+D beyond bounded two-ledger local FREE composition", "full MOVE/FREE native coverage and reused DATA recovery", "fresh Get/read pin model and full MOVE/FREE coverage recovery", "runtime worker freeze/credits",
                     "full online control rotation", "partial/corrupt framing", "quorum", "liveness"],
    }
    (run / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    results = {}
    for name, expected in selected.items():
        states = run / (name + "-states")
        command = ["java", "-Xmx512m", "-XX:MaxDirectMemorySize=64m", "-XX:+UseParallelGC",
                   "-cp", str(tool), "tlc2.TLC", "-workers", "1", "-fp", "0", "-seed", "1",
                   "-coverage", "1", "-config", str(run / "configs" / (name + ".cfg")),
                   "-metadir", str(states), "ProfileInitialAvailability.tla" if "profile-initial-availability" in name
                   else "ProfileReadyLifecycle.tla" if "profile-ready-lifecycle" in name
                   else "ProfileInitialMetadata.tla" if "profile-initial-metadata" in name
                   else "ProfileReplacementInstall.tla" if "profile-replacement-install" in name
                   else "ProfileReplacementLifecycle.tla" if "profile-replacement-lifecycle" in name
                   else "ProfileMembershipCas.tla" if "profile-membership-cas" in name
                   else "ProfileRecoveryCloseWire.tla" if "profile-recovery-close" in name
                   else "ProfileRepairCopy.tla" if "profile-repair-copy" in name
                   else "ProfileRepairMembership.tla" if "profile-repair-membership" in name
                   else "ProfileRepairGrant.tla" if "profile-repair-grant" in name
                   else "ProfileClosedRepair.tla" if "profile-closed-repair" in name
                   else "ProfileReplicaHealth.tla" if "profile-replica-health" in name
                   else "ProfileMarkerCleanup.tla" if "profile-marker-cleanup" in name
                   else "ProfileReplicationWorker.tla" if "profile-replication-worker" in name
                   else "ProfileEmptyRepair.tla" if "profile-empty-repair" in name
                   else "ProfilePeerDiscovery.tla" if "profile-peer-discovery" in name
                   else "ProfileRegisteredConnection.tla" if "profile-registered-connection" in name
                   else "ProfileWriterClose.tla" if "profile-writer-close" in name
                   else "ProfileLogicalDelete.tla" if "profile-logical-delete" in name
                   else "ProfileDeletePublication.tla" if "profile-delete-publication" in name
                   else "ProfileDeleteRegistration.tla" if "profile-delete-registration" in name
                   else "ProfileDeleteApplied.tla" if "profile-delete-applied" in name
                   else "ProfileDeleteDelivery.tla" if "profile-delete-delivery" in name
                   else "ProfileServiceShutdown.tla" if "profile-service-shutdown" in name
                   else "ProfileRecoveryPeerPool.tla" if "profile-recovery-peer-pool" in name
                   else "ProfileServiceBootstrap.tla" if "profile-service-bootstrap" in name
                   else "ProfileMembershipAudit.tla" if "profile-membership-audit" in name
                   else "ProfileOpenReplicaAudit.tla" if "open-replica-audit" in name
                   else "ProfileReplicaAudit.tla" if "profile-replica-audit" in name
                   else "ProfileWholeRecovery.tla" if "profile-whole-recovery" in name
                   else "ProfileReadonlyOpen.tla" if "profile-readonly" in name
                   else "ProfileTerminalCut.tla" if "profile-terminal-cut" in name
                   else "ProfileRecoveryCompletion.tla" if "profile-recovery-completion" in name
                   else "ProfileRecoverySelection.tla" if "profile-recovery-selection" in name
                   else "ProfileRecoveryPrefix.tla" if "profile-recovery-prefix" in name
                   else "ProfileRecoveryObservation.tla" if "profile-recovery-observation" in name
                   else "ProfileRecoveryPrelude.tla" if "profile-recovery-prelude" in name
                   else "ProfileJournalCompaction.tla" if "profile-journal-compaction" in name
                   else "ProfileRestoreDependencies.tla" if "profile-restore-dependencies" in name
                   else "ClosedReconstructionPublication.tla" if "closed-reconstruction" in name
                   else "ClosedMovedImage.tla" if "closed-moved-image" in name
                   else "ProfileDeleteRetention.tla" if "profile-delete-retention" in name
                   else "ProfileDeleteEffective.tla" if "profile-delete-effective" in name
                   else "ProfileDeleteHandoffAck.tla" if "profile-handoff-ack" in name
                   else "ProfileDeleteAdoption.tla" if "profile-delete-adoption" in name
                   else "ProfileDeleteHandoff.tla" if "profile-delete-handoff" in name
                   else "ProfileDeleteSnapshot.tla" if "profile-delete-snapshot" in name
                   else "ProfilePhysicalReceipt.tla" if "profile-physical-receipt" in name
                   else "ProfilePhysicalDeleteWire.tla" if "profile-physical-wire" in name
                   else "ProfilePhysicalDelete.tla" if "profile-physical-delete" in name
                   else "ProfileWriteLac.tla" if "profile-write-lac" in name
                   else "ProfileLacEvidence.tla" if "profile-lac-evidence" in name
                   else "ProfileLacSource.tla" if "profile-lac-source" in name
                   else "ProfilePointReadDispatch.tla" if "profile-point-read" in name
                   else "ProfileSdkReplacement.tla" if "profile-sdk-replacement" in name
                   else "ProfileSdkAllDeleteFree.tla" if "profile-sdk-all-delete-free" in name
                   else "ProfileSdkDeleteFree.tla" if "profile-sdk-delete-free" in name
                   else "ProfileSdkBridge.tla" if "profile-sdk-bridge" in name
                   else "ProfileNormalClientSource.tla" if "profile-normal-client-source" in name
                   else "ProfileInitialClientCorrelation.tla" if "profile-initial-client" in name
                   else "ProfileInitialSocketDispatch.tla" if "profile-initial-socket" in name
                   else "ProfileInstallDispatch.tla" if "profile-install-dispatch" in name
                   else "ProfileReservationAuthority.tla" if "profile-reservation-authority" in name
                   else "ProfileMetadataCreate.tla" if "profile-metadata-create" in name
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
                   else "StartupRequirements.tla" if "startup-requirements" in name
                   else "ProfileListenerDrain.tla" if "profile-listener-drain" in name
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
                code = process.wait(timeout=args.timeout_seconds)
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
    good = all(r["status"] == ("PASS" if selected[name] is None else "EXPECTED_COUNTEREXAMPLE")
               for name, r in results.items())
    (run / "exit-code").write_text("0\n" if good else "1\n")
    files = [p for p in run.rglob("*") if p.is_file()]
    (run / "checksums.txt").write_text("".join(f"{sha256(p)}  {p.relative_to(run)}\n" for p in sorted(files)))
    return 0 if good else 1


if __name__ == "__main__":
    raise SystemExit(main())
