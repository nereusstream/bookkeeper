----------------------- MODULE ProfileSdkDeleteFree -----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License. You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. *)
EXTENDS Naturals, FiniteSets
VARIABLES s, ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight,
    ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced,
    ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, ac2Phase, ac2ControlLength, ac2ControlDurable,
    ac2ControlApplied, ac2Submitted, ac2Inflight, ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault,
    ac2FaultSeen, ac2FaultDurable, ac2ControlSynced, ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount,
    acLiveReplies, acSentRequests, ac1IoReturned, ac2IoReturned, acCoordinate
Sdk == INSTANCE ProfileSdkBridge WITH
    ForgetUnknown <- FALSE, DropRetrySource <- FALSE, ChangeRetryCoordinate <- FALSE,
    RepeatCallback <- FALSE, PublishClassicClose <- FALSE,
    s <- s,
    ac1Phase <- ac1Phase,
    ac1ControlLength <- ac1ControlLength,
    ac1ControlDurable <- ac1ControlDurable,
    ac1ControlApplied <- ac1ControlApplied,
    ac1Submitted <- ac1Submitted,
    ac1Inflight <- ac1Inflight,
    ac1Written <- ac1Written,
    ac1DataDurable <- ac1DataDurable,
    ac1Successes <- ac1Successes,
    ac1RuntimeFault <- ac1RuntimeFault,
    ac1FaultSeen <- ac1FaultSeen,
    ac1FaultDurable <- ac1FaultDurable,
    ac1ControlSynced <- ac1ControlSynced,
    ac1Validated <- ac1Validated,
    ac1DataSynced <- ac1DataSynced,
    ac1CrashCount <- ac1CrashCount,
    ac1PowerCount <- ac1PowerCount,
    ac2Phase <- ac2Phase,
    ac2ControlLength <- ac2ControlLength,
    ac2ControlDurable <- ac2ControlDurable,
    ac2ControlApplied <- ac2ControlApplied,
    ac2Submitted <- ac2Submitted,
    ac2Inflight <- ac2Inflight,
    ac2Written <- ac2Written,
    ac2DataDurable <- ac2DataDurable,
    ac2Successes <- ac2Successes,
    ac2RuntimeFault <- ac2RuntimeFault,
    ac2FaultSeen <- ac2FaultSeen,
    ac2FaultDurable <- ac2FaultDurable,
    ac2ControlSynced <- ac2ControlSynced,
    ac2Validated <- ac2Validated,
    ac2DataSynced <- ac2DataSynced,
    ac2CrashCount <- ac2CrashCount,
    ac2PowerCount <- ac2PowerCount,
    acLiveReplies <- acLiveReplies,
    acSentRequests <- acSentRequests,
    ac1IoReturned <- ac1IoReturned,
    ac2IoReturned <- ac2IoReturned,
    acCoordinate <- acCoordinate
CONSTANT ACDMutation
VARIABLES dfAlive, dfActualIo, dfVisibleIo, dfReaders, dfGate, dfPhase, dfHealthy, dfCrashed, dfVerifiedDead,
    dfProofIo, dfGeneration, dfForceSucceeded, dfRetired, dfWaiting, dfCd, dfCdFirst, dfCdSecond
deleteVars == <<dfAlive, dfActualIo, dfVisibleIo, dfReaders, dfGate, dfPhase, dfHealthy, dfCrashed,
    dfVerifiedDead, dfProofIo, dfGeneration, dfForceSucceeded, dfRetired, dfWaiting, dfCd, dfCdFirst,
    dfCdSecond>>
sdkVars == <<s, ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight,
    ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced,
    ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, ac2Phase, ac2ControlLength, ac2ControlDurable,
    ac2ControlApplied, ac2Submitted, ac2Inflight, ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault,
    ac2FaultSeen, ac2FaultDurable, ac2ControlSynced, ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount,
    acLiveReplies, acSentRequests, ac1IoReturned, ac2IoReturned, acCoordinate>>
freeState == <<dfAlive, dfActualIo, dfVisibleIo, dfReaders, dfGate, dfPhase, dfHealthy, dfCrashed,
              dfVerifiedDead, dfProofIo, dfGeneration, dfForceSucceeded, dfRetired, dfWaiting>>
allVars == <<sdkVars, deleteVars>>

(* One SDK entry at two original local storage episodes and one validated remote slot,
   plus two original logical-delete instances sharing one allocation at local slot 0.
   The neighbour is already fenced-CLOSED. Deletion starts after the first close;
   the SDK episode is then frozen. There is no OPEN/delete/replacement concurrency claim.
   The first ledger receives a CLOSED/fenced
   input only after successful ACK and actual original request drain. Native fence,
   metadata commit/birth and local tombstone force remain validated Java inputs.
   This finite live SDK episode excludes its process/power failures, replacement, restart;
   the original FREE allocator still permits its one crash. It excludes
   clocks, fairness and complete A+C+D/Gate claims; original standalone models retain those cuts. *)
DeleteFree == INSTANCE WholeAllocationFree WITH
    SkipLiveRecord <- FALSE, ReleasePendingGate <- FALSE,
    TrustRebootCounts <- FALSE, ApplyBeforeForce <- FALSE,
    alive <- dfAlive,
    actualIo <- dfActualIo,
    visibleIo <- dfVisibleIo,
    readers <- dfReaders,
    gate <- dfGate,
    phase <- dfPhase,
    healthy <- dfHealthy,
    crashed <- dfCrashed,
    verifiedDead <- dfVerifiedDead,
    proofIo <- dfProofIo,
    generation <- dfGeneration,
    forceSucceeded <- dfForceSucceeded,
    retired <- dfRetired,
    waiting <- dfWaiting,
    cd <- dfCd,
    cdFirst <- dfCdFirst,
    cdSecond <- dfCdSecond

InitACD == /\ Sdk!ACInit /\ DeleteFree!FreeInit
           /\ DeleteFree!CDFirst!Init /\ DeleteFree!CDSecond!Init
           /\ ~dfCdFirst.closed /\ ~dfCdFirst.fenced /\ ~dfCdFirst.offline
           /\ dfCdSecond.closed /\ dfCdSecond.fenced /\ ~dfCdSecond.offline
           /\ dfCd = [delivered |-> {}, forced |-> {}, applied |-> {}]

(* Original Submit/actual-return and allocation I/O ownership are the same transition. *)
SdkStorageStep ==
    /\ ~dfCdFirst.closed
    /\ Sdk!ACNext
    /\ ac1CrashCount' = 0 /\ ac2CrashCount' = 0
    /\ ac1PowerCount' = 0 /\ ac2PowerCount' = 0
    /\ IF Cardinality(ac1Inflight') > Cardinality(ac1Inflight)
       THEN DeleteFree!StartIo
       ELSE IF Cardinality(ac1Inflight') < Cardinality(ac1Inflight)
            THEN DeleteFree!FinishIo
            ELSE UNCHANGED freeState
    /\ UNCHANGED <<dfCd, dfCdFirst, dfCdSecond>>

SdkDrained == acSentRequests = {} /\ ac1Inflight = {} /\ ac2Inflight = {}
CloseFirst ==
    /\ ~dfCdFirst.closed /\ s.user = "success"
    /\ (SdkDrained \/ ACDMutation = "close-before-drain")
    /\ dfCdFirst' = [dfCdFirst EXCEPT !.closed = TRUE, !.fenced = TRUE]
    /\ UNCHANGED <<sdkVars, freeState, dfCd, dfCdSecond>>

(* Standalone StartIo/FinishIo cannot invent or complete the SDK's actual task. *)
DeleteStep ==
    /\ dfCdFirst.closed
    /\ (((DeleteFree!Pin \/ DeleteFree!Unpin \/ DeleteFree!Submit \/ DeleteFree!Force
        \/ DeleteFree!ForceUnknown \/ DeleteFree!Apply \/ DeleteFree!OwnerResolve
        \/ DeleteFree!Duplicate \/ DeleteFree!CancelWait \/ DeleteFree!EarlyClose \/ DeleteFree!Crash)
           /\ UNCHANGED <<dfCd, dfCdFirst, dfCdSecond>>)
        \/ DeleteFree!CDPrepare \/ DeleteFree!CDLogicalStep
        \/ (\E ledger \in {1, 2} : DeleteFree!CDDeliver(ledger)
            \/ DeleteFree!CDForceTombstone(ledger) \/ DeleteFree!CDFailTombstone(ledger)
            \/ DeleteFree!CDApplyTombstone(ledger)))
    /\ UNCHANGED sdkVars

AbandonIo ==
    /\ ACDMutation = "cancel-io" /\ s.stopped /\ dfActualIo = 1
    /\ dfActualIo' = 0 /\ dfVisibleIo' = 0
    /\ UNCHANGED <<sdkVars, dfAlive, dfReaders, dfGate, dfPhase, dfHealthy, dfCrashed,
                   dfVerifiedDead, dfProofIo, dfGeneration, dfForceSucceeded,
                   dfRetired, dfWaiting, dfCd, dfCdFirst, dfCdSecond>>

QuorumDelete ==
    /\ ACDMutation = "quorum-delete" /\ s.user = "success"
    /\ ~dfCdFirst.closed /\ 1 \notin dfCd.delivered
    /\ dfCd' = [dfCd EXCEPT !.delivered = @ \cup {1}]
    /\ UNCHANGED <<sdkVars, freeState, dfCdFirst, dfCdSecond>>

NextACD == SdkStorageStep \/ CloseFirst \/ DeleteStep \/ AbandonIo \/ QuorumDelete
SpecACD == InitACD /\ [][NextACD]_allVars

(* Once closed AND drained, SDK variables are frozen in every successor.
   Every reachable member of that class satisfies the original SDK/storage
   invariants before closure. Delete transitions then depend only on deleteVars;
   ac1Inflight/ac2Inflight/sent are all empty. This is a quotient of frozen
   histories, not a state constraint. A premature-close mutation is not merged. *)
LifecycleView == <<IF dfCdFirst.closed /\ SdkDrained THEN "drained" ELSE sdkVars, deleteVars>>

ACDTypeOK == Sdk!ACTypeOK /\ DeleteFree!CDTypeOK
ACDOriginalSdkStorage == Sdk!ACOriginalSdkContract /\ Sdk!ACOriginalStorageContract
                        /\ Sdk!ACAcknowledgmentHasOwnLocalPublication
                        /\ Sdk!ACAcknowledgmentCameFromLiveEpisode /\ Sdk!ACSuccessHasReturnedQuorum
ACDOriginalDeletion == DeleteFree!CDLogicalKeepsOriginalContract
                       /\ DeleteFree!CDDeliveryHasLogicalObligation
                       /\ DeleteFree!CDLocalEffectHasForce
                       /\ DeleteFree!CDRemainingLedgerStaysAllocated
                       /\ DeleteFree!CDFreeHasEveryLogicalAndLocalEffect
                       /\ DeleteFree!EveryOccupantWasTerminal
                       /\ DeleteFree!SubmittedFreeRetainsProtection
                       /\ DeleteFree!RebootCountsDoNotProveOldIoTerminal
                       /\ DeleteFree!FreeAuthorityHasBarrier
ACDActualIoMatchesSdk == dfActualIo = Cardinality(ac1Inflight)
ACDClosedHasOriginalDrain == dfCdFirst.closed => SdkDrained /\ s.user = "success"
ACDLocalDeleteHasFencedClose == 1 \in dfCd.delivered => dfCdFirst.closed /\ dfCdFirst.fenced
(* Negated reachability query, enabled only by the witness configuration.
   Its expected counterexample proves the full successful lifecycle is reachable. *)
ACDNoCompletedFree == ~(dfGeneration = 2 /\ dfCd.applied = {1, 2}
                        /\ dfCdFirst.logical /\ dfCdSecond.logical /\ s.user = "success")
=============================================================================
