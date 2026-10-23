--------------------- MODULE ProfileSdkAllDeleteFree ---------------------
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
EXTENDS ProfileSdkDeleteFree
VARIABLES ac3Phase, ac3ControlLength, ac3ControlDurable, ac3ControlApplied, ac3Submitted, ac3Inflight, ac3Written, ac3DataDurable, ac3Successes, ac3RuntimeFault, ac3FaultSeen, ac3FaultDurable, ac3ControlSynced, ac3Validated, ac3DataSynced, ac3CrashCount, ac3PowerCount, ac3IoReturned, thirdSent, thirdLiveReply
thirdVars == <<ac3Phase, ac3ControlLength, ac3ControlDurable, ac3ControlApplied, ac3Submitted, ac3Inflight, ac3Written, ac3DataDurable, ac3Successes, ac3RuntimeFault, ac3FaultSeen, ac3FaultDurable, ac3ControlSynced, ac3Validated, ac3DataSynced, ac3CrashCount, ac3PowerCount, ac3IoReturned>>
oldStorageVars == <<ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight, ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced, ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, ac2Phase, ac2ControlLength, ac2ControlDurable, ac2ControlApplied, ac2Submitted, ac2Inflight, ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault, ac2FaultSeen, ac2FaultDurable, ac2ControlSynced, ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount, acLiveReplies, acSentRequests, ac1IoReturned, ac2IoReturned, acCoordinate>>
allReplicaVars == <<sdkVars, deleteVars, thirdVars, thirdSent, thirdLiveReply>>
Third == INSTANCE SegmentAllocator WITH
    phase <- ac3Phase,
    controlLength <- ac3ControlLength,
    controlDurable <- ac3ControlDurable,
    controlApplied <- ac3ControlApplied,
    submitted <- ac3Submitted,
    inflight <- ac3Inflight,
    written <- ac3Written,
    dataDurable <- ac3DataDurable,
    successes <- ac3Successes,
    runtimeFault <- ac3RuntimeFault,
    faultSeen <- ac3FaultSeen,
    faultDurable <- ac3FaultDurable,
    controlSynced <- ac3ControlSynced,
    validated <- ac3Validated,
    dataSynced <- ac3DataSynced,
    crashCount <- ac3CrashCount,
    powerCount <- ac3PowerCount,
    ioReturned <- ac3IoReturned,
    Streams <- 1, Batches <- 1, Files <- 1, Epochs <- 1, Crashes <- 1, PowerLosses <- 1,
    RequireDataBarrier <- TRUE, RequireControlSync <- TRUE,
    RequireClosedSession <- TRUE, RequirePrefixIoReturn <- TRUE

(* The original SDK and both original storage episodes are inherited unchanged.
   Slot 2 now needs its own original allocator publication and actual task return.
   The shared allocation and both logical-delete instances remain those at slot 0.
   Fenced-CLOSED/metadata/birth are still validated inputs, not a modeled close RPC.
   This live finite episode does not add process/power recovery or a liveness claim. *)
ThirdBatch == <<1, 1>>
ThirdSubmitted == ThirdBatch \in ac3Submitted
ThirdReturned == ThirdBatch \in ac3IoReturned
ThirdPublished == ThirdBatch \in ac3Successes /\ ThirdBatch \in ac3DataDurable /\ ThirdReturned
ThirdLivePublication == ThirdPublished /\ ac3Phase = "live" /\ ac3CrashCount = 0
ThirdDrained == ~thirdSent /\ ac3Inflight = {}
EveryRequestDrained == SdkDrained /\ ThirdDrained

InitAll == InitACD /\ Third!Init /\ thirdSent = TRUE /\ thirdLiveReply = FALSE

(* The inherited action still chooses all original SDK/storage transitions.
   Only its formerly external slot-2 success is now coupled to a local episode. *)
SdkStepAll ==
    /\ SdkStorageStep
    /\ LET replying == s.phase[2] # "durable" /\ s'.phase[2] = "durable"
       IN /\ replying => thirdSent /\
                 (IF ACDMutation = "third-early-reply" THEN ThirdBatch \in ac3Written /\ ac3Phase = "live"
                  ELSE IF ACDMutation = "third-wrong-peer" THEN Sdk!ACPublished(0)
                  ELSE ThirdLivePublication)
          /\ thirdSent' = IF replying THEN FALSE ELSE thirdSent
          /\ thirdLiveReply' = IF replying THEN ThirdLivePublication ELSE thirdLiveReply
    /\ UNCHANGED thirdVars

ThirdPhysicalStep ==
    /\ ~dfCdFirst.closed /\ Third!Next
    /\ (ThirdBatch \in ac3Submitted' \ ac3Submitted => thirdSent)
    /\ ac3CrashCount' = 0 /\ ac3PowerCount' = 0
    /\ UNCHANGED <<sdkVars, deleteVars, thirdSent, thirdLiveReply>>

ThirdKnownNone ==
    /\ ~dfCdFirst.closed /\ thirdSent /\ ~ThirdSubmitted /\ Sdk!Refused(2, "NONE")
    /\ thirdSent' = FALSE
    /\ UNCHANGED <<oldStorageVars, deleteVars, thirdVars, thirdLiveReply>>
ThirdUnknown ==
    /\ ~dfCdFirst.closed /\ thirdSent /\ ThirdSubmitted /\ Sdk!Refused(2, "UNKNOWN")
    /\ UNCHANGED <<oldStorageVars, deleteVars, thirdVars, thirdSent, thirdLiveReply>>
ThirdRetry ==
    /\ ~dfCdFirst.closed /\ Sdk!Retry(2)
    /\ thirdSent' = TRUE
    /\ UNCHANGED <<oldStorageVars, deleteVars, thirdVars, thirdLiveReply>>
ThirdExpire ==
    /\ ~dfCdFirst.closed /\ Sdk!Expire(2)
    /\ UNCHANGED <<oldStorageVars, deleteVars, thirdVars, thirdSent, thirdLiveReply>>
(* A late result discarded by the caller can finish the original wire request,
   but cannot count as SDK success or complete its accepted I/O before return. *)
ThirdDiscardedReply ==
    /\ ~dfCdFirst.closed /\ thirdSent /\ ThirdSubmitted /\ ThirdReturned
    /\ s.phase[2] \in {"backoff", "failed"}
    /\ thirdSent' = FALSE
    /\ UNCHANGED <<sdkVars, deleteVars, thirdVars, thirdLiveReply>>

(* Caller delivery is already terminal. Its original request can settle only
   after accepted I/O has actually returned; this never supplies an SDK ACK. *)
OriginalDiscardedReply(target) ==
    /\ ACDMutation # "omit-discard" /\ ~dfCdFirst.closed /\ s.stopped
    /\ target \in acSentRequests /\ s.phase[target] = "failed"
    /\ Sdk!ACSubmitted(target)
    /\ (Sdk!ACReturned(target) \/ ACDMutation = "discard-before-return")
    /\ acSentRequests' = acSentRequests \ {target}
    /\ UNCHANGED <<s, ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight, ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced, ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, ac2Phase, ac2ControlLength, ac2ControlDurable, ac2ControlApplied, ac2Submitted, ac2Inflight, ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault, ac2FaultSeen, ac2FaultDurable, ac2ControlSynced, ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount, acLiveReplies, ac1IoReturned, ac2IoReturned, acCoordinate, deleteVars, thirdVars, thirdSent, thirdLiveReply>>

(* Native writer close stops the original SDK first, and then waits for every
   original request and accepted task. ACK quorum is neither of those facts. *)
CloseAll ==
    /\ CloseFirst
    /\ (s.stopped \/ ACDMutation = "source-close")
    /\ (ThirdDrained \/ ACDMutation = "third-drain")
    /\ UNCHANGED <<thirdVars, thirdSent, thirdLiveReply>>
DeleteAllStep == DeleteStep /\ UNCHANGED <<thirdVars, thirdSent, thirdLiveReply>>
NextAll == SdkStepAll \/ ThirdPhysicalStep \/ ThirdKnownNone \/ ThirdUnknown \/ ThirdRetry
           \/ ThirdExpire \/ ThirdDiscardedReply \/ CloseAll \/ DeleteAllStep
           \/ (\E target \in {0, 1} : OriginalDiscardedReply(target))
SpecAll == InitAll /\ [][NextAll]_allReplicaVars

(* At this cut every SDK/storage variable is frozen by all successors. Delete
   transitions depend only on deleteVars; the checked SDK/storage properties are
   already true and remain unchanged. Premature-close mutants do not use this cut. *)
AllAcknowledgedHistory ==
    s.user = "success" /\ s.stopped /\ (\A target \in 0..2 : s.phase[target] = "durable")
    /\ thirdLiveReply
AllDiscardedHistory(target) ==
    s.user = "success" /\ s.stopped /\ s.phase[target] = "failed" /\ s.everUnknown[target]
    /\ Sdk!ACSubmitted(target) /\ Sdk!ACReturned(target) /\ Sdk!ACPublished(target)
    /\ target \notin acLiveReplies /\ target \notin acSentRequests
    /\ s.phase[1 - target] = "durable" /\ s.phase[2] = "durable" /\ thirdLiveReply
(* Keep every exact frozen SDK predicate used by the reachability queries. *)
AllLifecycleView ==
    <<IF dfCdFirst.closed /\ EveryRequestDrained /\ s.stopped
      THEN <<"all-replicas-drained", AllAcknowledgedHistory,
             AllDiscardedHistory(0), AllDiscardedHistory(1)>>
      ELSE <<sdkVars, thirdVars, thirdSent, thirdLiveReply>>, deleteVars>>
AllTypeOK == ACDTypeOK /\ Third!TypeOK /\ thirdSent \in BOOLEAN /\ thirdLiveReply \in BOOLEAN
AllThirdStorageContract ==
    Third!ControlHasBarrier /\ Third!PhysicalSuccessIsContiguous /\ Third!KnownErrorRemainsIsolated
    /\ Third!RecoveredDataIsDurable /\ Third!LiveWriterHasOpenAuthority
    /\ Third!PhysicalSuccessWaitsIoReturn /\ Third!ProtectionWaitsActualIoReturn
AllThirdAckHasOwnPublication == s.phase[2] = "durable" => thirdLiveReply /\ ThirdPublished
AllClosedHasEverySourceAndIoDrain ==
    dfCdFirst.closed => EveryRequestDrained /\ s.stopped /\ (\A target \in 0..2 : ~s.source[target])
AllDiscardWaitsForActualReturn ==
    \A target \in {0, 1} : s.stopped /\ s.phase[target] = "failed" /\ Sdk!ACSubmitted(target)
        /\ target \notin acSentRequests => Sdk!ACReturned(target)
AllDiscardCanSettle ==
    \A target \in {0, 1} : ~dfCdFirst.closed /\ s.stopped /\ s.phase[target] = "failed"
        /\ target \in acSentRequests /\ Sdk!ACSubmitted(target) /\ Sdk!ACReturned(target)
        => ENABLED OriginalDiscardedReply(target)
AllCompletedLocalDeleteFree ==
    dfGeneration = 2 /\ dfCd.applied = {1, 2} /\ dfCdFirst.logical /\ dfCdSecond.logical
AllNoCompletedFree == ~(AllCompletedLocalDeleteFree /\ AllAcknowledgedHistory)
AllNoCompletedFreeAfterFirstDiscard == ~(AllCompletedLocalDeleteFree /\ AllDiscardedHistory(0))
AllNoCompletedFreeAfterSecondDiscard == ~(AllCompletedLocalDeleteFree /\ AllDiscardedHistory(1))
=============================================================================
