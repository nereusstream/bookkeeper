-------------------- MODULE ProfileSdkBridge --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS ForgetUnknown, DropRetrySource, ChangeRetryCoordinate, RepeatCallback, PublishClassicClose
VARIABLE s
VARIABLES ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight,
    ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced,
    ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, ac2Phase, ac2ControlLength, ac2ControlDurable,
    ac2ControlApplied, ac2Submitted, ac2Inflight, ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault,
    ac2FaultSeen, ac2FaultDurable, ac2ControlSynced, ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount,
    acLiveReplies, acSentRequests
ACStorageVars == <<ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight,
    ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced,
    ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, ac2Phase, ac2ControlLength, ac2ControlDurable,
    ac2ControlApplied, ac2Submitted, ac2Inflight, ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault,
    ac2FaultSeen, ac2FaultDurable, ac2ControlSynced, ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount,
    acLiveReplies, acSentRequests>>
vars == <<s, ACStorageVars>>
Targets == 0..2
MaxAttempts == 2
(* One admitted original SDK entry, three original slots, A=2. DATA futures
   supply full classified statuses; TLS/write/wire ownership is verified by
   the existing source/dispatch projections and actual socket tests. Time is
   an explicit expiry event, not a real clock. No replacement, recovery,
   metadata lease, fairness, byte/kernel totals, or full A+C+D claim. *)
SdkInit == s = [phase |-> [t \in Targets |-> "running"],
             attempts |-> [t \in Targets |-> 1], source |-> [t \in Targets |-> TRUE],
             unknown |-> [t \in Targets |-> FALSE], everUnknown |-> [t \in Targets |-> FALSE],
             coordinate |-> [t \in Targets |-> 0], callbacks |-> 0,
             user |-> "pending", stopped |-> FALSE, metadata |-> "OPEN"]
Durable(t) == /\ s.phase[t] = "running" /\ ~s.stopped
              /\ s' = [s EXCEPT !.phase[t] = "durable", !.source[t] = FALSE]
Refused(t, result) ==
    /\ s.phase[t] = "running" /\ ~s.stopped /\ result \in {"NONE", "UNKNOWN"}
    /\ LET retrying == s.attempts[t] < MaxAttempts
           observed == s.unknown[t] \/ result = "UNKNOWN"
       IN s' = [s EXCEPT !.phase[t] = IF retrying THEN "backoff" ELSE "failed",
                !.source[t] = retrying /\ ~DropRetrySource,
                !.unknown[t] = IF ForgetUnknown THEN result = "UNKNOWN" ELSE observed,
                !.everUnknown[t] = @ \/ result = "UNKNOWN"]
Retry(t) == /\ s.phase[t] = "backoff" /\ ~s.stopped
            /\ s' = [s EXCEPT !.phase[t] = "running", !.attempts[t] = @ + 1,
                     !.coordinate[t] = IF ChangeRetryCoordinate THEN @ + 1 ELSE @]
Expire(t) == /\ s.phase[t] \in {"running", "backoff"}
             /\ s' = [s EXCEPT !.phase[t] = "failed", !.source[t] = FALSE,
                      !.unknown[t] = @ \/ s.phase[t] = "running",
                      !.everUnknown[t] = @ \/ s.phase[t] = "running"]
Quorum == /\ (s.user = "pending" \/ RepeatCallback) /\ ~s.stopped
          /\ Cardinality({t \in Targets : s.phase[t] = "durable"}) >= 2
          /\ s' = [s EXCEPT !.user = "success", !.callbacks = IF @ < 2 THEN @ + 1 ELSE @]
Stop == /\ ~s.stopped
        /\ s' = [s EXCEPT !.stopped = TRUE,
                 !.user = IF @ = "pending" THEN "failed" ELSE @,
                 !.callbacks = IF s.user = "pending" THEN @ + 1 ELSE @,
                 !.unknown = [t \in Targets |-> s.unknown[t] \/ s.phase[t] = "running"],
                 !.everUnknown = [t \in Targets |-> s.everUnknown[t] \/ s.phase[t] = "running"],
                 !.phase = [t \in Targets |-> IF s.phase[t] = "durable" THEN "durable" ELSE "failed"],
                 !.source = [t \in Targets |-> FALSE],
                 !.metadata = IF PublishClassicClose THEN "CLOSED" ELSE @]
SdkNext == (\E t \in Targets : Durable(t) \/ Retry(t) \/ Expire(t)
                           \/ (\E result \in {"NONE", "UNKNOWN"} : Refused(t, result))) \/ Quorum \/ Stop
TypeOK == /\ s.user \in {"pending", "success", "failed"} /\ s.callbacks \in 0..2
          /\ s.metadata \in {"OPEN", "CLOSED"} /\ s.stopped \in BOOLEAN
          /\ \A t \in Targets : /\ s.phase[t] \in {"running", "backoff", "durable", "failed"}
                                 /\ s.attempts[t] \in 1..MaxAttempts
                                 /\ s.source[t] \in BOOLEAN /\ s.unknown[t] \in BOOLEAN
                                 /\ s.everUnknown[t] \in BOOLEAN /\ s.coordinate[t] \in 0..1
KnownNoneNeverClearsUnknown == \A t \in Targets : s.everUnknown[t] => s.unknown[t]
RetryKeepsSource == \A t \in Targets : s.phase[t] \in {"running", "backoff"} => s.source[t]
RetryKeepsCoordinate == \A t \in Targets : s.coordinate[t] = 0
OneOriginalCallback == s.callbacks <= 1
SuccessRequiresOriginalQuorum == s.user = "success" => Cardinality({t \in Targets : s.phase[t] = "durable"}) >= 2
LocalStopNeverPublishesClosed == s.metadata = "OPEN"


(* A+C composition: two independent managed storage episodes produce the
   local results for original SDK slots 0 and 1. Slot 2 remains a validated
   remote response input; no third storage model is inferred. One entry,
   one batch per episode, one crash in the pair, no fence/replacement or
   byte/CRC model. Sent requests can enter storage after caller timeout; known NONE ends
   that attempt. Accepted I/O runs independently of caller cancellation.
   Recovery synchronization cannot mint an original live DATA response. *)
ACMutation == "none"
ACEarlyReply == "early-reply"
ACWrongPeer == "wrong-peer"
ACRecoveryReply == "recovery-reply"
ACCancelIo == "cancel-io"
ACBlockLateSubmit == "block-late-submit"
ACFirst == INSTANCE SegmentAllocator WITH
    phase <- ac1Phase,
    controlLength <- ac1ControlLength,
    controlDurable <- ac1ControlDurable,
    controlApplied <- ac1ControlApplied,
    submitted <- ac1Submitted,
    inflight <- ac1Inflight,
    written <- ac1Written,
    dataDurable <- ac1DataDurable,
    successes <- ac1Successes,
    runtimeFault <- ac1RuntimeFault,
    faultSeen <- ac1FaultSeen,
    faultDurable <- ac1FaultDurable,
    controlSynced <- ac1ControlSynced,
    validated <- ac1Validated,
    dataSynced <- ac1DataSynced,
    crashCount <- ac1CrashCount,
    powerCount <- ac1PowerCount,
    Streams <- 1,
    Batches <- 1,
    Files <- 1,
    Epochs <- 1,
    Crashes <- 1,
    PowerLosses <- 1,
    RequireDataBarrier <- TRUE,
    RequireControlSync <- TRUE,
    RequireClosedSession <- TRUE
ACSecond == INSTANCE SegmentAllocator WITH
    phase <- ac2Phase,
    controlLength <- ac2ControlLength,
    controlDurable <- ac2ControlDurable,
    controlApplied <- ac2ControlApplied,
    submitted <- ac2Submitted,
    inflight <- ac2Inflight,
    written <- ac2Written,
    dataDurable <- ac2DataDurable,
    successes <- ac2Successes,
    runtimeFault <- ac2RuntimeFault,
    faultSeen <- ac2FaultSeen,
    faultDurable <- ac2FaultDurable,
    controlSynced <- ac2ControlSynced,
    validated <- ac2Validated,
    dataSynced <- ac2DataSynced,
    crashCount <- ac2CrashCount,
    powerCount <- ac2PowerCount,
    Streams <- 1,
    Batches <- 1,
    Files <- 1,
    Epochs <- 1,
    Crashes <- 1,
    PowerLosses <- 1,
    RequireDataBarrier <- TRUE,
    RequireControlSync <- TRUE,
    RequireClosedSession <- TRUE
ACFirstVars == <<ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Inflight,
    ac1Written, ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced,
    ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount>>
ACSecondVars == <<ac2Phase, ac2ControlLength, ac2ControlDurable, ac2ControlApplied, ac2Submitted, ac2Inflight,
    ac2Written, ac2DataDurable, ac2Successes, ac2RuntimeFault, ac2FaultSeen, ac2FaultDurable, ac2ControlSynced,
    ac2Validated, ac2DataSynced, ac2CrashCount, ac2PowerCount>>

Init == SdkInit /\ ACFirst!Init /\ ACSecond!Init /\ acLiveReplies = {} /\ acSentRequests = {0, 1}
Next == SdkNext /\ UNCHANGED ACStorageVars
Spec == Init /\ [][Next]_vars

ACBatch == <<1, 1>>
ACInit == Init
ACPhysicalFirst ==
    /\ ACFirst!Next
    /\ (ac1Submitted' # ac1Submitted => 0 \in acSentRequests
        /\ (ACMutation # "block-late-submit" \/ s.phase[0] = "running"))
    /\ ac1CrashCount' + ac2CrashCount <= 1
    /\ UNCHANGED <<s, ACSecondVars, acLiveReplies, acSentRequests>>
ACPhysicalSecond ==
    /\ ACSecond!Next
    /\ (ac2Submitted' # ac2Submitted => 1 \in acSentRequests
        /\ (ACMutation # "block-late-submit" \/ s.phase[1] = "running"))
    /\ ac2CrashCount' + ac1CrashCount <= 1
    /\ UNCHANGED <<s, ACFirstVars, acLiveReplies, acSentRequests>>
ACPublished(target) == ACBatch \in IF target = 0 THEN ac1Successes ELSE ac2Successes
ACWritten(target) == ACBatch \in IF target = 0 THEN ac1Written ELSE ac2Written
ACDurable(target) == ACBatch \in IF target = 0 THEN ac1DataDurable ELSE ac2DataDurable
ACLive(target) == IF target = 0 THEN ac1Phase = "live" /\ ac1CrashCount = 0
                 ELSE ac2Phase = "live" /\ ac2CrashCount = 0
ACRecovered(target) == IF target = 0 THEN ac1Phase = "ready" ELSE ac2Phase = "ready"
ACSubmitted(target) == ACBatch \in IF target = 0 THEN ac1Submitted ELSE ac2Submitted
ACReply(target) ==
    /\ IF ACMutation = "early-reply" THEN ACWritten(target) /\ ACLive(target)
       ELSE IF ACMutation = "wrong-peer" THEN ACPublished(0) /\ ACLive(0)
       ELSE IF ACMutation = "recovery-reply" THEN
                    (ACPublished(target) /\ ACLive(target)) \/ ACRecovered(target)
       ELSE ACPublished(target) /\ ACLive(target)
    /\ Durable(target)
    /\ acLiveReplies' = IF ACLive(target) /\ ACPublished(target)
                        THEN acLiveReplies \cup {target} ELSE acLiveReplies
    /\ acSentRequests' = acSentRequests \ {target}
    /\ UNCHANGED <<ACFirstVars, ACSecondVars>>
ACRemoteReply == /\ Durable(2) /\ UNCHANGED ACStorageVars
ACRetry(target) ==
    /\ Retry(target)
    /\ acSentRequests' = acSentRequests \cup {target}
    /\ UNCHANGED <<ACFirstVars, ACSecondVars, acLiveReplies>>
ACKnownNone(target) ==
    /\ Refused(target, "NONE") /\ ~ACSubmitted(target) /\ target \in acSentRequests
    /\ acSentRequests' = acSentRequests \ {target}
    /\ UNCHANGED <<ACFirstVars, ACSecondVars, acLiveReplies>>
ACSdkStep ==
    /\ Quorum \/ Stop
       \/ (\E target \in {0, 1} : Expire(target)
            \/ (Refused(target, "UNKNOWN") /\ ACSubmitted(target)))
    /\ UNCHANGED ACStorageVars
ACAbandonFirst ==
    /\ ACMutation = "cancel-io" /\ s.phase[0] # "running"
    /\ ac1Phase = "live" /\ ACBatch \in ac1Inflight
    /\ ac1Inflight' = {}
    /\ UNCHANGED <<ac1Phase, ac1ControlLength, ac1ControlDurable, ac1ControlApplied, ac1Submitted, ac1Written,
                  ac1DataDurable, ac1Successes, ac1RuntimeFault, ac1FaultSeen, ac1FaultDurable, ac1ControlSynced,
                  ac1Validated, ac1DataSynced, ac1CrashCount, ac1PowerCount, s, ACSecondVars, acLiveReplies, acSentRequests>>

ACNext == ACPhysicalFirst \/ ACPhysicalSecond \/ ACRemoteReply \/ ACSdkStep \/ ACAbandonFirst
          \/ (\E target \in {0, 1} : ACReply(target) \/ ACRetry(target) \/ ACKnownNone(target))
ACSubmitFirst == ACPhysicalFirst /\ ACFirst!Submit(ACBatch)
ACSubmitSecond == ACPhysicalSecond /\ ACSecond!Submit(ACBatch)
ACSpec == ACInit /\ [][ACNext]_vars
ACTypeOK == /\ TypeOK /\ ACFirst!TypeOK /\ ACSecond!TypeOK /\ acLiveReplies \subseteq {0, 1}
            /\ acSentRequests \subseteq {0, 1}
ACRetryRemainsEnabled ==
    \A target \in {0, 1} : s.phase[target] = "backoff" /\ ~s.stopped => ENABLED ACRetry(target)
ACSentCanReachStorageAfterWait ==
    /\ (0 \in acSentRequests /\ s.phase[0] = "failed" /\ ac1Phase = "live"
        /\ ~ACSubmitted(0) /\ ac1RuntimeFault = {} /\ ac1FaultDurable = {} => ENABLED ACSubmitFirst)
    /\ (1 \in acSentRequests /\ s.phase[1] = "failed" /\ ac2Phase = "live"
        /\ ~ACSubmitted(1) /\ ac2RuntimeFault = {} /\ ac2FaultDurable = {} => ENABLED ACSubmitSecond)
ACOriginalSdkContract == KnownNoneNeverClearsUnknown /\ RetryKeepsSource /\ RetryKeepsCoordinate
                         /\ OneOriginalCallback /\ SuccessRequiresOriginalQuorum /\ LocalStopNeverPublishesClosed
ACOriginalStorageContract ==
    ACFirst!ControlHasBarrier /\ ACSecond!ControlHasBarrier
    /\ ACFirst!PhysicalSuccessIsContiguous /\ ACSecond!PhysicalSuccessIsContiguous
    /\ ACFirst!KnownErrorRemainsIsolated /\ ACSecond!KnownErrorRemainsIsolated
    /\ ACFirst!RecoveredDataIsDurable /\ ACSecond!RecoveredDataIsDurable
    /\ ACFirst!LiveWriterHasOpenAuthority /\ ACSecond!LiveWriterHasOpenAuthority
ACAcknowledgmentHasOwnLocalPublication ==
    \A target \in {0, 1} : s.phase[target] = "durable" => ACPublished(target) /\ ACDurable(target)
ACAcknowledgmentCameFromLiveEpisode ==
    \A target \in {0, 1} : s.phase[target] = "durable" => target \in acLiveReplies
ACSuccessHasPhysicalQuorum == s.user = "success" =>
    Cardinality({target \in {0, 1} : s.phase[target] = "durable" /\ ACDurable(target)}
                \cup (IF s.phase[2] = "durable" THEN {2} ELSE {})) >= 2
ACCancellationKeepsStorageProtection ==
    /\ (ACBatch \in ac1Submitted /\ ac1CrashCount = 0 =>
           ACBatch \in ac1Inflight \/ ACBatch \in ac1DataDurable \/ 1 \in ac1RuntimeFault)
    /\ (ACBatch \in ac2Submitted /\ ac2CrashCount = 0 =>
           ACBatch \in ac2Inflight \/ ACBatch \in ac2DataDurable \/ 1 \in ac2RuntimeFault)

==============================================================================
