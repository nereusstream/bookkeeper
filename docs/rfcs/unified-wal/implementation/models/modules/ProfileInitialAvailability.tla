-------------------- MODULE ProfileInitialAvailability --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS AckOnly, IgnoreBinding, UnknownAsAck, HistoricalAsActive, RebuildMissing,
          IgnoreFinalDelete, IgnoreFinalMetadata
VARIABLE s
vars == <<s>>
Targets == {1, 2, 3}
(* One recovery round. Persistent phase: PREPARING1, candidate2, READY3, DELETE4.
   Target kind: current active1, known refusal2, UNKNOWN3, wrong binding4,
   historical durable activation now fenced5. Existing candidate/READY proves
   metadata existed: a missing required record must not be recreated.
   Target correlation/current state, strict bytes, birth checks and local I/O
   are inputs. Final metadata and root are snapshots, never a continuing lease.
   No actual writer/control wire, full replacement or physical delete/GC. *)
Init == \E phase \in 1..4, present \in BOOLEAN, kinds \in [Targets -> 1..5] :
    s = [head |-> phase, knownReference |-> phase \in 2..4, metadata |-> present,
         kinds |-> kinds, round |-> 0, sent |-> {}, received |-> {}, accepted |-> {},
         rebuilt |-> FALSE, available |-> FALSE, metadataChecked |-> FALSE,
         metadataFact |-> FALSE, checkedHead |-> phase, deleted |-> phase = 4,
         metadataRemoved |-> FALSE]
Recover == /\ s.round = 0
           /\ LET blocked == s.head = 4 \/ (s.knownReference /\ ~s.metadata /\ ~RebuildMissing)
              IN s' = [s EXCEPT !.round = IF blocked THEN 4 ELSE 1,
                       !.rebuilt = ~blocked /\ ~s.metadata,
                       !.metadata = IF blocked THEN s.metadata ELSE TRUE,
                       !.head = IF blocked THEN s.head ELSE 3]
Dispatch == /\ s.round = 1
            /\ s' = [s EXCEPT !.round = 2, !.sent = Targets]
Receive == \E target \in s.sent \ s.received :
    /\ s.round = 2
    /\ LET kind == s.kinds[target]
           success == kind = 1 \/ (kind = 3 /\ UnknownAsAck) \/ (kind = 4 /\ IgnoreBinding)
                      \/ (kind = 5 /\ HistoricalAsActive)
       IN s' = [s EXCEPT !.received = s.received \cup {target},
                !.accepted = IF success THEN s.accepted \cup {target} ELSE s.accepted]
CheckMetadata == /\ s.round = 2 /\ ~s.metadataChecked
                 /\ (s.received = Targets \/ (AckOnly /\ Cardinality(s.received) >= 2))
                 /\ s' = [s EXCEPT !.metadataChecked = TRUE, !.metadataFact = s.metadata]
Finish == /\ s.round = 2 /\ s.metadataChecked
          /\ s' = [s EXCEPT !.round = 4, !.checkedHead = s.head,
                   !.available = (s.accepted = Targets \/ (AckOnly /\ Cardinality(s.accepted) >= 2))
                                 /\ (s.metadataFact \/ IgnoreFinalMetadata)
                                 /\ (s.head = 3 \/ IgnoreFinalDelete)]
DeleteCut == /\ ~s.deleted /\ s.round \in 1..2
             /\ s' = [s EXCEPT !.deleted = TRUE, !.head = 4]
RemoveMetadata == /\ ~s.metadataRemoved /\ s.metadata /\ s.round \in 1..2
                  /\ s' = [s EXCEPT !.metadataRemoved = TRUE, !.metadata = FALSE]
Next == Recover \/ Dispatch \/ Receive \/ CheckMetadata \/ Finish \/ DeleteCut \/ RemoveMetadata
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.head \in 1..4 /\ s.checkedHead \in 1..4 /\ s.round \in 0..4
          /\ s.kinds \in [Targets -> 1..5] /\ s.sent \subseteq Targets /\ s.received \subseteq s.sent
          /\ s.accepted \subseteq s.received /\ s.metadata \in BOOLEAN /\ s.knownReference \in BOOLEAN
          /\ s.rebuilt \in BOOLEAN /\ s.available \in BOOLEAN /\ s.metadataChecked \in BOOLEAN
          /\ s.metadataFact \in BOOLEAN /\ s.deleted \in BOOLEAN /\ s.metadataRemoved \in BOOLEAN
RequiredMetadataIsNeverRebuilt == s.knownReference => ~s.rebuilt
AvailabilityWaitsForAllE == s.available => s.received = Targets
AvailabilityHasCurrentBoundResults == s.available => \A target \in Targets : s.kinds[target] # 4
AvailabilityDoesNotCountUnknown == s.available => \A target \in Targets : s.kinds[target] # 3
AvailabilityDoesNotCountHistoricalFence == s.available => \A target \in Targets : s.kinds[target] # 5
AvailabilityHasMatchingMetadataFact == s.available => s.metadataFact
AvailabilityObservesReadyNotDelete == s.available => s.checkedHead = 3

(* One SDK, one ledger/origin and at most two exact attempts. "native" is an opaque
   same-store APPLIED birth input, never a fact reconstructed from matching READY.
   Retained exact-request custody can retry before handoff; a different request for
   this ledger is rejected. The private create/init/config chain is not caller-owned.
   Actual work, writer and lease/pool terminals are inputs. Availability reuses the
   all-E actions above; kinds 1/2 suffice here, with the other target faults covered
   by the original Spec. All modeled ledger records are OPEN: no close/recovery,
   external delete, fairness, liveness, native ZK implementation or full A/C/D proof. *)
CreateSDKInit == \E existing \in BOOLEAN, kinds \in [Targets -> 1..2] :
    s = [head |-> IF existing THEN 3 ELSE 1, knownReference |-> existing, metadata |-> existing,
         kinds |-> kinds, round |-> 0, sent |-> {}, received |-> {}, accepted |-> {},
         rebuilt |-> FALSE, available |-> FALSE, metadataChecked |-> FALSE,
         metadataFact |-> FALSE, checkedHead |-> IF existing THEN 3 ELSE 1, deleted |-> FALSE,
         metadataRemoved |-> FALSE, createOrigin |-> IF existing THEN "existing" ELSE "none",
         createProof |-> "none", createClaim |-> FALSE, createConsumed |-> FALSE,
         createAttempt |-> 0, createStage |-> "idle", createWork |-> "none", createWriter |-> "none",
         createLease |-> "none", createPool |-> "open", createDependencies |-> "held",
         createClosing |-> FALSE, createStopped |-> FALSE, createCallerDone |-> FALSE,
         createDelivered |-> FALSE, createHandoffs |-> 0, createAdmission |-> "none"]
CreateSDKStart == /\ s.createStage = "idle" /\ ~s.createClosing /\ s.createAttempt < 2
                  /\ s' = [s EXCEPT !.createAttempt = @ + 1, !.createLease = "held", !.createWork = "pending",
                                    !.createStage = IF s.createClaim THEN "initializing" ELSE "claiming",
                                    !.createAdmission = "exact", !.createStopped = FALSE,
                                    !.createCallerDone = FALSE, !.createDelivered = FALSE, !.createWriter = "none",
                                    !.round = 0, !.knownReference = s.head \in 2..4,
                                    !.sent = {}, !.received = {}, !.accepted = {}, !.rebuilt = FALSE,
                                    !.available = FALSE, !.metadataChecked = FALSE, !.metadataFact = FALSE,
                                    !.checkedHead = s.head]
CreateSDKRejectMismatch == /\ s.createStage = "idle" /\ ~s.createClosing /\ s.createClaim
                           /\ s' = [s EXCEPT !.createAdmission = "mismatch-rejected"]
CreateSDKNativeApplied ==
    /\ s.createStage = "claiming" /\ s.createOrigin = "none"
    /\ LET stop == s.createStopped \/ s.createClosing \/ s.createCallerDone
       IN s' = [s EXCEPT !.createOrigin = "native", !.createProof = "native",
                         !.createClaim = ~s.createClosing,
                         !.createStage = IF stop THEN "failed" ELSE "initializing",
                         !.createWork = IF stop THEN "terminal" ELSE @]
CreateSDKUncertainBirth == /\ s.createStage = "claiming" /\ s.createOrigin = "none"
                          /\ s' = [s EXCEPT !.createOrigin = "existing", !.createStage = "failed",
                                            !.createWork = "terminal"]
CreateSDKObserveExisting(inferClaim) ==
    /\ s.createStage = "claiming" /\ s.createOrigin # "none"
    /\ LET infer == inferClaim /\ ~s.createClosing /\ ~s.createStopped /\ ~s.createCallerDone
       IN s' = [s EXCEPT !.createClaim = infer,
                         !.createProof = IF infer THEN "observed" ELSE @,
                         !.createStage = IF infer THEN "initializing" ELSE "failed",
                         !.createWork = IF infer THEN @ ELSE "terminal"]
CreateSDKAvailabilityStep == /\ s.createStage = "initializing"
                             /\ (Recover \/ Dispatch \/ Receive \/ CheckMetadata \/ Finish)
CreateSDKFinishInit ==
    /\ s.createStage = "initializing" /\ s.round = 4
    /\ LET configure == s.available /\ ~s.createStopped /\ ~s.createClosing /\ ~s.createCallerDone
       IN s' = [s EXCEPT !.createStage = IF configure THEN "configuring" ELSE "failed",
                         !.createWriter = IF configure THEN "local" ELSE @,
                         !.createWork = IF configure THEN @ ELSE "terminal"]
CreateSDKFinishConfig ==
    /\ s.createStage = "configuring"
    /\ LET ready == ~s.createStopped /\ ~s.createClosing /\ ~s.createCallerDone /\ s.createWriter = "local"
       IN s' = [s EXCEPT !.createWork = "terminal", !.createStage = IF ready THEN "ready" ELSE "failed",
                         !.createWriter = IF ready \/ @ = "terminal" THEN @ ELSE "closing"]
CreateSDKHandoff(retainClaim) ==
    /\ s.createStage = "ready" /\ ~s.createStopped /\ ~s.createClosing /\ ~s.createCallerDone
    /\ s.createHandoffs < 2
    /\ s' = [s EXCEPT !.createStage = "handoff", !.createWriter = "live",
                      !.createConsumed = TRUE, !.createClaim = retainClaim, !.createHandoffs = @ + 1,
                      !.createAdmission = "handoff"]
CreateSDKNoHandoff == /\ s.createStage = "ready"
                      /\ (s.createStopped \/ s.createClosing \/ s.createCallerDone)
                      /\ s' = [s EXCEPT !.createStage = "failed",
                                        !.createWriter = IF @ = "terminal" THEN @ ELSE "closing"]
CreateSDKDeliver == /\ s.createStage = "handoff" /\ ~s.createCallerDone
                    /\ s' = [s EXCEPT !.createStage = "delivered", !.createCallerDone = TRUE,
                                      !.createDelivered = TRUE]
CreateSDKLoseDelivery == /\ s.createStage = "handoff" /\ s.createCallerDone
                         /\ s' = [s EXCEPT !.createStage = "failed",
                                           !.createWriter = IF @ = "terminal" THEN @ ELSE "closing"]
CreateSDKCancelCaller ==
    /\ s.createStage \notin {"idle", "handoff", "delivered"} /\ ~s.createCallerDone
    /\ s' = [s EXCEPT !.createCallerDone = TRUE, !.createStopped = TRUE,
                      !.createWriter = IF @ \in {"local", "live"} THEN "closing" ELSE @]
CreateSDKCompleteCaller == /\ s.createStage # "idle" /\ ~s.createCallerDone
                           /\ s' = [s EXCEPT !.createCallerDone = TRUE]
CreateSDKGlobalStop == /\ ~s.createClosing
                       /\ s' = [s EXCEPT !.createClosing = TRUE, !.createStopped = TRUE,
                                         !.createClaim = FALSE, !.createAdmission = "stopped",
                                         !.createWriter = IF @ \in {"local", "live"} THEN "closing" ELSE @]
CreateSDKStopDeliveredWriter == /\ s.createDelivered /\ s.createWriter = "live"
                                /\ s' = [s EXCEPT !.createWriter = "closing"]
CreateSDKWriterTerminal == /\ s.createWriter = "closing"
                           /\ s' = [s EXCEPT !.createWriter = "terminal"]
CreateSDKReleaseLease(earlyWork, earlyWriter) ==
    /\ s.createLease = "held"
    /\ (s.createStage = "failed" \/ s.createWriter = "terminal" \/ s.createStopped \/ s.createClosing)
    /\ (s.createWork = "terminal" \/ (earlyWork /\ s.createStopped))
    /\ (s.createWriter \in {"none", "terminal"}
          \/ (earlyWriter /\ ~s.createDelivered /\ s.createWriter = "closing" /\ s.createWork = "terminal"))
    /\ s' = [s EXCEPT !.createLease = "closing"]
CreateSDKLeaseTerminal == /\ s.createLease = "closing"
                          /\ s' = [s EXCEPT !.createLease = "terminal"]
CreateSDKRetryIdle == /\ s.createLease = "terminal" /\ ~s.createClosing
                      /\ (s.createStage = "failed" \/ s.createWriter = "terminal")
                      /\ s' = [s EXCEPT !.createStage = "idle", !.createLease = "none",
                                        !.createWork = "none", !.createWriter = "none"]
CreateSDKClosePool == /\ s.createClosing /\ s.createLease \in {"none", "terminal"} /\ s.createPool = "open"
                      /\ s' = [s EXCEPT !.createPool = "closing"]
CreateSDKPoolTerminal == /\ s.createPool = "closing"
                         /\ s' = [s EXCEPT !.createPool = "terminal"]
CreateSDKReleaseDependencies == /\ s.createPool = "terminal" /\ s.createDependencies = "held"
                                /\ s' = [s EXCEPT !.createDependencies = "released"]
CreateSDKNext(inferClaim, retainClaim, earlyWork, earlyWriter) ==
    CreateSDKStart \/ CreateSDKRejectMismatch \/ CreateSDKNativeApplied \/ CreateSDKUncertainBirth
    \/ CreateSDKObserveExisting(inferClaim) \/ CreateSDKAvailabilityStep
    \/ CreateSDKFinishInit \/ CreateSDKFinishConfig \/ CreateSDKHandoff(retainClaim) \/ CreateSDKNoHandoff
    \/ CreateSDKDeliver \/ CreateSDKLoseDelivery \/ CreateSDKCancelCaller \/ CreateSDKCompleteCaller
    \/ CreateSDKGlobalStop \/ CreateSDKStopDeliveredWriter \/ CreateSDKWriterTerminal
    \/ CreateSDKReleaseLease(earlyWork, earlyWriter) \/ CreateSDKLeaseTerminal \/ CreateSDKRetryIdle
    \/ CreateSDKClosePool \/ CreateSDKPoolTerminal \/ CreateSDKReleaseDependencies
CreateSDKSpec == CreateSDKInit /\ [][CreateSDKNext(FALSE, FALSE, FALSE, FALSE)]_vars
CreateSDKObservedOriginSpec == CreateSDKInit /\ [][CreateSDKNext(TRUE, FALSE, FALSE, FALSE)]_vars
CreateSDKRetainedHandoffSpec == CreateSDKInit /\ [][CreateSDKNext(FALSE, TRUE, FALSE, FALSE)]_vars
CreateSDKEarlyWorkSpec == CreateSDKInit /\ [][CreateSDKNext(FALSE, FALSE, TRUE, FALSE)]_vars
CreateSDKEarlyWriterSpec == CreateSDKInit /\ [][CreateSDKNext(FALSE, FALSE, FALSE, TRUE)]_vars
CreateSDKTypeOK == TypeOK
    /\ s.createOrigin \in {"none", "native", "existing"} /\ s.createProof \in {"none", "native", "observed"}
    /\ s.createStage \in {"idle", "claiming", "initializing", "configuring", "ready", "handoff", "delivered", "failed"}
    /\ s.createWork \in {"none", "pending", "terminal"} /\ s.createAttempt \in 0..2 /\ s.createHandoffs \in 0..2
    /\ s.createWriter \in {"none", "local", "live", "closing", "terminal"}
    /\ s.createLease \in {"none", "held", "closing", "terminal"} /\ s.createPool \in {"open", "closing", "terminal"}
    /\ s.createDependencies \in {"held", "released"}
    /\ s.createAdmission \in {"none", "exact", "mismatch-rejected", "handoff", "stopped"}
    /\ \A name \in {"createClaim", "createConsumed", "createClosing", "createStopped",
                    "createCallerDone", "createDelivered"} : s[name] \in BOOLEAN
CreateWriterHasNativeBirth == s.createWriter # "none" => s.createProof = "native" /\ s.createOrigin = "native"
CreateRetainedClaimHasNativeBirth == s.createClaim
                                    => s.createProof = "native" /\ s.createOrigin = "native" /\ ~s.createConsumed
CreateHandoffConsumesClaim == s.createConsumed => ~s.createClaim
CreateMismatchPreservesOriginalClaim == s.createAdmission = "mismatch-rejected"
                                       => s.createClaim /\ s.createProof = "native" /\ ~s.createConsumed
CreateOneHandoffPerOrigin == s.createHandoffs <= 1
CreateWriterNeedsAllE == s.createWriter # "none" => s.available /\ s.received = Targets /\ s.accepted = Targets
CreatePrivateWorkPinsLease == s.createWork = "pending" => s.createLease = "held"
CreateUndeliveredWriterPinsLease == ~s.createDelivered /\ s.createWriter \in {"local", "live", "closing"}
                                   => s.createLease = "held"
CreateLiveWriterPinsLease == s.createWriter \in {"local", "live", "closing"} => s.createLease = "held"
CreateGlobalStopDropsClaim == s.createClosing => ~s.createClaim
CreateGlobalStopLeavesOpen == s.createClosing /\ s.createWriter # "none" => s.metadata
CreateDependenciesWaitActual == s.createDependencies = "released"
                               => s.createWork # "pending" /\ s.createWriter \in {"none", "terminal"}
                                   /\ s.createLease \in {"none", "terminal"} /\ s.createPool = "terminal"
=====================================================================
