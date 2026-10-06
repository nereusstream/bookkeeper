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
=====================================================================
