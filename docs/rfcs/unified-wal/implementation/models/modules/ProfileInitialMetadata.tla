-------------------- MODULE ProfileInitialMetadata --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS AckOnly, IgnoreResultBinding, IgnoreRootVersion, AcceptMissingRoot, IgnorePlan, ReturnWriter
VARIABLE s
vars == <<s>>
Targets == {1, 2, 3}
(* E/W/A = 3/3/2. Result kind: 1 exact durable, 2 known refusal,
   3 unknown, 4 apparent success with wrong binding. Clients dispatch
   nonblocking; actual per-target barriers and outcome correlation are inputs.
   Root can change before OR after the fresh read: metadata is not atomic with
   root, and an inert orphan after the read is legal. No READY, activation,
   target-retention CAS, actual TLS, metadata SerDe bytes or full A/D. *)
Init == \E kinds \in [Targets -> 1..4], validPlan \in BOOLEAN :
    s = [kinds |-> kinds, validPlan |-> validPlan, phase |-> 0, reserved |-> FALSE,
         sent |-> {}, received |-> {}, rootPresent |-> TRUE, rootVersion |-> 0,
         changed |-> FALSE, removed |-> FALSE, checkedPresent |-> FALSE,
         checkedVersion |-> 0, metadataIssued |-> FALSE, metadataVerified |-> FALSE,
         writer |-> FALSE]
Reserve == /\ s.phase = 0
           /\ s' = [s EXCEPT !.phase = IF s.validPlan \/ IgnorePlan THEN 1 ELSE 6,
                    !.reserved = s.validPlan \/ IgnorePlan]
Dispatch == /\ s.phase = 1
            /\ s' = [s EXCEPT !.phase = 2, !.sent = Targets]
Receive == \E target \in s.sent \ s.received :
    /\ s.phase = 2 /\ s' = [s EXCEPT !.received = s.received \cup {target}]
MatchingResults == {target \in s.received : s.kinds[target] = 1
                                          \/ (s.kinds[target] = 4 /\ IgnoreResultBinding)}
Join == /\ s.phase = 2
        /\ (s.received = Targets \/ (AckOnly /\ Cardinality(s.received) >= 2))
        /\ s' = [s EXCEPT !.phase = IF MatchingResults = Targets
                                   \/ (AckOnly /\ Cardinality(MatchingResults) >= 2) THEN 3 ELSE 6]
ChangeRoot == /\ ~s.changed /\ s.reserved /\ s.phase \in 1..5
              /\ s' = [s EXCEPT !.changed = TRUE, !.rootVersion = 1]
RemoveRoot == /\ ~s.removed /\ s.reserved /\ s.phase \in 1..5
              /\ s' = [s EXCEPT !.removed = TRUE, !.rootPresent = FALSE]
ReadRoot == /\ s.phase = 3
            /\ s' = [s EXCEPT !.checkedPresent = s.rootPresent, !.checkedVersion = s.rootVersion,
                     !.phase = IF (s.rootPresent \/ AcceptMissingRoot)
                                  /\ (s.rootVersion = 0 \/ IgnoreRootVersion) THEN 4 ELSE 6]
CreateMetadata == /\ s.phase = 4
                  /\ s' = [s EXCEPT !.phase = 5, !.metadataIssued = TRUE]
MetadataReply == /\ s.phase = 5
                 /\ s' = [s EXCEPT !.phase = 6, !.metadataVerified = TRUE, !.writer = ReturnWriter]
Next == Reserve \/ Dispatch \/ Receive \/ Join \/ ChangeRoot \/ RemoveRoot
        \/ ReadRoot \/ CreateMetadata \/ MetadataReply
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.kinds \in [Targets -> 1..4] /\ s.validPlan \in BOOLEAN /\ s.phase \in 0..6
          /\ s.sent \subseteq Targets /\ s.received \subseteq s.sent /\ s.reserved \in BOOLEAN
          /\ s.rootPresent \in BOOLEAN /\ s.rootVersion \in 0..1
          /\ s.changed \in BOOLEAN /\ s.removed \in BOOLEAN /\ s.checkedPresent \in BOOLEAN
          /\ s.checkedVersion \in 0..1 /\ s.metadataIssued \in BOOLEAN
          /\ s.metadataVerified \in BOOLEAN /\ s.writer \in BOOLEAN
InvalidPlanHasNoReservation == ~s.validPlan => ~s.reserved
MetadataWaitsForAllE == s.metadataIssued => s.received = Targets
MetadataHasExactDurableResults == s.metadataIssued => \A target \in Targets : s.kinds[target] = 1
MetadataHasSameRootSnapshot == s.metadataIssued => s.checkedVersion = 0
MetadataHasCommittedReservation == s.metadataIssued => s.checkedPresent
MetadataAloneNeverReturnsWriter == ~s.writer
=====================================================================
