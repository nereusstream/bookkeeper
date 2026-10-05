-------------------- MODULE ReadinessCas --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipBirthCheck, IgnoreReadinessGeneration, PromoteLostReply
VARIABLE s
vars == <<s>>
(* ZooKeeper atomic persistent multi, unique immutable birth marker, exact
   record/ACL/scope validation and ordered modifications are inputs.
   result: 0 pending, 1 applied/equivalent current target, 2 conflict, 3 unknown. *)
Init == \E requested \in {1, 2} :
    s = [birth |-> 1, generation |-> 1, version |-> 0, zxid |-> 1, phase |-> 0,
         expectedGeneration |-> requested, observedGeneration |-> 0, observedBirth |-> 0,
         observedVersion |-> 0, changed |-> FALSE, replaced |-> FALSE,
         applied |-> FALSE, unsafe |-> FALSE, lost |-> FALSE, queried |-> FALSE, result |-> 0]
Capture == /\ s.phase = 0
           /\ s' = [s EXCEPT !.observedGeneration = s.generation, !.observedBirth = s.birth,
                    !.observedVersion = s.version,
                    !.phase = IF s.expectedGeneration = s.generation \/ IgnoreReadinessGeneration THEN 1 ELSE 4,
                    !.result = IF s.expectedGeneration = s.generation \/ IgnoreReadinessGeneration THEN 0 ELSE 2]
Submit == /\ s.phase = 1 /\ s' = [s EXCEPT !.phase = 2]
Replace == /\ s.phase \in 1..4 /\ ~s.replaced
           /\ s' = [s EXCEPT !.birth = 2, !.version = 0, !.generation = 1,
                    !.zxid = @ + 1, !.replaced = TRUE]
ConcurrentUpdate == /\ s.phase \in 1..4 /\ ~s.changed
                    /\ s' = [s EXCEPT !.version = @ + 1, !.generation = 3,
                             !.zxid = @ + 1, !.changed = TRUE]
Commit == /\ s.phase = 2
          /\ LET allowed == s.version = s.observedVersion /\ (s.birth = s.observedBirth \/ SkipBirthCheck)
             IN s' = [s EXCEPT !.applied = allowed, !.phase = 3,
                      !.generation = IF allowed THEN 2 ELSE @,
                      !.version = IF allowed THEN @ + 1 ELSE @,
                      !.zxid = IF allowed THEN @ + 1 ELSE @,
                      !.unsafe = allowed /\ (s.birth # s.observedBirth
                                            \/ s.expectedGeneration # s.observedGeneration)]
Reply == /\ s.phase = 3
         /\ \E lost \in BOOLEAN :
             s' = [s EXCEPT !.phase = 4, !.lost = lost,
                   !.result = IF lost THEN IF PromoteLostReply THEN 1 ELSE 3
                              ELSE IF s.applied THEN 1 ELSE 2]
Query == /\ s.phase = 4
         /\ s' = [s EXCEPT !.queried = TRUE, !.phase = 5,
                  !.result = IF s.generation = 2 THEN 1 ELSE 2]
Next == Capture \/ Submit \/ Replace \/ ConcurrentUpdate \/ Commit \/ Reply \/ Query
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.birth \in 1..2 /\ s.generation \in 1..3 /\ s.version \in 0..2 /\ s.zxid \in 1..4
          /\ s.phase \in 0..5 /\ s.expectedGeneration \in 1..2 /\ s.result \in 0..3
          /\ s.changed \in BOOLEAN /\ s.replaced \in BOOLEAN /\ s.applied \in BOOLEAN
          /\ s.unsafe \in BOOLEAN /\ s.lost \in BOOLEAN /\ s.queried \in BOOLEAN
ReadinessCasPreservesBirthAndExpectedGeneration == ~s.unsafe
UnresolvedLostReplyNeverReportsApplied == ~(s.lost /\ ~s.queried /\ s.result = 1)
ReconciledSuccessMatchesCurrentRecord == s.queried /\ s.result = 1 => s.generation = 2
=====================================================================
