-------------------- MODULE ProfileMetadataCreate --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreInstance, IgnoreSidecar, IgnorePayload, AbsenceAsNone, SkipAcknowledgedRead
VARIABLE s
vars == <<s>>
(* One standard-metadata create-if-absent, one resolving read, and optional
   deletion/recreation or update. Node kind: 0 absent, 1 matching initial,
   2 other instance, 3 other sidecar, 4 other initial payload, 5 corrupt,
   6 legacy without backlink, 7 read error. A recreated node again has
   dataVersion zero. Results describe their own snapshot, never a lease.
   Atomic backend create/read and strict codec are assumed. This does not
   model all-E installs, reservation, READY, mutation CAS or the full A/D. *)
Init == \E kind \in 0..7 :
    s = [kind |-> kind, phase |-> 0, created |-> FALSE, acknowledged |-> FALSE,
         actualRead |-> FALSE, removed |-> FALSE,
         recreated |-> FALSE, updated |-> FALSE, version |-> 0,
         capturedKind |-> 0, capturedVersion |-> 0, returnedVersion |-> 0,
         result |-> 0]
Create == /\ s.phase = 0
          /\ s' = [s EXCEPT !.phase = IF s.kind = 0 THEN 1 ELSE 2,
                   !.created = s.kind = 0, !.kind = IF s.kind = 0 THEN 1 ELSE s.kind]
CreateReply == /\ s.phase = 1
               /\ s' = [s EXCEPT !.phase = IF SkipAcknowledgedRead THEN 4 ELSE 2,
                        !.acknowledged = TRUE,
                        !.capturedKind = IF SkipAcknowledgedRead THEN 1 ELSE s.capturedKind,
                        !.result = IF SkipAcknowledgedRead THEN 1 ELSE s.result]
LostReply == /\ s.phase = 1
             /\ s' = [s EXCEPT !.phase = 2]
Remove == /\ s.created /\ ~s.removed /\ s.phase \in 1..3
          /\ s' = [s EXCEPT !.removed = TRUE, !.kind = 0, !.version = 0]
Recreate == /\ s.removed /\ ~s.recreated /\ s.phase \in 1..3
            /\ s' = [s EXCEPT !.recreated = TRUE, !.kind = 2, !.version = 0]
Update == /\ s.kind \in 1..4 /\ ~s.updated /\ s.phase \in 1..3
          /\ s' = [s EXCEPT !.updated = TRUE, !.kind = 4, !.version = 1]
Read == /\ s.phase = 2
        /\ s' = [s EXCEPT !.phase = 3, !.actualRead = TRUE,
                 !.capturedKind = s.kind, !.capturedVersion = s.version]
Resolve == /\ s.phase = 3
           /\ LET k == s.capturedKind
                  matches == k \in 1..4 /\ (k # 2 \/ IgnoreInstance)
                             /\ (k # 3 \/ IgnoreSidecar) /\ (k # 4 \/ IgnorePayload)
              IN s' = [s EXCEPT !.phase = 4,
                       !.result = IF k = 0 THEN IF AbsenceAsNone THEN 6 ELSE 5
                                  ELSE IF k = 7 THEN 5
                                  ELSE IF k = 5 THEN 4
                                  ELSE IF matches THEN IF s.acknowledged THEN 1 ELSE 2 ELSE 3,
                       !.returnedVersion = IF matches THEN s.capturedVersion ELSE 0]
Next == Create \/ CreateReply \/ LostReply \/ Remove \/ Recreate \/ Update \/ Read \/ Resolve
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.kind \in 0..7 /\ s.phase \in 0..4 /\ s.version \in 0..1
          /\ s.created \in BOOLEAN /\ s.acknowledged \in BOOLEAN /\ s.actualRead \in BOOLEAN
          /\ s.removed \in BOOLEAN
          /\ s.recreated \in BOOLEAN /\ s.updated \in BOOLEAN
          /\ s.capturedKind \in 0..7 /\ s.capturedVersion \in 0..1
          /\ s.returnedVersion \in 0..1 /\ s.result \in 0..6
SuccessMatchesInstance == s.result \in {1, 2} => s.capturedKind # 2
SuccessMatchesSidecar == s.result \in {1, 2} => s.capturedKind # 3
SuccessMatchesInitialPayload == s.result \in {1, 2} => s.capturedKind # 4
SuccessfulSnapshotIsValid == s.result \in {1, 2} => s.capturedKind \in 1..4
SuccessKeepsCapturedVersion == s.result \in {1, 2} => s.returnedVersion = s.capturedVersion
SuccessHasActualMetadataSnapshot == s.result \in {1, 2} => s.actualRead
AbsenceDoesNotProveNoEffect == s.result # 6
=====================================================================
