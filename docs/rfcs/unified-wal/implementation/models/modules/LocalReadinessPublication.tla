-------------------- MODULE LocalReadinessPublication --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipFileSync, SkipDirectorySync, ReadUncertain, AdoptPending, IgnoreGeneration
VARIABLE s
vars == <<s>>
(* One exclusive writer and strict bounded scope/codec validation are inputs.
   Receipt 1 is confirmed; receipt 2 has different content and requested generation.
   Atomic rename selects a complete old/new record. Only successful file+directory
   barriers constrain crash survival. Reopen performs both recovery barriers.
   result: 0 pending, 1 exact durable target, 2 conflict/other record, 3 unknown. *)
Init == \E generation \in 1..2 :
    s = [phase |-> 0, requestedGeneration |-> generation, published |-> 1, cache |-> 1,
         pending |-> FALSE, fileSynced |-> FALSE, directorySynced |-> TRUE,
         poisoned |-> FALSE, queried |-> FALSE, crashed |-> FALSE, acked |-> FALSE,
         badAck |-> FALSE, badRead |-> FALSE, badTemp |-> FALSE,
         reusedGeneration |-> FALSE, result |-> 0]
Begin == /\ s.phase = 0
         /\ s' = [s EXCEPT !.phase = IF s.requestedGeneration > 1 \/ IgnoreGeneration THEN 1 ELSE 5,
                  !.result = IF s.requestedGeneration > 1 \/ IgnoreGeneration THEN 0 ELSE 2]
WriteTemp == /\ s.phase = 1
             /\ s' = [s EXCEPT !.phase = 2, !.pending = TRUE]
SyncFile == /\ s.phase = 2
            /\ \E success \in BOOLEAN :
                s' = [s EXCEPT !.phase = IF success THEN 3 ELSE 5,
                      !.fileSynced = success /\ ~SkipFileSync, !.poisoned = ~success,
                      !.result = IF success THEN 0 ELSE 3]
Rename == /\ s.phase = 3
          /\ s' = [s EXCEPT !.phase = 4, !.published = 2, !.pending = FALSE,
                   !.directorySynced = FALSE, !.reusedGeneration = s.requestedGeneration <= 1]
SyncDirectory == /\ s.phase = 4
                 /\ \E success \in BOOLEAN :
                     s' = [s EXCEPT !.phase = 5, !.directorySynced = success /\ ~SkipDirectorySync,
                           !.cache = IF success THEN 2 ELSE @, !.poisoned = ~success,
                           !.result = IF success THEN 0 ELSE 3]
Reply == /\ s.phase = 5 /\ s.result = 0
         /\ s' = [s EXCEPT !.phase = 6, !.result = 1, !.acked = TRUE,
                  !.badAck = ~(s.fileSynced /\ s.directorySynced)]
Query == /\ s.phase \in 5..6 /\ ~s.queried
         /\ LET unsafe == s.poisoned /\ ReadUncertain /\ s.published = 2
                matches == ~s.poisoned /\ s.cache = 2
            IN s' = [s EXCEPT !.queried = TRUE, !.badRead = unsafe,
                     !.result = IF unsafe \/ matches THEN 1 ELSE IF s.poisoned THEN 3 ELSE 2]
Crash == /\ s.phase \in 1..6 /\ ~s.crashed
         /\ \E retained \in (IF s.published = 2 /\ s.fileSynced /\ s.directorySynced THEN {2}
                             ELSE IF s.published = 2 THEN {1, 2} ELSE {1}) :
             s' = [s EXCEPT !.phase = 7, !.crashed = TRUE, !.published = retained, !.cache = 0,
                   !.fileSynced = FALSE, !.directorySynced = FALSE, !.poisoned = FALSE, !.result = 0]
Reopen == /\ s.phase = 7
          /\ LET adopt == s.pending /\ AdoptPending
             IN s' = [s EXCEPT !.phase = 8, !.badTemp = adopt,
                      !.published = IF adopt THEN 2 ELSE @, !.cache = IF adopt THEN 2 ELSE s.published,
                      !.fileSynced = TRUE, !.directorySynced = TRUE]
RecoveryFailure == /\ s.phase = 7
                   /\ s' = [s EXCEPT !.phase = 9, !.poisoned = TRUE, !.result = 3]
ReadRecovered == /\ s.phase = 8
                 /\ s' = [s EXCEPT !.phase = 9, !.result = IF s.cache = 2 THEN 1 ELSE 2]
Next == Begin \/ WriteTemp \/ SyncFile \/ Rename \/ SyncDirectory \/ Reply \/ Query
        \/ Crash \/ Reopen \/ RecoveryFailure \/ ReadRecovered
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in 0..9 /\ s.requestedGeneration \in 1..2
          /\ s.published \in 1..2 /\ s.cache \in 0..2 /\ s.result \in 0..3
          /\ \A field \in {"pending", "fileSynced", "directorySynced", "poisoned", "queried",
                           "crashed", "acked", "badAck", "badRead", "badTemp", "reusedGeneration"} :
                 s[field] \in BOOLEAN
AppliedHasFileAndDirectoryBarriers == ~s.badAck
UncertainInstanceNeverReturnsReceipt == ~s.badRead
UnpublishedTempNeverBecomesReceipt == ~s.badTemp
OneGenerationBindsOneReceipt == ~s.reusedGeneration
ConfirmedReceiptSurvivesCrash == s.acked /\ s.crashed => s.published = 2
=====================================================================
