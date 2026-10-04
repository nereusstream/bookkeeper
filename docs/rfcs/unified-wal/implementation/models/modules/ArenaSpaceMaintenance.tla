--------------------- MODULE ArenaSpaceMaintenance ---------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS IgnoreResources, RepeatUnchangedRound, LoseKnownRefusal, ResumeUnknown
VARIABLE s
vars == <<s>>
(* Two same-file shard windows, one tombstone and one known control refusal.
   Physical COPY/MOVE/FREE and dual-root durability remain separate contracts.
   No fairness, full filesystem budget, crash recovery or long-run claim. *)
Init == \E live \in BOOLEAN, resources \in BOOLEAN, control \in BOOLEAN :
    s = [stage |-> "idle", pressure |-> TRUE, live |-> live, resources |-> resources,
         control |-> control, version |-> 0, captured |-> 0, cursor |-> 0,
         visited |-> <<>>, freeBlocks |-> 0, started |-> FALSE,
         startResources |-> TRUE, startControl |-> FALSE, needsRetry |-> FALSE,
         capacityUsed |-> FALSE, unknown |-> FALSE, stopped |-> FALSE, admitted |-> FALSE]
ReleaseResources == /\ ~s.resources /\ s.stage = "idle"
                    /\ s' = [s EXCEPT !.resources = TRUE]
RotateControls == /\ s.control /\ s.stage = "idle"
                  /\ s' = [s EXCEPT !.control = FALSE, !.needsRetry = FALSE]
StartJob == /\ s.stage = "idle" /\ s.pressure /\ s.cursor < 2 /\ ~s.control
            /\ (s.resources \/ IgnoreResources)
            /\ s' = [s EXCEPT !.stage = "job", !.cursor = @ + 1, !.started = TRUE,
                      !.startResources = s.resources, !.startControl = s.control]
NoGain == /\ s.stage = "job" /\ s.live /\ Len(s.visited) < 3
          /\ s' = [s EXCEPT !.stage = "idle", !.visited = Append(@, s.cursor)]
Reclaim == /\ s.stage = "job" /\ ~s.live
           /\ s' = [s EXCEPT !.stage = "idle", !.freeBlocks = @ + 1]
KnownCapacityRefusal == /\ s.stage = "job" /\ ~s.capacityUsed
                       /\ s' = [s EXCEPT !.stage = "idle", !.control = TRUE,
                                 !.needsRetry = TRUE, !.capacityUsed = TRUE,
                                 !.cursor = IF LoseKnownRefusal THEN s.cursor ELSE 0,
                                 !.visited = <<>>, !.freeBlocks = 0]
Unknown == /\ s.stage = "job"
           /\ s' = [s EXCEPT !.stage = "halt", !.unknown = TRUE, !.stopped = TRUE]
Tombstone == /\ s.stage = "idle" /\ s.live /\ s.version = 0
             /\ s' = [s EXCEPT !.live = FALSE, !.version = 1]
NewRound == /\ s.stage = "idle" /\ s.cursor = 2 /\ s.freeBlocks < 2
            /\ (s.version # s.captured \/ RepeatUnchangedRound)
            /\ Len(s.visited) < 3
            /\ s' = [s EXCEPT !.cursor = 0, !.captured = s.version,
                      !.visited = IF s.version # s.captured THEN <<>> ELSE s.visited,
                      !.freeBlocks = 0]
ResumeData == /\ ~s.admitted /\ ((s.stage = "idle" /\ s.freeBlocks = 2 /\ ~s.control)
                              \/ (ResumeUnknown /\ s.unknown))
              /\ s' = [s EXCEPT !.pressure = FALSE, !.admitted = TRUE, !.stage = "halt"]
Next == ReleaseResources \/ RotateControls \/ StartJob \/ NoGain \/ Reclaim
        \/ KnownCapacityRefusal \/ Unknown \/ Tombstone \/ NewRound \/ ResumeData
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"idle", "job", "halt"} /\ s.cursor \in 0..2
          /\ s.freeBlocks \in 0..2 /\ s.version \in 0..1 /\ s.captured \in 0..1
          /\ Len(s.visited) <= 3
MaintenanceStartsWithActualResources == s.started => s.startResources
ControlRotationRetainsPriority == s.started => ~s.startControl
UnchangedFullLiveWindowsVisitedOnce ==
    \A window \in 1..2 : Cardinality({i \in 1..Len(s.visited) : s.visited[i] = window}) <= 1
KnownRefusalKeepsFreshProtectedWork == s.needsRetry => s.pressure /\ s.cursor = 0
UnknownStopsDataAdmission == s.unknown => s.stopped /\ ~s.admitted /\ s.pressure
RestoredAdmissionHasActualCapacity == s.admitted => s.freeBlocks = 2 /\ ~s.control
=============================================================================
