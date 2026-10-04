---------------------- MODULE MaintenanceReserve ----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, Integers, Sequences
CONSTANTS ConsumeReserve, ResetMergedBudget, IgnoreWorksetBound, ForgetRestartGrants
VARIABLE s
vars == <<s>>
Files == {0, 1}
Capacity == 7
Zero == [f \in Files |-> 0]
Requests == [file : Files, blocks : 1..2]
(* One graph-wide maintenance operation. FREE proof/force, actual extent
   geometry and DATA bytes are inputs from other slices/Java, not proved here.
   Fresh and retired slots count once; no filesystem space is inferred. *)
Init == \E limit \in 1..2, requests \in [1..3 -> Requests] :
          s = [stage |-> "plan", limit |-> limit, requests |-> requests, next |-> 1,
               free |-> [f \in Files |-> Capacity], inUse |-> Zero, planned |-> Zero,
               foregroundFree |-> [f \in Files |-> Capacity],
               restarted |-> FALSE, attempted |-> FALSE, file |-> 0,
               cost |-> 0, sources |-> 0]
Plan == /\ s.stage = "plan" /\ s.next <= 3
        /\ LET r == s.requests[s.next]
               available == s.free[r.file] - IF ResetMergedBudget THEN 0 ELSE s.planned[r.file]
               accepted == available - r.blocks >= IF ConsumeReserve THEN 0 ELSE s.limit
           IN s' = [s EXCEPT !.next = @ + 1,
                      !.planned[r.file] = @ + IF accepted THEN r.blocks ELSE 0]
Apply == /\ s.stage = "plan" /\ s.next = 4
         /\ s' = [s EXCEPT !.free = [f \in Files |-> s.free[f] - s.planned[f]],
                   !.inUse = [f \in Files |-> s.inUse[f] + s.planned[f]],
                   !.foregroundFree = [f \in Files |-> s.free[f] - s.planned[f]],
                   !.planned = Zero, !.stage = "runtime"]
Restart == /\ s.stage \in {"plan", "runtime", "partial"} /\ ~s.restarted
           /\ s' = [s EXCEPT !.free = [f \in Files |->
                        IF ForgetRestartGrants THEN Capacity ELSE Capacity - s.inUse[f]],
                     !.planned = Zero, !.stage = "runtime", !.restarted = TRUE]
StartMaintenance == /\ s.stage = "runtime" /\ ~s.attempted
                    /\ \E file \in Files, cost \in 1..3 :
                        /\ (cost <= s.limit \/ IgnoreWorksetBound)
                        /\ s.free[file] >= cost /\ s.inUse[file] >= cost + 1
                        /\ s' = [s EXCEPT !.free[file] = @ - cost, !.inUse[file] = @ + cost,
                                  !.file = file, !.cost = cost, !.sources = cost + 1,
                                  !.attempted = TRUE, !.stage = "maintenance"]
VerifiedFree == /\ s.stage = "maintenance"
                /\ s' = [s EXCEPT !.free[s.file] = @ + s.sources,
                          !.inUse[s.file] = @ - s.sources, !.stage = "done"]
Partial == /\ s.stage = "maintenance" /\ s' = [s EXCEPT !.stage = "partial"]
Refuse == /\ s.stage = "runtime" /\ s' = [s EXCEPT !.stage = "done"]
Next == Plan \/ Apply \/ Restart \/ StartMaintenance \/ VerifiedFree \/ Partial \/ Refuse
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"plan", "runtime", "maintenance", "partial", "done"}
          /\ s.limit \in 1..2 /\ s.next \in 1..4 /\ s.cost \in 0..3
          /\ \A f \in Files : s.inUse[f] \in 0..Capacity /\ s.free[f] \in 0..Capacity
WholeBlocksAccountOnce == \A f \in Files : s.free[f] + s.inUse[f] = Capacity
PlannedForegroundKeepsReserve == \A f \in Files : s.free[f] - s.planned[f] >=
                                                   IF s.stage = "plan" THEN s.limit ELSE 0
ForegroundKeepsReserve == \A f \in Files : s.foregroundFree[f] >= s.limit
DestinationFitsDeclaredWorkset == s.cost <= s.limit
=============================================================================
