-------------------- MODULE ProfileBackingBudget --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS UnchargedAllocation, DeltaOnlyResize, EarlyViewRelease,
          ReleaseOnClose, DoubleChargeResponse, TransferReadBacking
VARIABLE s
vars == <<s>>
Lanes == {"unclassified", "control"}
EmptyRoot == [capacity |-> 0, credit |-> 0, refs |-> 0,
              lane |-> "unclassified", allocatedLane |-> "unclassified"]
EmptyTemporary == [root |-> 0, capacity |-> 0, credit |-> 0, lane |-> "unclassified"]
(* One connection, two root backing buffers, at most two retained references,
   capacities 1..3, lane limits 6 and connection limit 8. Authenticated lane
   transfer is an input, checked by the existing FENCE dispatch slice. One
   workspace split models an exact response reservation. TLS engines, kernel
   buffers, Java objects, byte contents, fsync, timing and liveness are outside
   this projection. Allocation failure is represented by not acquiring credit
   or by discarding the already charged resize temporary. *)
Init == s = [open |-> TRUE, lane |-> "unclassified", base |-> 1,
             workspace |-> 2, workspaceCredit |-> 2,
             roots |-> <<EmptyRoot, EmptyRoot>>, temporary |-> EmptyTemporary]
Used(lane) == (IF s.lane = lane THEN s.base + s.workspaceCredit ELSE 0)
              + (IF s.roots[1].lane = lane THEN s.roots[1].credit ELSE 0)
              + (IF s.roots[2].lane = lane THEN s.roots[2].credit ELSE 0)
              + (IF s.temporary.lane = lane THEN s.temporary.credit ELSE 0)
Total == Used("unclassified") + Used("control")
Fits(bytes) == s.open /\ Total + bytes <= 8 /\ Used(s.lane) + bytes <= 6
Allocate(i, bytes) == /\ s.roots[i].refs = 0 /\ s.temporary.root # i /\ Fits(bytes)
                     /\ s' = [s EXCEPT !.roots[i] =
                         [capacity |-> bytes, credit |-> IF UnchargedAllocation THEN 0 ELSE bytes,
                          refs |-> 1, lane |-> s.lane, allocatedLane |-> s.lane]]
Retain(i) == /\ s.roots[i].refs = 1
             /\ s' = [s EXCEPT !.roots[i].refs = 2]
Release(i) == /\ s.roots[i].refs > 0 /\ s.temporary.root # i
              /\ s' = [s EXCEPT !.roots[i] = IF s.roots[i].refs = 1 THEN EmptyRoot
                         ELSE [s.roots[i] EXCEPT !.refs = 1,
                               !.credit = IF EarlyViewRelease THEN 0 ELSE @]]
BeginResize(i, bytes) ==
    LET charged == IF DeltaOnlyResize THEN bytes - s.roots[i].capacity ELSE bytes
    IN /\ s.roots[i].refs > 0 /\ bytes > s.roots[i].capacity
       /\ s.temporary.root = 0 /\ Fits(charged)
       /\ s' = [s EXCEPT !.temporary =
            [root |-> i, capacity |-> bytes, credit |-> charged, lane |-> s.lane]]
FinishResize == /\ s.temporary.root # 0
                /\ s' = [s EXCEPT !.roots[s.temporary.root] =
                    [s.roots[s.temporary.root] EXCEPT !.capacity = s.temporary.capacity,
                     !.credit = s.temporary.credit, !.lane = s.temporary.lane,
                     !.allocatedLane = s.temporary.lane], !.temporary = EmptyTemporary]
DiscardResize == /\ s.temporary.root # 0
                 /\ s' = [s EXCEPT !.temporary = EmptyTemporary]
SplitResponse(i) == /\ s.workspace = 2 /\ s.roots[i].refs = 0
                    /\ s.temporary.root # i /\ s.open
                    /\ s' = [s EXCEPT !.workspace = 1,
                         !.workspaceCredit = IF DoubleChargeResponse THEN @ ELSE 1,
                         !.roots[i] = [capacity |-> 1, credit |-> 1, refs |-> 1,
                                      lane |-> s.lane, allocatedLane |-> s.lane]]
ReleaseWorkspace == /\ s.workspace > 0
                    /\ s' = [s EXCEPT !.workspace = 0, !.workspaceCredit = 0]
Transfer == /\ s.open /\ s.lane = "unclassified"
            /\ Used("control") + s.base + s.workspaceCredit <= 6
            /\ s' = [s EXCEPT !.lane = "control",
                 !.roots = IF TransferReadBacking THEN
                    [i \in 1..2 |-> [s.roots[i] EXCEPT !.lane = "control"]] ELSE @]
Close == /\ s.open
         /\ s' = [s EXCEPT !.open = FALSE, !.base = 0,
              !.roots = IF ReleaseOnClose THEN
                 [i \in 1..2 |-> [s.roots[i] EXCEPT !.credit = 0]] ELSE @,
              !.temporary.credit = IF ReleaseOnClose THEN 0 ELSE @]
Next == (\E i \in 1..2 : Retain(i) \/ Release(i) \/ SplitResponse(i)
         \/ (\E bytes \in 1..3 : Allocate(i, bytes) \/ BeginResize(i, bytes)))
         \/ FinishResize \/ DiscardResize \/ ReleaseWorkspace \/ Transfer \/ Close
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.open \in BOOLEAN /\ s.lane \in Lanes /\ s.base \in 0..1
          /\ s.workspace \in 0..2 /\ s.workspaceCredit \in 0..2
          /\ \A i \in 1..2 : s.roots[i].capacity \in 0..3 /\ s.roots[i].credit \in 0..3
               /\ s.roots[i].refs \in 0..2 /\ s.roots[i].lane \in Lanes
               /\ s.roots[i].allocatedLane \in Lanes
          /\ s.temporary.root \in 0..2 /\ s.temporary.capacity \in 0..3
          /\ s.temporary.credit \in 0..3 /\ s.temporary.lane \in Lanes
BackingsKeepFullCredit == \A i \in 1..2 : s.roots[i].credit = s.roots[i].capacity
ResizeKeepsOldAndNewCredit == s.temporary.credit = s.temporary.capacity
ResponseSplitConservesReservation == s.workspaceCredit = s.workspace
ReadBackingKeepsAllocationLane == \A i \in 1..2 : s.roots[i].refs > 0 =>
    s.roots[i].lane = s.roots[i].allocatedLane
WithinConnectionAndBookieLimits == Total <= 8 /\ \A lane \in Lanes : Used(lane) <= 6
ReleasedRootsHaveNoCapacity == \A i \in 1..2 : s.roots[i].refs = 0 => s.roots[i].capacity = 0
=====================================================================
