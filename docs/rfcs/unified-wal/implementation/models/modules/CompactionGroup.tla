------------------------ MODULE CompactionGroup ------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreAllocationCost, FreeBeforeMove, SameFreeCut, ReleaseOnCancel, FreeIncompleteGroup
VARIABLE s
vars == <<s>>
(* One to three 4-unit sources and one to three 2-unit aligned COPY batches.
   Frozen input bytes, native I/O, identity and whole-allocation proofs are
   validated inputs. COPY and MOVE have distinct force/apply actions. *)
Init == \E b \in 1..3 : s = [stage |-> "idle", count |-> 0, chosen |-> FALSE, batches |-> b,
             allocated |-> FALSE, copyForced |-> FALSE, moveForced |-> FALSE,
             moved |-> 0, copyForces |-> 0, moveForces |-> 0, native |-> FALSE, freeForced |-> FALSE,
             freed |-> 0, proof |-> <<7, 8, 9>>, claimed |-> 0, finished |-> 0,
             guard |-> FALSE, waiting |-> TRUE]
DestinationCost == 4 * ((2 * s.batches + 3) \div 4)
Select(n) == /\ s.stage = "idle" /\ n \in 1..3
             /\ s' = [s EXCEPT !.count = n,
                      !.chosen = (4 * n > DestinationCost \/ IgnoreAllocationCost),
                      !.stage = IF 4 * n > DestinationCost \/ IgnoreAllocationCost THEN "frozen" ELSE "refused"]
Allocate == /\ s.stage = "frozen"
            /\ s' = [s EXCEPT !.allocated = TRUE, !.stage = "allocated"]
WriteCopy == /\ s.stage = "allocated"
             /\ s' = [s EXCEPT !.stage = "copy"]
ForceCopy == /\ s.stage = "copy"
             /\ s' = [s EXCEPT !.copyForced = TRUE, !.copyForces = @ + 1, !.stage = "copied"]
CopyFailure == /\ s.stage = "copy"
               /\ s' = [s EXCEPT !.stage = "failed"]
ForceMove == /\ s.stage = "copied"
             /\ s' = [s EXCEPT !.moveForced = TRUE, !.moveForces = @ + 1, !.stage = "move"]
ApplyMove == /\ s.stage = "move"
             /\ s' = [s EXCEPT !.moved = @ + 1, !.stage = "moved"]
NextCopy == /\ s.stage = "moved" /\ s.moved < s.batches
            /\ s' = [s EXCEPT !.stage = "allocated", !.copyForced = FALSE, !.moveForced = FALSE]
PublishNative == /\ s.stage = "moved"
                 /\ (s.moved = s.batches \/ FreeIncompleteGroup)
                 /\ s' = [s EXCEPT !.native = TRUE, !.stage = "native"]
NativeFailure == /\ s.stage = "moved"
                 /\ s.moved = s.batches
                 /\ s' = [s EXCEPT !.stage = "failed"]
PrepareFree == /\ s.stage = "native"
               /\ s' = [s EXCEPT !.stage = "prepared", !.guard = TRUE,
                        !.claimed = s.count,
                        !.proof = IF SameFreeCut THEN <<7, 7, 7>> ELSE <<7, 8, 9>>]
ForceFree == /\ s.stage = "prepared"
             /\ s' = [s EXCEPT !.freeForced = TRUE, !.stage = "free"]
ApplyFree == /\ s.stage = "free" /\ s.freed < s.count
             /\ s' = [s EXCEPT !.freed = @ + 1]
EarlyFree == /\ FreeBeforeMove /\ s.stage = "copied"
             /\ s' = [s EXCEPT !.freed = s.count, !.stage = "failed"]
FinishMember == /\ s.stage = "free" /\ s.freed = s.count /\ s.finished < s.claimed
                /\ s' = [s EXCEPT !.finished = @ + 1,
                         !.guard = IF s.finished + 1 = s.claimed THEN FALSE ELSE s.guard]
Finish == /\ ((s.stage = "free" /\ s.finished = s.claimed) \/ s.stage = "failed")
          /\ s' = [s EXCEPT !.stage = "done"]
Cancel == /\ s.stage # "idle" /\ s.waiting
          /\ s' = [s EXCEPT !.waiting = FALSE,
                   !.guard = IF ReleaseOnCancel THEN FALSE ELSE s.guard]
TypeOK == /\ s.stage \in {"idle", "refused", "frozen", "allocated", "copy", "copied",
                         "move", "moved", "native", "prepared", "free", "failed", "done"}
          /\ s.count \in 0..3 /\ s.freed \in 0..3 /\ s.claimed \in 0..3 /\ s.finished \in 0..3
          /\ s.batches \in 1..3 /\ s.moved \in 0..3 /\ s.copyForces \in 0..3 /\ s.moveForces \in 0..3
          /\ s.proof \in {<<7, 8, 9>>, <<7, 7, 7>>}
          /\ s.chosen \in BOOLEAN /\ s.allocated \in BOOLEAN /\ s.copyForced \in BOOLEAN
          /\ s.moveForced \in BOOLEAN /\ s.native \in BOOLEAN
          /\ s.freeForced \in BOOLEAN /\ s.guard \in BOOLEAN /\ s.waiting \in BOOLEAN
SelectionHasAllocationNetGain == s.chosen => 4 * s.count > DestinationCost
MovedTargetHasDurableCopy == s.moved > 0 => s.allocated /\ s.moved <= s.copyForces /\ s.moved <= s.moveForces
FreedSourcesHaveDurableSuccessor == s.freed > 0 => s.moved = s.batches /\ s.native /\ s.freeForced
AppliedFreeUsesOwnCut == \A i \in 1..s.freed : s.proof[i] = 6 + i
PendingMembersKeepProtection == s.finished < s.claimed => s.guard
Next == (\E n \in 1..3 : Select(n)) \/ Allocate \/ WriteCopy \/ ForceCopy \/ CopyFailure \/ ForceMove
        \/ ApplyMove \/ NextCopy \/ PublishNative \/ NativeFailure \/ PrepareFree \/ ForceFree
        \/ ApplyFree \/ EarlyFree \/ FinishMember \/ Finish \/ Cancel
Spec == Init /\ [][Next]_vars
=============================================================================
