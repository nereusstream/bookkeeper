------------------------ MODULE CompactionGroup ------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreAllocationCost, FreeBeforeMove, SameFreeCut, ReleaseOnCancel
VARIABLE s
vars == <<s>>
(* One or two 4-unit sources; 1 live unit each fits in one 4-unit destination.
   Frozen input bytes, native I/O, identity and whole-allocation proofs are
   validated inputs. COPY and MOVE have distinct force/apply actions. *)
Init == s = [stage |-> "idle", count |-> 0, chosen |-> FALSE,
             allocated |-> FALSE, copyForced |-> FALSE, moveForced |-> FALSE,
             moved |-> FALSE, native |-> FALSE, freeForced |-> FALSE,
             freed |-> 0, proof |-> <<7, 8>>, claimed |-> 0, finished |-> 0,
             guard |-> FALSE, waiting |-> TRUE]
Select(n) == /\ s.stage = "idle" /\ n \in 1..2
             /\ s' = [s EXCEPT !.count = n,
                      !.chosen = (4 * n > 4 \/ IgnoreAllocationCost),
                      !.stage = IF 4 * n > 4 \/ IgnoreAllocationCost THEN "frozen" ELSE "refused"]
Allocate == /\ s.stage = "frozen"
            /\ s' = [s EXCEPT !.allocated = TRUE, !.stage = "allocated"]
WriteCopy == /\ s.stage = "allocated"
             /\ s' = [s EXCEPT !.stage = "copy"]
ForceCopy == /\ s.stage = "copy"
             /\ s' = [s EXCEPT !.copyForced = TRUE, !.stage = "copied"]
ForceMove == /\ s.stage = "copied"
             /\ s' = [s EXCEPT !.moveForced = TRUE, !.stage = "move"]
ApplyMove == /\ s.stage = "move"
             /\ s' = [s EXCEPT !.moved = TRUE, !.stage = "moved"]
PublishNative == /\ s.stage = "moved"
                 /\ s' = [s EXCEPT !.native = TRUE, !.stage = "native"]
NativeFailure == /\ s.stage = "moved"
                 /\ s' = [s EXCEPT !.stage = "failed"]
PrepareFree == /\ s.stage = "native"
               /\ s' = [s EXCEPT !.stage = "prepared", !.guard = TRUE,
                        !.claimed = s.count,
                        !.proof = IF SameFreeCut THEN <<7, 7>> ELSE <<7, 8>>]
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
          /\ s.count \in 0..2 /\ s.freed \in 0..2 /\ s.claimed \in 0..2 /\ s.finished \in 0..2
          /\ s.proof \in {<<7, 8>>, <<7, 7>>}
          /\ s.chosen \in BOOLEAN /\ s.allocated \in BOOLEAN /\ s.copyForced \in BOOLEAN
          /\ s.moveForced \in BOOLEAN /\ s.moved \in BOOLEAN /\ s.native \in BOOLEAN
          /\ s.freeForced \in BOOLEAN /\ s.guard \in BOOLEAN /\ s.waiting \in BOOLEAN
SelectionHasAllocationNetGain == s.chosen => 4 * s.count > 4
MovedTargetHasDurableCopy == s.moved => s.allocated /\ s.copyForced /\ s.moveForced
FreedSourcesHaveDurableSuccessor == s.freed > 0 => s.moved /\ s.native /\ s.freeForced
AppliedFreeUsesOwnCut == \A i \in 1..s.freed : s.proof[i] = 6 + i
PendingMembersKeepProtection == s.finished < s.claimed => s.guard
Next == (\E n \in 1..2 : Select(n)) \/ Allocate \/ WriteCopy \/ ForceCopy \/ ForceMove
        \/ ApplyMove \/ PublishNative \/ NativeFailure \/ PrepareFree \/ ForceFree
        \/ ApplyFree \/ EarlyFree \/ FinishMember \/ Finish \/ Cancel
Spec == Init /\ [][Next]_vars
=============================================================================
