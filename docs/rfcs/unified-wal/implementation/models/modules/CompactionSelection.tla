------------------------ MODULE CompactionSelection ------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS CaptureAllFirst, IndexAbsence
VARIABLE s
vars == <<s>>
Allocations == 1..3
(* A bounded candidate window, exact current DATA counts and a held authority cut
   are inputs. Payload uses two owner buffers, not whole-group input units; destination is independent.
   Actual framing/CRC/permissions and whole-allocation successor proof remain
   implementation inputs, not consequences of this finite projection. *)
Init == \E live \in [Allocations -> 0..3], budget \in 1..3, spare \in BOOLEAN :
          \E rows \in SUBSET {i \in Allocations : live[i] > 0} :
          s = [stage |-> "idle", live |-> live, rows |-> rows, budget |-> budget, spare |-> spare,
               measured |-> [i \in Allocations |-> 0], selected |-> {}, owned |-> 0,
               moved |-> {}, proof |-> FALSE, barrier |-> FALSE, freed |-> {}]
Dead == {i \in Allocations : s.live[i] = 0}
InputCost(chosen) == (IF 1 \in chosen THEN s.live[1] ELSE 0)
                   + (IF 2 \in chosen THEN s.live[2] ELSE 0)
                   + (IF 3 \in chosen THEN s.live[3] ELSE 0)
Measure == /\ s.stage = "idle"
           /\ s' = [s EXCEPT !.stage = "measured",
                    !.measured = IF IndexAbsence THEN [i \in Allocations |-> IF i \in s.rows THEN 1 ELSE 0]
                                 ELSE s.live]
Select == /\ s.stage = "measured"
          /\ LET empty == {i \in Allocations : s.measured[i] = 0} IN
               s' = [s EXCEPT !.stage = "selected",
                     !.selected = IF empty # {} THEN empty ELSE Allocations]
Capture == /\ s.stage = "selected"
           /\ LET need == IF CaptureAllFirst THEN InputCost(Allocations) ELSE 0 IN
                s' = [s EXCEPT !.owned = IF need > s.budget THEN 0 ELSE need,
                      !.stage = IF need > s.budget \/ (InputCost(s.selected) > 0 /\ ~s.spare)
                                THEN "refused" ELSE "captured"]
Move == /\ s.stage = "captured" /\ InputCost(s.selected) > 0
        /\ s' = [s EXCEPT !.moved = s.selected, !.stage = "moved"]
CopyFailure == /\ s.stage = "captured" /\ InputCost(s.selected) > 0
               /\ s' = [s EXCEPT !.stage = "refused", !.owned = 0]
Prove == /\ s.stage = "moved" \/ (s.stage = "captured" /\ InputCost(s.selected) = 0)
         /\ LET valid == \A i \in s.selected : s.live[i] = 0 \/ i \in s.moved IN
              s' = [s EXCEPT !.proof = valid, !.stage = IF valid THEN "proof" ELSE "refused"]
ForceFree == /\ s.stage = "proof" /\ s.proof
             /\ s' = [s EXCEPT !.barrier = TRUE, !.stage = "forced"]
ApplyFree == /\ s.stage = "forced" /\ s.barrier
             /\ s' = [s EXCEPT !.freed = s.selected, !.stage = "done", !.owned = 0]
Next == Measure \/ Select \/ Capture \/ Move \/ CopyFailure \/ Prove \/ ForceFree \/ ApplyFree
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"idle", "measured", "selected", "captured", "moved", "proof", "forced", "done", "refused"}
          /\ s.live \in [Allocations -> 0..3] /\ s.measured \in [Allocations -> 0..3]
          /\ s.selected \subseteq Allocations /\ s.moved \subseteq Allocations /\ s.freed \subseteq Allocations
          /\ s.owned \in 0..s.budget /\ s.proof \in BOOLEAN /\ s.barrier \in BOOLEAN
SelectedSourcesUseActualData == s.stage \notin {"idle", "measured"}
                                => s.selected = (IF Dead # {} THEN Dead ELSE Allocations)
DeadCandidatesAvoidInputRefusal == ~(s.stage = "refused" /\ Dead # {})
FreedSourcesHaveWholeProof == s.freed # {} => s.proof /\ s.barrier
FreedCurrentRowsHaveSuccessor == \A i \in s.freed : s.live[i] = 0 \/ i \in s.moved
=============================================================================
