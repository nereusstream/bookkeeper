------------------------ MODULE CompactionSweep ------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS ResetEveryCall, ExpandRoundCut
VARIABLE s
vars == <<s>>
(* One fixed shard/file, three stable eligible grants, one grant per window,
   and up to two later grants. Sequence numbers are one-based here. A caller must keep
   making admitted attempts; grants in the initial cut remain eligible inputs.
   No DATA, resource completion, MOVE/FREE or scheduler liveness is inferred. *)
Window(after, cut) == {id \in 1..cut : id > after}
First(ids) == CHOOSE id \in ids : \A other \in ids : id <= other
Init == s = [after |-> 0, through |-> 3, high |-> 3,
             calls |-> 0, seen |-> {}, last |-> 0, rejected |-> FALSE]
Append == /\ s.high < 5
          /\ s' = [s EXCEPT !.high = @ + 1]
RejectAdmission == /\ ~s.rejected
                   /\ s' = [s EXCEPT !.rejected = TRUE]
Admit == /\ s.calls < 6
         /\ LET startAfter == IF ResetEveryCall THEN 0 ELSE s.after
                cut == IF ExpandRoundCut THEN s.high ELSE s.through
                wrap == Window(startAfter, cut) = {}
                through == IF wrap THEN s.high ELSE cut
                after == IF wrap THEN 0 ELSE startAfter
                chosen == First(Window(after, through))
            IN s' = [s EXCEPT !.after = chosen, !.through = through, !.last = chosen,
                     !.calls = @ + 1, !.seen = @ \cup {chosen}]
Next == Append \/ RejectAdmission \/ Admit
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.last \in 0..s.high /\ s.after \in 0..s.high /\ s.through \in 3..s.high
          /\ s.high \in 3..5 /\ s.calls \in 0..6 /\ s.seen \subseteq 1..s.high
          /\ s.rejected \in BOOLEAN
ThreeAdmittedCallsCoverStableRound == s.calls >= 3 => 1..3 \subseteq s.seen
FourthAdmittedCallRevisitsOldestGrant == s.calls = 4 => s.last = 1
=============================================================================
