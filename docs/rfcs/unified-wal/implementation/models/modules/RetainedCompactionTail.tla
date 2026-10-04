---------------------- MODULE RetainedCompactionTail ----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreTailBoundary, FreeRetained
VARIABLE s
vars == <<s>>
(* Two sources and one retained current COPY grant occupy the entire file.
   The retained grant has capacity four, an old used end of two, and is never
   a source. Its valid latest-grant/drained-prefix byte proof is an input;
   actual DATA CRC, Bookie cuts, ownership and fsync remain Java/other slices. *)
Init == \E demand \in 1..3, latest \in BOOLEAN, currentCopy \in BOOLEAN :
          s = [stage |-> "measure", demand |-> demand, latest |-> latest, currentCopy |-> currentCopy,
               oldEnd |-> 2, end |-> 2, start |-> 2, copied |-> 0, moved |-> 0,
               forced |-> FALSE, native |-> FALSE, freeForced |-> FALSE,
               sourceFreed |-> 0, retainedFreed |-> FALSE, newCapacity |-> 0]
Measure == /\ s.stage = "measure"
           /\ s' = [s EXCEPT !.stage = IF s.latest /\ s.currentCopy
                         /\ (s.demand <= 4 - s.oldEnd \/ IgnoreTailBoundary)
                         THEN "ready" ELSE "refused"]
Copy == /\ s.stage = "ready" /\ s.copied < s.demand
        /\ s' = [s EXCEPT !.start = s.end, !.end = @ + 1, !.stage = "copy", !.forced = FALSE]
ForceCopy == /\ s.stage = "copy"
             /\ s' = [s EXCEPT !.forced = TRUE, !.copied = @ + 1, !.stage = "copied"]
StopBeforeNextCopy == /\ s.stage = "ready"
                      /\ s' = [s EXCEPT !.stage = "refused"]
Move == /\ s.stage = "copied" /\ s.forced
        /\ s' = [s EXCEPT !.moved = @ + 1,
                 !.stage = IF s.moved + 1 = s.demand THEN "moved" ELSE "ready"]
Native == /\ s.stage = "moved"
          /\ s' = [s EXCEPT !.native = TRUE, !.stage = "proof"]
ForceFree == /\ s.stage = "proof"
             /\ s' = [s EXCEPT !.freeForced = TRUE, !.stage = "free"]
Free == /\ s.stage = "free"
        /\ s' = [s EXCEPT !.sourceFreed = 2, !.retainedFreed = FreeRetained, !.stage = "done"]
Next == Measure \/ Copy \/ ForceCopy \/ StopBeforeNextCopy \/ Move \/ Native \/ ForceFree \/ Free
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"measure", "ready", "copy", "copied", "moved", "proof", "free", "done", "refused"}
          /\ s.copied \in 0..s.demand /\ s.moved \in 0..s.copied /\ s.sourceFreed \in {0, 2}
          /\ s.newCapacity = 0
TailWritesUseUnusedRange == s.start >= s.oldEnd /\ s.end <= 4
RetainedCurrentCopyNeverFreed == ~s.retainedFreed
FreeHasCompleteCurrentSuccessor == s.sourceFreed > 0 => s.moved = s.demand /\ s.native /\ s.freeForced
=============================================================================
