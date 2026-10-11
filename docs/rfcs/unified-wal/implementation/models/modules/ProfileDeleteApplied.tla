-------------------- MODULE ProfileDeleteApplied --------------------
(*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*)
EXTENDS Naturals, FiniteSets
CONSTANTS SkipHole, CursorWithoutEffect, SuccessBeforeForce, LoseCheckpoint,
          ReleaseOldCustody, IgnoreAssignment, InheritIncarnation
VARIABLE s
vars == <<s>>
Through(records) == IF 2 \in records THEN 2 ELSE IF 1 \in records THEN 1 ELSE 0
(* Two events, fixed initial stream/assignment and actual incarnation.
   Exact cold authority, force outcome and checkpoint bytes are inputs.
   No registration cut, handoff, remote drain, FREE/GC or A+C+D proof. *)
Init == s = [pending |-> 0, records |-> {}, applied |-> {}, cursor |-> 0,
             forced |-> FALSE, success |-> FALSE, uncertainty |-> FALSE,
             held |-> TRUE, released |-> FALSE, physicalDone |-> FALSE,
             assignment |-> 1, incarnation |-> 1, cursorIncarnation |-> 1,
             restarted |-> FALSE]
Stage(n) == /\ n \in 1..2 /\ s.pending = 0 /\ n \notin s.applied
            /\ (n = s.cursor + 1 \/ SkipHole)
            /\ (s.assignment = 1 \/ IgnoreAssignment) /\ s.incarnation = 1
            /\ s' = [s EXCEPT !.pending = n, !.forced = FALSE, !.success = FALSE,
                     !.restarted = FALSE]
Force == /\ s.pending > 0 /\ ~s.forced
         /\ s' = [s EXCEPT !.records = @ \cup {s.pending}, !.forced = TRUE]
Apply == /\ s.pending > 0 /\ (s.forced \/ SuccessBeforeForce)
         /\ s' = [s EXCEPT !.applied = IF CursorWithoutEffect THEN @ ELSE @ \cup {s.pending},
                  !.cursor = s.pending, !.pending = 0, !.success = TRUE,
                  !.released = IF ReleaseOldCustody THEN TRUE ELSE @]
Fail == /\ s.pending > 0
        /\ s' = [s EXCEPT !.pending = 0, !.uncertainty = TRUE, !.success = FALSE]
Drain == /\ s.held /\ ~s.physicalDone
         /\ s' = [s EXCEPT !.physicalDone = TRUE, !.released = TRUE]
ReplaceAssignment == /\ s.assignment = 1 /\ s.pending = 0
                     /\ s' = [s EXCEPT !.assignment = 2, !.success = FALSE]
NewIncarnation == /\ s.incarnation = 1 /\ s.pending = 0 /\ s.cursor > 0
                  /\ s' = [s EXCEPT !.incarnation = 2, !.success = FALSE,
                           !.records = IF InheritIncarnation THEN @ ELSE {},
                           !.applied = IF InheritIncarnation THEN @ ELSE {},
                           !.cursor = IF InheritIncarnation THEN @ ELSE 0]
Replay == /\ s.pending = 0 /\ ~s.restarted
          /\ s' = [s EXCEPT !.cursor = IF LoseCheckpoint THEN 0 ELSE Through(s.records),
                   !.applied = s.records, !.success = FALSE, !.restarted = TRUE]
Next == (\E n \in 1..2 : Stage(n)) \/ Force \/ Apply \/ Fail \/ Drain \/ Replay
        \/ ReplaceAssignment \/ NewIncarnation
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.pending \in 0..2 /\ s.cursor \in 0..2
          /\ s.records \subseteq {1, 2} /\ s.applied \subseteq {1, 2}
CursorHasNoHole == s.applied = 1..s.cursor
CursorNeedsDurableEffect == 1..s.cursor \subseteq s.records
ReplayKeepsCursor == s.restarted => s.cursor = Through(s.records)
OldCustodyWaitsPhysical == s.released => s.physicalDone
ApplyUsesInitialAssignment == s.success => s.assignment = 1
CursorUsesActualIncarnation == s.cursor > 0 => s.cursorIncarnation = s.incarnation
=============================================================================
