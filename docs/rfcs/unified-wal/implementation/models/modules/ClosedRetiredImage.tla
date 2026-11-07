-------------------- MODULE ClosedRetiredImage --------------------
\* Licensed to the Apache Software Foundation (ASF) under one or more
\* contributor license agreements. See the NOTICE file distributed with
\* this work for additional information regarding copyright ownership.
\* The ASF licenses this file to you under the Apache License, Version 2.0
\* (the "License"); you may not use this file except in compliance
\* with the License. You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.
EXTENDS Integers, FiniteSets
CONSTANT Fault
VARIABLE s
vars == <<s>>

Slots == 1..4
CurrentGrants == {2, 4}
RequiredEntries == {1, 2}
(* Replay proves a retirement for slot 1 with sequence 1. Slot 3 has no
   grant and can have a distinct empty FREE, which proves no sequence. *)
Init ==
    \E oldEntry, emptyFree, foreign, missing, gap \in BOOLEAN :
        s = [oldEntry |-> oldEntry, emptyFree |-> emptyFree, foreign |-> foreign,
             missing |-> missing, gap |-> gap, phase |-> "SCAN",
             read |-> {}, entries |-> {}, sequences |-> {}, unexplained |-> FALSE]

Retired == {1} \cup (IF s.emptyFree THEN {3} ELSE {})
CurrentEntries == IF s.missing THEN {2} ELSE RequiredEntries
CurrentSequences == IF s.gap THEN {2, 4} ELSE {2, 3}
RetiredSequences == {1}

ReadSlot(i) ==
    /\ s.phase = "SCAN" /\ i \notin s.read
    /\ s' = [s EXCEPT !.read = @ \cup {i},
          !.entries = CASE i = 2 -> @ \cup {IF s.missing THEN 2 ELSE 1}
                        [] i = 4 -> @ \cup {2}
                        [] i = 1 /\ s.oldEntry /\ Fault = "ReadRetired" -> @ \cup {1}
                        [] OTHER -> @,
          !.sequences = CASE i = 2 -> @ \cup {2}
                          [] i = 4 -> @ \cup {IF s.gap THEN 4 ELSE 3}
                          [] OTHER -> @,
          !.unexplained = @ \/ (s.foreign /\ i \notin (Retired \cup CurrentGrants) /\ Fault # "SkipUnowned")]

Finish ==
    /\ s.phase = "SCAN" /\ s.read = Slots
    /\ s' = [s EXCEPT !.phase = IF s.entries = RequiredEntries /\ ~s.unexplained
                                    /\ (s.sequences \cup RetiredSequences = 1..3 \/ Fault = "BridgeGap")
                                THEN "ADMITTED" ELSE "REFUSED"]

Next == (\E i \in Slots : ReadSlot(i)) \/ Finish
Spec == Init /\ [][Next]_vars
AdmittedHasCurrentEntries == s.phase = "ADMITTED" => CurrentEntries = RequiredEntries
AdmittedSkipsOnlyProvenExtents ==
    s.phase = "ADMITTED" => (s.read = Slots /\ (~s.foreign \/ 3 \in Retired))
AdmittedHasExactSequences == s.phase = "ADMITTED" => CurrentSequences \cup RetiredSequences = 1..3
============================================================================
