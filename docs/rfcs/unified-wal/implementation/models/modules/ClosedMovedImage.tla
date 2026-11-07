-------------------- MODULE ClosedMovedImage --------------------
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

(* This original scope has no successful FREE or reuse. Valid COPY bindings
   come from actual Arena replay. Row 1 is intact old ordinary DATA.
   Row 4 is a durable COPY with no applied MOVE, even if its payload matches. *)
Init ==
    s = [phase |-> "IMAGE", present |-> {1, 2}, intact |-> {1, 2}, committed |-> {2}, current |-> 2,
         remaining |-> {}, unexplained |-> {}, sawCurrent |-> FALSE]

MoveAgain ==
    /\ s.phase = "IMAGE" /\ s.current = 2 /\ s.current \in s.present \cap s.intact
    /\ s' = [s EXCEPT !.present = @ \cup {3}, !.intact = @ \cup {3},
                      !.committed = @ \cup {3}, !.current = 3]

WriteOrphan ==
    /\ s.phase = "IMAGE" /\ 4 \notin s.present
    /\ s' = [s EXCEPT !.present = @ \cup {4}, !.intact = @ \cup {4}]

LoseCurrent ==
    /\ s.phase = "IMAGE" /\ s.current \in s.present
    /\ s' = [s EXCEPT !.present = @ \ {s.current}]

ChangeCopyBinding(i) ==
    /\ s.phase = "IMAGE" /\ i \in (s.present \cap s.intact) \ {1}
    /\ s' = [s EXCEPT !.intact = @ \ {i}]

BeginScan ==
    /\ s.phase = "IMAGE"
    /\ s' = [s EXCEPT !.phase = "SCAN", !.remaining = s.present]

Scan(i) ==
    LET exactCopy == i \in s.committed \cap s.intact
        explained == i = 1 \/ exactCopy \/ (Fault = "AcceptOrphan" /\ i = 4 /\ i \in s.intact)
    IN
    /\ s.phase = "SCAN" /\ i \in s.remaining
    /\ s' = [s EXCEPT !.remaining = @ \ {i},
                      !.unexplained = IF explained THEN @ ELSE @ \cup {i},
                      !.sawCurrent = @ \/ (i = s.current /\ exactCopy)]

FinishScan ==
    /\ s.phase = "SCAN" /\ s.remaining = {}
    /\ s' = [s EXCEPT !.phase = IF s.unexplained = {} /\ (s.sawCurrent \/ Fault = "SkipCurrent")
                                THEN "ADMITTED" ELSE "REFUSED"]

Next == MoveAgain \/ WriteOrphan \/ LoseCurrent \/ BeginScan \/ FinishScan
        \/ (\E i \in 1..4 : ChangeCopyBinding(i) \/ Scan(i))
Spec == Init /\ [][Next]_vars
AdmittedHasCurrentCopy == s.phase = "ADMITTED" => s.current \in s.present \cap s.intact
AdmittedExplainsAllCopies == s.phase = "ADMITTED" => s.present \ {1} \subseteq s.committed \cap s.intact
============================================================================
