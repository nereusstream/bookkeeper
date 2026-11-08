-------------------- MODULE ClosedSealedReuse --------------------
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

Extent == 1..2
(* Native replay supplies the close provenance and selected coordinate.
   Page 2 can contain an orphan written by the next OPEN past a partial seal. *)
Init ==
    \E healthy, matchesGeneration, beforeOpen \in BOOLEAN,
       end \in 1..2, selector \in 1..3, page2 \in {"CURRENT", "ORPHAN", "ZERO", "STALE"} :
        s = [healthy |-> healthy, matchesGeneration |-> matchesGeneration, beforeOpen |-> beforeOpen,
             end |-> end, selector |-> selector, page2 |-> page2, phase |-> "OBSERVED", current |-> {}]

ExactSeal == s.healthy /\ s.matchesGeneration /\ s.beforeOpen
FullSeal == ExactSeal /\ s.end = 2

Start ==
    /\ s.phase = "OBSERVED"
    /\ (ExactSeal \/ Fault = "IgnoreProvenance")
    /\ (s.end = 2 \/ (Fault = "AllowPartial" /\ s.page2 = "ORPHAN"))
    /\ s' = [s EXCEPT !.phase = "SCAN"]

ReadCurrent(i) ==
    /\ s.phase = "SCAN" /\ i \notin s.current /\ i <= s.end
    /\ i = 1 \/ s.page2 = "CURRENT"
    /\ s' = [s EXCEPT !.current = @ \cup {i}]

Admit ==
    /\ s.phase = "SCAN" /\ s.current = 1..s.end
    /\ s.selector \in s.current \/ Fault = "IgnoreSelector"
    /\ s' = [s EXCEPT !.phase = "ADMITTED"]

Next == Start \/ (\E i \in Extent : ReadCurrent(i)) \/ Admit
Spec == Init /\ [][Next]_vars
AdmittedHasFullHealthySeal == s.phase = "ADMITTED" => FullSeal
AdmittedHasSelectedCurrentBytes == s.phase = "ADMITTED" => s.current = Extent /\ s.selector \in s.current
============================================================================
