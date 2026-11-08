-------------------- MODULE ClosedReconstructedTail --------------------
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
EXTENDS Integers
CONSTANT Fault
VARIABLE s
vars == <<s>>

Init ==
    \E full, closedEntries, committedCopy, healthy, exactReady \in BOOLEAN :
        s = [full |-> full, closedEntries |-> closedEntries, committedCopy |-> committedCopy,
             healthy |-> healthy, exactReady |-> exactReady, phase |-> "OBSERVED",
             published |-> FALSE, source |-> "NONE", end |-> 0, cutoff |-> FALSE]

Admit ==
    /\ s.phase = "OBSERVED" /\ ~s.healthy /\ s.closedEntries
    /\ (s.full \/ Fault = "RequiredOnly")
    /\ (s.committedCopy \/ Fault = "IgnoreCopy")
    /\ s' = [s EXCEPT !.phase = "ADMITTED"]

Publish ==
    /\ s.phase = "ADMITTED"
    /\ s' = [s EXCEPT !.phase = "PUBLISHED", !.published = TRUE]

ForceAction ==
    /\ s.phase = "PUBLISHED" \/ (Fault = "BeforePublication" /\ s.phase = "ADMITTED")
    /\ s' = [s EXCEPT !.phase = "FORCED"]

Replay ==
    /\ s.phase = "FORCED"
    /\ s' = [s EXCEPT !.phase = "RESOLVED", !.end = 2, !.cutoff = TRUE,
                  !.source = IF Fault = "PretendHealthy" THEN "HEALTHY_CLOSE" ELSE "RECONSTRUCTION"]

FirstOpen ==
    /\ s.phase = "RESOLVED" /\ s.exactReady
    /\ s' = [s EXCEPT !.phase = "OPEN"]

Next == Admit \/ Publish \/ ForceAction \/ Replay \/ FirstOpen
Spec == Init /\ [][Next]_vars
AdmissionHasFullCurrentExtent ==
    s.phase # "OBSERVED" => s.full /\ s.closedEntries /\ ~s.healthy
AdmissionHasCommittedCopy == s.phase # "OBSERVED" => s.committedCopy
ResolutionFollowsPublication == s.phase \in {"FORCED", "RESOLVED", "OPEN"} => s.published
ReplayPublishesReconstructedEnd ==
    s.phase \in {"RESOLVED", "OPEN"} => s.end = 2 /\ s.cutoff /\ s.source = "RECONSTRUCTION"
OpenRequiresExactReady == s.phase = "OPEN" => s.exactReady
============================================================================
