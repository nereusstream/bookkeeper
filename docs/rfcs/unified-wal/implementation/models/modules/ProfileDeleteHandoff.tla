-------------------- MODULE ProfileDeleteHandoff --------------------
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
CONSTANTS IgnoreAdmissionCas, IgnoreHeadCas, IgnoreSnapshot, IgnoreCommit, IgnoreStop, EarlyRelease
VARIABLE s
vars == <<s>>
(* Native complete snapshot, logical COMMITTED effects, stream scope and actual ZK
   transactions are supplied Java facts. This model has no G+1 local adoption,
   EFFECTIVE publication, old-route retirement or GC implementation. *)
Init == s \in [head : {1}, headVersion : {1}, admission : {0}, capturedVersion : {0},
               capturedHead : {1}, captured : {FALSE}, snapshot : BOOLEAN,
               committed : BOOLEAN, prepared : {FALSE}, cut : {0}, stopped : {FALSE},
               held : {FALSE}, returned : {FALSE}, releasedEarly : {FALSE},
               stoppedPrepared : {FALSE}]
Capture == /\ ~s.captured
           /\ s' = [s EXCEPT !.captured = TRUE, !.capturedVersion = s.admission,
                     !.capturedHead = s.headVersion, !.held = TRUE]
NewOldEvent == /\ s.captured /\ s.head = 1
               /\ (s.capturedVersion = s.admission \/ IgnoreAdmissionCas)
               /\ s' = [s EXCEPT !.head = 2, !.headVersion = 2]
Prepare == /\ s.captured /\ ~s.prepared /\ (~s.stopped \/ IgnoreStop)
           /\ (s.snapshot \/ IgnoreSnapshot) /\ (s.committed \/ IgnoreCommit)
           /\ (s.capturedHead = s.headVersion \/ IgnoreHeadCas)
           /\ s' = [s EXCEPT !.prepared = TRUE, !.cut = s.capturedHead,
                     !.admission = 1, !.stoppedPrepared = s.stopped]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
Return == /\ s.held /\ ~s.returned /\ s' = [s EXCEPT !.returned = TRUE]
Release == /\ s.held /\ (s.returned \/ EarlyRelease)
           /\ s' = [s EXCEPT !.held = FALSE, !.releasedEarly = ~s.returned]
Next == Capture \/ NewOldEvent \/ Prepare \/ Stop \/ Return \/ Release
Spec == Init /\ [][Next]_vars
TypeOK == s.head \in {1,2} /\ s.admission \in {0,1}
HandoffPreparedHasSnapshot == s.prepared => s.snapshot
HandoffPreparedHasCommittedEffects == s.prepared => s.committed
HandoffPreparedCutIsStable == s.prepared => s.head = s.cut
HandoffStopStartsNoPreparation == ~s.stoppedPrepared
HandoffReleaseWaitsActual == ~s.releasedEarly
=============================================================================
