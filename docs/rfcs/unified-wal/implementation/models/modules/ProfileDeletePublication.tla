-------------------- MODULE ProfileDeletePublication --------------------
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
CONSTANTS PrefixBeforeIndex, EarlyLogical, ConsumeInert, IgnoreBirth,
          StartAfterStop, ReleaseEarly, OverlapProbe
VARIABLE s
vars == <<s>>
Targets == {1, 2}
(* Two targets, one logical reference and a publication prefix. ZK atomicity,
   fenced-CLOSED, immutable source and actual I/O/owner terminals are inputs.
   No TLS/fsync/hardware, full 1088-target run, GC/handoff or A+C+D proof. *)
Init == s = [indexed |-> {}, prefix |-> 0, logical |-> FALSE, birth |-> 1,
             reported |-> FALSE, reportBound |-> TRUE, applied |-> {},
             stopped |-> FALSE, afterStop |-> FALSE, owned |-> 0, cold |-> FALSE,
             submitted |-> FALSE, physical |-> FALSE, drained |-> FALSE]
Prepare == /\ s.prefix < 2 /\ s.birth = 1
           /\ s' = [s EXCEPT !.indexed = IF PrefixBeforeIndex THEN @ ELSE @ \cup {s.prefix + 1},
                    !.prefix = @ + 1]
Activate == /\ ~s.logical /\ (s.prefix = 2 \/ EarlyLogical) /\ s.birth = 1
            /\ s' = [s EXCEPT !.logical = TRUE]
RecreatePrefix == /\ s.birth = 1 /\ s' = [s EXCEPT !.birth = 2]
Report == /\ s.logical /\ ~s.reported /\ (s.birth = 1 \/ IgnoreBirth)
          /\ s' = [s EXCEPT !.reported = TRUE, !.reportBound = s.birth = 1]
Start == /\ (~s.stopped \/ StartAfterStop)
         /\ (s.owned = 0 \/ OverlapProbe) /\ s.owned < 2
         /\ s' = [s EXCEPT !.owned = @ + 1, !.afterStop = @ \/ s.stopped, !.cold = FALSE,
                  !.submitted = 1 \in s.indexed /\ (s.logical \/ ConsumeInert) /\ s.birth = 1,
                  !.physical = FALSE]
ColdReturn == /\ s.owned > 0 /\ ~s.cold /\ s' = [s EXCEPT !.cold = TRUE]
Physical == /\ s.owned > 0 /\ s.submitted /\ ~s.physical
            /\ s' = [s EXCEPT !.physical = TRUE, !.applied = @ \cup {1}]
Finish == /\ s.owned > 0 /\ s.cold /\ (~s.submitted \/ s.physical \/ ReleaseEarly)
          /\ s' = [s EXCEPT !.owned = 0, !.drained = s.stopped]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE,
                               !.drained = s.owned = 0]
Next == Prepare \/ Activate \/ RecreatePrefix \/ Report \/ Start \/ ColdReturn \/ Physical \/ Finish \/ Stop
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.indexed \subseteq Targets /\ s.prefix \in 0..2 /\ s.owned \in 0..2
LogicalHasEveryDelivery == s.logical => s.indexed = Targets
AppliedNeedsLogicalActivation == s.applied # {} => s.logical
ReportedPublicationHasBirth == s.reported => s.reportBound
StoppedStartsNoProbe == ~s.afterStop
FollowerDrainWaitsActual == s.drained /\ s.submitted => s.cold /\ s.physical
OneOwnedProbe == s.owned <= 1
=============================================================================
