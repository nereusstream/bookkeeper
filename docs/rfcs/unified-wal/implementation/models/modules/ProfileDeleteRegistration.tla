-------------------- MODULE ProfileDeleteRegistration --------------------
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
EXTENDS Integers
CONSTANTS IgnoreLocalCursor, SkipHeadCheck, DefaultMissing, IgnoreGeneration, IgnoreIncarnation, DemoteOnGrowth
VARIABLE s
vars == <<s>>
(* Initial finite single-stream assignment. Local applied effects, committed
   journal/head and ZK multi atomicity are inputs. No TLS/fsync, snapshot,
   handoff, online driver, physical completion or full A+C+D proof. *)
Init == s = [head |-> 0, local |-> 0, prepared |-> -1, cut |-> -1, registered |-> FALSE,
             known |-> TRUE, generation |-> 1, incarnation |-> 1, badGrowth |-> FALSE]
Event == /\ s.head < 2
         /\ s' = [s EXCEPT !.head = @ + 1,
                  !.badGrowth = @ \/ (DemoteOnGrowth /\ s.registered),
                  !.registered = IF DemoteOnGrowth THEN FALSE ELSE @]
Apply == /\ s.known /\ s.generation = 1 /\ s.incarnation = 1 /\ s.local < s.head
         /\ s' = [s EXCEPT !.local = @ + 1]
Observe == /\ s.prepared = -1 /\ ~s.registered
           /\ (s.known \/ DefaultMissing)
           /\ (s.generation = 1 \/ IgnoreGeneration)
           /\ (s.incarnation = 1 \/ IgnoreIncarnation)
           /\ (s.local = s.head \/ IgnoreLocalCursor)
           /\ s' = [s EXCEPT !.prepared = s.head]
Register == /\ s.prepared >= 0 /\ ~s.registered
            /\ (s.head = s.prepared \/ SkipHeadCheck)
            /\ (s.known \/ DefaultMissing)
            /\ (s.generation = 1 \/ IgnoreGeneration)
            /\ (s.incarnation = 1 \/ IgnoreIncarnation)
            /\ s' = [s EXCEPT !.registered = TRUE, !.cut = s.head]
RetryStale == /\ s.prepared >= 0 /\ ~s.registered /\ s.prepared # s.head
              /\ s' = [s EXCEPT !.prepared = -1]
Missing == /\ ~s.registered /\ s.known /\ s' = [s EXCEPT !.known = FALSE]
ChangedGeneration == /\ ~s.registered /\ s.generation = 1
                     /\ s' = [s EXCEPT !.generation = 2, !.prepared = -1]
ChangedIncarnation == /\ ~s.registered /\ s.incarnation = 1
                      /\ s' = [s EXCEPT !.incarnation = 2, !.prepared = -1]
Next == Event \/ Apply \/ Observe \/ Register \/ RetryStale \/ Missing \/ ChangedGeneration \/ ChangedIncarnation
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.head \in 0..2 /\ s.local \in 0..2 /\ s.prepared \in -1..2 /\ s.cut \in -1..2
WritableCoveredPublicationCut == s.registered => s.local >= s.cut
WritableHasAssignment == s.registered => s.known
WritableHasEffectiveGeneration == s.registered => s.generation = 1
WritableUsesOriginalIncarnation == s.registered => s.incarnation = 1
OrdinaryHeadGrowthKeepsRegistration == ~s.badGrowth
=============================================================================
