-------------------- MODULE ProfileJournalCompaction --------------------
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
CONSTANTS SkipSnapshot, SkipEffective, SkipSelectionCAS, DropEffects, SkipPhysicalBirth, ReleaseEarly
VARIABLE s
vars == <<s>>
(* Complete native bytes, actual local adoption/FREE and transaction identity
   are Java-produced facts. No target-history, elapsed-window or backup GC is modeled. *)
Init == s = [complete |-> FALSE, effective |-> FALSE, version |-> 0, observed |-> 0,
             rows |-> {1,2}, effects |-> {1,2}, active |-> FALSE, custody |-> FALSE,
             accepted |-> FALSE, committed |-> FALSE, cancelled |-> FALSE, done |-> FALSE,
             badSnapshot |-> FALSE, badEffective |-> FALSE, badCAS |-> FALSE,
             indexBirth |-> 1, physicallyFree |-> FALSE, physicalVisible |-> FALSE]
Snapshot == /\ ~s.complete /\ s' = [s EXCEPT !.complete = TRUE]
Effective == /\ s.complete /\ ~s.effective /\ s' = [s EXCEPT !.effective = TRUE]
LoseSnapshot == /\ s.effective /\ s.complete /\ ~s.active /\ 1 \in s.rows
                /\ s' = [s EXCEPT !.complete = FALSE]
Accept == /\ ~s.active /\ ~s.done
          /\ (s.complete \/ SkipSnapshot) /\ (s.effective \/ SkipEffective)
          /\ s' = [s EXCEPT !.active = TRUE, !.custody = TRUE, !.accepted = TRUE,
                    !.observed = s.version, !.badSnapshot = ~s.complete, !.badEffective = ~s.effective]
Advance == /\ s.complete /\ s.version = 0 /\ s' = [s EXCEPT !.version = 1]
Commit == /\ s.active /\ ~s.committed /\ (s.version = s.observed \/ SkipSelectionCAS)
          /\ s' = [s EXCEPT !.committed = TRUE, !.rows = s.rows \ {1},
                    !.effects = IF DropEffects THEN s.effects \ {1} ELSE s.effects,
                    !.badCAS = (s.version # s.observed)]
Reject == /\ s.active /\ ~s.committed /\ s.version # s.observed /\ ~SkipSelectionCAS
          /\ s' = [s EXCEPT !.active = FALSE, !.custody = FALSE, !.accepted = FALSE, !.done = TRUE]
Cancel == /\ s.active /\ ~s.cancelled
          /\ s' = [s EXCEPT !.cancelled = TRUE, !.custody = IF ReleaseEarly THEN FALSE ELSE s.custody]
Reply == /\ s.active /\ s.committed
         /\ s' = [s EXCEPT !.active = FALSE, !.custody = FALSE, !.accepted = FALSE, !.done = TRUE]
Free == /\ ~s.physicallyFree /\ s' = [s EXCEPT !.physicallyFree = TRUE]
RecreateIndex == /\ 1 \notin s.rows /\ s.indexBirth = 1
                 /\ s' = [s EXCEPT !.indexBirth = 2, !.physicalVisible = FALSE]
Physical == /\ s.physicallyFree /\ 1 \notin s.rows /\ s.complete /\ 1 \in s.effects
            /\ (s.indexBirth = 1 \/ SkipPhysicalBirth) /\ ~s.physicalVisible
            /\ s' = [s EXCEPT !.physicalVisible = TRUE]
Next == Snapshot \/ Effective \/ LoseSnapshot \/ Accept \/ Advance \/ Commit \/ Reject \/ Cancel \/ Reply
        \/ Free \/ RecreateIndex \/ Physical
Spec == Init /\ [][Next]_vars
TypeOK == s.rows \subseteq {1,2} /\ s.effects \subseteq {1,2} /\ s.version \in 0..1
PrunedHasCompleteSnapshot == ~s.committed \/ ~s.badSnapshot
PrunedHasEffectiveTakeover == ~s.committed \/ ~s.badEffective
PrunedKeepsEveryEffect == {1,2} \ s.rows \subseteq s.effects
PrunedHasNativeSelectionCAS == ~s.badCAS
PhysicalReceiptKeepsOriginalBirth == ~s.physicalVisible \/ s.indexBirth = 1
CancellationKeepsActualCustody == ~s.active \/ s.custody
=============================================================================
