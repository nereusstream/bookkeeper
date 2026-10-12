-------------------- MODULE ProfilePhysicalReceipt --------------------
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
CONSTANTS IgnoreScope, IgnoreCommit, IgnoreBirth, QuorumOnly, IgnoreStop, EarlyRelease
VARIABLE s
vars == <<s>>
Targets == {1, 2, 3}
(* Native UIC8 reuse, exact metadata/event scope, immutable ZK commit and actual
   cold procedure return are external Java facts, not produced by this model.
   No secure erase, terminal decommission or history-GC authority is modeled. *)
Init == s \in [scope : BOOLEAN, reusable : SUBSET Targets, bound : {TRUE},
               accepted : {{}}, committed : {{}}, seen : {{}}, current : {0},
               held : {FALSE}, returned : {FALSE}, stopped : {FALSE},
               startedAfterStop : {FALSE}, observedBadBirth : {FALSE},
               releasedEarly : {FALSE}, reported : {FALSE},
               reportBirth : {TRUE}, reportStored : {FALSE}]
Start(t) == /\ t \in s.reusable \ s.accepted /\ ~s.held
            /\ (s.scope \/ IgnoreScope) /\ (~s.stopped \/ IgnoreStop)
            /\ s' = [s EXCEPT !.accepted = @ \cup {t}, !.current = t,
                      !.held = TRUE, !.returned = FALSE,
                      !.startedAfterStop = @ \/ s.stopped]
Commit(t) == /\ t \in s.accepted \ s.committed
             /\ s' = [s EXCEPT !.committed = @ \cup {t}]
Return == /\ s.held /\ ~s.returned /\ s' = [s EXCEPT !.returned = TRUE]
Release == /\ s.held /\ (s.returned \/ EarlyRelease)
           /\ s' = [s EXCEPT !.held = FALSE, !.current = 0,
                     !.releasedEarly = @ \/ ~s.returned]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
Recreate == /\ s.bound /\ s' = [s EXCEPT !.bound = FALSE]
Observe(t) == /\ t \notin s.seen /\ (s.bound \/ IgnoreBirth)
              /\ (t \in s.committed \/ (IgnoreCommit /\ t \in s.reusable))
              /\ s' = [s EXCEPT !.seen = @ \cup {t}, !.observedBadBirth = @ \/ ~s.bound]
Report == /\ ~s.reported /\ ~s.stopped /\ (s.bound \/ IgnoreBirth)
          /\ (IF QuorumOnly THEN Cardinality(s.seen) >= 2 ELSE Targets \subseteq s.seen)
          /\ s' = [s EXCEPT !.reported = TRUE, !.reportBirth = s.bound,
                    !.reportStored = Targets \subseteq s.committed]
Next == (\E t \in Targets : Start(t) \/ Commit(t) \/ Observe(t))
        \/ Return \/ Release \/ Stop \/ Recreate \/ Report
Spec == Init /\ [][Next]_vars
TypeOK == s.current \in Targets \cup {0}
ReceiptWritesNeedScope == s.accepted # {} => s.scope
ReceiptObservationsNeedCommit == s.seen \subseteq s.committed
ReceiptObservationsNeedBirth == ~s.observedBadBirth
ReceiptCompletionNeedsEveryStoredTarget == s.reported => s.reportStored
ReceiptCompletionNeedsBirth == s.reported => s.reportBirth
ReceiptStopStartsNoPublication == ~s.startedAfterStop
ReceiptReleaseWaitsProcedure == ~s.releasedEarly
=============================================================================
