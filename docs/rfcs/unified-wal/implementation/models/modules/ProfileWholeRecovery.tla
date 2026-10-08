-------------------- MODULE ProfileWholeRecovery --------------------
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
EXTENDS Integers, FiniteSets
CONSTANTS AckQuorum, TrustUnverified, Reselect, NewContext, NewIntentOnRetry,
          WorkAfterStop, ReleaseBeforeChild, SuccessBeforeClosed, CountUnknown, DropHistory
VARIABLE s
vars == <<s>>
(* Durable metadata/point proof and per-target actual outcomes are inputs.
   Retry adopts one published context/selection/intent. No AutoRecovery marker is produced or cleared here. *)
Init == s \in [phase : {"open"}, proof : BOOLEAN, replies : [1..3 -> BOOLEAN],
               ctx : {0}, selected : {-1}, intents : {[i \in 1..3 |-> 0]}, head : {{}},
               member : {1}, child : {FALSE}, reserve : {FALSE}, stopped : {FALSE},
               workedStopped : {FALSE}, closed : {FALSE}, success : {FALSE}, retried : {FALSE}]
Freeze == /\ s.phase = "open" /\ s' = [s EXCEPT !.phase = "verify", !.ctx = 1, !.reserve = TRUE]
Select == /\ s.phase = "verify" /\ (s.proof \/ TrustUnverified)
          /\ s' = [s EXCEPT !.phase = "selected", !.selected = 2]
DeferProof == /\ s.phase = "verify" /\ ~s.proof
              /\ s' = [s EXCEPT !.phase = "done", !.reserve = FALSE]
StartTarget == /\ s.phase = "selected" /\ s.member <= 3 /\ (~s.stopped \/ WorkAfterStop)
               /\ s' = [s EXCEPT !.phase = "child", !.child = TRUE, !.workedStopped = s.stopped,
                         !.intents[s.member] = IF @ = 0 THEN 1 ELSE @]
FinishTarget == /\ s.phase = "child" /\ s.child
                /\ s' = [s EXCEPT !.child = FALSE, !.member = @ + 1,
                          !.head = IF s.replies[s.member] \/ CountUnknown THEN @ \cup {s.member} ELSE @,
                          !.phase = "selected"]
Stop == /\ s.phase \in {"verify", "selected", "child", "closing", "submitted"} /\ ~s.stopped
        /\ s' = [s EXCEPT !.stopped = TRUE, !.reserve = IF ReleaseBeforeChild THEN FALSE ELSE @]
FinishMembers == /\ s.phase = "selected" /\ s.member = 4
                 /\ s' = [s EXCEPT !.phase = "closing"]
SubmitClose == /\ s.phase = "closing" /\ ~s.stopped /\ Cardinality(s.head) >= AckQuorum
               /\ s' = [s EXCEPT !.phase = "submitted"]
Close == /\ s.phase = "submitted"
         /\ s' = [s EXCEPT !.closed = TRUE, !.phase = "done", !.reserve = FALSE,
                   !.success = ~s.stopped,
                   !.head = IF DropHistory THEN {} ELSE @]
Defer == /\ s.phase \in {"closing", "selected"} /\ (s.stopped \/ Cardinality(s.head) < AckQuorum)
         /\ ~s.child /\ s' = [s EXCEPT !.phase = "done", !.reserve = FALSE]
Retry == /\ s.phase = "done" /\ ~s.closed /\ s.selected # -1 /\ ~s.retried
         /\ s' = [s EXCEPT !.phase = "selected", !.member = 1, !.stopped = FALSE, !.reserve = TRUE,
                   !.retried = TRUE, !.ctx = IF NewContext THEN 2 ELSE @,
                   !.selected = IF Reselect THEN 3 ELSE @,
                   !.intents = IF NewIntentOnRetry THEN [i \in 1..3 |-> IF s.intents[i] = 1 THEN 2 ELSE 0] ELSE @]
PrematureSuccess == /\ SuccessBeforeClosed /\ s.phase = "closing"
                    /\ s' = [s EXCEPT !.success = TRUE]
Next == Freeze \/ Select \/ DeferProof \/ StartTarget \/ FinishTarget \/ Stop \/ FinishMembers
        \/ SubmitClose \/ Close \/ Defer \/ Retry \/ PrematureSuccess
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"open", "verify", "selected", "child", "closing", "submitted", "done"}
SelectionHasVerifiedProof == s.selected # -1 => s.proof
SelectionIsFixed == s.selected \in {-1, 2}
ContextIsAdopted == s.ctx \in {0, 1}
TargetIdentityIsStable == \A i \in 1..3 : s.intents[i] \in {0, 1}
StoppedDoesNotStartWork == ~s.workedStopped
ActualChildOwnsGate == s.child => s.reserve
SuccessHasMatchingClosed == s.success => s.closed /\ s.selected = 2 /\ s.ctx = 1
QuorumCountsActualDurable == \A i \in s.head : s.replies[i]
ClosedKeepsCleanupHistory == s.closed => Cardinality(s.head) >= AckQuorum
=============================================================================
