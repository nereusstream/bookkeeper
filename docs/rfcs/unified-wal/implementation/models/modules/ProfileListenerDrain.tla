-------------------- MODULE ProfileListenerDrain --------------------
(* Licensed to the Apache Software Foundation (ASF) under one
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
   under the License. *)
EXTENDS Naturals
CONSTANTS EarlyDrain, SkipWorkerBarrier, AdmitAfterStop, DisconnectReturnsCredit, CancelCompletesDrain
VARIABLE s
vars == <<s>>
(* Native channel closure, queued worker registration, tracked credit ownership
   and actual cold/output/backing terminal events are inputs produced by Java.
   This projection does not close borrowed storage/loops or prove TLS/fsync. *)
Init == s = [stopped |-> FALSE, parent |-> TRUE, child |-> TRUE, queued |-> TRUE,
             late |-> FALSE, barrier |-> FALSE, credit |-> TRUE, cold |-> TRUE,
             output |-> TRUE, cancelled |-> FALSE, reopened |-> FALSE, done |-> FALSE]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
CloseParent == /\ s.stopped /\ s.parent /\ s' = [s EXCEPT !.parent = FALSE]
Disconnect == /\ s.stopped /\ s.child
              /\ s' = [s EXCEPT !.child = FALSE, !.credit = IF DisconnectReturnsCredit THEN FALSE ELSE @]
RegisterQueued == /\ ~s.parent /\ s.queued
                  /\ s' = [s EXCEPT !.queued = FALSE, !.late = TRUE]
CloseLate == /\ s.late /\ s' = [s EXCEPT !.late = FALSE]
WorkerBarrier == /\ ~s.parent /\ ~s.queued /\ ~s.barrier
                 /\ s' = [s EXCEPT !.barrier = TRUE]
ColdTerminal == /\ s.cold /\ s' = [s EXCEPT !.cold = FALSE]
OutputTerminal == /\ s.output /\ s' = [s EXCEPT !.output = FALSE]
ReturnCredit == /\ ~s.cold /\ ~s.output /\ s.credit /\ s' = [s EXCEPT !.credit = FALSE]
AttemptAdmission == /\ s.stopped /\ ~s.reopened /\ AdmitAfterStop
                    /\ s' = [s EXCEPT !.reopened = TRUE]
CancelCaller == /\ s.stopped /\ ~s.cancelled
                /\ s' = [s EXCEPT !.cancelled = TRUE, !.done = IF CancelCompletesDrain THEN TRUE ELSE @]
Complete == /\ s.stopped /\ ~s.done
            /\ ((~s.parent /\ ~s.child /\ ~s.credit
                 /\ ((s.barrier /\ ~s.queued /\ ~s.late) \/ SkipWorkerBarrier)) \/ EarlyDrain)
            /\ s' = [s EXCEPT !.done = TRUE]
Next == Stop \/ CloseParent \/ Disconnect \/ RegisterQueued \/ CloseLate \/ WorkerBarrier
        \/ ColdTerminal \/ OutputTerminal \/ ReturnCredit \/ AttemptAdmission \/ CancelCaller \/ Complete
Spec == Init /\ [][Next]_vars
TypeOK == \A field \in DOMAIN s : s[field] \in BOOLEAN
RetiredListenerCannotAdmit == ~s.reopened
DisconnectKeepsActualCredit == (s.cold \/ s.output) => s.credit
ListenerDoneHasNoTrackedCustody == s.done => ~s.parent /\ ~s.child /\ ~s.credit
ListenerDoneWaitsWorkerHandoff == s.done => s.barrier /\ ~s.queued /\ ~s.late
CallerCannotCompleteActualListenerDrain == (s.cancelled /\ (s.parent \/ s.child \/ s.credit)) => ~s.done
=====================================================================
