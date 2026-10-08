-------------------- MODULE ProfileReadonlyOpen --------------------
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
CONSTANTS IgnoreDelete, IgnoreBirth, IgnoreIncarnation, SkipFinalGate, ReturnCancelled,
          ReleaseOnCancel, WrongBoundary, ReturnSuffix, AllowMutation
VARIABLE s
vars == <<s>>
(* Cold permission/birth/identity and actual read completion are inputs.
   A delete after the final live check can have a late response; it is not a new access lease. *)
Init == s \in [phase : {"idle"}, deleted : {FALSE}, birth : {0}, peer : {0, 1},
               gate : {FALSE}, gateDeleted : {FALSE}, gateBirth : {0}, gatePeer : {0},
               stopped : {FALSE}, permit : {FALSE}, delivered : {FALSE}, prefix : {2}, length : {48},
               mutation : {FALSE}, read : {"idle"}, io : {0}, readPermit : {FALSE},
               cancelledRead : {FALSE}, exposed : {FALSE}]
Start == /\ s.phase = "idle" /\ s' = [s EXCEPT !.phase = "construct", !.permit = TRUE]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Restore == /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
CancelOpen == /\ s.phase \in {"construct", "checked"} /\ ~s.stopped
              /\ s' = [s EXCEPT !.stopped = TRUE, !.permit = IF ReleaseOnCancel THEN FALSE ELSE @]
Check == /\ s.phase = "construct"
         /\ s' = [s EXCEPT !.phase = "checked",
                   !.gate = (~s.deleted \/ IgnoreDelete) /\ (s.birth = 0 \/ IgnoreBirth)
                               /\ (s.peer = 0 \/ IgnoreIncarnation),
                   !.gateDeleted = s.deleted, !.gateBirth = s.birth, !.gatePeer = s.peer]
Publish == /\ s.phase = "checked" \/ (SkipFinalGate /\ s.phase = "construct")
           /\ s' = [s EXCEPT !.phase = "done", !.permit = FALSE,
                     !.delivered = (s.gate \/ SkipFinalGate) /\ (~s.stopped \/ ReturnCancelled),
                     !.prefix = IF WrongBoundary THEN 3 ELSE @, !.length = IF WrongBoundary THEN 64 ELSE @]
Read == /\ s.delivered /\ s.read = "idle"
        /\ s' = [s EXCEPT !.read = "pending", !.io = 1, !.readPermit = TRUE]
CancelRead == /\ s.read = "pending" /\ ~s.cancelledRead
              /\ s' = [s EXCEPT !.cancelledRead = TRUE, !.readPermit = IF ReleaseOnCancel THEN FALSE ELSE @]
FinishRead == /\ s.read = "pending"
              /\ s' = [s EXCEPT !.read = "done", !.io = 0, !.readPermit = FALSE,
                        !.exposed = ReturnSuffix /\ ~s.cancelledRead]
Mutate == /\ s.delivered /\ AllowMutation /\ s' = [s EXCEPT !.mutation = TRUE]
Next == Start \/ Delete \/ Restore \/ CancelOpen \/ Check \/ Publish \/ Read \/ CancelRead \/ FinishRead \/ Mutate
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"idle", "construct", "checked", "done"} /\ s.io \in 0..1
OpenHadFinalGate == s.delivered => s.gate
OpenHadLiveAdoption == s.delivered => ~s.gateDeleted
OpenHadSameBirth == s.delivered => s.gateBirth = 0
OpenHadExpectedIncarnation == s.delivered => s.gatePeer = 0
CancelledOpenNotReturned == s.delivered => ~s.stopped
ActualOpenOwnsPermit == s.phase \in {"construct", "checked"} => s.permit
ActualReadOwnsPermit == s.io = 1 => s.readPermit
ReadonlyHasFixedBoundary == s.delivered => s.prefix = 2 /\ s.length = 48
ReadonlyDoesNotExposeSuffix == ~s.exposed
ReadonlyDoesNotMutate == ~s.mutation
=============================================================================
