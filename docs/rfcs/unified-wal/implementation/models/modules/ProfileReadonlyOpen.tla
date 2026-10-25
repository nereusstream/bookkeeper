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
               cancelledRead : {FALSE}, exposed : {FALSE}, drained : {FALSE}]
Start == /\ s.phase = "idle" /\ s' = [s EXCEPT !.phase = "construct", !.permit = TRUE]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Restore == /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
CancelOpen == /\ s.phase \in {"construct", "checked"} /\ ~s.stopped
              /\ s' = [s EXCEPT !.stopped = TRUE, !.permit = IF ReleaseOnCancel THEN FALSE ELSE @,
                        !.drained = IF ReleaseOnCancel THEN TRUE ELSE @]
Check == /\ s.phase = "construct"
         /\ s' = [s EXCEPT !.phase = "checked",
                   !.gate = (~s.deleted \/ IgnoreDelete) /\ (s.birth = 0 \/ IgnoreBirth)
                               /\ (s.peer = 0 \/ IgnoreIncarnation),
                   !.gateDeleted = s.deleted, !.gateBirth = s.birth, !.gatePeer = s.peer]
Publish == /\ s.phase = "checked" \/ (SkipFinalGate /\ s.phase = "construct")
           /\ s' = [s EXCEPT !.phase = "done", !.permit = FALSE, !.drained = TRUE,
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
DrainNeedsActualOpenTerminal == s.drained => s.phase = "done" /\ ~s.permit
ActualReadOwnsPermit == s.io = 1 => s.readPermit
ReadonlyHasFixedBoundary == s.delivered => s.prefix = 2 /\ s.length = 48
ReadonlyDoesNotExposeSuffix == ~s.exposed
ReadonlyDoesNotMutate == ~s.mutation

(* One SDK-owned open/reader. The original Spec and its cold-open drain retain their meaning.
   Actual cold, READ output, handle, lease and pool terminals are abstract Java inputs;
   caller future completion/cancellation and interrupted waits do not supply those facts.
   This is finite safety only: no fairness, liveness, fsync, TLS or resource-budget claim. *)
SDKInit == s \in [phase : {"idle"}, deleted : {FALSE}, birth : {0}, peer : {0, 1},
                  gate : {FALSE}, gateDeleted : {FALSE}, gateBirth : {0}, gatePeer : {0},
                  stopped : {FALSE}, permit : {FALSE}, delivered : {FALSE}, prefix : {2}, length : {48},
                  mutation : {FALSE}, read : {"idle"}, io : {0}, readPermit : {FALSE},
                  cancelledRead : {FALSE}, exposed : {FALSE}, drained : {FALSE},
                  sdkClosing : {FALSE}, sdkHandle : {"none"}, sdkLease : {"none"}, sdkPool : {"open"},
                  sdkDependencies : {"held"}, sdkClosed : {FALSE}, sdkOpenCallerDone : {FALSE},
                  sdkCloseWait : {"none"}, sdkCloseSource : {"none"}]
SDKStart == /\ ~s.sdkClosing /\ s.phase = "idle"
            /\ s' = [s EXCEPT !.phase = "construct", !.permit = TRUE, !.sdkLease = "held"]
SDKConstructHandle == /\ s.phase = "construct" /\ s.sdkHandle = "none"
                      /\ s' = [s EXCEPT !.sdkHandle = "local"]
SDKCheck == /\ s.sdkHandle = "local" /\ Check
SDKCancelOpen == /\ s.phase \in {"construct", "checked"} /\ ~s.sdkOpenCallerDone
                 /\ s' = [s EXCEPT !.stopped = TRUE, !.sdkOpenCallerDone = TRUE]
SDKCompleteOpenCaller == /\ s.phase \in {"construct", "checked"} /\ ~s.sdkOpenCallerDone
                         /\ s' = [s EXCEPT !.sdkOpenCallerDone = TRUE]
SDKPublish == /\ s.phase = "checked"
              /\ LET deliver == s.gate /\ ~s.stopped /\ ~s.sdkClosing /\ ~s.sdkOpenCallerDone
                 IN s' = [s EXCEPT !.phase = "done", !.delivered = deliver,
                           !.permit = ~deliver, !.drained = deliver, !.sdkOpenCallerDone = TRUE,
                           !.sdkHandle = IF deliver THEN "live" ELSE "closing"]
SDKRead == /\ ~s.sdkClosing /\ s.sdkHandle = "live" /\ Read
SDKDetachReadCaller == /\ s.read = "pending" /\ ~s.cancelledRead
                       /\ s' = [s EXCEPT !.cancelledRead = TRUE]
SDKCloseHandle == /\ s.sdkHandle = "live"
                  /\ s' = [s EXCEPT !.sdkHandle = "closing"]
SDKHandleTerminal == /\ s.sdkHandle = "closing" /\ s.io = 0 /\ ~s.readPermit
                     /\ s' = [s EXCEPT !.sdkHandle = "terminal",
                               !.permit = IF ~s.delivered THEN FALSE ELSE @,
                               !.drained = IF ~s.delivered THEN TRUE ELSE @]
SDKRequestClose == /\ ~s.sdkClosed
                   /\ s' = [s EXCEPT !.sdkClosing = TRUE, !.sdkCloseWait = "waiting",
                             !.sdkCloseSource = "external",
                             !.stopped = IF s.phase \in {"construct", "checked"} THEN TRUE ELSE @,
                             !.sdkHandle = IF @ = "live" THEN "closing" ELSE @]
SDKSelfCloseRejected == /\ ~s.sdkClosing /\ s.sdkLease \in {"held", "closing"}
                       /\ s' = [s EXCEPT !.sdkCloseSource = "self-rejected"]
SDKDetachCloseCaller == /\ s.sdkCloseWait = "waiting"
                        /\ s' = [s EXCEPT !.sdkCloseWait = "detached"]
SDKInterruptClose == /\ s.sdkCloseWait = "waiting"
                     /\ s' = [s EXCEPT !.sdkCloseWait = "interrupted"]
SDKColdTerminal == s.phase = "done" /\ s.drained /\ ~s.permit
SDKDeliveredHandleTerminal == s.sdkHandle = "terminal" /\ s.io = 0 /\ ~s.readPermit
SDKReleaseLease(earlyCold, earlyRead) ==
    /\ s.sdkLease = "held" /\ (s.sdkClosing \/ s.sdkHandle = "terminal")
    /\ (SDKColdTerminal \/ earlyCold)
    /\ (~s.delivered \/ SDKDeliveredHandleTerminal
          \/ (earlyRead /\ s.sdkHandle = "closing" /\ s.io = 1))
    /\ s' = [s EXCEPT !.sdkLease = "closing"]
SDKLeaseTerminal == /\ s.sdkLease = "closing"
                    /\ s' = [s EXCEPT !.sdkLease = "terminal"]
SDKClosePool == /\ s.sdkClosing /\ s.sdkLease \in {"none", "terminal"} /\ s.sdkPool = "open"
                /\ s' = [s EXCEPT !.sdkPool = "closing"]
SDKPoolTerminal == /\ s.sdkPool = "closing"
                   /\ s' = [s EXCEPT !.sdkPool = "terminal"]
SDKReleaseDependencies(earlyPool) ==
    /\ s.sdkClosing /\ s.sdkLease \in {"none", "terminal"} /\ s.sdkDependencies = "held"
    /\ (s.sdkPool = "terminal" \/ (earlyPool /\ s.sdkPool = "closing"))
    /\ s' = [s EXCEPT !.sdkDependencies = "releasing"]
SDKDependenciesTerminal == /\ s.sdkDependencies = "releasing"
                           /\ s' = [s EXCEPT !.sdkDependencies = "terminal", !.sdkClosed = TRUE]
SDKNext(earlyCold, earlyRead, earlyPool) ==
    SDKStart \/ Delete \/ Restore \/ SDKConstructHandle \/ SDKCheck \/ SDKCancelOpen
    \/ SDKCompleteOpenCaller \/ SDKPublish \/ SDKRead \/ SDKDetachReadCaller \/ FinishRead
    \/ SDKCloseHandle \/ SDKHandleTerminal \/ SDKRequestClose \/ SDKSelfCloseRejected
    \/ SDKDetachCloseCaller \/ SDKInterruptClose \/ SDKReleaseLease(earlyCold, earlyRead)
    \/ SDKLeaseTerminal \/ SDKClosePool \/ SDKPoolTerminal \/ SDKReleaseDependencies(earlyPool)
    \/ SDKDependenciesTerminal
SDKSpec == SDKInit /\ [][SDKNext(FALSE, FALSE, FALSE)]_vars
SDKEarlyColdSpec == SDKInit /\ [][SDKNext(TRUE, FALSE, FALSE)]_vars
SDKEarlyReadSpec == SDKInit /\ [][SDKNext(FALSE, TRUE, FALSE)]_vars
SDKEarlyPoolSpec == SDKInit /\ [][SDKNext(FALSE, FALSE, TRUE)]_vars
SDKTypeOK == TypeOK /\ s.sdkClosing \in BOOLEAN /\ s.sdkClosed \in BOOLEAN
             /\ s.sdkHandle \in {"none", "local", "live", "closing", "terminal"}
             /\ s.sdkLease \in {"none", "held", "closing", "terminal"}
             /\ s.sdkPool \in {"open", "closing", "terminal"}
             /\ s.sdkDependencies \in {"held", "releasing", "terminal"}
             /\ s.sdkOpenCallerDone \in BOOLEAN
             /\ s.sdkCloseWait \in {"none", "waiting", "detached", "interrupted"}
             /\ s.sdkCloseSource \in {"none", "external", "self-rejected"}
SDKColdOpenPinsLease == (s.phase \in {"construct", "checked"}
                         \/ (s.phase = "done" /\ ~s.delivered /\ s.sdkHandle = "closing"))
                        => s.sdkLease = "held"
SDKHandlePinsLease == s.sdkHandle \in {"local", "live", "closing"} => s.sdkLease = "held"
SDKActualReadPinsLease == s.io = 1 => s.sdkLease = "held"
SDKPoolWaitsForLease == s.sdkPool # "open" => s.sdkLease \in {"none", "terminal"}
SDKDependenciesWaitForPool == s.sdkDependencies # "held" => s.sdkPool = "terminal"
SDKClosedNeedsActualDependencies == s.sdkClosed => s.sdkDependencies = "terminal"
SDKSelfCloseRejectsBeforeStop == s.sdkCloseSource = "self-rejected"
                               => ~s.sdkClosing /\ s.sdkPool = "open"
                                   /\ s.sdkDependencies = "held" /\ ~s.sdkClosed
=============================================================================
