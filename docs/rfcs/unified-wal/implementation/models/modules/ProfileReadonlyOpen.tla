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

(* Independent, finite shared-SDK safety slice: two operation slots, two distinct ledger IDs,
   one Epoch acquisition/lease, and one representative CONTROL and READ per child.
   Private algorithm/handle drain, actual CONTROL wire/output terminal, READ wire terminal,
   response roots/retained views, acquisition observation, cold work and transport terminal are inputs.
   Each child's response root may retain the same native backing as its sibling's root;
   readRefs counts only this child's response/retained-view custody, never a global refCnt.
   A failed acquisition reply may precede actual cold drain after every child has stopped;
   that Epoch blocks admission and its last slot still pins actual cold/transport termination.
   Logical result/cancellation does not discharge any of those actual owners. A non-last
   child can finish while its sibling retains a response; the last slot waits for the Epoch.
   Fresh-origin consume-once remains in the existing model. This slice makes no liveness,
   fsync, TLS, remote-abort, allocator or native-memory-budget claim. *)
SharedSDKChildren == {1, 2}
SharedSDKLedgers == {1, 2}
SharedSDKEmptyChild == [phase |-> "idle", ledger |-> 0, logicalDone |-> FALSE,
                        readyObserved |-> FALSE, privateWork |-> FALSE,
                        control |-> "idle", read |-> "idle", readRefs |-> 0]
SharedSDKActive == {i \in SharedSDKChildren : s.children[i].phase \in {"live", "stopping"}}
SharedSDKActiveCount == (IF 1 \in SharedSDKActive THEN 1 ELSE 0)
                       + (IF 2 \in SharedSDKActive THEN 1 ELSE 0)
SharedSDKOtherActive(i) == SharedSDKActive \ {i}
SharedSDKOwnActualTerminal(i, earlyControl, earlyRead, earlyObservation) ==
    /\ s.children[i].logicalDone /\ ~s.children[i].privateWork
    /\ (s.children[i].readyObserved \/ earlyObservation)
    /\ (s.children[i].control # "pending" \/ earlyControl)
    /\ s.children[i].read # "wire"
    /\ (s.children[i].readRefs = 0 \/ earlyRead)
SharedSDKAllActualTerminal ==
    \A i \in SharedSDKChildren : SharedSDKOwnActualTerminal(i, FALSE, FALSE, FALSE)
                                  \/ s.children[i].phase = "idle"
SharedSDKInit == s = [children |-> [i \in SharedSDKChildren |-> SharedSDKEmptyChild],
                      parent |-> "none", acquisition |-> "none", transportOpen |-> FALSE,
                      coldActualPending |-> FALSE, retiring |-> FALSE,
                      sdkClosing |-> FALSE, pool |-> "open", dependencies |-> "held"]
SharedSDKAdmit(i, ledger) ==
    /\ ~s.sdkClosing /\ ~s.retiring /\ s.parent \in {"none", "held"}
    /\ s.children[i].phase = "idle" /\ SharedSDKActiveCount < 2
    /\ \A j \in SharedSDKActive : s.children[j].ledger # ledger
    /\ s' = [s EXCEPT !.children[i] = [@ EXCEPT !.phase = "live", !.ledger = ledger,
                                               !.privateWork = TRUE],
                       !.parent = "held", !.acquisition = IF @ = "none" THEN "cold" ELSE @,
                       !.coldActualPending = IF s.acquisition = "none" THEN TRUE ELSE @,
                       !.transportOpen = TRUE]
SharedSDKAcquired == /\ s.parent = "held" /\ s.acquisition = "cold" /\ ~s.retiring
                     /\ s' = [s EXCEPT !.acquisition = "ready", !.coldActualPending = FALSE]
SharedSDKObserveReady(i) ==
    /\ s.children[i].phase # "idle" /\ ~s.children[i].readyObserved
    /\ s.acquisition \in {"ready", "terminal"}
    /\ s' = [s EXCEPT !.children[i].readyObserved = TRUE]
SharedSDKStopColdAcquisition ==
    /\ s.parent = "held" /\ s.acquisition = "cold" /\ ~s.retiring
    /\ SharedSDKActive # {}
    /\ \A i \in SharedSDKActive : s.children[i].phase = "stopping"
    /\ s' = [s EXCEPT !.retiring = TRUE, !.acquisition = "terminal"]
SharedSDKLogicalResult(i) ==
    /\ s.children[i].phase = "live" /\ ~s.children[i].logicalDone
    /\ s' = [s EXCEPT !.children[i].logicalDone = TRUE]
SharedSDKBeginControl(i) ==
    /\ s.children[i].phase = "live" /\ s.acquisition = "ready"
    /\ s.children[i].control = "idle"
    /\ s' = [s EXCEPT !.children[i].control = "pending"]
SharedSDKControlTerminal(i) ==
    /\ s.children[i].control = "pending"
    /\ s' = [s EXCEPT !.children[i].control = "terminal"]
SharedSDKBeginRead(i) ==
    /\ s.children[i].phase = "live" /\ s.acquisition = "ready"
    /\ s.children[i].read = "idle"
    /\ s' = [s EXCEPT !.children[i].read = "wire"]
SharedSDKReadResponse(i) ==
    /\ s.children[i].read = "wire"
    /\ s' = [s EXCEPT !.children[i].read = "response", !.children[i].readRefs = 1]
SharedSDKRetainReadView(i) ==
    /\ s.children[i].read = "response" /\ s.children[i].readRefs = 1
    /\ s' = [s EXCEPT !.children[i].readRefs = 2]
SharedSDKReleaseReadView(i) ==
    /\ s.children[i].readRefs > 0
    /\ s' = [s EXCEPT !.children[i].readRefs = @ - 1]
SharedSDKStopChild(i) ==
    /\ s.children[i].phase = "live"
    /\ s' = [s EXCEPT !.children[i].phase = "stopping", !.children[i].logicalDone = TRUE]
SharedSDKPrivateTerminal(i, earlyObservation) ==
    /\ s.children[i].phase = "stopping" /\ s.children[i].privateWork
    /\ (s.children[i].readyObserved \/ earlyObservation)
    /\ s' = [s EXCEPT !.children[i].privateWork = FALSE]
SharedSDKNonLastChildTerminal(i, earlyControl, earlyRead, earlyObservation) ==
    /\ s.parent = "held" /\ s.children[i].phase = "stopping"
    /\ SharedSDKOwnActualTerminal(i, earlyControl, earlyRead, earlyObservation)
    /\ SharedSDKOtherActive(i) # {}
    /\ s' = [s EXCEPT !.children[i].phase = "terminal"]
SharedSDKRetireEpoch(earlyControl, earlyRead, earlyLease, earlyObservation) ==
    /\ s.parent = "held"
    /\ \E i \in SharedSDKActive :
           /\ s.children[i].phase = "stopping"
           /\ SharedSDKOwnActualTerminal(i, earlyControl, earlyRead, earlyObservation)
           /\ (SharedSDKOtherActive(i) = {} \/ earlyLease)
    /\ s' = [s EXCEPT !.parent = "draining", !.retiring = TRUE]
SharedSDKColdTerminal ==
    /\ s.retiring /\ s.acquisition = "terminal" /\ s.coldActualPending
    /\ s' = [s EXCEPT !.coldActualPending = FALSE, !.transportOpen = FALSE]
SharedSDKTransportTerminal ==
    /\ s.parent = "draining" /\ s.acquisition = "ready" /\ s.transportOpen
    /\ s' = [s EXCEPT !.acquisition = "terminal", !.transportOpen = FALSE]
SharedSDKParentTerminal ==
    /\ s.parent = "draining" /\ s.acquisition = "terminal"
    /\ ~s.coldActualPending /\ ~s.transportOpen
    /\ s' = [s EXCEPT !.parent = "terminal"]
SharedSDKLastChildTerminal(i) ==
    /\ s.parent = "terminal" /\ s.children[i].phase = "stopping"
    /\ SharedSDKOwnActualTerminal(i, FALSE, FALSE, FALSE) /\ SharedSDKOtherActive(i) = {}
    /\ s' = [s EXCEPT !.children[i].phase = "terminal"]
SharedSDKRequestClose ==
    /\ ~s.sdkClosing
    /\ s' = [s EXCEPT !.sdkClosing = TRUE,
                       !.children = [i \in SharedSDKChildren |->
                           IF s.children[i].phase = "live"
                           THEN [s.children[i] EXCEPT !.phase = "stopping", !.logicalDone = TRUE]
                           ELSE s.children[i]]]
SharedSDKClosePool ==
    /\ s.sdkClosing /\ SharedSDKActive = {} /\ s.parent \in {"none", "terminal"}
    /\ s.pool = "open"
    /\ s' = [s EXCEPT !.pool = "draining"]
SharedSDKPoolTerminal == /\ s.pool = "draining" /\ s' = [s EXCEPT !.pool = "terminal"]
SharedSDKReleaseDependencies(earlyDependencies) ==
    /\ s.sdkClosing /\ s.dependencies = "held"
    /\ (s.pool = "terminal" \/ (earlyDependencies /\ SharedSDKActive # {}))
    /\ s' = [s EXCEPT !.dependencies = "released"]
SharedSDKNext(earlyControl, earlyRead, earlyLease, earlyDependencies, earlyObservation) ==
    (\E i \in SharedSDKChildren, ledger \in SharedSDKLedgers : SharedSDKAdmit(i, ledger))
    \/ SharedSDKAcquired \/ SharedSDKStopColdAcquisition
    \/ (\E i \in SharedSDKChildren :
           SharedSDKObserveReady(i) \/ SharedSDKLogicalResult(i)
           \/ SharedSDKBeginControl(i) \/ SharedSDKControlTerminal(i)
           \/ SharedSDKBeginRead(i) \/ SharedSDKReadResponse(i) \/ SharedSDKRetainReadView(i)
           \/ SharedSDKReleaseReadView(i) \/ SharedSDKStopChild(i)
           \/ SharedSDKPrivateTerminal(i, earlyObservation)
           \/ SharedSDKNonLastChildTerminal(i, earlyControl, earlyRead, earlyObservation)
           \/ SharedSDKLastChildTerminal(i))
    \/ SharedSDKRetireEpoch(earlyControl, earlyRead, earlyLease, earlyObservation)
    \/ SharedSDKColdTerminal \/ SharedSDKTransportTerminal \/ SharedSDKParentTerminal
    \/ SharedSDKRequestClose \/ SharedSDKClosePool \/ SharedSDKPoolTerminal
    \/ SharedSDKReleaseDependencies(earlyDependencies)
SharedSDKSpec == SharedSDKInit /\ [][SharedSDKNext(FALSE, FALSE, FALSE, FALSE, FALSE)]_vars
SharedSDKEarlyControlSpec == SharedSDKInit /\ [][SharedSDKNext(TRUE, FALSE, FALSE, FALSE, FALSE)]_vars
SharedSDKEarlyReadViewSpec == SharedSDKInit /\ [][SharedSDKNext(FALSE, TRUE, FALSE, FALSE, FALSE)]_vars
SharedSDKEarlyLeaseSpec == SharedSDKInit /\ [][SharedSDKNext(FALSE, FALSE, TRUE, FALSE, FALSE)]_vars
SharedSDKEarlyDependenciesSpec == SharedSDKInit /\ [][SharedSDKNext(FALSE, FALSE, FALSE, TRUE, FALSE)]_vars
SharedSDKEarlyObservationSpec == SharedSDKInit /\ [][SharedSDKNext(FALSE, FALSE, FALSE, FALSE, TRUE)]_vars
SharedSDKTypeOK ==
    /\ s.children \in [SharedSDKChildren ->
           [phase : {"idle", "live", "stopping", "terminal"}, ledger : {0, 1, 2}, logicalDone : BOOLEAN,
            readyObserved : BOOLEAN, privateWork : BOOLEAN, control : {"idle", "pending", "terminal"},
            read : {"idle", "wire", "response"}, readRefs : 0..2]]
    /\ s.parent \in {"none", "held", "draining", "terminal"}
    /\ s.acquisition \in {"none", "cold", "ready", "terminal"}
    /\ s.transportOpen \in BOOLEAN /\ s.sdkClosing \in BOOLEAN
    /\ s.coldActualPending \in BOOLEAN /\ s.retiring \in BOOLEAN
    /\ s.pool \in {"open", "draining", "terminal"} /\ s.dependencies \in {"held", "released"}
SharedSDKBoundedDistinctLedgers ==
    /\ SharedSDKActiveCount <= 2
    /\ \A i \in SharedSDKActive : s.children[i].ledger \in SharedSDKLedgers
    /\ \A i, j \in SharedSDKActive : i # j => s.children[i].ledger # s.children[j].ledger
SharedSDKSurvivorPinsAcquisition ==
    (\E i \in SharedSDKChildren : s.children[i].phase = "live")
    => s.parent = "held" /\ ~s.retiring /\ s.acquisition \in {"cold", "ready"} /\ s.transportOpen
SharedSDKQueuedObservationPinsAdmission ==
    \A i \in SharedSDKChildren : s.children[i].phase = "terminal" => s.children[i].readyObserved
SharedSDKRetiringBlocksAdmission == s.retiring => ~ENABLED (\E i \in SharedSDKChildren,
    ledger \in SharedSDKLedgers : SharedSDKAdmit(i, ledger))
SharedSDKColdActualPinsLastSlot == s.coldActualPending
    => SharedSDKActive # {} /\ s.parent \in {"held", "draining"}
SharedSDKPrivateActualPinsChild ==
    \A i \in SharedSDKChildren : s.children[i].privateWork
       => i \in SharedSDKActive /\ s.parent = "held"
SharedSDKControlActualPinsChild ==
    \A i \in SharedSDKChildren : s.children[i].control = "pending"
       => i \in SharedSDKActive /\ s.parent = "held"
SharedSDKReadWirePinsChild ==
    \A i \in SharedSDKChildren : s.children[i].read = "wire"
       => i \in SharedSDKActive /\ s.parent = "held"
SharedSDKReadViewsPinChild ==
    \A i \in SharedSDKChildren : s.children[i].readRefs > 0
       => i \in SharedSDKActive /\ s.parent = "held"
SharedSDKChildCloseIndependent ==
    \A i \in SharedSDKChildren :
       (s.children[i].phase = "stopping" /\ SharedSDKOwnActualTerminal(i, FALSE, FALSE, FALSE)
        /\ SharedSDKOtherActive(i) # {})
       => ENABLED SharedSDKNonLastChildTerminal(i, FALSE, FALSE, FALSE)
SharedSDKParentWaitsForActualChildren ==
    s.parent \in {"draining", "terminal"} => SharedSDKAllActualTerminal
SharedSDKParentWaitsForColdAndTransport ==
    s.parent = "terminal" => s.acquisition = "terminal" /\ ~s.coldActualPending /\ ~s.transportOpen
SharedSDKPoolWaitsForParent ==
    s.pool # "open" => SharedSDKActive = {} /\ s.parent \in {"none", "terminal"}
SharedSDKDependenciesWaitForActualChildren ==
    s.dependencies = "released"
    => SharedSDKActive = {} /\ s.parent \in {"none", "terminal"}
        /\ s.acquisition \in {"none", "terminal"} /\ ~s.coldActualPending
        /\ ~s.transportOpen /\ s.pool = "terminal"
=============================================================================
