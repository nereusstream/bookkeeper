-------------------- MODULE ProfileWriterClose --------------------
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
CONSTANTS EarlyEnter, OmitAtomicCut, PromoteTail, IgnoreDigest, CancelCompletes, IgnoreLength, IgnoreWriterKind
VARIABLE s
vars == <<s>>
(* A finite ordinary-close projection. Confirmed P/L, DATA terminals, source bytes,
   matching context/cut and durable terminal ACK are inputs. No TLS/fsync producers,
   clocks, full metadata history, SDK allocation, fairness, liveness or full A/C/D. *)
Init == \E p \in {-1, 0}, held, badLength, oldReader \in BOOLEAN :
    s = [phase |-> "writing", p |-> p, len |-> IF p = -1 THEN 0 ELSE 16,
         held |-> held, badLength |-> badLength, oldReader |-> oldReader, adoptedKind |-> FALSE, stopped |-> FALSE,
         entered |-> FALSE, enteredDrained |-> FALSE, enteredCut |-> FALSE,
         cutPresent |-> FALSE, digestMatches |-> TRUE, adopted |-> FALSE,
         adoptedBound |-> FALSE, prefix |-> -2, prefixLength |-> 0,
         verified |-> FALSE, closed |-> FALSE, done |-> FALSE, cancelled |-> FALSE]
Stop == /\ s.phase = "writing"
        /\ s' = [s EXCEPT !.phase = "stopping", !.stopped = TRUE]
DataTerminal == /\ s.held /\ s' = [s EXCEPT !.held = FALSE]
Enter == /\ s.phase = "stopping" /\ (~s.held \/ EarlyEnter)
         /\ s' = [s EXCEPT !.phase = "recovery", !.entered = TRUE,
                  !.enteredDrained = ~s.held, !.enteredCut = ~OmitAtomicCut,
                  !.cutPresent = ~OmitAtomicCut]
LoseOrChangeCut == /\ s.phase = "recovery" /\ s.cutPresent /\ ~s.adopted
                   /\ s' = [s EXCEPT !.cutPresent = FALSE, !.digestMatches = FALSE]
Adopt == /\ s.phase = "recovery" /\ ~s.adopted /\ (s.digestMatches \/ IgnoreDigest) /\ (~s.oldReader \/ IgnoreWriterKind)
         /\ s' = [s EXCEPT !.adopted = TRUE, !.adoptedBound = s.digestMatches, !.adoptedKind = ~s.oldReader,
                  !.prefix = IF s.cutPresent /\ ~PromoteTail /\ ~s.oldReader THEN s.p ELSE s.p + 1,
                  !.prefixLength = IF s.cutPresent /\ ~PromoteTail /\ ~s.oldReader THEN s.len ELSE s.len + 16]
Verify == /\ s.adopted /\ ~s.verified /\ (~s.badLength \/ IgnoreLength)
          /\ s' = [s EXCEPT !.verified = TRUE,
                   !.prefixLength = IF s.badLength THEN s.len + 16 ELSE @]
DurableClose == /\ s.verified /\ ~s.closed
                /\ s' = [s EXCEPT !.closed = TRUE, !.done = TRUE]
CancelCaller == /\ s.stopped /\ ~s.done /\ ~s.cancelled
                /\ s' = [s EXCEPT !.cancelled = TRUE, !.done = CancelCompletes]
Next == Stop \/ DataTerminal \/ Enter \/ LoseOrChangeCut \/ Adopt \/ Verify \/ DurableClose \/ CancelCaller
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"writing", "stopping", "recovery"}
          /\ s.p \in {-1, 0} /\ s.len \in {0, 16} /\ s.prefix \in {-2, -1, 0, 1}
          /\ s.prefixLength \in {0, 16, 32}
          /\ \A name \in {"held", "badLength", "stopped", "entered", "enteredDrained", "enteredCut",
                            "cutPresent", "digestMatches", "adopted", "adoptedBound", "verified", "closed",
                            "done", "cancelled", "oldReader", "adoptedKind"} : s[name] \in BOOLEAN
EnterWaitsActualData == s.entered => s.enteredDrained
RecoveryAtomicallyFreezesCut == s.entered => s.enteredCut
AdoptionNeedsBoundCut == s.adopted => s.adoptedBound
ClosedMatchesWriterPrefix == s.closed => s.prefix = s.p /\ s.prefixLength = s.len
AdoptionNeedsSupportedKind == s.adopted => s.adoptedKind
CompletionWaitsData == s.done => ~s.held

(* SDK ownership prerequisite only; the original Spec remains the ordinary-close projection.
   An accepted ordered close can precede its local freeze/request. Bounded result replies
   are separate from actual recovery termination, and socket drain belongs to the lease.
   Actual DATA/cold/lease terminals and SDK admission state are inputs; no writer create,
   fairness, liveness or full A/C/D is modeled. *)
OwnerInit == \E p \in {-1, 0}, held, badLength, oldReader \in BOOLEAN :
    s = [phase |-> "writing", p |-> p, len |-> IF p = -1 THEN 0 ELSE 16,
         held |-> held, badLength |-> badLength, oldReader |-> oldReader, adoptedKind |-> FALSE, stopped |-> FALSE,
         entered |-> FALSE, enteredDrained |-> FALSE, enteredCut |-> FALSE,
         cutPresent |-> FALSE, digestMatches |-> TRUE, adopted |-> FALSE,
         adoptedBound |-> FALSE, prefix |-> -2, prefixLength |-> 0,
         verified |-> FALSE, closed |-> FALSE, done |-> FALSE, cancelled |-> FALSE,
         ownerClose |-> "none", ownerCold |-> "none", ownerActual |-> FALSE,
         ownerStopping |-> FALSE, ownerStopWon |-> FALSE, ownerLateRejected |-> FALSE,
         ownerLease |-> "held", ownerReply |-> "none", ownerCallback |-> "none",
         ownerSdkClosed |-> FALSE, ownerEnteredLive |-> FALSE]
OwnerQueueClose == /\ s.ownerClose = "none" /\ ~s.ownerStopping
                   /\ s' = [s EXCEPT !.ownerClose = "queued"]
OwnerFreezeRequest == /\ s.ownerClose = "queued"
                      /\ s' = [s EXCEPT !.phase = "stopping", !.stopped = TRUE, !.ownerClose = "request"]
OwnerSdkClosed == /\ ~s.ownerSdkClosed /\ s' = [s EXCEPT !.ownerSdkClosed = TRUE]
OwnerStopLocal(omitQueued) ==
    /\ ~s.ownerStopping
    /\ s' = [s EXCEPT !.ownerStopping = TRUE, !.stopped = TRUE,
                      !.phase = IF @ = "writing" THEN "stopping" ELSE @,
                      !.ownerStopWon = s.ownerClose = "none",
                      !.ownerActual = IF s.ownerClose = "none"
                                         \/ (omitQueued /\ s.ownerClose = "queued") THEN TRUE ELSE @]
OwnerRejectLateClose == /\ s.ownerStopping /\ s.ownerClose = "none" /\ ~s.ownerLateRejected
                        /\ s' = [s EXCEPT !.ownerLateRejected = TRUE,
                                          !.ownerReply = "failure", !.ownerCallback = "failure"]
OwnerEnter == /\ s.ownerClose = "request" /\ ~s.held /\ ~s.ownerSdkClosed
              /\ s' = [s EXCEPT !.phase = "recovery", !.entered = TRUE,
                                !.enteredDrained = TRUE, !.enteredCut = ~OmitAtomicCut,
                                !.cutPresent = ~OmitAtomicCut, !.ownerEnteredLive = ~s.ownerSdkClosed,
                                !.ownerClose = "recovery", !.ownerCold = "pending"]
OwnerRejectAdmission == /\ s.ownerClose = "request" /\ ~s.held /\ s.ownerSdkClosed
                        /\ s' = [s EXCEPT !.ownerClose = "terminal", !.ownerActual = TRUE,
                                          !.ownerReply = "failure", !.ownerCallback = "failure"]
OwnerBoundedFailure(earlyResult) ==
    /\ s.ownerClose = "recovery" /\ s.ownerReply = "none"
    /\ \E reply \in {"deadline", "cancelled"} :
        s' = [s EXCEPT !.ownerReply = reply, !.ownerCallback = "failure",
                       !.cancelled = reply = "cancelled",
                       !.ownerActual = IF earlyResult THEN TRUE ELSE @]
OwnerActualFailure == /\ s.ownerClose = "recovery"
                      /\ s' = [s EXCEPT !.ownerClose = "terminal", !.ownerCold = "terminal",
                                        !.ownerActual = TRUE,
                                        !.ownerReply = IF @ = "none" THEN "failure" ELSE @,
                                        !.ownerCallback = "failure"]
OwnerDurableClose == /\ s.ownerClose = "recovery" /\ s.verified /\ ~s.closed
                     /\ s' = [s EXCEPT !.closed = TRUE, !.done = TRUE,
                                       !.ownerClose = "terminal", !.ownerCold = "terminal", !.ownerActual = TRUE,
                                       !.ownerReply = IF @ = "none" THEN "success" ELSE @]
OwnerReleaseLease == /\ s.ownerActual /\ ~s.held /\ s.ownerLease = "held"
                     /\ s' = [s EXCEPT !.ownerLease = "closing"]
OwnerLeaseTerminal == /\ s.ownerLease = "closing"
                      /\ s' = [s EXCEPT !.ownerLease = "terminal"]
OwnerSuccessCallback(earlyCallback) ==
    /\ s.ownerReply = "success" /\ s.ownerCallback = "none"
    /\ (s.ownerLease = "terminal" \/ earlyCallback)
    /\ s' = [s EXCEPT !.ownerCallback = "success"]
OwnerRecoveryStep == /\ s.ownerClose = "recovery" /\ (LoseOrChangeCut \/ Adopt \/ Verify)
OwnerNext(earlyResult, omitQueued, earlyCallback) ==
    OwnerQueueClose \/ OwnerFreezeRequest \/ OwnerSdkClosed \/ OwnerStopLocal(omitQueued)
    \/ OwnerRejectLateClose \/ DataTerminal \/ OwnerEnter \/ OwnerRejectAdmission
    \/ OwnerRecoveryStep \/ OwnerBoundedFailure(earlyResult)
    \/ OwnerActualFailure \/ OwnerDurableClose \/ OwnerReleaseLease \/ OwnerLeaseTerminal
    \/ OwnerSuccessCallback(earlyCallback)
OwnerSpec == OwnerInit /\ [][OwnerNext(FALSE, FALSE, FALSE)]_vars
OwnerEarlyResultSpec == OwnerInit /\ [][OwnerNext(TRUE, FALSE, FALSE)]_vars
OwnerOmitQueuedSpec == OwnerInit /\ [][OwnerNext(FALSE, TRUE, FALSE)]_vars
OwnerEarlyCallbackSpec == OwnerInit /\ [][OwnerNext(FALSE, FALSE, TRUE)]_vars
OwnerTypeOK == TypeOK
               /\ s.ownerClose \in {"none", "queued", "request", "recovery", "terminal"}
               /\ s.ownerCold \in {"none", "pending", "terminal"}
               /\ s.ownerLease \in {"held", "closing", "terminal"}
               /\ s.ownerReply \in {"none", "deadline", "cancelled", "failure", "success"}
               /\ s.ownerCallback \in {"none", "failure", "success"}
               /\ \A name \in {"ownerActual", "ownerStopping", "ownerStopWon", "ownerLateRejected",
                               "ownerSdkClosed", "ownerEnteredLive"} : s[name] \in BOOLEAN
OwnerActualNeedsColdTerminal == s.ownerActual => s.ownerCold # "pending"
OwnerAcceptedClosePinsActual == s.ownerClose \in {"queued", "request", "recovery"} => ~s.ownerActual
OwnerLeaseWaitsActual == s.ownerLease # "held" => s.ownerActual /\ ~s.held /\ s.ownerCold # "pending"
OwnerSuccessWaitsLease == s.ownerCallback = "success" => s.ownerLease = "terminal" /\ s.ownerReply = "success"
OwnerFailureKeepsEarlyReply == s.ownerReply \in {"deadline", "cancelled", "failure"}
                             => s.ownerCallback = "failure"
OwnerLocalStopHasNoClosedAuthority == s.ownerStopWon => ~s.entered /\ ~s.closed /\ ~s.cutPresent
OwnerSdkClosedAdmissionIsRejected == s.entered => s.ownerEnteredLive
=============================================================================
