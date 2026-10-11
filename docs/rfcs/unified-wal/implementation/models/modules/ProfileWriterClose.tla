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
=============================================================================
