-------------------- MODULE ProfileRecoveryPrefix --------------------
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
CONSTANTS AckQuorum, EarlyTerminal, UnknownAsMissing, UnknownAsExhausted,
          IgnoreRequired, IgnoreConflict, SkipPrior, IgnoreLength, ReleaseOnCancel,
          IgnoreContext, SkipSourceGate, PublishClosed
VARIABLE s
vars == <<s>>
(* One exact coordinate after a checked prior prefix; all callbacks terminate.
   Source outcomes already abstract CRC and the original fenced point transport.
   A candidate still needs recovery Adds and matching CLOSED; this only verifies.
   No byte producer, global authority proof, scheduler, clocks or liveness. *)
Init == s \in [phase : {"collect"}, replies : [1..3 -> {"a", "b", "missing", "corrupt", "unknown"}],
               fenced : BOOLEAN, bound : BOOLEAN, required : BOOLEAN, contiguous : BOOLEAN,
               lengthOK : BOOLEAN, current : {TRUE}, waiting : {TRUE}, reserved : {TRUE},
               seen : {{}}, outcome : {""}, closed : {FALSE}]
Vote(i) == IF (s.fenced /\ s.bound) \/ SkipSourceGate THEN s.replies[i] ELSE "unknown"
Used(i) == IF i \in s.seen THEN Vote(i) ELSE "unknown"
Has(v) == \E i \in 1..3 : Used(i) = v
ActualMissing == Cardinality({i \in 1..3 : Vote(i) = "missing"})
Missing == Cardinality({i \in 1..3 : Used(i) = "missing"
                                      \/ (UnknownAsMissing /\ Used(i) = "unknown")})
Finite == \A i \in 1..3 : Used(i) \in {"missing", "corrupt"}
                            \/ (UnknownAsExhausted /\ Used(i) = "unknown")
EndSource == /\ s.phase = "collect" /\ s.seen # 1..3
             /\ \E i \in (1..3) \ s.seen : s' = [s EXCEPT !.seen = @ \cup {i}]
Finish == /\ s.phase = "collect"
          /\ (s.seen = 1..3 \/ (EarlyTerminal /\ s.seen # {}))
          /\ LET data == Has("a") \/ Has("b")
                 conflict == Has("a") /\ Has("b")
                 outcome == IF ~s.current /\ ~IgnoreContext THEN "authority"
                            ELSE IF ~s.contiguous /\ ~SkipPrior THEN "deferred"
                            ELSE IF conflict /\ ~IgnoreConflict THEN "quarantine"
                            ELSE IF data THEN IF s.lengthOK \/ IgnoreLength THEN "candidate" ELSE "quarantine"
                            ELSE IF ~s.required \/ IgnoreRequired
                              THEN IF Missing >= 4 - AckQuorum THEN "tail" ELSE "deferred"
                            ELSE IF Finite THEN "loss" ELSE "deferred"
             IN s' = [s EXCEPT !.phase = "done", !.outcome = outcome, !.reserved = FALSE,
                      !.closed = PublishClosed]
Cancel == /\ s.phase = "collect" /\ s.waiting
          /\ s' = [s EXCEPT !.waiting = FALSE, !.reserved = IF ReleaseOnCancel THEN FALSE ELSE @]
ChangeContext == /\ s.phase = "collect" /\ s.current /\ s' = [s EXCEPT !.current = FALSE]
Next == EndSource \/ Finish \/ Cancel \/ ChangeContext
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"collect", "done"} /\ s.seen \subseteq 1..3
          /\ s.outcome \in {"", "candidate", "tail", "loss", "deferred", "quarantine", "authority"}
AllSourcesEnded == s.phase = "done" => s.seen = 1..3
TailHasDefinitiveCoverage == s.outcome = "tail" => ActualMissing >= 4 - AckQuorum
TailIsAfterRequired == s.outcome = "tail" => ~s.required
LossHasRequiredFiniteSet == s.outcome = "loss" =>
                           (s.required /\ (\A i \in 1..3 : Vote(i) \in {"missing", "corrupt"}))
ConflictsQuarantine == s.outcome = "candidate" => ~(Has("a") /\ Has("b"))
ProgressHasContiguousPrefix == s.outcome \in {"candidate", "tail"} => s.contiguous
CandidateHasMatchingLength == s.outcome = "candidate" => s.lengthOK
EvidenceHasCurrentContext == s.outcome \in {"candidate", "tail", "loss"} => s.current
EvidenceHasBoundFencedSources == s.outcome \in {"candidate", "tail", "loss"} => (s.bound /\ s.fenced)
InFlightOwnsReservation == s.phase = "collect" => s.reserved
NoClosedPublication == ~s.closed
=============================================================================
