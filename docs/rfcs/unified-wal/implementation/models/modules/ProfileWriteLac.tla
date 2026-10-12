-------------------- MODULE ProfileWriteLac --------------------
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
CONSTANTS A, IgnoreScope, IgnoreDigest, AheadConfirmed, SkipForce, DuplicateVote,
          UnknownAck, EarlyRelease, AdvanceFenced, LoseReplay
VARIABLE s
vars == <<s>>
Peers == 1..3
(* A single writer-confirmed cut 1, original digest/scope validation, actual
   force/apply and independent local source/output/wire terminal are inputs
   from Java. No TLS, CRC, fsync, ZK, general lifecycle or fairness is produced. *)
Init == s \in [scoped : BOOLEAN, digest : BOOLEAN, started : {FALSE}, cut : {-1},
               io : {[p \in Peers |-> "idle"]}, durable : {{}}, applied : {{}},
               everApplied : {{}}, ack : {{}}, votes : {0}, returned : {FALSE},
               sourceDone : {{}}, wireDone : {{}}, stopped : {FALSE},
               canceled : {FALSE}, released : {FALSE}, fenced : {FALSE},
               advanced : {FALSE}, duplicateResolved : {FALSE}, replayed : {FALSE}]
Start == /\ ~s.started /\ ~s.stopped /\ ~s.fenced
         /\ (s.scoped \/ IgnoreScope) /\ (s.digest \/ IgnoreDigest)
         /\ s' = [s EXCEPT !.started = TRUE, !.cut = IF AheadConfirmed THEN 2 ELSE 1,
                   !.io = [p \in Peers |-> "queued"]]
Force(p) == /\ s.started /\ s.io[p] = "queued"
            /\ s' = [s EXCEPT !.io[p] = "forced", !.durable = @ \cup {p}]
Apply(p) == /\ s.started /\ (s.io[p] = "forced" \/ (SkipForce /\ s.io[p] = "queued"))
            /\ s' = [s EXCEPT !.io[p] = "applied", !.applied = @ \cup {p},
                      !.everApplied = @ \cup {p}]
Fail(p) == /\ s.started /\ s.io[p] = "queued"
           /\ s' = [s EXCEPT !.io[p] = "unknown"]
Reply(p) == /\ s.io[p] \in {"applied", "unknown"} /\ p \notin s.wireDone
            /\ LET good == s.io[p] = "applied" \/ UnknownAck
                   nextAck == IF good THEN s.ack \cup {p} ELSE s.ack
               IN s' = [s EXCEPT !.wireDone = @ \cup {p}, !.ack = nextAck,
                                  !.votes = Cardinality(nextAck)]
(* Encoding/output may end while remote force remains in flight. *)
Output(p) == /\ s.started /\ p \notin s.sourceDone
             /\ s' = [s EXCEPT !.sourceDone = @ \cup {p}]
Timeout(p) == /\ s.started /\ p \notin s.wireDone
              /\ s' = [s EXCEPT !.wireDone = @ \cup {p}]
RepeatVote == /\ DuplicateVote /\ s.ack # {} /\ s.votes < 3
              /\ s' = [s EXCEPT !.votes = @ + 1]
Return == /\ ~s.returned /\ ~s.canceled /\ s.votes >= A
          /\ s' = [s EXCEPT !.returned = TRUE]
Cancel == /\ ~s.canceled /\ s' = [s EXCEPT !.canceled = TRUE]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
Release == /\ s.stopped /\ ~s.released
           /\ ((s.sourceDone = Peers /\ s.wireDone = Peers) \/ ~s.started \/ EarlyRelease)
           /\ s' = [s EXCEPT !.released = TRUE]
Fence == /\ ~s.fenced /\ \A p \in Peers : s.io[p] # "queued" /\ s.io[p] # "forced"
         /\ s' = [s EXCEPT !.fenced = TRUE]
Advance == /\ s.fenced /\ AdvanceFenced /\ ~s.advanced
           /\ s' = [s EXCEPT !.advanced = TRUE]
Duplicate == /\ s.fenced /\ s.applied # {} /\ ~s.duplicateResolved
             /\ s' = [s EXCEPT !.duplicateResolved = TRUE]
Replay == /\ s.fenced /\ ~s.replayed
          /\ s' = [s EXCEPT !.replayed = TRUE, !.applied = IF LoseReplay THEN {} ELSE @]
Next == Start \/ (\E p \in Peers : Force(p) \/ Apply(p) \/ Fail(p) \/ Reply(p) \/ Output(p) \/ Timeout(p))
        \/ RepeatVote \/ Return \/ Cancel \/ Stop \/ Release \/ Fence \/ Advance \/ Duplicate \/ Replay
Spec == Init /\ [][Next]_vars
TypeOK == s.votes \in 0..3 /\ s.cut \in -1..2 /\ s.ack \subseteq Peers
WriteLacHasScope == s.started => s.scoped
WriteLacHasDigest == s.started => s.digest
WriteLacUsesConfirmedCut == s.cut <= 1
WriteLacAppliedNeedsForce == s.everApplied \subseteq s.durable
WriteLacAckIsDurable == s.ack \subseteq s.everApplied
WriteLacQuorumIsDistinct == s.returned => Cardinality(s.ack) >= A
WriteLacReleaseWaitsActual == s.released /\ s.started => s.sourceDone = Peers /\ s.wireDone = Peers
WriteLacFenceNeverAdvances == ~s.advanced
WriteLacReplayPreservesAuthority == s.replayed => s.everApplied \subseteq s.applied
=============================================================================
