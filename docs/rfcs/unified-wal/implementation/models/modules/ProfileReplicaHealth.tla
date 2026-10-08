-------------------- MODULE ProfileReplicaHealth --------------------
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
CONSTANTS IgnoreAll, IgnoreUnknown, IgnoreLength, IgnoreEmpty, IgnoreBirth, IgnoreVersion,
          IgnoreDelete, IgnoreStop, ReleaseEarly
VARIABLE s
vars == <<s>>
Entries == 0..1
Peers == 0..2
(* Reply/digest/force results are finite inputs. No marker deletion, worker lock,
   TLS, fsync, digest production or physical media stability is proved here. *)
Init == \E bad \in Entries, peer \in Peers, outcome \in {"FOUND", "MISSING", "CORRUPT", "UNKNOWN"},
           p \in {-1, 1}, targets \in {2, 3}, lengthOK \in BOOLEAN :
        s = [phase |-> "accepted", p |-> p, targets |-> targets, lengthOK |-> lengthOK,
             copies |-> [e \in Entries |-> [b \in Peers |-> IF e = bad /\ b = peer THEN outcome ELSE "FOUND"]],
             next |-> 0, seen |-> {}, healthy |-> TRUE, inFlight |-> FALSE, active |-> TRUE,
             stopped |-> FALSE, birth |-> 0, version |-> 0, deleted |-> FALSE,
             proof |-> FALSE, proofBirth |-> 0, proofVersion |-> 0, proofDeleted |-> FALSE, proofStopped |-> FALSE]
Restore == /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
Change == /\ s.version = 0 /\ s' = [s EXCEPT !.version = 1]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Stop == /\ ~s.stopped /\ s.phase # "done"
        /\ s' = [s EXCEPT !.stopped = TRUE, !.active = IF ReleaseEarly THEN FALSE ELSE @]
Fence == /\ s.phase = "accepted"
         /\ s' = [s EXCEPT !.phase = IF s.targets = 3 \/ IgnoreEmpty THEN "scan" ELSE "recheck",
                   !.healthy = s.targets = 3 \/ IgnoreEmpty]
Read == /\ s.phase = "scan" /\ s.next <= s.p /\ (~s.stopped \/ IgnoreStop)
        /\ s' = [s EXCEPT !.phase = "reading", !.inFlight = TRUE]
CopyOK(e) == (\A b \in Peers : s.copies[e][b] = "FOUND")
             \/ (IgnoreAll /\ Cardinality({b \in Peers : s.copies[e][b] = "FOUND"}) >= 2)
             \/ (IgnoreUnknown /\ \A b \in Peers : s.copies[e][b] \in {"FOUND", "UNKNOWN"})
Reply == /\ s.phase = "reading"
         /\ s' = [s EXCEPT !.phase = "scan", !.inFlight = FALSE, !.next = @ + 1,
                   !.seen = @ \cup {s.next}, !.healthy = @ /\ CopyOK(s.next)]
EndScan == /\ s.phase = "scan" /\ (s.next > s.p \/ s.stopped)
           /\ s' = [s EXCEPT !.phase = "recheck"]
Deliver == /\ s.phase = "recheck"
           /\ s' = [s EXCEPT !.phase = "done", !.active = FALSE,
                     !.proof = s.healthy /\ s.next > s.p /\ (s.lengthOK \/ IgnoreLength)
                               /\ (~s.stopped \/ IgnoreStop) /\ (s.birth = 0 \/ IgnoreBirth)
                               /\ (s.version = 0 \/ IgnoreVersion) /\ (~s.deleted \/ IgnoreDelete),
                     !.proofBirth = s.birth, !.proofVersion = s.version,
                     !.proofDeleted = s.deleted, !.proofStopped = s.stopped]
Next == Restore \/ Change \/ Delete \/ Stop \/ Fence \/ Read \/ Reply \/ EndScan \/ Deliver
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"accepted", "scan", "reading", "recheck", "done"}
AllRequiredCopiesFound == s.proof => \A e \in 0..s.p, b \in Peers : s.copies[e][b] = "FOUND"
UnknownNeverHealthy == s.proof => \A e \in 0..s.p, b \in Peers : s.copies[e][b] # "UNKNOWN"
MatchingLength == s.proof => s.lengthOK
EmptyRequiresAllTargets == s.proof /\ s.p = -1 => s.targets = 3
FreshBirth == s.proof => s.proofBirth = 0
FreshVersion == s.proof => s.proofVersion = 0
DeletePreventsProof == s.proof => ~s.proofDeleted
StoppedNeverGetsProof == s.proof => ~s.proofStopped
ActualReadOwnsGate == s.inFlight => s.active
=============================================================================
