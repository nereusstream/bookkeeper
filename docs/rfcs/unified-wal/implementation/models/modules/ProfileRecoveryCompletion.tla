-------------------- MODULE ProfileRecoveryCompletion --------------------
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
CONSTANTS AckQuorum, CountUnknown, CountDuplicate, IgnoreCapability, SkipBirth,
          WrongLength, WrongContext, ReopenClosed, ReleaseOnCancel, SkipTerminal, EmptyLastMode, SkipHistoricalFloor,
          CreateSyntheticGrant, WrongPrefix
ASSUME EmptyLastMode \in {"nonempty", "ledger-empty", "historical-empty"}
LastStart == IF EmptyLastMode = "historical-empty" THEN 3 ELSE 0
SelectedPrefixes == IF EmptyLastMode = "ledger-empty" THEN {-1}
                    ELSE IF EmptyLastMode = "historical-empty" THEN {-1, 2} ELSE {2}
VARIABLE s
vars == <<s>>
(* E=W=3, a fixed selected P/L, verified full-copy or existing-empty-target RPC inputs.
   No durability producer: terminal/hello facts come from actual Java UIC4/type11 or UIC6/type12.
   Reply loss and delete do not change matching historical CLOSED. *)
Init == s \in [phase : {"acks"}, replies : [1..3 -> {"durable", "unknown"}],
               capability : [1..3 -> BOOLEAN], terminal : [1..3 -> BOOLEAN],
               acks : {{}}, deliveries : {0}, birth : {0}, stopped : {FALSE}, reserved : {TRUE},
               closed : {FALSE}, length : {0}, prefix : {-2}, selected : SelectedPrefixes,
               grant : {EmptyLastMode = "nonempty"},
               context : {1}, deleted : {FALSE}, outcome : {""}]
Deliver == /\ s.phase = "acks" /\ s.deliveries < 3
           /\ \E i \in 1..3 :
               LET accepted == (s.replies[i] = "durable" \/ CountUnknown)
                               /\ (s.capability[i] \/ IgnoreCapability) /\ (s.terminal[i] \/ SkipTerminal)
               IN s' = [s EXCEPT !.acks = IF accepted THEN @ \cup {i} ELSE @,
                        !.deliveries = @ + 1]
Restore == /\ s.phase = "acks" /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
Cancel == /\ s.phase = "acks" /\ ~s.stopped
          /\ s' = [s EXCEPT !.stopped = TRUE, !.reserved = IF ReleaseOnCancel THEN FALSE ELSE @]
Publish == /\ s.phase = "acks"
           /\ LET enough == (IF CountDuplicate THEN s.deliveries ELSE Cardinality(s.acks)) >= AckQuorum
                  ok == enough /\ ~s.stopped /\ (s.birth = 0 \/ SkipBirth)
                        /\ (s.selected >= LastStart - 1 \/ SkipHistoricalFloor)
              IN s' = [s EXCEPT !.phase = "done", !.closed = ok, !.reserved = FALSE,
                       !.length = IF ok THEN IF WrongLength THEN 64 ELSE IF s.selected = -1 THEN 0 ELSE 48 ELSE 0,
                       !.prefix = IF ok THEN IF WrongPrefix THEN s.selected + 1 ELSE s.selected ELSE @,
                       !.context = IF ok /\ WrongContext THEN 2 ELSE @,
                       !.outcome = IF ok THEN "matching-closed" ELSE "deferred"]
Delete == /\ s.phase = "done" /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Reopen == /\ s.phase = "done" /\ s.closed /\ ReopenClosed
          /\ s' = [s EXCEPT !.closed = FALSE]
SyntheticGrant == /\ EmptyLastMode # "nonempty" /\ CreateSyntheticGrant /\ ~s.grant
                  /\ s' = [s EXCEPT !.grant = TRUE]
Next == SyntheticGrant \/ Deliver \/ Restore \/ Cancel \/ Publish \/ Delete \/ Reopen
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"acks", "done"} /\ s.acks \subseteq 1..3 /\ s.deliveries \in 0..3
ClosedHasDistinctQuorum == s.closed => Cardinality(s.acks) >= AckQuorum
ClosedHasActualTerminal == s.closed => (\A i \in s.acks : s.replies[i] = "durable" /\ s.terminal[i])
ClosedHasCapability == s.closed => (\A i \in s.acks : s.capability[i])
ClosedHasSameBirth == s.closed => s.birth = 0
ClosedHasMatchingLength == s.closed => s.length = IF s.selected = -1 THEN 0 ELSE 48
ClosedHasMatchingPrefix == s.closed => s.prefix = s.selected
ClosedContainsFrozenHistory == s.closed => s.selected >= LastStart - 1
EmptyCloseCreatesNoGrant == EmptyLastMode # "nonempty" => ~s.grant
ClosedHasMatchingContext == s.closed => s.context = 1
MatchingClosedNeverReopens == s.outcome = "matching-closed" => s.closed
CancelledDoesNotPublish == s.stopped => ~s.closed
ActualAttemptOwnsPermit == s.phase = "acks" => s.reserved
=============================================================================
