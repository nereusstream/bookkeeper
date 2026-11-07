-------------------- MODULE ProfileRestoreDependencies --------------------
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
EXTENDS Integers, Sequences, FiniteSets
CONSTANT Mutation
VARIABLE s
vars == <<s>>
Intents == 1..2
NoTicket == <<-1, -1>>
Through(i) == i - 1

(* Each intent names one original image; a retry can instead supply a different
   image. Verified ZooKeeper births, digests, and authenticated target CLOSE
   are input facts. The scan budget is scaled down independently of admission. *)
Init == \E mode \in {"LEGACY", "FLOOR", "G1"}, budget \in 1..2 :
    s = [mode |-> mode, budget |-> budget, version |-> 0, floor |-> 0,
         ticket |-> [i \in Intents |-> NoTicket], admitted |-> {}, rows |-> <<>>,
         badFloor |-> FALSE, phase |-> [i \in Intents |-> 0], deleted |-> {},
         hole |-> 0, badProof |-> 0, retry |-> <<0, 0, FALSE>>, result |-> "NONE",
         cut |-> -1, seen |-> <<>>, settled |-> {}, hadReceipt |-> {}]

Prepare(i) ==
    LET current == <<s.version, s.floor>> IN
    /\ s.mode \notin {"G2", "REPLACED"} /\ i \notin (s.admitted \cup s.deleted)
    /\ Through(i) >= s.floor /\ s.ticket[i] # current
    /\ s' = [s EXCEPT !.ticket[i] = current]

Admit(i) ==
    /\ s.mode \notin {"G2", "REPLACED"} /\ i \notin (s.admitted \cup s.deleted)
    /\ s.ticket[i] # NoTicket
    /\ (s.mode # "G1" \/ s.ticket[i][1] = s.version)
    /\ (s.ticket[i][2] = s.floor \/ Mutation = "StaleFloor")
    /\ s' = [s EXCEPT !.admitted = @ \cup {i}, !.ticket[i] = NoTicket,
              !.version = IF s.mode = "G1" THEN @ + 1 ELSE @,
              !.rows = IF s.mode = "G1" /\ Mutation # "SplitAdmission" THEN Append(@, i) ELSE @,
              !.badFloor = @ \/ Through(i) < s.floor]

AdvanceFloor ==
    /\ s.mode \in {"FLOOR", "G1"} /\ s.floor = 0
    /\ s' = [s EXCEPT !.floor = 1]

ChangeScope ==
    /\ s.mode = "G1"
    /\ \E changed \in {"G2", "REPLACED"} : s' = [s EXCEPT !.mode = changed]

Retry(i, sameImage) ==
    /\ s.mode = "G1" /\ i \in s.admitted /\ s.retry[1] = 0 /\ i \notin s.deleted
    /\ (sameImage \/ Mutation = "WrongImage")
    /\ \E j \in 1..Len(s.rows) :
        /\ s.rows[j] = i /\ s.hole # j
        /\ s' = [s EXCEPT !.retry = <<i, j, sameImage>>]

(* 1: member published; 2: matching durable CLOSE arrived; 3: original
   output drained; 4: exact protected completion receipt committed. *)
AdvanceClose(i) ==
    /\ i \in s.admitted /\ i \notin s.deleted /\ s.phase[i] < 4
    /\ (s.phase[i] < 3 \/ s.mode \notin {"G2", "REPLACED"})
    /\ s' = [s EXCEPT !.phase[i] = @ + 1]

Delete(i) ==
    /\ i \notin s.deleted
    /\ s' = [s EXCEPT !.deleted = @ \cup {i}, !.ticket[i] = NoTicket]

(* Fault on a read of this ordinal. The native append log itself stays immutable. *)
LoseRow ==
    /\ s.hole = 0 /\ Len(s.rows) > 0
    /\ \E j \in 1..Len(s.rows) : s' = [s EXCEPT !.hole = j]

LoseEmptyProof ==
    /\ s.phase[2] = 4 /\ s.badProof = 0
    /\ s' = [s EXCEPT !.badProof = 2]

ReadableHead == s.hole = 0 \/ s.hole # s.version

Begin ==
    /\ s.cut = -1
    /\ IF s.mode # "G1" /\ Mutation # "LegacyCoverage"
       THEN s' = [s EXCEPT !.result = IF s.mode = "REPLACED" THEN "QUARANTINED" ELSE "UNCOVERED"]
       ELSE IF ~ReadableHead THEN s' = [s EXCEPT !.result = "QUARANTINED"]
       ELSE IF s.version > s.budget /\ Mutation # "IgnoreBudget"
            THEN s' = [s EXCEPT !.result = "LIMIT"]
            ELSE s' = [s EXCEPT !.cut = s.version]

ReadNext ==
    LET ordinal == Len(s.seen) + 1 IN
    /\ ordinal <= s.cut /\ ordinal <= Len(s.rows)
    /\ LET i == s.rows[ordinal]
           exactReceipt == s.phase[i] = 4 /\ s.badProof # i
       IN IF s.hole = ordinal \/ (s.badProof = i /\ Mutation # "IgnoreEmptyProof")
          THEN s' = [s EXCEPT !.result = "QUARANTINED"]
          ELSE s' = [s EXCEPT !.seen = Append(@, i),
                     !.hadReceipt = IF exactReceipt THEN @ \cup {i} ELSE @,
                     !.settled = IF exactReceipt
                                      \/ (Mutation = "EarlySettlement" /\ s.phase[i] = 2)
                                      \/ (Mutation = "DeleteSettles" /\ i \in s.deleted)
                                      \/ (Mutation = "IgnoreEmptyProof" /\ s.badProof = i)
                                 THEN @ \cup {i} ELSE @]

Finish ==
    /\ s.cut >= 0 /\ Len(s.seen) = s.cut
    /\ s' = [s EXCEPT !.result =
        IF (s.mode = "G1" \/ Mutation \in {"LegacyCoverage", "IgnoreScopeChange"})
                /\ (s.version = s.cut \/ Mutation = "SkipRevalidation")
                /\ ReadableHead
        THEN "COMPLETE" ELSE "UNAVAILABLE"]

Timeout == /\ s.cut >= 0 /\ s' = [s EXCEPT !.result = "UNAVAILABLE"]

Next ==
    /\ s.result = "NONE"
    /\ \/ \E i \in Intents : Prepare(i) \/ Admit(i) \/ AdvanceClose(i) \/ Delete(i)
           \/ (\E sameImage \in BOOLEAN : Retry(i, sameImage))
       \/ AdvanceFloor \/ ChangeScope \/ LoseRow \/ LoseEmptyProof \/ Begin \/ ReadNext \/ Finish \/ Timeout
Spec == Init /\ [][Next]_vars

AtomicAdmission ==
    s.mode \in {"G1", "G2", "REPLACED"} =>
        /\ s.version = Len(s.rows)
        /\ Len(s.rows) = Cardinality(s.admitted)
        /\ {s.rows[j] : j \in 1..Len(s.rows)} = s.admitted
AdmittedAboveFloor == ~s.badFloor
RetryKeepsOriginalImage ==
    s.retry[1] # 0 =>
        /\ s.retry[3]
        /\ s.retry[2] \in 1..Len(s.rows)
        /\ s.rows[s.retry[2]] = s.retry[1]
CompleteHasCoverage == s.result = "COMPLETE" => s.mode = "G1"
CompleteHasCurrentCut == s.result = "COMPLETE" => s.cut = s.version /\ s.seen = s.rows
CompleteWithinBudget == s.result = "COMPLETE" => s.cut <= s.budget
SettledHasExactReceipt == s.settled \subseteq s.hadReceipt
NoObservedMixedOutcome ==
    ~(s.result = "COMPLETE" /\ Len(s.seen) = 2 /\ Cardinality(s.settled) = 1
        /\ s.retry[1] # 0 /\ (s.settled \cap s.deleted) # {})
============================================================================
