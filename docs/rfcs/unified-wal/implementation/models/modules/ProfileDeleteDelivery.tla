-------------------- MODULE ProfileDeleteDelivery --------------------
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
EXTENDS Naturals, FiniteSets, Sequences
CONSTANTS NonAtomicHead, DuplicateOnRetry, IgnoreAssignment, IgnoreBirth, InventMissing, OneStreamPerLedger
VARIABLE s
vars == <<s>>
Ledgers == {1, 2}
(* Two ledger tombstones, one target incarnation and one initial assignment.
   Logical completion, ZK atomicity and actual births are inputs. No local
   delete-applied/cursor/registration/handoff/GC, TLS/fsync or full A+C+D. *)
Init == s = [head |-> 0, events |-> <<>>, receipts |-> {}, assignment |-> 1,
             incarnation |-> 1, born |-> TRUE, missing |-> FALSE, returned |-> FALSE,
             returnBound |-> TRUE, streams |-> {}, acknowledged |-> {}]
Commit(l) == /\ l \in Ledgers /\ s.head < 2 /\ s.born /\ s.assignment = 1
             /\ (l \notin s.receipts \/ DuplicateOnRetry)
             /\ s' = [s EXCEPT !.head = @ + 1,
                      !.events = IF NonAtomicHead THEN @ ELSE Append(@, l),
                      !.receipts = @ \cup {l},
                      !.streams = @ \cup {IF OneStreamPerLedger THEN l ELSE 1}]
Retry(l) == /\ l \in s.receipts /\ s.born
            /\ s' = [s EXCEPT !.acknowledged = @ \cup {l}]
ReplaceAssignment == /\ s.assignment = 1 /\ s' = [s EXCEPT !.assignment = 2, !.returned = FALSE]
Recreate == /\ s.born /\ s.head > 0 /\ s' = [s EXCEPT !.born = FALSE, !.returned = FALSE]
LoseEvent == /\ s.head > 0 /\ ~s.missing /\ s' = [s EXCEPT !.missing = TRUE, !.returned = FALSE]
Read == /\ s.head > 0 /\ ~s.returned /\ (s.assignment = 1 \/ IgnoreAssignment)
        /\ (s.born \/ IgnoreBirth) /\ (~s.missing \/ InventMissing)
        /\ s' = [s EXCEPT !.returned = TRUE,
                 !.returnBound = s.assignment = 1 /\ s.born /\ ~s.missing]
Next == (\E l \in Ledgers : Commit(l) \/ Retry(l)) \/ ReplaceAssignment \/ Recreate \/ LoseEvent \/ Read
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.head \in 0..2 /\ s.receipts \subseteq Ledgers /\ s.streams \subseteq Ledgers
          /\ s.assignment \in {1, 2} /\ s.incarnation = 1
HeadCommitHasNoHole == s.head = Len(s.events)
SameRequestUsesOnePosition == \A i, j \in 1..Len(s.events) : s.events[i] = s.events[j] => i = j
ReturnedHasExactAuthority == s.returned => s.returnBound
OneSharedStream == Cardinality(s.streams) <= 1
=============================================================================
