-------------------- MODULE ProfileDeleteRetention --------------------
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
EXTENDS Integers
CONSTANTS SkipPolicyCAS, SplitCommit, FollowCurrentPolicy, IgnoreBirth, BackfillLegacy
VARIABLE s
vars == <<s>>
(* Native transaction identities and complete byte witnesses are Java facts.
   This finite projection supplies no elapsed-time or backup/GC authority. *)
Window(g) == IF g = 1 THEN 420 ELSE IF g = 2 THEN 2 ELSE 0
Init == s = [current |-> 0, observed |-> -1, deleted |-> FALSE, receipt |-> 0,
             window |-> 0, nativeTogether |-> FALSE, stale |-> FALSE,
             legacy |-> FALSE, birth |-> 1, savedBirth |-> 1, accepted |-> FALSE]
Publish == /\ s.current < 2
           /\ s' = [s EXCEPT !.current = @ + 1,
                     !.window = IF FollowCurrentPolicy /\ s.receipt > 0
                                THEN Window(s.current + 1) ELSE s.window]
Observe == /\ ~s.deleted /\ s.observed # s.current
           /\ s' = [s EXCEPT !.observed = s.current]
Delete == /\ ~s.deleted /\ s.observed >= 0
          /\ (s.observed = s.current \/ SkipPolicyCAS \/ s.observed = 0)
          /\ s' = [s EXCEPT !.deleted = TRUE, !.receipt = s.observed,
                    !.window = Window(s.observed), !.legacy = (s.observed = 0),
                    !.nativeTogether = ~SplitCommit,
                    !.stale = (s.observed > 0 /\ s.observed # s.current)]
Recreate == /\ s.receipt > 0 /\ s.birth = 1
            /\ s' = [s EXCEPT !.birth = 2, !.accepted = FALSE]
Read == /\ s.deleted /\ ~s.accepted
        /\ (s.birth = s.savedBirth \/ IgnoreBirth)
        /\ s' = [s EXCEPT !.accepted = TRUE]
Backfill == /\ BackfillLegacy /\ s.legacy /\ s.current > 0 /\ s.receipt = 0
            /\ s' = [s EXCEPT !.receipt = s.current, !.window = Window(s.current)]
Next == Publish \/ Observe \/ Delete \/ Recreate \/ Read \/ Backfill
Spec == Init /\ [][Next]_vars
TypeOK == s.current \in 0..2 /\ s.receipt \in 0..2 /\ s.birth \in 1..2
RetentionHasCurrentPolicyCAS == ~s.stale
RetentionHasAtomicTombstone == s.receipt = 0 \/ s.nativeTogether
RetentionKeepsOriginalWindows == s.receipt = 0 \/ s.window = Window(s.receipt)
RetentionRejectsRecreatedBirth == ~s.accepted \/ s.birth = s.savedBirth
RetentionDoesNotBackfillLegacy == ~s.legacy \/ s.receipt = 0
=============================================================================
