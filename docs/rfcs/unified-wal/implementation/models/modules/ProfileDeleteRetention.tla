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
CONSTANTS SkipPolicyCAS, SplitCommit, FollowCurrentPolicy, IgnoreBirth, BackfillLegacy,
          SkipTerminalCut, ReuseOldObservation, SkipWindowRevalidation
VARIABLE s
vars == <<s>>
(* Native transaction identities and complete byte witnesses are Java facts.
   Two logical ticks abstract the complete frozen window; no wall clock, actual
   nanoseconds, native byte proof, backup acceptance or GC permission is modeled. *)
Window(g) == IF g = 1 THEN 420 ELSE IF g = 2 THEN 2 ELSE 0
Init == s = [current |-> 0, observed |-> -1, deleted |-> FALSE, receipt |-> 0,
             window |-> 0, nativeTogether |-> FALSE, stale |-> FALSE,
             legacy |-> FALSE, birth |-> 1, savedBirth |-> 1, accepted |-> FALSE,
             terminals |-> {}, originalTerminals |-> {}, barrier |-> FALSE,
             windowBirth |-> 0, epoch |-> 1, observation |-> 0, ticks |-> 0,
             cutVersion |-> 1, savedCutVersion |-> 1, windowAccepted |-> FALSE]
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
            /\ s' = [s EXCEPT !.birth = 2, !.accepted = FALSE, !.windowAccepted = FALSE]
Read == /\ s.deleted /\ ~s.accepted
        /\ (s.birth = s.savedBirth \/ IgnoreBirth)
        /\ s' = [s EXCEPT !.accepted = TRUE]
Backfill == /\ BackfillLegacy /\ s.legacy /\ s.current > 0 /\ s.receipt = 0
            /\ s' = [s EXCEPT !.receipt = s.current, !.window = Window(s.current)]
Terminal(t) == /\ s.deleted /\ t \notin s.terminals
               /\ s' = [s EXCEPT !.terminals = @ \cup {t}]
StartBarrier == /\ s.receipt > 0 /\ ~s.barrier /\ s.birth = s.savedBirth
                /\ (s.terminals = {1, 2} \/ SkipTerminalCut)
                /\ s' = [s EXCEPT !.barrier = TRUE, !.windowBirth = s.birth,
                          !.originalTerminals = s.terminals]
FreshWindow == /\ s.windowBirth = s.birth /\ s.birth = s.savedBirth /\ s.terminals = {1, 2}
               /\ s.cutVersion = s.savedCutVersion
ObserveWindow == /\ s.barrier /\ s.observation # s.epoch
                 /\ (FreshWindow \/ SkipWindowRevalidation)
                 /\ s' = [s EXCEPT !.observation = s.epoch, !.ticks = 0,
                           !.windowAccepted = FALSE]
Tick == /\ s.barrier /\ s.observation > 0 /\ s.ticks < 2
        /\ s' = [s EXCEPT !.ticks = @ + 1]
Restart == /\ s.barrier /\ s.epoch = 1
           /\ s' = [s EXCEPT !.epoch = 2, !.windowAccepted = FALSE,
                    !.observation = IF ReuseOldObservation THEN @ ELSE 0,
                    !.ticks = IF ReuseOldObservation THEN @ ELSE 0]
LoseTerminalReceipt == /\ s.barrier /\ 2 \in s.terminals
                       /\ s' = [s EXCEPT !.terminals = @ \ {2}, !.cutVersion = 2,
                                 !.windowAccepted = FALSE]
AcceptWindow == /\ s.barrier /\ s.ticks = 2 /\ ~s.windowAccepted
                /\ (s.observation = s.epoch \/ ReuseOldObservation)
                /\ (FreshWindow \/ SkipWindowRevalidation)
                /\ s' = [s EXCEPT !.windowAccepted = TRUE]
Next == Publish \/ Observe \/ Delete \/ Recreate \/ Read \/ Backfill
        \/ (\E t \in {1, 2} : Terminal(t)) \/ StartBarrier \/ ObserveWindow
        \/ Tick \/ Restart \/ LoseTerminalReceipt \/ AcceptWindow
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.current \in 0..2 /\ s.receipt \in 0..2 /\ s.birth \in 1..2
          /\ s.terminals \subseteq {1, 2} /\ s.originalTerminals \subseteq {1, 2}
          /\ s.epoch \in 1..2 /\ s.observation \in 0..2 /\ s.ticks \in 0..2
RetentionHasCurrentPolicyCAS == ~s.stale
RetentionHasAtomicTombstone == s.receipt = 0 \/ s.nativeTogether
RetentionKeepsOriginalWindows == s.receipt = 0 \/ s.window = Window(s.receipt)
RetentionRejectsRecreatedBirth == ~s.accepted \/ s.birth = s.savedBirth
RetentionDoesNotBackfillLegacy == ~s.legacy \/ s.receipt = 0
WindowHasAllOriginalTerminals == ~s.windowAccepted \/ s.originalTerminals = {1, 2}
WindowHasFreshNativeCut == ~s.windowAccepted \/ FreshWindow
WindowHasLocalObservation == ~s.windowAccepted \/ s.observation = s.epoch
WindowHasFullDelay == ~s.windowAccepted \/ s.ticks = 2
=============================================================================
