------------------------ MODULE NativeMoveCoverage ------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License. You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. *)
EXTENDS Naturals
CONSTANTS IgnoreOldPut, ReleaseOnCancel, TrustStaleRows, SkipTargetValidation
VARIABLE s
vars == <<s>>
Init == s = [selector |-> 1, native |-> 1, persisted |-> 1, page |-> 1,
             oldPut |-> TRUE, stage |-> "idle", guard |-> FALSE, waiting |-> TRUE,
             life |-> "live", crashed |-> FALSE, targetValid |-> TRUE, chosen |-> 0]
(* COPY/barrier/grants/MOVE selection are validated inputs from the other slices. *)
Move == /\ s.life = "live" /\ s.stage = "idle" /\ s.selector < 3
        /\ s' = [s EXCEPT !.selector = @ + 1]
OldPut == /\ s.life = "live" /\ s.oldPut /\ s.stage = "idle"
          /\ s' = [s EXCEPT !.oldPut = FALSE, !.native = 1]
Prepare == /\ s.life = "live" /\ s.stage = "idle" /\ s.selector > 1
           /\ (~s.oldPut \/ IgnoreOldPut)
           /\ s' = [s EXCEPT !.stage = "prepared", !.guard = TRUE]
NativeUpdate == /\ s.life = "live" /\ s.stage = "prepared"
                /\ s' = [s EXCEPT !.native = s.selector, !.stage = "visible"]
Flush == /\ s.life = "live" /\ s.stage = "visible"
         /\ s' = [s EXCEPT !.persisted = s.native, !.stage = "flushed"]
ForcePage == /\ s.life = "live" /\ s.stage = "flushed"
             /\ s' = [s EXCEPT !.page = s.native, !.stage = "published"]
NativeFailure == /\ s.life = "live" /\ s.stage = "prepared"
                 /\ s' = [s EXCEPT !.stage = "failed"]
Finish == /\ s.life = "live" /\ s.stage \in {"published", "failed"}
          /\ s' = [s EXCEPT !.stage = "idle", !.guard = FALSE]
Cancel == /\ s.life = "live" /\ s.stage # "idle" /\ s.waiting
          /\ s' = [s EXCEPT !.waiting = FALSE,
                   !.guard = IF ReleaseOnCancel THEN FALSE ELSE s.guard]
Crash == /\ s.life = "live" /\ ~s.crashed
         /\ s' = [s EXCEPT !.life = "recovery", !.crashed = TRUE,
                  !.native = s.persisted, !.stage = "idle", !.guard = FALSE, !.oldPut = FALSE]
CorruptTarget == /\ s.life = "recovery" /\ s.selector > 1 /\ s.targetValid
                 /\ s' = [s EXCEPT !.targetValid = FALSE]
Usable == s.page = s.native /\ (TrustStaleRows \/ s.native = s.selector)
Recover == /\ s.life = "recovery"
           /\ (s.targetValid \/ (Usable /\ SkipTargetValidation))
           /\ s' = [s EXCEPT !.life = "ready", !.chosen = IF Usable THEN s.page ELSE s.selector]
TypeOK == /\ s.selector \in 1..3 /\ s.native \in 1..3 /\ s.persisted \in 1..3 /\ s.page \in 1..3
          /\ s.oldPut \in BOOLEAN /\ s.guard \in BOOLEAN /\ s.waiting \in BOOLEAN
          /\ s.crashed \in BOOLEAN /\ s.targetValid \in BOOLEAN /\ s.chosen \in 0..3
          /\ s.life \in {"live", "recovery", "ready"}
          /\ s.stage \in {"idle", "prepared", "visible", "flushed", "published", "failed"}
CapturedOwnersHadNoOldPut == s.stage # "idle" => ~s.oldPut
MaintenanceKeepsProtection == s.stage # "idle" => s.guard
RecoveredLocatorUsesSelector == s.life = "ready" => s.chosen = s.selector
CoveredReadyValidatesCurrentBytes == s.life = "ready" => s.targetValid
Next == Move \/ OldPut \/ Prepare \/ NativeUpdate \/ Flush \/ ForcePage \/ NativeFailure
        \/ Finish \/ Cancel \/ Crash \/ CorruptTarget \/ Recover
Spec == Init /\ [][Next]_vars
=============================================================================
