-------------------- MODULE ProfileMembershipAudit --------------------
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
CONSTANTS ClassicMasks, AuditClassic, IgnoreScope, IgnoreCut, IgnoreFresh, UnknownIsMissing,
          IgnoreIncarnation, GrantOnMarker, OverflowQueue, DropAdminReferences, RetireProfile
VARIABLE s
vars == <<s>>
(* Root/metadata/registration facts are inputs, not TLS/ZK/fsync or DATA absence producers. *)
Init == s \in [profile : BOOLEAN, scoped : BOOLEAN, stable : BOOLEAN, fresh : BOOLEAN,
               peer : {"FOUND", "NOT_FOUND", "UNKNOWN", "CORRUPT"}, incarnation : BOOLEAN,
               checked : {FALSE}, marker : {FALSE}, grant : {FALSE}, queued : {0}, closed : {FALSE}, adminChecked : {FALSE}, indexed : {FALSE}, retired : {FALSE}]
KnownLoss == s.peer = "NOT_FOUND" \/ (s.peer = "FOUND" /\ ~s.incarnation)
Qualifies == s.profile /\ s.scoped /\ s.stable /\ s.fresh
Loss == s.peer = "NOT_FOUND" \/ (s.peer = "FOUND" /\ ~s.incarnation /\ ~IgnoreIncarnation)
        \/ (UnknownIsMissing /\ s.peer \in {"UNKNOWN", "CORRUPT"})
Mark == (s.profile \/ AuditClassic) /\ (s.scoped \/ IgnoreScope) /\ (s.stable \/ IgnoreCut)
        /\ (s.fresh \/ IgnoreFresh) /\ Loss /\ ~ClassicMasks
Inspect == /\ ~s.checked
           /\ s' = [s EXCEPT !.checked = TRUE, !.marker = Mark, !.grant = Mark /\ GrantOnMarker]
Notify == /\ ~s.closed /\ s.queued < 2
          /\ s' = [s EXCEPT !.queued = IF OverflowQueue THEN @ + 1 ELSE 1]
Run == /\ s.queued = 1 /\ s' = [s EXCEPT !.queued = 0]
Close == /\ ~s.closed /\ s' = [s EXCEPT !.closed = TRUE, !.queued = 0]
(* The shared public index and legacy decommission use all known ensemble/marker references,
   independently of this observer's descriptor/cohort scope. No retained-history GC is produced. *)
AdminIndex == /\ ~s.adminChecked
              /\ s' = [s EXCEPT !.adminChecked = TRUE, !.indexed = ~(s.profile /\ DropAdminReferences)]
Decommission == /\ s.adminChecked /\ ~s.retired /\ (~(s.profile /\ s.indexed) \/ RetireProfile)
                /\ s' = [s EXCEPT !.retired = TRUE]
Next == Inspect \/ Notify \/ Run \/ Close \/ AdminIndex \/ Decommission
Spec == Init /\ [][Next]_vars
TypeOK == s.queued \in 0..2 /\ s.peer \in {"FOUND", "NOT_FOUND", "UNKNOWN", "CORRUPT"}
MarksBelongToProfile == s.marker => s.profile
MarksHaveValidScope == s.marker => s.scoped
MarksUseStableCut == s.marker => s.stable
MarksUseFreshPeers == s.marker => s.fresh
UnknownNeverMeansMissing == s.marker => s.peer \in {"FOUND", "NOT_FOUND"}
KnownLossSchedulesCheck == s.checked /\ Qualifies /\ KnownLoss => s.marker
MarkerIsSchedulingOnly == ~s.grant
AdminIndexPreservesReferences == s.adminChecked => s.indexed
LegacyDoesNotRetireProfile == s.retired => ~s.profile
NotificationsAreCoalesced == s.queued <= 1
=============================================================================
