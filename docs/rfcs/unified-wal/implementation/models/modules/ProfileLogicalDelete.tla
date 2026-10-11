-------------------- MODULE ProfileLogicalDelete --------------------
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
EXTENDS Naturals
CONSTANTS SkipClose, EarlyTargets, IgnoreHistory, EarlyLogical, IgnoreReference, ReleaseOnCancel, WaitOffline
VARIABLE s
vars == <<s>>
AllTargets == {"initial", "retired", "repair"}
(* Fenced CLOSED, actual ZK durability/birth/version, retained history and cold terminal
   are inputs. No TLS/files/fsync producer, physical delivery/cursor/rejoin, full
   metadata topology, clocks/fairness/liveness, or full A+C+D combination. *)
Init == \E closed, fenced, offline \in BOOLEAN :
    s = [closed |-> closed, fenced |-> fenced, offline |-> offline, cut |-> FALSE,
         seal |-> FALSE, manifest |-> FALSE, born |-> TRUE, targets |-> {},
         logical |-> FALSE, reported |-> FALSE, physical |-> FALSE, oldRead |-> TRUE,
         cold |-> TRUE, gate |-> TRUE, cancelled |-> FALSE, attempted |-> FALSE]
Cut == /\ ~s.cut /\ ~s.cancelled /\ s.cold /\ ((s.closed /\ s.fenced) \/ SkipClose)
       /\ s' = [s EXCEPT !.cut = TRUE]
Seal == /\ s.cut /\ ~s.seal /\ ~s.cancelled /\ s.cold /\ s' = [s EXCEPT !.seal = TRUE]
Targets == /\ s.cut /\ ~s.manifest /\ ~s.cancelled /\ s.cold /\ (s.seal \/ EarlyTargets)
           /\ s' = [s EXCEPT !.manifest = TRUE,
                    !.targets = IF IgnoreHistory THEN {"initial"} ELSE AllTargets,
                    !.attempted = ~s.seal]
Logical == /\ s.cut /\ ~s.logical /\ ~s.cancelled /\ s.cold /\ (~s.offline \/ ~WaitOffline)
           /\ ((s.seal /\ s.manifest) \/ EarlyLogical)
           /\ s' = [s EXCEPT !.logical = TRUE]
Recreate == /\ s.logical /\ s.born /\ s' = [s EXCEPT !.born = FALSE, !.reported = FALSE]
Query == /\ s.logical /\ ~s.reported /\ (s.born \/ IgnoreReference)
         /\ s' = [s EXCEPT !.reported = TRUE]
Cancel == /\ ~s.cancelled /\ s.cold
          /\ s' = [s EXCEPT !.cancelled = TRUE, !.gate = IF ReleaseOnCancel THEN FALSE ELSE @]
ColdDone == /\ s.cold /\ (s.logical \/ s.cancelled \/ ~s.closed \/ ~s.fenced) /\ s' = [s EXCEPT !.cold = FALSE, !.gate = FALSE]
Next == Cut \/ Seal \/ Targets \/ Logical \/ Recreate \/ Query \/ Cancel \/ ColdDone
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.targets \subseteq AllTargets
          /\ \A name \in {"closed", "fenced", "offline", "cut", "seal", "manifest", "born",
                            "logical", "reported", "physical", "oldRead", "cold", "gate", "cancelled",
                            "attempted"} : s[name] \in BOOLEAN
DeleteNeedsFencedClosed == s.cut => s.closed /\ s.fenced
TargetsFollowStandardSeal == ~s.attempted
LogicalHasCompleteObligation == s.logical => s.seal /\ s.manifest /\ s.targets = AllTargets
ReportRequiresManifestBirth == s.reported => s.born
ColdOwnsGate == s.cold => s.gate
LogicalIsNotPhysicalOrAccessBarrier == ~s.physical /\ s.oldRead
OfflineDoesNotPreventLogical == s.offline /\ s.seal /\ s.manifest /\ ~s.cancelled /\ s.cold => ENABLED Logical \/ s.logical
=============================================================================
