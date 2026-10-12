-------------------- MODULE ProfileOpenReplicaAudit --------------------
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
CONSTANTS IgnoreScope, NoCut, UseTail, FenceWriter, UnknownIsHole, IgnoreFresh,
          ClearOpen, EarlyDrain, EarlyRelease
VARIABLE s
vars == <<s>>
(* Actual scoped LAC cut, native source outcome/stable absence, fresh metadata,
   and health/peer lease termination are Java inputs, not produced by this projection. *)
Init == s \in [scoped : BOOLEAN, known : BOOLEAN, fresh : BOOLEAN, stableMissing : BOOLEAN,
               status : {"HEALTHY", "CORRUPT", "MISSING", "UNKNOWN"},
               phase : {"new"}, cut : {0}, tail : {0}, observed : {-1},
               held : {FALSE}, read : {FALSE}, fenced : {FALSE},
               healthActual : {FALSE}, leaseActual : {FALSE}, marker : {FALSE}, cleared : {FALSE}]
Acquire == /\ s.phase = "new" /\ (s.scoped \/ IgnoreScope) /\ (s.known \/ NoCut)
           /\ s' = [s EXCEPT !.phase = "running", !.held = TRUE,
                             !.healthActual = TRUE, !.leaseActual = TRUE]
Skip == /\ s.phase = "new" /\ (~s.scoped \/ ~s.known)
        /\ s' = [s EXCEPT !.phase = "done"]
AdvanceWriter == /\ s.tail = 0 /\ s' = [s EXCEPT !.tail = 1]
Read == /\ s.phase = "running" /\ ~s.read
        /\ s' = [s EXCEPT !.read = TRUE, !.observed = IF UseTail THEN s.tail ELSE s.cut,
                          !.fenced = FenceWriter]
HealthDone == /\ s.phase = "running" /\ s.read /\ s.healthActual
              /\ s' = [s EXCEPT !.healthActual = FALSE]
Drain == /\ s.phase = "running" /\ s.read /\ (~s.healthActual \/ EarlyDrain)
         /\ s' = [s EXCEPT !.phase = "draining"]
LeaseDone == /\ s.phase = "draining" /\ s.leaseActual
             /\ s' = [s EXCEPT !.leaseActual = FALSE]
Finish == /\ s.phase = "draining" /\ (~s.leaseActual \/ EarlyRelease)
          /\ s' = [s EXCEPT !.phase = "done", !.held = FALSE,
                    !.marker = (s.fresh \/ IgnoreFresh)
                               /\ (s.status = "CORRUPT"
                                    \/ (s.status = "MISSING" /\ s.stableMissing)
                                    \/ (UnknownIsHole /\ s.status = "UNKNOWN")),
                    !.cleared = ClearOpen /\ s.status = "HEALTHY"]
Next == Acquire \/ Skip \/ AdvanceWriter \/ Read \/ HealthDone \/ Drain \/ LeaseDone \/ Finish
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"new", "running", "draining", "done"} /\ s.observed \in -1..1
OpenAuditHasScopeAndKnownCut == s.held => s.scoped /\ s.known
OpenAuditNeverScansGrowingTail == s.observed <= s.cut
OpenAuditNeverFencesWriter == ~s.fenced
OpenAuditMarkerHasActualSource == s.marker => s.status = "CORRUPT"
                                             \/ (s.status = "MISSING" /\ s.stableMissing)
OpenAuditMarkerUsesFinalFresh == s.marker => s.fresh
OpenAuditNeverClearsMarker == ~s.cleared
OpenAuditWaitsActualHealth == s.phase \in {"draining", "done"} => ~s.healthActual
OpenAuditWaitsActualLease == ~s.held => ~s.leaseActual
=============================================================================
