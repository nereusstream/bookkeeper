-------------------- MODULE ProfileRepairGrant --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipAdmission, SkipInstall, IgnoreDelete, IgnoreBirth, SkipAuth,
          IgnoreTuple, AckBeforeForce, ForgetRetention, ActivateNormal
VARIABLE s
vars == <<s>>
(* One fixed repair slot, immutable plan, admitted target, local grant and DATA.
   No membership publication, grant close or CLOSED. Real bytes/auth/force are Java facts. *)
Init == s \in [phase : {"inert"}, installed : BOOLEAN, authorized : BOOLEAN, tuple : BOOLEAN,
               deleted : {FALSE}, birth : {1}, admitted : {FALSE}, retained : {FALSE},
               granted : {FALSE}, forced : {FALSE}, data : {FALSE}, normal : {FALSE}]
Install == /\ s.phase = "inert" /\ ~s.installed
           /\ s' = [s EXCEPT !.installed = TRUE]
Admission == /\ s.phase = "inert"
             /\ LET accept == (s.installed \/ SkipInstall) /\ (s.birth = 1 \/ IgnoreBirth)
                                /\ (~s.deleted \/ IgnoreDelete)
                IN s' = [s EXCEPT !.phase = "admission", !.admitted = accept,
                         !.retained = accept /\ ~ForgetRetention]
Delete == /\ ~s.deleted /\ s.phase \in {"inert", "admission"} /\ s' = [s EXCEPT !.deleted = TRUE]
Restore == /\ s.phase = "inert" /\ s.birth = 1 /\ s' = [s EXCEPT !.birth = 2]
Grant == /\ s.phase \in {"inert", "admission"}
         /\ LET accept == (s.admitted \/ SkipAdmission) /\ (s.installed \/ SkipInstall)
                            /\ (s.authorized \/ SkipAuth) /\ (s.tuple \/ IgnoreTuple)
                            /\ (~s.deleted \/ IgnoreDelete)
            IN s' = [s EXCEPT !.phase = "grant", !.granted = accept,
                     !.forced = accept /\ ~AckBeforeForce,
                     !.normal = accept /\ ActivateNormal]
Copy == /\ s.phase = "grant" /\ s.granted
        /\ s' = [s EXCEPT !.phase = "copied", !.data = TRUE]
Next == Install \/ Admission \/ Delete \/ Restore \/ Grant \/ Copy
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"inert", "admission", "grant", "copied"}
AdmissionHasInstallAndBirth == s.admitted => (s.installed /\ s.birth = 1)
GrantHasAdmissionAndInstall == s.granted => (s.admitted /\ s.installed)
GrantHasAuthAndExactTuple == s.granted => (s.authorized /\ s.tuple)
GrantHasActualForce == s.granted => s.forced
AdmittedTargetIsRetained == s.admitted => s.retained
RecoveryDoesNotActivateNormal == ~s.normal
DataHasRecoveryAuthority == s.data => (s.granted /\ s.forced /\ s.admitted /\ s.installed)
(* Delete can follow a historical grant; it must prevent fresh grants. *)
DeletePreventsFreshGrant == s.phase = "grant" /\ s.granted => ~s.deleted
=============================================================================
