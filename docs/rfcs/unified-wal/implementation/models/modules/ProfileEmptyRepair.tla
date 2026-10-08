-------------------- MODULE ProfileEmptyRepair --------------------
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
CONSTANTS IgnoreAdmission, PublishBeforeBoundary, FakeData, GrantData, ActivateNormal, ChangeCut,
          IgnoreDelete, IgnoreBirth, ForgetReceipt, NewOperation, ReleaseEarly
VARIABLE s
vars == <<s>>
(* UIC6 actual force/apply and metadata guard observations are inputs.
   P=-1/L=0 or historical P=2/L=48 is an already verified selected input.
   No DATA coordinate, grant, crypto, TLS, fsync producer or liveness is modeled. *)
Init == \E originalP \in {-1, 2} : s \in [phase : {"prepared"}, admitted : {FALSE}, installed : {FALSE}, boundary : {FALSE},
               published : {FALSE}, receipt : {FALSE}, empty : {TRUE}, data : {FALSE}, grant : {FALSE},
               normal : {FALSE}, p : {originalP}, len : {IF originalP = -1 THEN 0 ELSE 48},
               originalP : {originalP}, originalLength : {IF originalP = -1 THEN 0 ELSE 48}, operation : {0}, inFlight : {FALSE}, permit : {TRUE},
               deleted : {FALSE}, birth : {0}, publishDelete : {FALSE}, publishBirth : {0}]
Install == /\ s.phase = "prepared" /\ s' = [s EXCEPT !.phase = "installed", !.installed = TRUE]
Admit == /\ s.phase = "installed" /\ ~s.deleted /\ s.birth = 0
         /\ s' = [s EXCEPT !.phase = "admitted", !.admitted = TRUE]
Submit == /\ s.phase = "admitted" \/ (IgnoreAdmission /\ s.phase = "installed")
          /\ ~s.deleted /\ s.birth = 0
          /\ s' = [s EXCEPT !.phase = "submitted", !.inFlight = TRUE, !.data = FakeData, !.grant = GrantData]
Apply == /\ s.phase = "submitted" /\ s' = [s EXCEPT !.phase = "durable", !.boundary = TRUE, !.inFlight = FALSE]
Publish == /\ s.phase = "durable" \/ (PublishBeforeBoundary /\ s.phase = "submitted")
           /\ (~s.deleted \/ IgnoreDelete) /\ (s.birth = 0 \/ IgnoreBirth)
           /\ s' = [s EXCEPT !.phase = "published", !.published = TRUE, !.receipt = ~ForgetReceipt,
                     !.publishDelete = s.deleted, !.publishBirth = s.birth, !.normal = ActivateNormal,
                     !.p = IF ChangeCut THEN @ + 1 ELSE @, !.len = IF ChangeCut THEN @ + 1 ELSE @]
LoseReply == /\ s.phase = "durable" /\ s' = [s EXCEPT !.phase = "admitted",
                                              !.operation = IF NewOperation THEN 1 ELSE @]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Restore == /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
Stop == /\ ReleaseEarly /\ s.inFlight /\ s.permit /\ s' = [s EXCEPT !.permit = FALSE]
Next == Install \/ Admit \/ Submit \/ Apply \/ Publish \/ LoseReply \/ Delete \/ Restore \/ Stop
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"prepared", "installed", "admitted", "submitted", "durable", "published"}
BoundaryRequiresAdmission == s.boundary => s.admitted
PublicationRequiresBoundary == s.published => s.boundary
NoFakeData == ~s.data
NoDataGrant == ~s.grant
NoNormalActivation == ~s.normal
OriginalCutIsPreserved == s.p = s.originalP /\ s.len = s.originalLength
DeleteBlocksPublication == s.published => ~s.publishDelete
OriginalBirthAtPublication == s.published => s.publishBirth = 0
PublicationRetainsReceipt == s.published => s.receipt
RetryRetainsOperation == s.operation = 0
ActualBoundaryOwnsPermit == s.inFlight => s.permit
=============================================================================
