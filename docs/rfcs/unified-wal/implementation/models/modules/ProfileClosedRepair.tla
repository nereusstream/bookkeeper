-------------------- MODULE ProfileClosedRepair --------------------
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
CONSTANTS IgnoreBirth, IgnoreDelete, Reopen, WrongBoundary, MoveCloseCut, IgnoreCloseOffset,
          PublishBeforeCopy, ExposeBeforeClose, RecopyPublished
VARIABLE s
vars == <<s>>
(* The close cut is in standard metadata. Payload/force/guard outcomes are inputs,
   no ledger reopen or underreplication completion is part of this protocol. *)
Init == s \in [phase : {"closed"}, birth : {0}, publishedBirth : {0}, deleted : {FALSE}, state : {"CLOSED"},
               p : {2}, len : {48}, closeIndex : {3}, closeBefore : {6}, head : {3}, version : {7},
               copied : {FALSE}, copyCount : {0}, before : {7}, published : {FALSE}, publishedDeleted : {FALSE}, readable : {FALSE}]
Restore == /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Prepare == /\ s.phase = "closed" /\ (~s.deleted \/ IgnoreDelete) /\ (s.birth = 0 \/ IgnoreBirth)
           /\ s' = [s EXCEPT !.phase = "prepared", !.state = IF Reopen THEN "IN_RECOVERY" ELSE @]
Copy == /\ s.phase = "prepared" /\ s' = [s EXCEPT !.phase = "copied", !.copied = TRUE, !.copyCount = 1]
Publish == /\ s.phase = "copied" \/ (PublishBeforeCopy /\ s.phase = "prepared")
           /\ (~s.deleted \/ IgnoreDelete) /\ (s.birth = 0 \/ IgnoreBirth)
           /\ s' = [s EXCEPT !.phase = "published", !.published = TRUE, !.publishedBirth = s.birth, !.publishedDeleted = s.deleted, !.head = @ + 1, !.version = @ + 1,
                     !.before = IF IgnoreCloseOffset THEN 6 ELSE 7,
                     !.p = IF WrongBoundary THEN 3 ELSE @, !.len = IF WrongBoundary THEN 64 ELSE @,
                     !.closeIndex = IF MoveCloseCut THEN 4 ELSE @, !.closeBefore = IF MoveCloseCut THEN 7 ELSE @]
CloseGrant == /\ s.phase = "published" /\ ~s.deleted
              /\ s' = [s EXCEPT !.phase = "readable", !.readable = TRUE]
EarlyRead == /\ ExposeBeforeClose /\ s.phase = "published" /\ s' = [s EXCEPT !.readable = TRUE]
Resume == /\ s.phase = "published" /\ RecopyPublished /\ s.copyCount = 1
          /\ s' = [s EXCEPT !.copyCount = 2]
Next == Restore \/ Delete \/ Prepare \/ Copy \/ Publish \/ CloseGrant \/ EarlyRead \/ Resume
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"closed", "prepared", "copied", "published", "readable"}
ClosedNeverReopens == s.state = "CLOSED"
ClosedBoundaryIsPreserved == s.p = 2 /\ s.len = 48
OriginalCloseCutIsPreserved == s.closeIndex = 3 /\ s.closeBefore = 6
PublicationHasSameBirth == s.published => s.publishedBirth = 0
PublicationBeforeDelete == s.published => ~s.publishedDeleted
PublicationHasDurableCopy == s.published => s.copied
RepairVersionIncludesCloseOffset == s.published => s.before = 7 /\ s.version = 2 + s.head + 2
TargetReadableAfterGrantClose == s.readable => s.phase = "readable"
PublishedResumeDoesNotRecopy == s.copyCount <= 1
=============================================================================
