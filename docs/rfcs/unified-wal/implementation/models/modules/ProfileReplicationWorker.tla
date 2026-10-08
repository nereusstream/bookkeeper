-------------------- MODULE ProfileReplicationWorker --------------------
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
CONSTANTS Ack, FallBackClassic, ClearAfterQuorum, PublishBeforeData, ActivateNormal,
          NewIntentOnRetry, RecopyPublished, ReleaseOnStop, ChangeTask, IgnoreDelete,
          IgnoreDescriptor, CountUnknown, RepairEmpty, ExceedBound, ChangePrefix
VARIABLE s
vars == <<s>>
(* Verified selection, copy force, response delivery and guarded removal are
   inputs from the separate point/copy/close/health/marker protocols. This is the
   enabled worker composition and ownership projection, not a producer of
   fsync, TLS, crypto, metadata birth guards, wall-clock deadlines or liveness. *)
Init == \E copies \in {1, 2, 3}, closed \in BOOLEAN, empty \in BOOLEAN,
           enabled \in BOOLEAN, metadata \in BOOLEAN, descriptor \in BOOLEAN :
        s = [phase |-> "route", copies |-> copies, initialCopies |-> copies, closed |-> closed,
             empty |-> empty, enabled |-> enabled, metadata |-> metadata, descriptor |-> descriptor,
             p |-> IF empty THEN -1 ELSE 2, len |-> IF empty THEN 0 ELSE 3,
             intent |-> 0, originalIntent |-> 0, published |-> FALSE, durable |-> FALSE,
             unknown |-> FALSE, copied |-> 0, repairs |-> 0, normal |-> FALSE,
             stop |-> FALSE, inFlight |-> FALSE, lock |-> TRUE, task |-> 0, deleted |-> FALSE,
             cleared |-> FALSE, clearedCopies |-> 0, clearedTask |-> 0, clearedDeleted |-> FALSE]
Route == /\ s.phase = "route"
         /\ s' = [s EXCEPT !.phase = IF s.enabled /\ s.metadata /\ (s.descriptor \/ IgnoreDescriptor)
                                     THEN "recover" ELSE IF FallBackClassic \/ (~s.enabled /\ ~s.metadata)
                                     THEN "classic" ELSE "pending"]
Close == /\ s.phase = "recover" /\ (s.closed \/ s.copies >= Ack)
         /\ s' = [s EXCEPT !.closed = TRUE, !.phase = "health"]
NeedCopy == /\ (s.phase = "recover" /\ ~s.closed /\ s.copies < Ack)
                  \/ (s.phase = "health" /\ s.copies < 3)
            /\ s' = [s EXCEPT !.phase = "plan"]
Plan == /\ s.phase = "plan" /\ (~s.empty \/ RepairEmpty) /\ (s.repairs < 1 \/ ExceedBound) /\ ~s.stop
        /\ s' = [s EXCEPT !.phase = "copy", !.intent = IF @ = 0 THEN 1 ELSE @, !.originalIntent = 1,
                  !.inFlight = TRUE]
CopyOK == /\ s.phase = "copy"
          /\ s' = [s EXCEPT !.phase = "ready", !.inFlight = FALSE, !.durable = TRUE, !.copied = @ + 1]
CopyUnknown == /\ s.phase = "copy"
               /\ s' = [s EXCEPT !.phase = "unknown", !.inFlight = FALSE, !.unknown = TRUE]
Publish == /\ s.phase = "ready" \/ (PublishBeforeData /\ s.phase = "copy")
                  \/ (CountUnknown /\ s.phase = "unknown")
           /\ ~s.stop /\ (~s.deleted \/ IgnoreDelete)
           /\ s' = [s EXCEPT !.phase = "published", !.published = TRUE, !.copies = @ + 1,
                     !.repairs = @ + 1, !.normal = ActivateNormal,
                     !.p = IF ChangePrefix THEN @ + 1 ELSE @]
CloseGrant == /\ s.phase = "published" /\ ~s.stop /\ ~s.deleted
              /\ s' = [s EXCEPT !.phase = "recover"]
ReplyLost == /\ s.phase = "published" /\ ~s.stop
             /\ s' = [s EXCEPT !.phase = "resume"]
Resume == /\ s.phase = "resume" /\ ~s.stop /\ ~s.deleted
          /\ s' = [s EXCEPT !.phase = "published", !.intent = IF NewIntentOnRetry THEN @ + 1 ELSE @,
                    !.copied = IF RecopyPublished THEN @ + 1 ELSE @]
Clear == /\ s.phase = "health" /\ s.closed /\ (s.copies = 3 \/ (ClearAfterQuorum /\ s.copies >= Ack))
         /\ ~s.stop /\ (~s.deleted \/ IgnoreDelete)
         /\ s' = [s EXCEPT !.phase = "done", !.cleared = TRUE, !.clearedCopies = s.copies,
                   !.clearedTask = IF ChangeTask THEN 1 ELSE s.task, !.clearedDeleted = s.deleted, !.lock = FALSE]
Stop == /\ ~s.stop /\ s.phase # "done"
        /\ s' = [s EXCEPT !.stop = TRUE, !.lock = IF ReleaseOnStop THEN FALSE ELSE @]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Drain == /\ s.stop /\ ~s.inFlight /\ s.lock /\ s' = [s EXCEPT !.lock = FALSE]
Next == Route \/ Close \/ NeedCopy \/ Plan \/ CopyOK \/ CopyUnknown \/ Publish \/ CloseGrant \/ ReplyLost
        \/ Resume \/ Clear \/ Stop \/ Delete \/ Drain
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"route", "recover", "classic", "pending", "health", "plan", "copy", "ready", "unknown", "published", "resume", "done"}
NoClassicForProfile == (s.metadata \/ s.enabled) => s.phase # "classic"
ClearRequiresAllCopies == s.cleared => s.clearedCopies = 3
PublicationRequiresDurableData == s.published => s.durable
NoNormalActivation == ~s.normal
SameIntentOnResume == s.published => s.intent = s.originalIntent
PublishedResumeDoesNotRecopy == /\ s.copied <= s.repairs + 1
                              /\ (s.phase \in {"published", "resume", "recover", "health", "done"} => s.copied <= s.repairs)
ActualChildRetainsTask == s.inFlight => s.lock
OriginalTaskAtClear == s.cleared => s.clearedTask = 0
DeletePreventsCompletion == s.cleared => ~s.clearedDeleted
DescriptorRequired == s.phase # "classic" /\ s.phase # "pending" /\ s.phase # "route" => s.descriptor
UnknownDoesNotPublish == s.unknown => ~s.published
EmptyMissingRemainsPending == s.empty /\ s.initialCopies < 3 => ~s.published
WorkIsBounded == s.repairs <= 1
PrefixIsFixed == s.p = IF s.empty THEN -1 ELSE 2
=============================================================================
