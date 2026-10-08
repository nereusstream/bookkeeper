-------------------- MODULE ProfileMarkerCleanup --------------------
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
CONSTANTS IgnoreMarkerBirth, IgnoreMarkerVersion, IgnoreMetadata, IgnoreDelete, IgnoreLock,
          IgnoreIssuer, IgnoreExpiry, ReuseProof, IgnoreStop, ReleaseEarly, WrongRelease, AdoptNewTask
VARIABLE s
vars == <<s>>
(* Health is an input produced by the separate bounded health scan, never an
   A-quorum boolean. Session-managed pair removal is modeled; administrative
   lock delete/reset, root restoration, physical media stability and liveness
   are outside this protocol. Completion records the cleanup cut, not future state. *)
Init == s \in [phase : {"proof"}, issuer : {0, 1}, expired : {FALSE, TRUE}, used : {FALSE, TRUE},
               active : {TRUE}, stopped : {FALSE}, inFlight : {FALSE}, markerBirth : {0}, markerVersion : {0},
               markerWitness : {FALSE}, lock : {0}, ownWitness : {TRUE}, taskGeneration : {0}, metadata : {0}, deleted : {FALSE},
               claimed : {FALSE}, claimIssuer : {0}, claimExpired : {FALSE}, claimUsed : {FALSE},
               submittedStopped : {FALSE}, cleared : {FALSE}, clearMarkerBirth : {0}, clearMarkerVersion : {0},
               clearMetadata : {0}, clearDeleted : {FALSE}, clearLock : {0}, clearTaskGeneration : {0}, reported : {FALSE},
               localObligation : {TRUE}, foreignDeleted : {FALSE}]
MarkerUpdate == /\ s.markerVersion = 0 /\ s' = [s EXCEPT !.markerVersion = 1]
MarkerRecreate == /\ s.markerBirth = 0
                  /\ s' = [s EXCEPT !.markerBirth = 1, !.markerVersion = 0, !.markerWitness = FALSE]
MetadataChange == /\ s.metadata = 0 /\ s' = [s EXCEPT !.metadata = 1]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
Expire == /\ ~s.expired /\ s' = [s EXCEPT !.expired = TRUE]
Stop == /\ ~s.stopped /\ s.phase # "done"
        /\ s' = [s EXCEPT !.stopped = TRUE, !.active = IF ReleaseEarly THEN FALSE ELSE @]
LoseSession == /\ s.lock = 0 /\ s' = [s EXCEPT !.lock = -1, !.ownWitness = FALSE]
ReplaceLock == /\ s.lock = -1 /\ s' = [s EXCEPT !.lock = 1, !.taskGeneration = 1]
Reacquire == /\ s.lock = -1
             /\ s' = [s EXCEPT !.lock = 0, !.ownWitness = TRUE, !.taskGeneration = 1]
Claim == /\ s.phase = "proof" /\ (s.issuer = 0 \/ IgnoreIssuer) /\ (~s.expired \/ IgnoreExpiry)
         /\ (~s.used \/ ReuseProof) /\ (~s.stopped \/ IgnoreStop)
         /\ s' = [s EXCEPT !.phase = "claimed", !.claimed = TRUE, !.used = TRUE,
                   !.claimIssuer = s.issuer, !.claimExpired = s.expired, !.claimUsed = s.used]
Arm == /\ s.phase = "claimed" /\ (~s.stopped \/ IgnoreStop)
       /\ (s.markerBirth = 0 \/ IgnoreMarkerBirth) /\ (s.markerVersion = 0 \/ IgnoreMarkerVersion)
       /\ ((s.lock = 0 /\ s.ownWitness) \/ IgnoreLock)
       /\ s' = [s EXCEPT !.phase = "submitted", !.inFlight = TRUE, !.submittedStopped = s.stopped,
                 !.markerWitness = TRUE]
Clear == /\ s.phase = "submitted"
         /\ ((s.markerBirth = 0 /\ s.markerWitness) \/ IgnoreMarkerBirth)
         /\ (s.markerVersion = 0 \/ IgnoreMarkerVersion) /\ (s.metadata = 0 \/ IgnoreMetadata)
         /\ (~s.deleted \/ IgnoreDelete) /\ ((s.lock = 0 /\ s.ownWitness) \/ IgnoreLock)
         /\ (s.taskGeneration = 0 \/ AdoptNewTask \/ IgnoreLock)
         /\ s' = [s EXCEPT !.phase = "committed", !.cleared = TRUE,
                   !.clearMarkerBirth = s.markerBirth, !.clearMarkerVersion = s.markerVersion,
                   !.clearMetadata = s.metadata, !.clearDeleted = s.deleted, !.clearLock = s.lock,
                   !.clearTaskGeneration = s.taskGeneration,
                   !.lock = -1, !.ownWitness = FALSE, !.markerWitness = FALSE]
Ack == /\ s.phase = "committed"
       /\ s' = [s EXCEPT !.phase = "done", !.reported = TRUE, !.inFlight = FALSE, !.active = FALSE,
                 !.localObligation = FALSE]
LostReply == /\ s.phase = "committed"
             /\ s' = [s EXCEPT !.phase = "done", !.inFlight = FALSE, !.active = FALSE]
Release == /\ s.phase = "done" /\ s.localObligation
           /\ s' = [s EXCEPT !.localObligation = FALSE,
                     !.foreignDeleted = s.lock = 1 /\ WrongRelease,
                     !.lock = IF s.ownWitness \/ WrongRelease THEN -1 ELSE @, !.ownWitness = FALSE]
Next == MarkerUpdate \/ MarkerRecreate \/ MetadataChange \/ Delete \/ Expire \/ Stop \/ LoseSession
        \/ ReplaceLock \/ Reacquire \/ Claim \/ Arm \/ Clear \/ Ack \/ LostReply \/ Release
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"proof", "claimed", "submitted", "committed", "done"}
OriginalMarkerBirth == s.cleared => s.clearMarkerBirth = 0
OriginalMarkerVersion == s.cleared => s.clearMarkerVersion = 0
MatchingMetadata == s.cleared => s.clearMetadata = 0
DeleteKeepsTask == s.cleared => ~s.clearDeleted
OriginalWorkerLock == s.cleared => s.clearLock = 0
OriginalAcquiredTask == s.cleared => s.clearTaskGeneration = 0
OriginalIssuer == s.claimed => s.claimIssuer = 0
LiveProofAtClaim == s.claimed => ~s.claimExpired
ProofUsedOnce == s.claimed => ~s.claimUsed
StopBeforeSubmissionKeepsTask == s.cleared => ~s.submittedStopped
ActualCommandOwnsGate == s.inFlight => s.active
UnknownReleaseKeepsForeignLock == ~s.foreignDeleted
=============================================================================
