-------------------- MODULE ClosedReconstructionPublication --------------------
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
EXTENDS Integers, FiniteSets
CONSTANTS SecondKind, Mutation
VARIABLE s
vars == <<s>>

Files == 1..2
Members == 1..2
Attempts == 1..2
ScopeBirth == 1
OriginalEpoch == 1
OriginalCut == 1

(* The original immutable identities and exact SELF/EMPTY proof are input facts.
   Object (a,i) identifies file i from attempt a. Generation 0 is original;
   generation 3 denotes a byte-identical replacement inode. *)
OriginalMember(i) == [birth |-> i, version |-> 1, kind |-> IF i = 1 THEN "SELF" ELSE SecondKind,
                       lifecycle |-> "CLOSED", predecessor |-> 0]
OriginalMembers == [i \in Members |-> OriginalMember(i)]
NonemptyMembers == {i \in Members : OriginalMember(i).kind = "SELF"}
Objects(a) == [i \in Files |-> <<a, i>>]
EmptyTicket == [version |-> 0, birth |-> 0, closed |-> {}, synced |-> FALSE]
NoReady == [attempt |-> 0, ticket |-> EmptyTicket, claim |-> <<0, 0, 0>>, checks |-> {}, objects |-> Objects(0)]

Init ==
    s = [member |-> OriginalMembers, changed |-> FALSE,
         presentOriginal |-> Files, nativeBirth |-> 0,
         admission |-> [birth |-> 0, files |-> {}, objects |-> Objects(0),
                       members |-> {}, snapshot |-> OriginalMembers],
         admissionSeen |-> FALSE,
         owner |-> 1, live |-> TRUE, readyBeforeHalt |-> FALSE, publishCrash |-> FALSE, splitCrash |-> FALSE,
         claimExpected |-> [a \in Attempts |-> -1], claimState |-> [a \in Attempts |-> "NEW"],
         shadow |-> <<0, 0, 0>>, terminalShadow |-> FALSE,
         candidate |-> [a \in Attempts |-> [i \in Files |-> 0]],
         synced |-> {}, discarded |-> {}, imageFailure |-> FALSE,
         ticket |-> [a \in Attempts |-> EmptyTicket], submitted |-> {},
         lateRejected |-> FALSE, ready |-> NoReady, readyReply |-> "NONE", seen |-> FALSE,
         path |-> [i \in Files |-> 0], active |-> Objects(0),
         stage |-> 0, checkedPrefix |-> FALSE, checkedActive |-> FALSE, tampered |-> FALSE,
         action |-> 0, replayed |-> FALSE, afterForceCrash |-> FALSE, badResolution |-> FALSE,
         epoch |-> OriginalEpoch, cutoff |-> 0, boot |-> 0, startup |-> "OFF",
         healthyRetry |-> FALSE, refused |-> {}, retryRefused |-> FALSE, badOpen |-> FALSE, badWire |-> FALSE,
         deleteForce |-> FALSE, tombstone |-> {}, cursor |-> {}, obligations |-> Members,
         localData |-> Members, grant |-> 0, oldFreed |-> {}, oldAppended |-> FALSE,
         newAppended |-> FALSE, readSurvivor |-> FALSE]

Admitted == s.admission.birth # 0
Ready == s.ready.attempt # 0
ExactAdmissionBirth == Admitted /\ s.nativeBirth = s.admission.birth
OriginalClosed == {i \in Members : s.member[i] = s.admission.snapshot[i]}
Deleted == {i \in Members : s.member[i].lifecycle = "DELETE"}
ExactDelete(i) ==
    /\ i \in Deleted
    /\ s.member[i].birth = s.admission.snapshot[i].birth
    /\ s.member[i].kind = s.admission.snapshot[i].kind
    /\ s.member[i].predecessor = s.admission.snapshot[i].version
    /\ s.member[i].version = s.admission.snapshot[i].version + 1
ExactRejoin == \A i \in Members : i \in OriginalClosed \/ ExactDelete(i)

ChangeMember(i, change) ==
    /\ ~s.changed /\ s.startup \in {"OFF", "STARTING"} /\ s.epoch = OriginalEpoch
    /\ s' = [s EXCEPT !.changed = TRUE, !.member[i] =
        CASE change \in {"DELETE", "WRONG_DELETE"} ->
            [@ EXCEPT !.lifecycle = "DELETE", !.version = @ + 1,
                      !.predecessor = IF change = "DELETE" THEN s.member[i].version ELSE 0]
          [] change = "VERSION" -> [@ EXCEPT !.version = @ + 1]
          [] OTHER -> [@ EXCEPT !.birth = @ + 2]]

LoseRequiredFile ==
    /\ ~Admitted /\ 2 \in s.presentOriginal
    /\ s' = [s EXCEPT !.presentOriginal = @ \ {2}]

ReplaceAdmissionBirth ==
    /\ ExactAdmissionBirth /\ s.startup = "OFF" /\ s.epoch = OriginalEpoch
    /\ s' = [s EXCEPT !.nativeBirth = ScopeBirth + 1]

Admit(files, members) ==
    /\ s.live /\ ~Admitted /\ s.epoch = OriginalEpoch
    /\ files = Files /\ files \subseteq s.presentOriginal /\ members = Members
    /\ \A i \in members : s.member[i] = OriginalMember(i)
    /\ s' = [s EXCEPT !.admission = [birth |-> ScopeBirth, files |-> files,
                                     objects |-> s.active, members |-> members, snapshot |-> s.member],
                      !.nativeBirth = ScopeBirth, !.shadow = <<0, ScopeBirth, 0>>]

ReadAdmission ==
    /\ s.live /\ ExactAdmissionBirth /\ ~s.admissionSeen
    /\ s' = [s EXCEPT !.admissionSeen = TRUE]

SubmitClaim(a) ==
    /\ s.live /\ s.owner = a /\ s.admissionSeen /\ ExactAdmissionBirth /\ ~Ready /\ ~s.imageFailure
    /\ s.claimState[a] = "NEW" /\ OriginalClosed = Members
    /\ s' = [s EXCEPT !.claimExpected[a] = s.shadow[3], !.claimState[a] = "SUBMITTED"]

CommitClaim(a) ==
    /\ s.claimState[a] = "SUBMITTED"
    /\ IF ExactAdmissionBirth /\ ~s.terminalShadow /\ ~s.imageFailure /\ s.claimExpected[a] = s.shadow[3]
       THEN s' = [s EXCEPT !.shadow = <<a, s.admission.birth, s.claimExpected[a] + 1>>,
                          !.claimState[a] = "COMMITTED"]
       ELSE s' = [s EXCEPT !.claimState[a] = "REJECTED"]

LoseClaimReply(a) ==
    /\ s.claimState[a] = "COMMITTED"
    /\ s' = [s EXCEPT !.claimState[a] = "LOST"]

Claim(a) == <<a, s.admission.birth, s.claimExpected[a] + 1>>

ReadClaim(a) ==
    /\ s.live /\ s.owner = a /\ ExactAdmissionBirth /\ ~Ready /\ ~s.imageFailure
    /\ s.claimState[a] \in {"COMMITTED", "LOST"} /\ s.shadow = Claim(a)
    /\ s' = [s EXCEPT !.claimState[a] = "OBSERVED"]

CanCopy(a) ==
    /\ s.live /\ s.owner = a /\ ExactAdmissionBirth /\ ~Ready /\ ~s.imageFailure
    /\ s.claimState[a] = "OBSERVED" /\ s.shadow = Claim(a)

StartWrite(a, i) ==
    /\ CanCopy(a) /\ s.candidate[a][i] = 0
    /\ \A before \in 1..(i - 1) : s.candidate[a][before] = 5
    /\ s' = [s EXCEPT !.candidate[a][i] = 1]

(* The actual completion of an already issued write can arrive after the
   issuing JVM halts. It can only affect that attempt's object. *)
FinishWrite(a, i) ==
    /\ s.candidate[a][i] = 1
    /\ s' = [s EXCEPT !.candidate[a][i] = 2]

ForceCandidate(a, i) ==
    /\ CanCopy(a) /\ s.candidate[a][i] = 2
    /\ s' = [s EXCEPT !.candidate[a][i] = 3]

ReadBack(a, i) ==
    /\ CanCopy(a) /\ s.candidate[a][i] = 3
    /\ s' = [s EXCEPT !.candidate[a][i] = 4]

FirstClose(a, i) ==
    /\ CanCopy(a) /\ s.candidate[a][i] = 4
    /\ s' = [s EXCEPT !.candidate[a][i] = 5]

ObserveImageFailure(a, i) ==
    /\ CanCopy(a) /\ s.candidate[a][i] \in {2, 4}
    /\ s' = [s EXCEPT !.imageFailure = TRUE]

ClosedFiles(a) == {i \in Files : s.candidate[a][i] = 5}

SyncCandidates(a) ==
    /\ CanCopy(a) /\ a \notin s.synced /\ ClosedFiles(a) = Files
    /\ s' = [s EXCEPT !.synced = @ \cup {a}, !.discarded = @ \cup (1..(a - 1))]

SubmitReady(a) ==
    /\ CanCopy(a) /\ s.ticket[a] = EmptyTicket /\ OriginalClosed = Members
    /\ (a \in s.synced \/ (Mutation = "EarlyReady" /\ 1 \in ClosedFiles(a)))
    /\ s' = [s EXCEPT !.ticket[a] =
        [version |-> s.shadow[3], birth |-> s.shadow[2],
         closed |-> ClosedFiles(a), synced |-> a \in s.synced],
        !.submitted = @ \cup {a}]

CommitReady(a) ==
    LET t == s.ticket[a]
        exactClaim == s.shadow = <<a, t.birth, t.version>>
        exactMembers == OriginalClosed
    IN
    /\ a \in s.submitted
    /\ IF ExactAdmissionBirth /\ ~Ready /\ ~s.imageFailure /\ t.birth = s.admission.birth
           /\ (exactClaim \/ Mutation = "StaleReady")
           /\ (exactMembers = Members \/ (Mutation = "SkipSecondMember" /\ 1 \in exactMembers))
       THEN s' = [s EXCEPT !.ready = [attempt |-> a, ticket |-> t, objects |-> Objects(a),
                                      claim |-> s.shadow, checks |-> exactMembers],
                          !.terminalShadow = TRUE, !.shadow = <<a, t.birth, s.shadow[3] + 1>>,
                          !.submitted = @ \ {a}, !.readyReply = "PENDING"]
       ELSE s' = [s EXCEPT !.submitted = @ \ {a},
                          !.lateRejected = @ \/ (~Ready /\ a = 1 /\ s.shadow[1] = 2 /\ ~exactClaim
                                                /\ ExactAdmissionBirth /\ exactMembers = Members /\ ~s.imageFailure)]

LoseReadyReply ==
    /\ s.readyReply = "PENDING"
    /\ s' = [s EXCEPT !.readyReply = "LOST"]

ReadReady ==
    /\ s.live /\ s.admissionSeen /\ ExactAdmissionBirth /\ Ready /\ ~s.seen
    /\ s.terminalShadow /\ s.ready.ticket.birth = s.admission.birth
    /\ s' = [s EXCEPT !.seen = TRUE,
                      !.readyReply = IF s.readyReply = "PENDING" THEN "RETURNED" ELSE @]

HaltShadow ==
    /\ s.live /\ s.owner = 1 /\ Admitted /\ ~s.seen
    /\ s' = [s EXCEPT !.live = FALSE, !.admissionSeen = FALSE, !.readyBeforeHalt = Ready]

Takeover ==
    /\ ~s.live /\ s.owner = 1 /\ ~s.publishCrash
    /\ s' = [s EXCEPT !.owner = 2, !.live = TRUE]

HaltPublication ==
    /\ s.live /\ s.seen /\ ~s.publishCrash /\ s.action <= 2 /\ s.boot = 0
    /\ s' = [s EXCEPT !.live = FALSE, !.admissionSeen = FALSE, !.seen = FALSE, !.publishCrash = TRUE,
                      !.checkedPrefix = FALSE, !.checkedActive = FALSE,
                      !.splitCrash = s.path[1] = 4 /\ s.path[2] = 1,
                      !.afterForceCrash = s.action = 2 /\ ~s.replayed, !.replayed = FALSE, !.cutoff = 0]

ResumePublication ==
    /\ ~s.live /\ s.publishCrash
    /\ s' = [s EXCEPT !.live = TRUE]

(* Visible rename effects are retained across JVM halt. Barriers are
   independent facts. No transition here represents machine power loss. *)
CanonicalPublicationPrefix ==
    /\ (s.path[2] > 0 => s.path[1] >= 4)
    /\ ((\E i \in Files : s.path[i] >= 5) => (\A i \in Files : s.path[i] >= 4))
    /\ (s.stage > 0 => (\A i \in Files : s.path[i] = 6))

CheckPublicationPrefix ==
    /\ s.live /\ s.seen /\ s.action = 0 /\ ~s.checkedPrefix
    /\ CanonicalPublicationPrefix
    /\ \A i \in Files :
        s.active[i] = CASE s.path[i] = 0 -> s.admission.objects[i]
                        [] s.path[i] \in 1..2 -> <<-1, i>>
                        [] OTHER -> s.ready.objects[i]
    /\ s' = [s EXCEPT !.checkedPrefix = TRUE]

CanPublish == s.live /\ s.seen /\ Ready /\ s.checkedPrefix /\ s.action = 0

MoveOld(i) ==
    /\ CanPublish /\ s.path[i] = 0 /\ s.active[i] = <<0, i>>
    /\ \A before \in 1..(i - 1) : s.path[before] = 4
    /\ s' = [s EXCEPT !.path[i] = 1, !.active[i] = <<-1, i>>]

SyncOldMove(i) ==
    /\ CanPublish /\ s.path[i] = 1
    /\ s' = [s EXCEPT !.path[i] = 2]

InstallNew(i) ==
    /\ CanPublish /\ s.path[i] = 2 /\ s.ready.attempt \notin s.discarded
    /\ s' = [s EXCEPT !.path[i] = 3, !.active[i] = s.ready.objects[i]]

SyncNewMove(i) ==
    /\ CanPublish /\ s.path[i] = 3
    /\ s' = [s EXCEPT !.path[i] = 4]

ExactActive == {i \in Files : s.active[i] = s.ready.objects[i]}
VerifyAllActive ==
    /\ CanPublish /\ ~s.checkedActive /\ ExactActive = Files
    /\ \A i \in Files : s.path[i] >= 4
    /\ s' = [s EXCEPT !.checkedActive = TRUE]

RemoveOld(i) ==
    /\ CanPublish /\ s.checkedActive /\ s.path[i] = 4
    /\ \A before \in 1..(i - 1) : s.path[before] = 6
    /\ s' = [s EXCEPT !.path[i] = 5]

SyncRemoval(i) ==
    /\ CanPublish /\ s.path[i] = 5
    /\ s' = [s EXCEPT !.path[i] = 6]

RemoveStaging ==
    /\ CanPublish /\ s.stage = 0 /\ \A i \in Files : s.path[i] = 6
    /\ s' = [s EXCEPT !.stage = 1]

SyncStagingRemoval ==
    /\ CanPublish /\ s.stage = 1
    /\ s' = [s EXCEPT !.stage = 2]

PublishedVector ==
    /\ Ready /\ ExactActive = Files /\ s.stage = 2
    /\ \A i \in Files : s.path[i] = 6

IssueResolution ==
    /\ CanPublish /\ s.epoch = OriginalEpoch
    /\ ((PublishedVector /\ s.checkedActive)
         \/ (Mutation = "PartialResolution" /\ s.path[1] = 4 /\ s.path[2] < 4)
         \/ (Mutation = "SkipCleanup" /\ s.checkedActive /\ ExactActive = Files))
    /\ s' = [s EXCEPT !.action = 1, !.badResolution = ~(PublishedVector /\ s.checkedActive)]

ForceResolution ==
    /\ s.action = 1
    /\ s' = [s EXCEPT !.action = 2]

ApplyOrReplayResolution ==
    /\ s.live /\ s.action = 2 /\ ~s.replayed
    /\ s' = [s EXCEPT !.replayed = TRUE, !.cutoff = OriginalCut]

(* The injected replacement keeps the published bytes and header. Only the
   identity of the later active file changes before first OPEN. *)
ReplaceSecondActive ==
    /\ s.action = 2 /\ PublishedVector /\ s.epoch = OriginalEpoch /\ ~s.tampered
    /\ s' = [s EXCEPT !.tampered = TRUE, !.active[2] = <<3, 2>>]

BeginStartup ==
    /\ s.live /\ s.replayed /\ s.boot < 2 /\ s.startup = "OFF"
    /\ s' = [s EXCEPT !.boot = @ + 1, !.startup = "STARTING"]

OpenStorage ==
    /\ s.startup = "STARTING"
    /\ (s.epoch # OriginalEpoch
         \/ (/\ ExactAdmissionBirth
             /\ (PublishedVector \/ (Mutation = "File0Only" /\ 1 \in ExactActive /\ s.stage = 2))
             /\ ExactRejoin))
    /\ s' = [s EXCEPT !.badOpen = @ \/ (s.epoch = OriginalEpoch /\ ~PublishedVector),
                      !.startup = "OPEN", !.epoch = @ + 1]

ForceDelete ==
    /\ s.startup = "OPEN" /\ Deleted # {} /\ ~s.deleteForce
    /\ s' = [s EXCEPT !.deleteForce = TRUE]

ApplyDelete ==
    /\ s.startup = "OPEN" /\ s.deleteForce /\ s.cursor # Deleted
    /\ s' = [s EXCEPT !.tombstone = Deleted, !.cursor = Deleted, !.localData = @ \ Deleted]

ReconcileStartup ==
    /\ s.startup = "OPEN" /\ s.tombstone = Deleted /\ s.cursor = Deleted
    /\ s' = [s EXCEPT !.startup = "RECONCILED"]

PublishWritable ==
    /\ s.startup = "RECONCILED"
    /\ s' = [s EXCEPT !.startup = "WRITABLE"]

(* The first episode can stop after local OPEN or catch-up and before it
   opens wire. A healthy restart after publication also takes a fresh gate. *)
CloseFirstEpisode ==
    /\ s.boot = 1 /\ s.startup \in {"OPEN", "RECONCILED", "WRITABLE"}
    /\ s' = [s EXCEPT !.startup = "OFF", !.healthyRetry = s.startup # "WRITABLE",
                      !.replayed = FALSE, !.cutoff = 0]

WireRequest ==
    /\ s.startup \in {"OPEN", "RECONCILED"} /\ s.boot \notin s.refused /\ ~s.badWire
    /\ IF s.boot = 2 /\ s.healthyRetry /\ s.cursor # Deleted /\ Mutation = "RetrySkipsGate"
       THEN s' = [s EXCEPT !.badWire = TRUE]
       ELSE s' = [s EXCEPT !.refused = @ \cup {s.boot},
                          !.retryRefused = @ \/ (s.boot = 2 /\ s.cursor # Deleted)]

ReadSurvivor ==
    /\ s.startup = "WRITABLE" /\ Deleted = {2} /\ 1 \in s.localData /\ ~s.readSurvivor
    /\ s' = [s EXCEPT !.readSurvivor = TRUE]

NewGrant ==
    /\ s.startup = "WRITABLE" /\ s.grant = 0
    /\ s' = [s EXCEPT !.grant = OriginalCut + 1]

FreeDeletedGrant ==
    /\ s.startup = "WRITABLE" /\ Deleted \cap NonemptyMembers # {} /\ s.oldFreed = {}
    /\ s' = [s EXCEPT !.oldFreed = Deleted \cap NonemptyMembers]

ResumeOldTail(i) ==
    /\ s.startup = "WRITABLE" /\ i \notin s.oldFreed /\ ~s.oldAppended
    /\ (OriginalCut > s.cutoff \/ Mutation = "IgnoreCutoff")
    /\ s' = [s EXCEPT !.oldAppended = TRUE]

AppendNew ==
    /\ s.startup = "WRITABLE" /\ s.grant > s.cutoff /\ ~s.newAppended
    /\ s' = [s EXCEPT !.newAppended = TRUE]

Next ==
    \/ LoseRequiredFile \/ ReplaceAdmissionBirth \/ ReadAdmission
    \/ \E i \in Members, change \in {"DELETE", "WRONG_DELETE", "VERSION", "BIRTH"} : ChangeMember(i, change)
    \/ \E files \in SUBSET Files, members \in SUBSET Members : Admit(files, members)
    \/ \E a \in Attempts : SubmitClaim(a) \/ CommitClaim(a) \/ LoseClaimReply(a) \/ ReadClaim(a)
                         \/ SyncCandidates(a) \/ SubmitReady(a) \/ CommitReady(a)
    \/ \E a \in Attempts, i \in Files : StartWrite(a, i) \/ FinishWrite(a, i) \/ ForceCandidate(a, i)
                 \/ ReadBack(a, i) \/ FirstClose(a, i) \/ ObserveImageFailure(a, i)
    \/ \E i \in Files : MoveOld(i) \/ SyncOldMove(i) \/ InstallNew(i) \/ SyncNewMove(i)
                      \/ RemoveOld(i) \/ SyncRemoval(i)
    \/ HaltShadow \/ Takeover \/ LoseReadyReply \/ ReadReady \/ HaltPublication \/ ResumePublication
    \/ CheckPublicationPrefix \/ VerifyAllActive \/ RemoveStaging \/ SyncStagingRemoval
    \/ IssueResolution \/ ForceResolution \/ ApplyOrReplayResolution \/ ReplaceSecondActive
    \/ BeginStartup \/ OpenStorage \/ ForceDelete \/ ApplyDelete \/ ReconcileStartup \/ PublishWritable
    \/ CloseFirstEpisode \/ WireRequest \/ ReadSurvivor \/ NewGrant \/ FreeDeletedGrant \/ AppendNew
    \/ \E i \in NonemptyMembers : ResumeOldTail(i)

Spec == Init /\ [][Next]_vars

CompleteAdmission ==
    Admitted => /\ s.admission.birth = ScopeBirth /\ s.admission.files = Files
                /\ s.admission.files \subseteq s.presentOriginal /\ s.admission.members = Members
                /\ s.admission.snapshot = OriginalMembers /\ s.admission.objects = Objects(0)
ReadyHasAllMembers == Ready => s.ready.checks = s.admission.members
ReadyUsesOriginalClaim ==
    Ready => /\ s.terminalShadow /\ s.ready.attempt \notin s.discarded
             /\ s.ready.ticket.birth = s.admission.birth
             /\ s.ready.claim = <<s.ready.attempt, s.ready.ticket.birth, s.ready.ticket.version>>
ReadyHasClosedCandidates ==
    Ready => /\ s.ready.ticket.closed = Files /\ s.ready.ticket.synced
             /\ s.ready.objects = Objects(s.ready.attempt)
ResolutionAfterPublication ==
    /\ (s.action = 2 => ~s.badResolution)
    /\ (s.replayed => s.action = 2)
FirstOpenChecksEveryFile == ~s.badOpen
FirstOpenChecksEveryMember == s.epoch > OriginalEpoch => ExactRejoin
StartupWireWaitsForAdmission ==
    /\ ~s.badWire
    /\ (s.startup = "WRITABLE" => (s.cursor = Deleted /\ s.tombstone = Deleted))
OldGrantStaysReadOnly == ~s.oldAppended /\ (s.startup # "OFF" => s.cutoff = OriginalCut)
DeletePreservesNeighbor ==
    /\ s.obligations = Members
    /\ s.oldFreed \subseteq (s.tombstone \cap NonemptyMembers)
    /\ s.localData = Members \ s.tombstone

NoCompleteRecovery ==
    ~(s.startup = "WRITABLE" /\ s.boot = 2 /\ s.afterForceCrash /\ s.newAppended
      /\ s.owner = 2 /\ s.readyBeforeHalt /\ s.ready.attempt = 1 /\ s.readyReply = "LOST")
NoRejectedLateReady == ~(s.lateRejected /\ s.ready.attempt = 2 /\ PublishedVector)
NoDeleteRecovery ==
    ~(s.splitCrash /\ s.cursor = {2} /\ s.readSurvivor /\ s.oldFreed = {2})
NoGatedHealthyRetry ==
    ~(s.healthyRetry /\ s.retryRefused /\ s.startup = "WRITABLE" /\ s.boot = 2 /\ s.cursor = {2})
============================================================================
