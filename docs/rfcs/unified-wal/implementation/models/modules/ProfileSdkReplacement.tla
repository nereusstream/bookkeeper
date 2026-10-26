-------------------- MODULE ProfileSdkReplacement --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS AckQuorum, MaxChanges, SkipActivation, EarlyGate, AcceptOld, ChangeSource, ChangeCoordinate,
          ResendUnchanged, RepeatCallback
VARIABLE s
vars == <<s>>
Entries == 0..1
ASSUME /\ AckQuorum \in {2, 3} /\ MaxChanges \in 1..2
Old(c) == IF c <= 1 THEN {1, 2} ELSE {3, 4}
New(c) == IF c <= 1 THEN {3, 4} ELSE {5, 6}
Original == {0, 1, 2}
Mapping(c) == IF c = 0 THEN Original ELSE {0} \cup New(c)
Targets == 0..(2 * MaxChanges + 2)
(* Two pending original entries, E=W=3/A=2 or 3, one or two same-start
   changes of two slots. All entries remain pending before another change.
   The control stages abstract the separately checked real preparation, guarded metadata
   CAS, retained authority and durable owner-applied activation receipts.
   Old request references terminate independently from the SDK gate. Source
   and coordinate identities are logical values, not buffer or I/O models.
   No same-ID rejoin, full quorum algorithm, fairness, clock or A+C+D claim. *)
Init == s = [phase |-> "writing", change |-> 0, gate |-> FALSE, stopped |-> FALSE,
             installed |-> {}, metadata |-> FALSE, authority |-> FALSE,
             activated |-> {}, available |-> FALSE, mapping |-> Original,
             revoked |-> {}, acks |-> [e \in Entries |-> {}],
             callbacks |-> [e \in Entries |-> 0], user |-> [e \in Entries |-> "pending"],
             requests |-> Entries \X Old(1), resentUnchanged |-> FALSE,
             source |-> [e \in Entries |-> 0], coordinate |-> [e \in Entries |-> e]]
Ack(e, t) ==
    /\ ~s.stopped /\ t \in s.mapping
    /\ (t = 0 \/ (<<e, t>> \in s.requests
                   /\ (t \notin New(s.change) \/ s.phase \in {"resend", "open"})))
    /\ t \notin s.acks[e]
    /\ s' = [s EXCEPT !.acks[e] = @ \cup {t}, !.requests = @ \ {<<e, t>>}]
Start == /\ s.phase \in {"writing", "open"} /\ ~s.stopped /\ s.change < MaxChanges
         /\ \A e \in Entries : s.user[e] = "pending"
         /\ s' = [s EXCEPT !.change = @ + 1, !.gate = TRUE, !.phase = "install",
                  !.installed = {}, !.metadata = FALSE, !.authority = FALSE,
                  !.activated = {}, !.available = FALSE, !.revoked = {}]
Install(t) == /\ s.phase = "install" /\ ~s.stopped /\ t \notin s.installed
              /\ s' = [s EXCEPT !.installed = @ \cup {t}]
Membership == /\ s.phase = "install" /\ s.installed = New(s.change) /\ ~s.stopped
              /\ s' = [s EXCEPT !.metadata = TRUE, !.phase = "authority"]
Publish == /\ s.phase = "authority"
           /\ s' = [s EXCEPT !.authority = TRUE, !.phase = "activate"]
Activate(t) == /\ s.phase = "activate" /\ ~s.stopped /\ t \notin s.activated
               /\ s' = [s EXCEPT !.activated = @ \cup {t}]
ControlReady == /\ s.phase = "activate"
                /\ (SkipActivation \/ (~s.stopped /\ s.activated = New(s.change)))
                /\ s' = [s EXCEPT !.available = TRUE, !.phase = "mapping"]
Switch == /\ s.phase = "mapping" /\ ~s.stopped
          /\ s' = [s EXCEPT !.mapping = Mapping(s.change), !.phase = "revoking"]
Revoke(t) == /\ s.phase = "revoking" /\ t \notin s.revoked /\ ~s.stopped
             /\ s' = [s EXCEPT !.revoked = @ \cup {t},
                      !.acks = [e \in Entries |-> s.acks[e] \ {t}]]
Resend == /\ s.phase = "revoking" /\ s.revoked = Old(s.change) /\ ~s.stopped
          /\ s' = [s EXCEPT !.phase = "resend", !.resentUnchanged = ResendUnchanged,
                   !.requests = @ \cup (Entries \X New(s.change)),
                   !.source = [e \in Entries |-> IF ChangeSource THEN 1 ELSE 0],
                   !.coordinate = [e \in Entries |-> IF ChangeCoordinate THEN e + 1 ELSE e]]
Open == /\ ~s.stopped /\ (s.phase = "resend" \/ (EarlyGate /\ s.phase = "revoking"))
        /\ s' = [s EXCEPT !.gate = FALSE, !.phase = "open"]
Late(e, t) == /\ t \notin s.mapping /\ <<e, t>> \in s.requests
             /\ s' = [s EXCEPT !.requests = @ \ {<<e, t>>},
                      !.acks[e] = IF AcceptOld THEN @ \cup {t} ELSE @]
Callback(e) == /\ ~s.gate /\ ~s.stopped /\ s.phase \in {"writing", "open"}
               /\ (s.user[e] = "pending" \/ RepeatCallback) /\ s.callbacks[e] < 2
               /\ (e = 0 \/ s.user[0] = "success")
               /\ Cardinality(s.acks[e] \cap s.mapping) >= AckQuorum
               /\ s' = [s EXCEPT !.callbacks[e] = @ + 1, !.user[e] = "success"]
Stop == /\ ~s.stopped
        /\ s' = [s EXCEPT !.stopped = TRUE, !.gate = TRUE,
                 !.callbacks = [e \in Entries |-> s.callbacks[e] + IF s.user[e] = "pending" THEN 1 ELSE 0],
                 !.user = [e \in Entries |-> IF s.user[e] = "pending" THEN "failed" ELSE s.user[e]]]
Next == Start \/ Membership \/ Publish \/ ControlReady \/ Switch \/ Resend \/ Open \/ Stop
        \/ (\E t \in New(s.change) : Install(t) \/ Activate(t))
        \/ (\E t \in Old(s.change) : Revoke(t))
        \/ (\E e \in Entries : Callback(e) \/ (\E t \in Targets : Ack(e, t))
                                          \/ (\E t \in Targets : Late(e, t)))
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.change \in 0..MaxChanges /\ s.mapping \in {Mapping(c) : c \in 0..MaxChanges}
          /\ s.installed \subseteq New(s.change) /\ s.activated \subseteq New(s.change)
          /\ s.revoked \subseteq Old(s.change) /\ s.requests \subseteq Entries \X Targets
          /\ \A e \in Entries : /\ s.callbacks[e] \in 0..2
                                 /\ s.user[e] \in {"pending", "success", "failed"}
                                 /\ s.acks[e] \subseteq Targets
AvailabilityIncludesActivation == s.available => (s.metadata /\ s.authority /\ s.activated = New(s.change))
GateRequiresWholeTransition == s.phase = "open" => s.revoked = Old(s.change)
RetiredReplicaCannotAck == s.phase = "open" => \A e \in Entries : s.acks[e] \subseteq s.mapping
ResendKeepsSource == \A e \in Entries : s.source[e] = 0
ResendKeepsCoordinate == \A e \in Entries : s.coordinate[e] = e
OnlyAffectedSlotsResent == ~s.resentUnchanged
OneOriginalCallback == \A e \in Entries : s.callbacks[e] <= 1
CallbacksStayContiguous == s.user[1] = "success" => s.user[0] = "success"
SuccessHasCurrentQuorum == \A e \in Entries : s.user[e] = "success" => Cardinality(s.acks[e] \cap s.mapping) >= AckQuorum

(* Independent owner-lifecycle projection; the original Spec above is unchanged.
   One replacementWork token is registered before the ordered task is queued. It covers
   begin/cold work, INSTALL, metadata CAS, activation, final read, the ordered completed
   callback, and at most two consecutive changes using the same episode token.
   An accepted control phase can finish after stop; it cannot admit a new replacement or
   adopt clients/metadata and resend normal writes after the lifecycle gate has closed.
   Local DATA/header termination, accepted explicit-close actual termination and actual
   replacement/callback termination are separate inputs. Raw local stop supplies only
   the local resource terminal. ownerActual is the aggregate required before the SDK
   starts draining its per-child DATA/CONTROL scopes; their output/lease/dependency drain
   is covered by the shared-SDK slice. No new ACK, recovery/CLOSED, remote-abort, fairness,
   liveness, fsync, TLS or native-allocation claim is made here. *)
OwnerPendingPhases == {"queued", "begin", "install", "metadata", "activation", "read", "callback", "continuing"}
OwnerColdAndCallbackPhases == OwnerPendingPhases \ {"queued"}
OwnerAdmitting == ~s.ownerStopping /\ ~s.closeStarted /\ ~s.stopped
OwnerInit == s = [phase |-> "idle", change |-> 0, token |-> FALSE, logicalDone |-> FALSE,
                  stopped |-> FALSE, ownerStopping |-> FALSE, closeStarted |-> FALSE,
                  localActual |-> FALSE, closeActual |-> "unsettled",
                  replacementStopped |-> FALSE, ownerActual |-> FALSE,
                  adoptions |-> 0, resends |-> 0, adoptedAfterStop |-> FALSE]
OwnerQueueReplacement(omitQueueToken) ==
    /\ OwnerAdmitting /\ s.phase = "idle" /\ s.change = 0
    /\ s' = [s EXCEPT !.phase = "queued", !.change = 1, !.token = ~omitQueueToken]
OwnerRunQueued ==
    /\ s.phase = "queued"
    /\ s' = [s EXCEPT !.phase = IF OwnerAdmitting THEN "begin" ELSE "done",
                       !.token = OwnerAdmitting]
OwnerControlPhase ==
    /\ s.phase \in {"begin", "install", "metadata", "activation"}
    /\ s' = [s EXCEPT !.phase = CASE @ = "begin" -> "install"
                                      [] @ = "install" -> "metadata"
                                      [] @ = "metadata" -> "activation"
                                      [] OTHER -> "read"]
OwnerFinalResult ==
    /\ s.phase = "read"
    /\ s' = [s EXCEPT !.phase = "callback", !.logicalDone = TRUE]
OwnerRunCallback(continue, allowStoppedAdoption) ==
    /\ s.phase = "callback"
    /\ LET adopt == OwnerAdmitting \/ allowStoppedAdoption
           next == adopt /\ continue /\ s.change < 2
       IN s' = [s EXCEPT !.phase = IF next THEN "continuing" ELSE "done",
                         !.change = IF next THEN @ + 1 ELSE @, !.token = next,
                         !.adoptions = IF adopt THEN @ + 1 ELSE @,
                         !.resends = IF adopt THEN @ + 1 ELSE @,
                         !.adoptedAfterStop = @ \/ (adopt /\ ~OwnerAdmitting)]
OwnerBeginConsecutive ==
    /\ s.phase = "continuing"
    /\ s' = [s EXCEPT !.phase = IF OwnerAdmitting THEN "begin" ELSE "done",
                       !.token = OwnerAdmitting]
OwnerRawLocalStop ==
    /\ ~s.stopped
    /\ s' = [s EXCEPT !.stopped = TRUE, !.logicalDone = TRUE]
OwnerRequestStop ==
    /\ ~s.ownerStopping
    /\ s' = [s EXCEPT !.ownerStopping = TRUE, !.stopped = TRUE, !.logicalDone = TRUE,
                       !.closeActual = IF s.closeStarted THEN @ ELSE "terminal"]
OwnerAcceptExplicitClose ==
    /\ ~s.ownerStopping /\ ~s.closeStarted
    /\ s' = [s EXCEPT !.closeStarted = TRUE, !.closeActual = "pending"]
OwnerRunExplicitClose ==
    /\ s.closeStarted /\ ~s.stopped
    /\ s' = [s EXCEPT !.stopped = TRUE, !.logicalDone = TRUE]
OwnerLocalResourcesTerminal ==
    /\ s.stopped /\ ~s.localActual
    /\ s' = [s EXCEPT !.localActual = TRUE]
OwnerExplicitCloseTerminal ==
    /\ s.closeActual = "pending" /\ s.localActual
    /\ s' = [s EXCEPT !.closeActual = "terminal"]
OwnerReplacementTerminal(earlyActual) ==
    /\ s.localActual /\ ~s.replacementStopped
    /\ (~s.token \/ (earlyActual /\ s.logicalDone /\ s.phase \in OwnerColdAndCallbackPhases))
    /\ s' = [s EXCEPT !.replacementStopped = TRUE]
OwnerAggregateTerminal ==
    /\ ~s.ownerActual /\ s.localActual /\ s.closeActual = "terminal" /\ s.replacementStopped
    /\ s' = [s EXCEPT !.ownerActual = TRUE]
OwnerNext(omitQueueToken, earlyActual, allowStoppedAdoption) ==
    OwnerQueueReplacement(omitQueueToken) \/ OwnerRunQueued \/ OwnerControlPhase \/ OwnerFinalResult
    \/ (\E continue \in BOOLEAN : OwnerRunCallback(continue, allowStoppedAdoption))
    \/ OwnerBeginConsecutive \/ OwnerRawLocalStop \/ OwnerRequestStop
    \/ OwnerAcceptExplicitClose \/ OwnerRunExplicitClose \/ OwnerLocalResourcesTerminal
    \/ OwnerExplicitCloseTerminal \/ OwnerReplacementTerminal(earlyActual) \/ OwnerAggregateTerminal
OwnerSpec == OwnerInit /\ [][OwnerNext(FALSE, FALSE, FALSE)]_vars
OwnerOmittedQueueTokenSpec == OwnerInit /\ [][OwnerNext(TRUE, FALSE, FALSE)]_vars
OwnerEarlyActualSpec == OwnerInit /\ [][OwnerNext(FALSE, TRUE, FALSE)]_vars
OwnerStoppedAdoptionSpec == OwnerInit /\ [][OwnerNext(FALSE, FALSE, TRUE)]_vars
OwnerTypeOK ==
    /\ s.phase \in OwnerPendingPhases \cup {"idle", "done"} /\ s.change \in 0..2
    /\ s.token \in BOOLEAN /\ s.logicalDone \in BOOLEAN /\ s.stopped \in BOOLEAN
    /\ s.ownerStopping \in BOOLEAN /\ s.closeStarted \in BOOLEAN /\ s.localActual \in BOOLEAN
    /\ s.closeActual \in {"unsettled", "pending", "terminal"}
    /\ s.replacementStopped \in BOOLEAN /\ s.ownerActual \in BOOLEAN
    /\ s.adoptions \in 0..2 /\ s.resends \in 0..2 /\ s.adoptedAfterStop \in BOOLEAN
OwnerEpisodeIsBounded == s.adoptions <= s.change /\ s.resends = s.adoptions
OwnerPendingEpisodeOwnsToken == s.phase \in OwnerPendingPhases => s.token
OwnerReplacementStoppedNeedsActual == s.replacementStopped
    => s.localActual /\ ~s.token /\ s.phase \in {"idle", "done"}
OwnerActualWaitsForQueuedEpisode == s.ownerActual => s.phase # "queued"
OwnerActualWaitsForReplacementEpisode == s.ownerActual => s.phase \in {"idle", "done"}
OwnerActualJoinsLocalAndAcceptedClose == s.ownerActual
    => s.localActual /\ s.closeActual = "terminal" /\ s.replacementStopped
OwnerStoppedCallbackDoesNotAdopt == ~s.adoptedAfterStop
=============================================================================
