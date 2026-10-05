-------------------- MODULE ProfileFenceDispatch --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreAuth, LeaveWorkspaceUnclassified, ReplyBeforeForce,
          ReleaseOnDisconnect, NoneOnUnknown, BlockExistingControl
VARIABLE s
vars == <<s>>
(* One fully parsed FENCE, one actual control I/O and its response. Exact TLS,
   context/credential validation, existing pre-cut drain, physical force and
   owner terminal are inputs, not proofs of crypto, fsync, full Model A/C,
   total TLS/socket bytes, timing, fairness, startup or quorum. DATA is full;
   a later unclassified-slot exhaustion cannot block this established lane. *)
Init == \E authorized, fits, precut \in BOOLEAN :
    s = [phase |-> "candidate", authorized |-> authorized, fits |-> fits, precut |-> precut,
         control |-> FALSE, workspace |-> "unclassified", credit |-> TRUE,
         submitted |-> FALSE, inFlight |-> FALSE, ioDone |-> FALSE, ownerDone |-> FALSE,
         outcome |-> "pending", queued |-> FALSE, buffer |-> FALSE,
         response |-> "unmade", closed |-> FALSE, unclassifiedFull |-> FALSE,
         probeSeen |-> FALSE, probeProgress |-> FALSE, secondSucceeded |-> FALSE]
Authorize == /\ s.phase = "candidate" /\ ~s.closed
             /\ (s.authorized \/ IgnoreAuth) /\ s.fits
             /\ s' = [s EXCEPT !.phase = "authorized", !.control = TRUE,
                      !.workspace = IF LeaveWorkspaceUnclassified THEN @ ELSE "control"]
Refuse == /\ s.phase = "candidate" /\ ~s.closed
          /\ ((~s.authorized /\ ~IgnoreAuth) \/ ~s.fits)
          /\ s' = [s EXCEPT !.phase = "queued", !.outcome = "none", !.queued = TRUE]
WaitPrecut == /\ s.phase = "authorized" /\ ~s.closed /\ s.precut
              /\ s' = [s EXCEPT !.phase = "queued", !.outcome = "none", !.queued = TRUE]
Submit == /\ s.phase = "authorized" /\ ~s.closed /\ ~s.precut
          /\ s' = [s EXCEPT !.phase = "submitted", !.submitted = TRUE, !.inFlight = TRUE]
ForceTerminal == /\ s.phase = "submitted" /\ ~s.ioDone
                 /\ \E outcome \in {"durable", "unknown"} :
                     s' = [s EXCEPT !.ioDone = TRUE, !.outcome = outcome]
OwnerTerminal == /\ s.phase = "submitted" /\ s.ioDone /\ ~s.ownerDone
                 /\ s' = [s EXCEPT !.ownerDone = TRUE, !.inFlight = FALSE,
                          !.phase = IF s.closed THEN "done" ELSE "queued",
                          !.queued = ~s.closed, !.credit = ~s.closed]
BeginResponse == /\ s.phase = "queued" /\ ~s.closed
                 /\ s' = [s EXCEPT !.phase = "writing", !.queued = FALSE, !.buffer = TRUE,
                          !.response = IF s.outcome = "durable" THEN "success"
                                       ELSE IF s.outcome = "unknown" /\ ~NoneOnUnknown THEN "unknown"
                                       ELSE "none"]
EarlyResponse == /\ ReplyBeforeForce /\ s.phase = "submitted" /\ ~s.ioDone /\ ~s.closed
                 /\ s' = [s EXCEPT !.phase = "writing", !.buffer = TRUE, !.response = "success"]
WriteTerminal == /\ s.buffer
                 /\ s' = [s EXCEPT !.phase = "done", !.buffer = FALSE, !.credit = FALSE]
Disconnect == /\ ~s.closed
              /\ s' = [s EXCEPT !.closed = TRUE,
                       !.credit = IF (s.inFlight \/ s.queued \/ s.buffer) /\ ~ReleaseOnDisconnect
                                  THEN @ ELSE FALSE]
SkipQueued == /\ s.closed /\ s.queued
              /\ s' = [s EXCEPT !.phase = "done", !.queued = FALSE, !.credit = FALSE]
ExhaustUnclassified == /\ s.control /\ ~s.unclassifiedFull
                       /\ s' = [s EXCEPT !.unclassifiedFull = TRUE]
ProbeExisting == /\ s.control /\ s.phase = "done" /\ ~s.closed
                 /\ s.unclassifiedFull /\ ~s.probeSeen
                 /\ s' = [s EXCEPT !.probeSeen = TRUE, !.probeProgress = ~BlockExistingControl]
SecondWrongCredential == /\ s.control /\ s.phase = "done" /\ ~s.closed /\ ~s.secondSucceeded
                         /\ s' = [s EXCEPT !.secondSucceeded = IgnoreAuth]
Next == Authorize \/ Refuse \/ WaitPrecut \/ Submit \/ ForceTerminal \/ OwnerTerminal
        \/ BeginResponse \/ EarlyResponse \/ WriteTerminal \/ Disconnect \/ SkipQueued
        \/ ExhaustUnclassified \/ ProbeExisting \/ SecondWrongCredential
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"candidate", "authorized", "submitted", "queued", "writing", "done"}
          /\ s.workspace \in {"unclassified", "control"}
          /\ s.outcome \in {"pending", "none", "durable", "unknown"}
          /\ s.response \in {"unmade", "success", "unknown", "none"}
          /\ \A field \in {"authorized", "fits", "precut", "control", "credit", "submitted",
                           "inFlight", "ioDone", "ownerDone", "queued", "buffer", "closed",
                           "unclassifiedFull", "probeSeen", "probeProgress", "secondSucceeded"} : s[field] \in BOOLEAN
ControlRequiresExactAuthorization == (s.control => s.authorized /\ s.fits) /\ ~s.secondSucceeded
ControlWorkspaceMovesAtomically == s.control /\ s.credit => s.workspace = "control"
SuccessRequiresFenceForceAndOwner == s.response = "success" =>
    s.control /\ s.authorized /\ s.fits /\ s.ioDone /\ s.ownerDone /\ s.outcome = "durable"
ControlCopiesWaitForActualTerminal == s.inFlight => s.credit
ResponseWaitsForActualTerminal == s.queued \/ s.buffer => s.credit
UnknownIsNotPreIoNone == s.response = "none" => ~s.submitted
ExistingControlSurvivesUnclassifiedFull == s.probeSeen => s.probeProgress
=====================================================================
