-------------------- MODULE ProfileInitialSocketDispatch --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipCapability, SkipAuthorization, LeaveWorkspace, ReleaseOnStop,
          ReplyBeforeForce, UnknownAsNone, LateAdmission
VARIABLE s
vars == <<s>>
(* One bounded initial control frame. TLS identity, exact cold authority and
   capacity are inputs. EL authorization can run after its cold caller times out.
   A physical UNKNOWN response does not end actual I/O custody. No crypto, byte
   allocator proof, complete ZK lifecycle, SDK/quorum, timing or fsync proof. *)
Init == \E cap, authorized, fits, ioOK \in BOOLEAN :
    s = [phase |-> "cold", cap |-> cap, authorized |-> authorized, fits |-> fits, ioOK |-> ioOK,
         cold |-> TRUE, waiting |-> TRUE, control |-> FALSE, workspace |-> "unclassified",
         credit |-> TRUE, submitted |-> FALSE, io |-> FALSE, forced |-> FALSE, applied |-> FALSE,
         reply |-> "pending", responseQueued |-> FALSE, writing |-> FALSE, responseDone |-> FALSE,
         closed |-> FALSE, lateEffect |-> FALSE]
ColdRead == /\ s.phase = "cold"
            /\ s' = [s EXCEPT !.phase = "el"]
Admit == /\ s.phase = "el" /\ s.waiting /\ ~s.closed
         /\ (s.cap \/ SkipCapability) /\ (s.authorized \/ SkipAuthorization) /\ s.fits
         /\ s' = [s EXCEPT !.phase = "submit", !.control = TRUE,
                  !.workspace = IF LeaveWorkspace THEN @ ELSE "control"]
Reject == /\ s.cold /\ s.phase \in {"cold", "el", "submit"}
          /\ (~s.waiting \/ s.closed \/ ~(s.cap \/ SkipCapability)
                  \/ ~(s.authorized \/ SkipAuthorization) \/ ~s.fits)
          /\ s' = [s EXCEPT !.phase = "done", !.cold = FALSE,
                   !.responseQueued = ~s.closed, !.reply = "none"]
ColdAdmissionTimeout == /\ s.phase = "el" /\ s.waiting
                        /\ s' = [s EXCEPT !.waiting = FALSE, !.cold = FALSE,
                                 !.reply = "none", !.responseQueued = ~s.closed]
LateEl == /\ s.phase = "el" /\ ~s.waiting
          /\ s' = [s EXCEPT !.phase = "done", !.control = LateAdmission,
                   !.lateEffect = LateAdmission, !.submitted = LateAdmission, !.io = LateAdmission, !.cold = FALSE]
Submit == /\ s.phase = "submit" /\ s.waiting /\ ~s.closed
          /\ s' = [s EXCEPT !.phase = "io", !.submitted = TRUE, !.io = TRUE]
Force == /\ s.phase = "io" /\ ~s.applied
         /\ s' = [s EXCEPT !.phase = "owner", !.forced = s.ioOK]
OwnerTerminal == /\ s.phase = "owner"
                 /\ s' = [s EXCEPT !.phase = "done", !.io = FALSE, !.applied = s.forced,
                          !.cold = FALSE, !.responseQueued = s.responseQueued \/ (~s.closed /\ s.reply = "pending"),
                          !.reply = IF s.reply = "pending" THEN IF s.forced THEN "success"
                                    ELSE "unknown" ELSE s.reply]
PhysicalTimeout == /\ s.phase \in {"io", "owner"} /\ s.cold
                   /\ s' = [s EXCEPT !.cold = FALSE, !.responseQueued = ~s.closed,
                            !.reply = IF UnknownAsNone THEN "none" ELSE "unknown"]
EarlyReply == /\ ReplyBeforeForce /\ s.phase = "io" /\ ~s.closed
              /\ s' = [s EXCEPT !.reply = "success", !.responseQueued = TRUE]
BeginWrite == /\ s.responseQueued /\ ~s.closed /\ ~s.writing /\ ~s.responseDone
              /\ s' = [s EXCEPT !.responseQueued = FALSE, !.writing = TRUE]
WriteDone == /\ s.writing
             /\ s' = [s EXCEPT !.writing = FALSE, !.responseDone = TRUE,
                      !.credit = IF ReleaseOnStop THEN FALSE ELSE @]
Disconnect == /\ ~s.closed
              /\ s' = [s EXCEPT !.closed = TRUE, !.waiting = FALSE,
                       !.credit = IF ReleaseOnStop THEN FALSE ELSE @]
DiscardQueued == /\ s.closed /\ s.responseQueued
                 /\ s' = [s EXCEPT !.responseQueued = FALSE]
Release == /\ (s.closed \/ s.responseDone) /\ ~s.cold /\ ~s.io
           /\ ~s.writing /\ ~s.responseQueued /\ s.credit
           /\ s' = [s EXCEPT !.credit = FALSE]
Next == ColdRead \/ Admit \/ Reject \/ ColdAdmissionTimeout \/ LateEl \/ Submit
        \/ Force \/ OwnerTerminal \/ PhysicalTimeout \/ EarlyReply \/ BeginWrite
        \/ WriteDone \/ Disconnect \/ DiscardQueued \/ Release
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"cold", "el", "submit", "io", "owner", "done"}
          /\ s.workspace \in {"unclassified", "control"}
          /\ s.reply \in {"pending", "success", "unknown", "none"}
          /\ \A field \in {"cap", "authorized", "fits", "ioOK", "cold", "waiting", "control", "credit",
                           "submitted", "io", "forced", "applied", "responseQueued", "writing",
                           "responseDone", "closed", "lateEffect"} : s[field] \in BOOLEAN
ControlRequiresNegotiation == s.control => s.cap
ControlRequiresExactAuthority == s.control => s.authorized /\ s.fits
WorkspaceMovesWithControl == s.control /\ s.credit => s.workspace = "control"
ColdAndIoCopiesRemainCharged == s.cold \/ s.io \/ s.responseQueued \/ s.writing => s.credit
SuccessRequiresActualForceAndOwner == s.reply = "success" => s.forced /\ s.applied
SubmittedUnknownIsNeverNone == s.submitted => s.reply # "none"
TimedOutAdmissionNeverSubmits == ~s.lateEffect
=============================================================================
