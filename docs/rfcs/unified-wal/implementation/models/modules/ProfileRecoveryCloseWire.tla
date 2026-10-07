-------------------- MODULE ProfileRecoveryCloseWire --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipCapability, SkipAuthority, SkipBinding, SkipPrecut, AckBeforeForce,
          ReleaseOnCancel, ActivateNormal, AllowAfterCut, ReplyBeforeTerminal
VARIABLE s
vars == <<s>>
(* Independent committed close authority is an input. No membership/close-authority
   publisher, byte/crypto producer, cluster CLOSED, total budget or liveness proof. *)
Init == s \in [phase : {"request"}, capability : BOOLEAN, principal : BOOLEAN,
               authority : BOOLEAN, binding : BOOLEAN, tickets : {0, 1}, cut : {FALSE},
               forced : {FALSE}, terminal : {FALSE}, custody : {TRUE}, canceled : {FALSE},
               readable : {FALSE}, normal : {FALSE}, late : {FALSE}, replied : {FALSE}]
Authorize == /\ s.phase = "request"
             /\ LET accepted == (s.capability \/ SkipCapability)
                                  /\ ((s.principal /\ s.authority) \/ SkipAuthority)
                                  /\ (s.binding \/ SkipBinding)
                IN s' = [s EXCEPT !.phase = IF accepted THEN "cut" ELSE "rejected",
                         !.cut = accepted, !.terminal = ~accepted, !.custody = accepted]
DataTerminal == /\ s.phase = "cut" /\ s.tickets = 1
                /\ s' = [s EXCEPT !.tickets = 0]
BeginIo == /\ s.phase = "cut" /\ (s.tickets = 0 \/ SkipPrecut)
           /\ s' = [s EXCEPT !.phase = "io"]
Force == /\ s.phase = "io" /\ ~s.forced /\ s' = [s EXCEPT !.forced = TRUE]
Apply == /\ s.phase = "io" /\ (s.forced \/ AckBeforeForce)
         /\ s' = [s EXCEPT !.phase = "done", !.terminal = TRUE, !.custody = FALSE,
                  !.readable = TRUE, !.normal = ActivateNormal, !.replied = TRUE]
FailIo == /\ s.phase = "io"
          /\ s' = [s EXCEPT !.phase = "unknown", !.terminal = TRUE, !.custody = FALSE]
Cancel == /\ ~s.canceled
          /\ s' = [s EXCEPT !.canceled = TRUE,
                   !.custody = IF ReleaseOnCancel THEN FALSE ELSE @]
LateAdd == /\ s.cut /\ ~s.late /\ AllowAfterCut /\ s' = [s EXCEPT !.late = TRUE]
EarlyReply == /\ s.phase = "io" /\ ReplyBeforeTerminal /\ ~s.replied
              /\ s' = [s EXCEPT !.replied = TRUE]
Replay == /\ s.phase = "done" /\ s' = [s EXCEPT !.replied = TRUE]
Next == Authorize \/ DataTerminal \/ BeginIo \/ Force \/ Apply \/ FailIo \/ Cancel \/ LateAdd \/ EarlyReply \/ Replay
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"request", "cut", "io", "done", "unknown", "rejected"}
CutHasCapability == s.cut => s.capability
CutHasAuthority == s.cut => (s.principal /\ s.authority)
CutHasExactBinding == s.cut => s.binding
CloseWaitsForPrecut == s.phase \in {"io", "done"} => s.tickets = 0
ReadableHasForce == s.readable => s.forced
CustodyUntilTerminal == ~s.terminal => s.custody
NormalNeverActivates == ~s.normal
CloseStopsRecoveryAdmission == ~s.late
DurableReplyHasTerminal == s.replied => (s.terminal /\ s.readable /\ s.forced /\ s.tickets = 0)
=============================================================================
