-------------------- MODULE ProfilePointReadDispatch --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipAuthorization, ClaimUnprovedMissing, ReleaseOnCancel, NotifyOnCancel,
          CloseQueuedLoan, ReleaseOutputEarly, IgnoreReplyBinding
VARIABLE s
vars == <<s>>
(* One immutable query and one admitted native Get/read. Coverage represents
   the full physical inventory or fresh hot/index miss proof, not a cache hint.
   Fence/stability includes no pending work and no uncommitted recovery range.
   TLS, codec/bytes and physical proof production use real Java tests and prior
   finite slices. No clock, fairness, full recovery, total memory or A+C+D claim. *)
Init == /\ s \in [phase : {"header"}, authorized : BOOLEAN, negotiated : BOOLEAN,
                 binding : BOOLEAN, found : BOOLEAN, coverage : BOOLEAN,
                 fenced : BOOLEAN, stable : BOOLEAN, waiting : {TRUE}, io : {FALSE},
                 owner : {FALSE}, observer : {0}, credit : {TRUE}, loan : {FALSE},
                 queued : {FALSE}, output : {FALSE}, written : {FALSE}, result : {"none"},
                 reported : {"none"}]
Admit == /\ s.phase = "header" /\ s.waiting
         /\ IF s.negotiated /\ (s.authorized \/ SkipAuthorization)
               THEN s' = [s EXCEPT !.phase = "io"]
               ELSE s' = [s EXCEPT !.phase = "closed", !.credit = FALSE]
IoEnd == /\ s.phase = "io" /\ ~s.io /\ s' = [s EXCEPT !.io = TRUE]
OwnerEnd == /\ s.phase = "io" /\ s.io /\ ~s.owner
            /\ LET result == IF s.found THEN "found"
                             ELSE IF ClaimUnprovedMissing \/ (s.coverage /\ s.fenced /\ s.stable)
                                  THEN "missing" ELSE "refused"
               IN s' = [s EXCEPT !.phase = "owner", !.owner = TRUE, !.observer = 1,
                        !.result = result, !.queued = s.waiting,
                        !.loan = s.waiting /\ s.found /\ ~CloseQueuedLoan,
                        !.credit = s.waiting]
Cancel == /\ s.waiting
          /\ s' = [s EXCEPT !.waiting = FALSE,
                   !.phase = IF @ = "header" THEN "closed" ELSE @,
                   !.credit = IF ReleaseOnCancel \/ s.phase = "header" THEN FALSE ELSE @,
                   !.observer = IF NotifyOnCancel /\ ~s.owner THEN 1 ELSE @]
Visit == /\ s.queued
         /\ s' = [s EXCEPT !.queued = FALSE, !.loan = FALSE,
                  !.output = s.waiting, !.credit = s.waiting /\ ~ReleaseOutputEarly,
                  !.reported = IF s.waiting /\ (s.binding \/ IgnoreReplyBinding)
                                THEN s.result ELSE "none"]
WriteEnd == /\ s.output /\ ~s.written
            /\ s' = [s EXCEPT !.written = TRUE, !.output = FALSE, !.credit = FALSE]
Next == Admit \/ IoEnd \/ OwnerEnd \/ Cancel \/ Visit \/ WriteEnd
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"header", "io", "owner", "closed"}
          /\ s.observer \in 0..1 /\ s.result \in {"none", "found", "missing", "refused"}
          /\ s.reported \in {"none", "found", "missing", "refused"}
AdmissionRequiresPermission == s.phase \in {"io", "owner"} => (s.authorized /\ s.negotiated)
MissingRequiresCompleteStableFence == s.result = "missing" => (s.coverage /\ s.fenced /\ s.stable)
InFlightKeepsCredit == s.phase = "io" => s.credit
ObserverWaitsForActualTerminal == s.observer = 1 => (s.io /\ s.owner)
QueuedConsumerOwnsLoan == s.queued /\ s.result = "found" => s.loan
OutputWaitsForWrite == s.output => s.credit
ReplyRequiresBinding == s.reported \in {"found", "missing"} => s.binding
=============================================================================
