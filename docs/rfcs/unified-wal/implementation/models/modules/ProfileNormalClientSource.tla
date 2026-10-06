-------------------- MODULE ProfileNormalClientSource --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS CopyBeforeQuota, FreeQueuedSourceOnCancel, IgnoreEntryBound, PromoteNonDurable, ReleaseOutputOnReply
VARIABLE s
vars == <<s>>
(* One ordinary client request; real TLS/header binding and server force/owner
   evidence are supplied by existing client/dispatch slices. Input source views,
   encoded output, wire wait and local quota are distinct lifetimes. No crypto,
   bytes/pool/kernel, metadata, full quorum SDK, timing, fairness or full A/C/D. *)
Init == \E bound, fits \in BOOLEAN, outcome \in {"durable", "none", "unknown"} :
    s = [bound |-> bound, fits |-> fits, outcome |-> outcome, phase |-> "new",
         admitted |-> FALSE, source |-> FALSE, output |-> FALSE, quota |-> FALSE,
         cancelled |-> FALSE, writeDone |-> FALSE, wireDone |-> FALSE,
         submitted |-> FALSE, reply |-> "pending"]
Admit == /\ s.phase = "new"
         /\ LET accepted == (s.bound \/ IgnoreEntryBound) /\ s.fits
            IN s' = [s EXCEPT !.phase = IF accepted THEN "queued" ELSE "done",
                     !.admitted = accepted, !.source = accepted,
                     !.quota = accepted /\ ~CopyBeforeQuota,
                     !.reply = IF accepted THEN "pending" ELSE "none"]
Cancel == /\ s.phase \in {"queued", "sent"} /\ ~s.cancelled /\ s.reply = "pending"
          /\ s' = [s EXCEPT !.cancelled = TRUE, !.reply = "cancelled",
                   !.source = IF FreeQueuedSourceOnCancel THEN FALSE ELSE @]
AbortQueued == /\ s.phase = "queued" /\ s.cancelled
               /\ s' = [s EXCEPT !.phase = "done", !.source = FALSE,
                        !.writeDone = TRUE, !.wireDone = TRUE, !.quota = FALSE]
Encode == /\ s.phase = "queued" /\ ~s.cancelled
          /\ s' = [s EXCEPT !.phase = "sent", !.source = FALSE,
                   !.output = TRUE, !.submitted = TRUE]
WriteTerminal == /\ s.phase = "sent" /\ ~s.writeDone
                 /\ s' = [s EXCEPT !.writeDone = TRUE, !.output = FALSE]
Reply == /\ s.phase = "sent" /\ ~s.wireDone
         /\ s' = [s EXCEPT !.wireDone = TRUE,
                  !.reply = IF s.cancelled THEN "cancelled"
                    ELSE IF s.outcome = "durable" \/ PromoteNonDurable THEN "success" ELSE s.outcome,
                  !.quota = IF ReleaseOutputOnReply THEN FALSE ELSE @]
TransportUnknown == /\ s.phase = "sent" /\ ~s.wireDone
                    /\ s' = [s EXCEPT !.wireDone = TRUE,
                             !.reply = IF s.cancelled THEN "cancelled" ELSE "unknown"]
Release == /\ s.phase = "sent" /\ s.writeDone /\ s.wireDone /\ s.quota
           /\ s' = [s EXCEPT !.phase = "done", !.quota = FALSE]
Next == Admit \/ Cancel \/ AbortQueued \/ Encode \/ WriteTerminal \/ Reply \/ TransportUnknown \/ Release
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"new", "queued", "sent", "done"}
          /\ s.outcome \in {"durable", "none", "unknown"}
          /\ s.reply \in {"pending", "cancelled", "success", "none", "unknown"}
          /\ \A field \in {"bound", "fits", "admitted", "source", "output", "quota", "cancelled",
                            "writeDone", "wireDone", "submitted"} : s[field] \in BOOLEAN
SourceSnapshotRequiresQuota == s.source => s.quota
QueuedSourceWaitsForTaskDecision == s.phase = "queued" => s.source
SubmittedEntryFitsInstalledBound == s.submitted => s.bound
SuccessRequiresDurableStatus == s.reply = "success" => s.outcome = "durable"
OutputWaitsForActualWrite == s.output => s.quota
SlotWaitsForWriteAndWire == s.admitted /\ ~(s.writeDone /\ s.wireDone) /\ s.phase # "done" => s.quota
==============================================================================
