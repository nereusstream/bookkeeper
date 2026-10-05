-------------------- MODULE ProfileNormalDispatch --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreHeaderBound, ReplyBeforeBarrier, NoneOnUnknown,
          ReleaseIoOnCancel, ReleaseResponseOnCancel
VARIABLE s
vars == <<s>>
(* One normal request and one actual downstream response buffer. Correct TLS,
   canonical framing, local credential/activation checks, charged synchronous
   source copying and actual I/O/owner terminals are implementation inputs.
   This is not a proof of TLS/socket total bytes, actual fsync, readiness,
   transport delivery, quorum, optional operations, timing or liveness. *)
Init == \E headerGood, authorized \in BOOLEAN :
    s = [phase |-> "header", headerGood |-> headerGood, authorized |-> authorized,
         frameEver |-> FALSE, frame |-> FALSE, responseCredit |-> FALSE,
         responseQueued |-> FALSE, responseBuffer |-> FALSE, copied |-> FALSE, submitted |-> FALSE,
         ioOwned |-> FALSE, ioDone |-> FALSE, ownerDone |-> FALSE,
         outcome |-> "pending", response |-> "unmade", closed |-> FALSE]
ReceiveHeader == /\ s.phase = "header" /\ ~s.closed
                 /\ IF s.headerGood \/ IgnoreHeaderBound
                    THEN s' = [s EXCEPT !.phase = "frame", !.frameEver = TRUE, !.frame = TRUE]
                    ELSE s' = [s EXCEPT !.phase = "rejected", !.closed = TRUE]
ReserveResponse == /\ s.phase = "frame" /\ ~s.closed
                   /\ s' = [s EXCEPT !.phase = "dispatch", !.responseCredit = TRUE]
RefuseBeforeIo == /\ s.phase = "dispatch" /\ ~s.closed /\ ~s.authorized
                  /\ s' = [s EXCEPT !.phase = "result", !.outcome = "none"]
CopyAndSubmit == /\ s.phase = "dispatch" /\ ~s.closed /\ s.authorized
                 /\ s' = [s EXCEPT !.phase = "submitted", !.copied = TRUE,
                          !.submitted = TRUE, !.ioOwned = TRUE]
FinishIo == /\ s.phase = "submitted" /\ ~s.ioDone
            /\ \E outcome \in {"durable", "unknown"} :
                s' = [s EXCEPT !.ioDone = TRUE, !.outcome = outcome]
FinishOwner == /\ s.phase = "submitted" /\ s.ioDone /\ ~s.ownerDone
               /\ s' = [s EXCEPT !.phase = "result", !.ownerDone = TRUE]
ReleaseIo == /\ s.ownerDone /\ s.ioOwned /\ s' = [s EXCEPT !.ioOwned = FALSE]
QueueResponse == /\ s.phase = "result" /\ ~s.closed
                 /\ s' = [s EXCEPT !.phase = "queued", !.responseQueued = TRUE]
BeginResponse == /\ s.phase = "queued" /\ ~s.closed
                 /\ s' = [s EXCEPT !.phase = "writing", !.responseBuffer = TRUE,
                          !.responseQueued = FALSE,
                          !.response = IF s.outcome = "durable" THEN "success"
                                       ELSE IF s.outcome = "unknown" /\ ~NoneOnUnknown THEN "unknown"
                                       ELSE "none"]
EarlyResponse == /\ ReplyBeforeBarrier /\ s.phase = "submitted" /\ ~s.ioDone /\ ~s.closed
                 /\ s' = [s EXCEPT !.phase = "writing", !.responseBuffer = TRUE, !.response = "success"]
Disconnect == /\ ~s.closed
              /\ s' = [s EXCEPT !.closed = TRUE,
                       !.frame = IF (s.responseBuffer \/ s.responseQueued) /\ ~ReleaseResponseOnCancel
                                 THEN @ ELSE FALSE,
                       !.responseCredit = IF (s.responseBuffer \/ s.responseQueued) /\ ~ReleaseResponseOnCancel
                                          THEN @ ELSE FALSE,
                       !.ioOwned = IF ReleaseIoOnCancel THEN FALSE ELSE @]
SkipQueuedResponse == /\ s.responseQueued /\ s.closed
                      /\ s' = [s EXCEPT !.phase = "done", !.responseQueued = FALSE,
                               !.frame = FALSE, !.responseCredit = FALSE]
WriteTerminal == /\ s.responseBuffer
                 /\ s' = [s EXCEPT !.phase = "done", !.responseBuffer = FALSE,
                          !.frame = FALSE, !.responseCredit = FALSE]
Next == ReceiveHeader \/ ReserveResponse \/ RefuseBeforeIo \/ CopyAndSubmit
        \/ FinishIo \/ FinishOwner \/ ReleaseIo \/ QueueResponse \/ BeginResponse \/ EarlyResponse
        \/ Disconnect \/ SkipQueuedResponse \/ WriteTerminal
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"header", "frame", "dispatch", "submitted", "result", "queued", "writing", "done", "rejected"}
          /\ s.outcome \in {"pending", "none", "durable", "unknown"}
          /\ s.response \in {"unmade", "success", "unknown", "none"}
          /\ \A field \in {"headerGood", "authorized", "frameEver", "frame", "responseCredit",
                          "responseQueued", "responseBuffer", "copied", "submitted", "ioOwned", "ioDone",
                          "ownerDone", "closed"} :
                 s[field] \in BOOLEAN
HeaderBoundPrecedesBody == s.frameEver => s.headerGood
SuccessRequiresActualDurability == s.response = "success" =>
    s.authorized /\ s.ioDone /\ s.ownerDone /\ s.outcome = "durable"
NoneIsNotUnknownData == s.response = "none" => ~s.submitted
IoProtectionWaitsForActualTerminals == s.submitted /\ (~s.ioDone \/ ~s.ownerDone) => s.ioOwned
ResponseProtectionWaitsForWriteTerminal == s.responseBuffer \/ s.responseQueued => s.frame /\ s.responseCredit
SubmittedSourceWasCopied == s.submitted => s.copied
=====================================================================
