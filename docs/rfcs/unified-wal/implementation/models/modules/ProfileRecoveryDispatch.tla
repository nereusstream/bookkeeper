-------------------- MODULE ProfileRecoveryDispatch --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreTuple, IgnoreSource, IgnoreClose, ActivateNormal, NoneOnUnknown, ReleaseIoOnCancel
VARIABLE s
vars == <<s>>
(* One disposable recovery Add under an already installed local grant. Exact
   route/credential/intent/generations/range and CRC-valid coordinates are inputs;
   this projection checks authorization, charged copying, close/result and actual
   terminal ordering. Shared response ownership is covered by ProfileNormalDispatch.
   No RepairIntent coordinator, missing proof, quorum, TLS/socket total accounting,
   cryptography, real fsync, wall-clock deadlines, fairness or liveness is proved. *)
Init == \E exactTuple, sourceAvailable \in BOOLEAN :
    s = [phase |-> "dispatch", exactTuple |-> exactTuple, sourceAvailable |-> sourceAvailable,
         grantOpen |-> TRUE, authorizedAtAdmission |-> FALSE, sourceAcquired |-> FALSE,
         copied |-> FALSE, submitted |-> FALSE, ioOwned |-> FALSE, ioDone |-> FALSE,
         ownerDone |-> FALSE, grantAtResult |-> FALSE, normalActive |-> FALSE,
         outcome |-> "pending", response |-> "unmade", closed |-> FALSE]
Refuse == /\ s.phase = "dispatch" /\ ~s.closed
          /\ ~(s.exactTuple /\ s.grantOpen /\ s.sourceAvailable)
          /\ s' = [s EXCEPT !.phase = "result", !.outcome = "none"]
CopyAndSubmit == /\ s.phase = "dispatch" /\ ~s.closed
                 /\ ((s.exactTuple /\ s.grantOpen) \/ IgnoreTuple)
                 /\ (s.sourceAvailable \/ IgnoreSource)
                 /\ s' = [s EXCEPT !.phase = "submitted", !.submitted = TRUE,
                          !.authorizedAtAdmission = s.exactTuple /\ s.grantOpen,
                          !.sourceAcquired = s.sourceAvailable, !.copied = TRUE, !.ioOwned = TRUE]
CloseGrant == /\ s.grantOpen /\ s' = [s EXCEPT !.grantOpen = FALSE]
FinishIo == /\ s.phase = "submitted" /\ ~s.ioDone
            /\ \E outcome \in {"durable", "unknown"} :
                  s' = [s EXCEPT !.ioDone = TRUE, !.outcome = outcome]
FinishOwner == /\ s.phase = "submitted" /\ s.ioDone /\ ~s.ownerDone
               /\ s' = [s EXCEPT !.phase = "result", !.ownerDone = TRUE,
                        !.grantAtResult = s.grantOpen,
                        !.outcome = IF @ = "durable" /\ ~s.grantOpen /\ ~IgnoreClose THEN "stale" ELSE @,
                        !.normalActive = ActivateNormal /\ s.outcome = "durable"]
ReleaseIo == /\ s.ownerDone /\ s.ioOwned /\ s' = [s EXCEPT !.ioOwned = FALSE]
Respond == /\ s.phase = "result" /\ ~s.closed
           /\ s' = [s EXCEPT !.phase = "done",
                    !.response = IF s.outcome = "durable" THEN "success"
                                 ELSE IF s.outcome = "none" \/ NoneOnUnknown THEN "none" ELSE "unknown"]
Disconnect == /\ ~s.closed /\ s' = [s EXCEPT !.closed = TRUE,
                                  !.ioOwned = IF ReleaseIoOnCancel THEN FALSE ELSE @]
Next == Refuse \/ CopyAndSubmit \/ CloseGrant \/ FinishIo \/ FinishOwner \/ ReleaseIo \/ Respond \/ Disconnect
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"dispatch", "submitted", "result", "done"}
          /\ s.outcome \in {"pending", "none", "durable", "unknown", "stale"}
          /\ s.response \in {"unmade", "none", "success", "unknown"}
          /\ \A field \in {"exactTuple", "sourceAvailable", "grantOpen", "authorizedAtAdmission",
                           "sourceAcquired", "copied", "submitted", "ioOwned", "ioDone", "ownerDone",
                           "grantAtResult", "normalActive", "closed"} : s[field] \in BOOLEAN
SubmittedRequiresExactOpenGrant == s.submitted => s.authorizedAtAdmission
CopyRequiresSourceCredit == s.copied => s.sourceAcquired
SuccessRequiresCurrentGrantAtDecision == s.response = "success" =>
    s.authorizedAtAdmission /\ s.ioDone /\ s.ownerDone /\ s.grantAtResult /\ s.outcome = "durable"
RecoveryDoesNotActivateNormal == ~s.normalActive
NoneIsNotSubmittedData == s.response = "none" => ~s.submitted
IoProtectionWaitsForActualTerminals == s.submitted /\ (~s.ioDone \/ ~s.ownerDone) => s.ioOwned
=====================================================================
