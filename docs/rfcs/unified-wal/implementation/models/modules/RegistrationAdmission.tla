-------------------- MODULE RegistrationAdmission --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreSession, AutoReopen, AllowLateOpen, ReleaseOnRetire
VARIABLE s
vars == <<s>>
(* One bound session/nonce and one actually submitted DATA I/O. Exact owned
   reconciliation, local identity scope and completed startup checks are inputs.
   Known pre-I/O refusal and already copied/released source views are outside this slice.
   Transport loss and event observation are separate: this is not an
   instantaneous failure detector or a continuous remote authority lease. *)
Init == \E matches \in BOOLEAN :
    s = [matches |-> matches, published |-> FALSE, connected |-> TRUE, session |-> 1,
         loss |-> FALSE, observed |-> FALSE, reconnected |-> FALSE,
         retired |-> FALSE, everRetired |-> FALSE, closed |-> FALSE, gate |-> 0,
         readPhase |-> 0, capturedMatch |-> FALSE, admitted |-> FALSE, unsafe |-> FALSE,
         ioDone |-> FALSE, ownerDone |-> FALSE, released |-> FALSE]
Publish == /\ ~s.published /\ s.connected /\ s.session = 1 /\ ~s.retired
           /\ s' = [s EXCEPT !.published = TRUE]
CaptureRead == /\ s.readPhase = 0 /\ s.published /\ ~s.retired
               /\ s' = [s EXCEPT !.readPhase = 1, !.capturedMatch = s.matches]
CompleteRead == /\ s.readPhase = 1
                /\ LET open == s.capturedMatch /\ s.connected /\ s.session = 1
                               /\ (~s.retired \/ AllowLateOpen)
                   IN s' = [s EXCEPT !.readPhase = 2, !.gate = IF open THEN 1 ELSE 2,
                            !.retired = ~open, !.everRetired = @ \/ ~open]
TransportLoss == /\ ~s.loss
                 /\ \E session \in 1..2 :
                     s' = [s EXCEPT !.loss = TRUE, !.connected = FALSE, !.session = session]
ObserveLoss == /\ s.loss /\ ~s.observed
               /\ s' = [s EXCEPT !.observed = TRUE, !.retired = TRUE, !.everRetired = TRUE,
                        !.gate = 2, !.released = @ \/ (ReleaseOnRetire /\ s.admitted)]
Reconnect == /\ s.loss /\ ~s.reconnected
             /\ s' = [s EXCEPT !.connected = TRUE, !.reconnected = TRUE,
                      !.gate = IF AutoReopen /\ s.retired /\ s.published THEN 1 ELSE @,
                      !.retired = IF AutoReopen /\ s.published THEN FALSE ELSE @]
Close == /\ ~s.closed
         /\ s' = [s EXCEPT !.closed = TRUE, !.retired = TRUE, !.everRetired = TRUE,
                  !.gate = 2, !.released = @ \/ (ReleaseOnRetire /\ s.admitted)]
Admit == /\ ~s.admitted /\ s.gate = 1 /\ ~s.retired
         /\ ((s.connected /\ s.session = 1) \/ IgnoreSession)
         /\ s' = [s EXCEPT !.admitted = TRUE, !.unsafe = ~s.connected \/ s.session # 1]
FinishIo == /\ s.admitted /\ ~s.ioDone /\ s' = [s EXCEPT !.ioDone = TRUE]
FinishOwner == /\ s.ioDone /\ ~s.ownerDone /\ s' = [s EXCEPT !.ownerDone = TRUE]
Release == /\ s.ownerDone /\ ~s.released /\ s' = [s EXCEPT !.released = TRUE]
Next == Publish \/ CaptureRead \/ CompleteRead \/ TransportLoss \/ ObserveLoss \/ Reconnect
        \/ Close \/ Admit \/ FinishIo \/ FinishOwner \/ Release
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.session \in 1..2 /\ s.gate \in 0..2 /\ s.readPhase \in 0..2
          /\ \A field \in {"matches", "published", "connected", "loss", "observed", "reconnected",
                          "retired", "everRetired", "closed", "capturedMatch", "admitted", "unsafe",
                          "ioDone", "ownerDone", "released"} : s[field] \in BOOLEAN
AdmissionUsesOriginalConnectedSession == ~s.unsafe
RetiredAttemptNeverReopens == s.everRetired => s.retired /\ s.gate = 2
OutstandingOwnershipWaitsForActualTerminals == s.released => s.ioDone /\ s.ownerDone
OpenRequiresExactReconciliation == s.gate = 1 => s.published /\ s.capturedMatch
=====================================================================
