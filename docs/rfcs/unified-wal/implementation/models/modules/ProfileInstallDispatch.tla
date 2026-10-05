-------------------- MODULE ProfileInstallDispatch --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipExact, EarlySuccess, ReleaseOnStop, ResultBeforeOwner, ActivateNormal, UnknownAsNone
VARIABLE s
vars == <<s>>
(* One cold typed INSTALL using the reserved control driver. Kind 1 is absent,
   2 is the same durable install, 3 is a conflicting route. Normal state of an
   existing route may be true: a retry must neither activate nor deactivate it.
   Exact principal/committed scope/descriptor/physical target and available
   control capacity are inputs. StopWaiting never cancels the actual command.
   No TLS/secret bytes, full all-E coordinator, READY, CAS or production ACK. *)
Init == \E exact \in BOOLEAN, capacity \in BOOLEAN, kind \in 1..3,
           ioOK \in BOOLEAN, normal \in BOOLEAN :
           /\ (kind = 1 => ~normal)
           /\ s = [exact |-> exact, capacity |-> capacity, kind |-> kind, ioOK |-> ioOK,
                    initialNormal |-> normal, normal |-> normal, phase |-> 0,
                    submitted |-> FALSE, held |-> FALSE, stopped |-> FALSE,
                    forced |-> FALSE, ioTerminal |-> FALSE, ownerTerminal |-> FALSE,
                    applied |-> FALSE, actualResult |-> 0, result |-> 0]
Admit == /\ s.phase = 0
         /\ LET accepted == (s.exact \/ SkipExact) /\ s.capacity
            IN s' = [s EXCEPT !.phase = IF accepted THEN 1 ELSE 6,
                     !.submitted = accepted, !.held = accepted,
                     !.result = IF ~accepted THEN 4 ELSE IF EarlySuccess THEN 1 ELSE 0]
Write == /\ s.phase = 1 /\ s' = [s EXCEPT !.phase = 2]
Force == /\ s.phase = 2
         /\ s' = [s EXCEPT !.phase = 3, !.ioTerminal = TRUE, !.forced = s.ioOK,
                  !.result = IF ResultBeforeOwner /\ s.ioOK THEN 1 ELSE s.result]
Apply == /\ s.phase = 3
         /\ s' = [s EXCEPT !.phase = 4, !.ownerTerminal = TRUE, !.applied = s.forced,
                  !.actualResult = IF ~s.forced THEN 5 ELSE s.kind,
                  !.normal = IF ActivateNormal /\ s.forced /\ s.kind \in 1..2 THEN TRUE ELSE s.normal]
Finish == /\ s.phase = 4
          /\ s' = [s EXCEPT !.phase = 6, !.held = FALSE,
                   !.result = IF s.stopped THEN s.result ELSE s.actualResult]
StopWaiting == /\ s.phase \in 1..4 /\ ~s.stopped
               /\ s' = [s EXCEPT !.stopped = TRUE, !.result = IF UnknownAsNone THEN 4 ELSE 5,
                        !.held = IF ReleaseOnStop THEN FALSE ELSE s.held]
Next == Admit \/ Write \/ Force \/ Apply \/ Finish \/ StopWaiting
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.kind \in 1..3 /\ s.phase \in 0..6 /\ s.result \in 0..5 /\ s.actualResult \in 0..5
          /\ s.exact \in BOOLEAN /\ s.capacity \in BOOLEAN /\ s.ioOK \in BOOLEAN
          /\ s.initialNormal \in BOOLEAN /\ s.normal \in BOOLEAN /\ s.submitted \in BOOLEAN
          /\ s.held \in BOOLEAN /\ s.stopped \in BOOLEAN /\ s.forced \in BOOLEAN
          /\ s.ioTerminal \in BOOLEAN /\ s.ownerTerminal \in BOOLEAN /\ s.applied \in BOOLEAN
AdmissionHasExactAuthorityAndTarget == s.submitted => s.exact
InstallSuccessHasBarrier == s.result \in 1..2 => s.forced
InstallSuccessHasOwnerApply == s.result \in 1..2 => s.ownerTerminal /\ s.applied
SubmittedOwnershipWaitsForBothTerminals ==
    s.submitted /\ ~(s.ioTerminal /\ s.ownerTerminal) => s.held
InstallPreservesNormalState == s.normal = s.initialNormal
SubmittedUnknownNeverBecomesNone == s.submitted => s.result # 4
=====================================================================
