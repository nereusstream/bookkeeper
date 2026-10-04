---------------------- MODULE RuntimeControlRotation ----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS ReleaseOnCancel, ReleaseBeforeOwner, ResetSession, ReopenAfterFailure
VARIABLE s
vars == <<s>>
(* One bounded graph request. Whole-file pins/admissions and both driver
   reservations are inputs; per-owner physical dual-root protocols remain
   separate models. No atomic Bookie+Arena commit or liveness is assumed. *)
Init == \E active \in 0..1, pins \in 0..1 :
          s = [stage |-> "idle", active |-> active, pins |-> pins,
               paused |-> FALSE, drivers |-> FALSE, guard |-> FALSE,
               cancelled |-> FALSE, attempted |-> FALSE, failed |-> FALSE,
               stopped |-> FALSE, bookieRoot |-> 0, arenaRoot |-> 0,
               sessionOpen |-> TRUE, cursor |-> 2, result |-> "none"]
Admit == /\ s.stage = "idle"
         /\ s' = [s EXCEPT !.stage = "claimed", !.paused = TRUE, !.drivers = TRUE]
DataTerminal == /\ s.active = 1 /\ s' = [s EXCEPT !.active = 0]
Unpin == /\ s.pins = 1 /\ s' = [s EXCEPT !.pins = 0]
Protect == /\ s.stage = "claimed" /\ s.active = 0 /\ s.pins = 0
           /\ s' = [s EXCEPT !.stage = "bookie", !.guard = TRUE, !.attempted = TRUE,
                     !.sessionOpen = ~ResetSession, !.cursor = IF ResetSession THEN 0 ELSE @]
Refuse == /\ s.stage = "claimed" /\ (s.active # 0 \/ s.pins # 0)
          /\ s' = [s EXCEPT !.stage = "owner", !.failed = TRUE]
CancelWait == /\ s.stage \in {"claimed", "bookie", "arena", "owner"} /\ ~s.cancelled
              /\ s' = [s EXCEPT !.cancelled = TRUE,
                        !.paused = IF ReleaseOnCancel THEN FALSE ELSE @,
                        !.drivers = IF ReleaseOnCancel THEN FALSE ELSE @,
                        !.guard = IF ReleaseOnCancel THEN FALSE ELSE @]
BookieDone == /\ s.stage = "bookie"
              /\ s' = [s EXCEPT !.stage = "arena", !.bookieRoot = 1]
BookieUnknown == /\ s.stage = "bookie"
                 /\ s' = [s EXCEPT !.stage = "owner", !.failed = TRUE,
                           !.paused = IF ReleaseBeforeOwner THEN FALSE ELSE @,
                           !.drivers = IF ReleaseBeforeOwner THEN FALSE ELSE @,
                           !.guard = IF ReleaseBeforeOwner THEN FALSE ELSE @]
ArenaTerminal == /\ s.stage = "arena"
                 /\ \E failed \in BOOLEAN :
                       s' = [s EXCEPT !.stage = "owner", !.failed = failed,
                             !.arenaRoot = IF failed THEN 0 ELSE 1,
                             !.paused = IF ReleaseBeforeOwner THEN FALSE ELSE @,
                             !.drivers = IF ReleaseBeforeOwner THEN FALSE ELSE @,
                             !.guard = IF ReleaseBeforeOwner THEN FALSE ELSE @]
OwnerResolve == /\ s.stage = "owner"
                /\ s' = [s EXCEPT !.stage = "done", !.paused = FALSE,
                          !.drivers = FALSE, !.guard = FALSE,
                          !.stopped = s.attempted /\ s.failed /\ ~ReopenAfterFailure,
                          !.result = IF ~s.failed THEN "ROTATED" ELSE
                                      IF s.attempted THEN "UNKNOWN" ELSE "TRANSIENT_UNAVAILABLE"]
Next == Admit \/ DataTerminal \/ Unpin \/ Protect \/ Refuse \/ CancelWait
        \/ BookieDone \/ BookieUnknown \/ ArenaTerminal \/ OwnerResolve
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"idle", "claimed", "bookie", "arena", "owner", "done"}
          /\ s.active \in 0..1 /\ s.pins \in 0..1
          /\ s.bookieRoot \in 0..1 /\ s.arenaRoot \in 0..1 /\ s.cursor \in 0..2
          /\ s.result \in {"none", "ROTATED", "UNKNOWN", "TRANSIENT_UNAVAILABLE"}
RotationKeepsReservations == s.stage \in {"claimed", "bookie", "arena", "owner"}
                            => s.paused /\ s.drivers
GuardWaitsForOriginalOwner == s.attempted /\ s.stage # "done" => s.guard
ProtectedDataIsTerminal == s.guard => s.active = 0 /\ s.pins = 0
OpenSessionAndCursorRemain == s.sessionOpen /\ s.cursor = 2
ArenaRootHasCompletedBookie == s.arenaRoot = 1 => s.bookieRoot = 1
SuccessHasBothRoots == s.result = "ROTATED" => s.arenaRoot = 1 /\ s.bookieRoot = 1
UnknownStopsGraph == s.result = "UNKNOWN" => s.stopped
=============================================================================
