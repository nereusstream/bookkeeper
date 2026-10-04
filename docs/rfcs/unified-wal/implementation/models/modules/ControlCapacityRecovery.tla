---------------------- MODULE ControlCapacityRecovery ----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS RotateOversize, IgnorePressurePause, ForgetBlockedDemand
VARIABLE s
vars == <<s>>
(* One capacity refusal and one automatic rotation. The request's ability to
   fit a fresh journal, actual resource terminal events and physical rotation
   results are inputs; this is not a filesystem budget or liveness proof. *)
Init == \E fits \in BOOLEAN, active \in 0..1, pins \in 0..1, busy \in BOOLEAN :
          s = [stage |-> "idle", fits |-> fits, active |-> active, pins |-> pins,
               busy |-> busy, pressure |-> FALSE, paused |-> FALSE, ioStarts |-> 0,
               entryAttempted |-> FALSE, entryAccepted |-> FALSE, attemptPressure |-> FALSE,
               failed |-> FALSE, failureObserved |-> FALSE, stopped |-> FALSE]
CapacityRefusal == /\ s.stage = "idle"
                   /\ s' = [s EXCEPT !.stage = "waiting",
                             !.pressure = s.fits \/ RotateOversize]
EntryAttempt == /\ s.stage = "waiting" /\ ~s.entryAttempted
                /\ LET accepted == ~s.pressure \/ IgnorePressurePause
                   IN s' = [s EXCEPT !.entryAttempted = TRUE, !.attemptPressure = s.pressure,
                             !.entryAccepted = accepted, !.active = @ + IF accepted THEN 1 ELSE 0]
DataTerminal == /\ s.active > 0 /\ s' = [s EXCEPT !.active = @ - 1]
Unpin == /\ s.pins = 1 /\ s' = [s EXCEPT !.pins = 0]
ControlTerminal == /\ s.busy /\ s' = [s EXCEPT !.busy = FALSE]
Pump == /\ s.stage = "waiting"
        /\ IF s.pressure /\ s.active = 0 /\ s.pins = 0 /\ ~s.busy
              THEN s' = [s EXCEPT !.stage = "io", !.paused = TRUE, !.ioStarts = @ + 1]
              ELSE s' = [s EXCEPT !.pressure = IF ForgetBlockedDemand THEN FALSE ELSE @]
IoTerminal == /\ s.stage = "io"
              /\ \E failed \in BOOLEAN :
                    s' = [s EXCEPT !.stage = "owner", !.failed = failed, !.pressure = FALSE]
OwnerResolve == /\ s.stage = "owner"
                /\ s' = [s EXCEPT !.stage = "done", !.paused = FALSE,
                          !.stopped = s.failed, !.failureObserved = s.failed]
Next == CapacityRefusal \/ EntryAttempt \/ DataTerminal \/ Unpin \/ ControlTerminal
        \/ Pump \/ IoTerminal \/ OwnerResolve
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"idle", "waiting", "io", "owner", "done"}
          /\ s.active \in 0..2 /\ s.pins \in 0..1 /\ s.ioStarts \in 0..1
OversizeNeverStartsRotation == ~s.fits => ~s.pressure /\ s.ioStarts = 0
PressureRefusesNewEntries == s.entryAttempted /\ s.attemptPressure => ~s.entryAccepted
BlockedDemandRemains == s.fits /\ s.stage = "waiting" => s.pressure
AutomaticIoHasTerminalResources == s.ioStarts = 1 => s.active = 0 /\ s.pins = 0 /\ ~s.busy
PauseWaitsForOwner == s.stage \in {"io", "owner"} => s.paused
FailureIsObservableAndStopped == s.stage = "done" /\ s.failed => s.failureObserved /\ s.stopped
=============================================================================
