-------------------- MODULE ProfileReplicaAudit --------------------
(*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*)
EXTENDS Integers
CONSTANTS IgnoreClosed, IgnoreScope, UnknownIsHole, IgnoreFresh, ClearHealthy,
          EarlyDrain, EarlyRelease, EarlySDK, DoubleBorrow
VARIABLE s
vars == <<s>>
(* CLOSED/descriptor scope, rich native health status, actual health/lease terminal and final cut
   are inputs; this does not produce TLS/ZK/fsync or complete loss/allocator/liveness evidence. *)
Init == s \in [closed : BOOLEAN, scoped : BOOLEAN, free : BOOLEAN, fresh : BOOLEAN,
               status : {"HEALTHY", "HOLE", "UNKNOWN", "CONFLICT", "DELETED", "LIMIT"},
               phase : {"new"}, held : {FALSE}, healthActual : {FALSE}, leaseActual : {FALSE},
               marker : {FALSE}, cleared : {FALSE}, sdkClosed : {FALSE}, reservations : {0}]
Acquire == /\ s.phase = "new" /\ s.free
           /\ (s.closed \/ IgnoreClosed) /\ (s.scoped \/ IgnoreScope)
           /\ s' = [s EXCEPT !.phase = "running", !.held = TRUE,
                             !.healthActual = TRUE, !.leaseActual = TRUE, !.reservations = 1]
Skip == /\ s.phase = "new" /\ (~s.free \/ ~s.closed \/ ~s.scoped)
        /\ s' = [s EXCEPT !.phase = "done"]
BorrowAgain == /\ DoubleBorrow /\ s.held /\ s.reservations = 1
               /\ s' = [s EXCEPT !.reservations = 2]
HealthDone == /\ s.phase = "running" /\ s.healthActual
              /\ s' = [s EXCEPT !.healthActual = FALSE]
Drain == /\ s.phase = "running" /\ (~s.healthActual \/ EarlyDrain)
         /\ s' = [s EXCEPT !.phase = "draining"]
LeaseDone == /\ s.phase = "draining" /\ s.leaseActual
             /\ s' = [s EXCEPT !.leaseActual = FALSE]
Finish == /\ s.phase = "draining" /\ (~s.leaseActual \/ EarlyRelease)
          /\ s' = [s EXCEPT !.phase = "done", !.held = FALSE, !.reservations = 0,
                            !.marker = (s.fresh \/ IgnoreFresh)
                                      /\ (s.status = "HOLE" \/ (UnknownIsHole /\ s.status = "UNKNOWN")),
                            !.cleared = ClearHealthy /\ s.status = "HEALTHY"]
SDKClose == /\ ~s.sdkClosed /\ (s.phase = "done" \/ EarlySDK)
            /\ s' = [s EXCEPT !.sdkClosed = TRUE]
Next == Acquire \/ Skip \/ BorrowAgain \/ HealthDone \/ Drain \/ LeaseDone \/ Finish \/ SDKClose
Spec == Init /\ [][Next]_vars
TypeOK == s.reservations \in 0..2 /\ s.phase \in {"new", "running", "draining", "done"}
AuditOnlyClosed == s.healthActual => s.closed
AuditOnlyScoped == s.healthActual => s.scoped
DrainWaitsActualHealth == s.phase \in {"draining", "done"} => ~s.healthActual
CapacityWaitsActualLease == ~s.held => ~s.leaseActual
UnknownNeverSchedulesMissing == s.marker => s.status = "HOLE"
MarkerNeedsFinalFreshCut == s.marker => s.fresh
ObserverNeverClearsMarker == ~s.cleared
SharedPoolReservationIsBounded == s.reservations <= 1
SDKWaitsActualHealthAndLease == s.sdkClosed => ~s.healthActual /\ ~s.leaseActual
=============================================================================
