-------------------- MODULE ProfileRecoveryPeerPool --------------------
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
CONSTANTS Oversubscribe, EarlyCold, EarlyTransport, EarlyTask, CloseBorrowed, IgnoreVerified, IgnoreIncarnation, EarlyChild
VARIABLE s
vars == <<s>>
(* One configured pair / one actual lease. Registry, HELLO and actual terminals are inputs;
   not a TLS/kernel/heap byte producer, full memory budget, remote abort, clock or liveness proof. *)
Init == s \in [capacity : {0}, cold : {FALSE}, transportClosed : {TRUE}, verified : BOOLEAN,
               incarnation : {1, 2}, lease : {FALSE}, child : {FALSE}, childDone : {FALSE},
               draining : {FALSE}, returned : {FALSE}, task : {TRUE}, marker : {TRUE}, closing : {FALSE}]
Acquire == /\ ~s.closing /\ ~s.returned /\ (s.capacity = 0 \/ Oversubscribe)
           /\ s' = [s EXCEPT !.capacity = @ + 2, !.cold = TRUE, !.transportClosed = FALSE]
ColdDone == /\ s.cold /\ s' = [s EXCEPT !.cold = FALSE]
Deliver == /\ s.capacity > 0 /\ ~s.cold /\ ~s.closing /\ ~s.lease /\ ~s.draining /\ ~s.childDone
           /\ (s.verified \/ IgnoreVerified) /\ (s.incarnation = 1 \/ IgnoreIncarnation)
           /\ s' = [s EXCEPT !.lease = TRUE]
BeginChild == /\ s.lease /\ ~s.child /\ ~s.childDone /\ s' = [s EXCEPT !.child = TRUE]
ChildDone == /\ s.child /\ s' = [s EXCEPT !.child = FALSE, !.childDone = TRUE]
PoolClose == /\ ~s.closing
             /\ s' = [s EXCEPT !.closing = TRUE, !.draining = ~s.lease,
                       !.transportClosed = IF s.lease /\ CloseBorrowed THEN TRUE ELSE @]
LeaseClose == /\ s.lease /\ (s.childDone \/ EarlyChild)
              /\ s' = [s EXCEPT !.lease = FALSE, !.draining = TRUE]
TransportDone == /\ s.draining /\ ~s.transportClosed
                 /\ s' = [s EXCEPT !.transportClosed = TRUE]
Return == /\ s.capacity > 0 /\ ~s.lease /\ ~s.child
          /\ (~s.cold \/ EarlyCold) /\ (s.transportClosed \/ EarlyTransport)
          /\ s' = [s EXCEPT !.capacity = 0, !.returned = TRUE]
ClearTask == /\ s.task /\ ((s.childDone /\ s.returned) \/ EarlyTask)
             /\ s' = [s EXCEPT !.task = FALSE, !.marker = FALSE]
Next == Acquire \/ ColdDone \/ Deliver \/ BeginChild \/ ChildDone \/ PoolClose \/ LeaseClose \/ TransportDone
        \/ Return \/ ClearTask
Spec == Init /\ [][Next]_vars
TypeOK == s.capacity \in {0, 2, 4, 6}
ConnectionReservationIsBounded == s.capacity <= 2
ColdBeforeCapacityReturn == s.returned => ~s.cold
TransportBeforeCapacityReturn == s.returned => s.transportClosed
OriginalTaskUntilDrained == ~s.task => s.childDone /\ s.returned
BorrowedLeasePinsTransport == s.lease => ~s.transportClosed
LeaseHasVerifiedPeers == s.lease => s.verified
NoSilentIncarnationRejoin == s.lease => s.incarnation = 1
ActualChildPinsLease == s.child => s.lease
=============================================================================
