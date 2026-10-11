-------------------- MODULE ProfileServiceBootstrap --------------------
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
CONSTANTS DefaultEnabled, IgnoreContext, IgnoreAudit, IgnoreReady, EarlySDK, EarlyOwner, LeakFailure
VARIABLE s
vars == <<s>>
(* SDK/ZK ownership, supported audit scope, verified service readiness and actual
   worker/pool terminals are inputs; no TLS/fsync/SDK allocator or liveness producer. *)
Init == s \in [explicit : BOOLEAN, context : BOOLEAN, audits : BOOLEAN, ready : BOOLEAN,
               phase : {"new"}, owners : {FALSE}, sdk : {FALSE}, enabled : {FALSE},
               active : {FALSE}, poolDrained : {FALSE}, stopping : {FALSE}, sdkClosed : {FALSE},
               ownersClosed : {FALSE}, failed : {FALSE}, returned : {FALSE}]
Allocate == /\ s.phase = "new"
            /\ s' = [s EXCEPT !.phase = "allocated", !.owners = s.explicit \/ DefaultEnabled, !.sdk = TRUE]
Bind == /\ s.phase = "allocated"
        /\ (((s.context \/ IgnoreContext) /\ (s.audits \/ IgnoreAudit)) \/ ~s.explicit)
        /\ s' = [s EXCEPT !.phase = "bound", !.enabled = s.explicit \/ DefaultEnabled]
Fail == /\ s.phase = "allocated" /\ s' = [s EXCEPT !.phase = "failed", !.failed = TRUE, !.stopping = TRUE]
Start == /\ s.phase = "bound" /\ (s.ready \/ IgnoreReady)
         /\ s' = [s EXCEPT !.phase = "running", !.active = TRUE]
Stop == /\ s.phase \in {"bound", "running"} /\ ~s.stopping /\ s' = [s EXCEPT !.stopping = TRUE]
WorkerDone == /\ s.stopping /\ s.active /\ s' = [s EXCEPT !.active = FALSE]
PoolDone == /\ s.stopping /\ ~s.active /\ ~s.poolDrained /\ s' = [s EXCEPT !.poolDrained = TRUE]
SDKClose == /\ s.stopping /\ ~s.sdkClosed /\ (s.poolDrained \/ EarlySDK)
            /\ s' = [s EXCEPT !.sdkClosed = TRUE]
OwnerClose == /\ s.stopping /\ ~s.ownersClosed /\ (s.sdkClosed \/ EarlyOwner)
              /\ s' = [s EXCEPT !.ownersClosed = TRUE]
Return == /\ s.stopping /\ ~s.returned /\ ((s.sdkClosed /\ s.ownersClosed) \/ (s.failed /\ LeakFailure))
          /\ s' = [s EXCEPT !.returned = TRUE]
Next == Allocate \/ Bind \/ Fail \/ Start \/ Stop \/ WorkerDone \/ PoolDone \/ SDKClose \/ OwnerClose \/ Return
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"new", "allocated", "bound", "failed", "running"}
DefaultIsOff == s.enabled => s.explicit
EnabledUsesContext == s.enabled => s.context
EnabledUsesSupportedAuditScope == s.enabled => s.audits
StartNeedsReady == s.active => s.ready
SDKWaitsForPool == s.sdkClosed => s.poolDrained
OwnersWaitForSDK == s.ownersClosed => s.sdkClosed
FailureReturnKeepsNoOwners == s.returned /\ s.failed => s.sdkClosed /\ s.ownersClosed
=============================================================================
