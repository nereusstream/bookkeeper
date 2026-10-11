-------------------- MODULE ProfileRegisteredConnection --------------------
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
CONSTANTS IgnorePre, IgnoreHello, IgnorePost, EarlyCold, EarlyWrite, EarlyHelloWrite, EarlyTransport, NoRetire
VARIABLE s
vars == <<s>>
(* Verified registration/HELLO/post-read facts and actual terminals are inputs.
   No TLS/ZK/fsync producer, remote cancellation, budget composition, clock or liveness. *)
Init == s \in [phase : {"new"}, preOK : BOOLEAN, helloOK : BOOLEAN, postOK : BOOLEAN,
               cold : {FALSE}, transport : {FALSE}, helloWrite : {FALSE}, ready : {FALSE},
               readyStopped : {FALSE}, stopped : {FALSE}, checked : {FALSE}, available : {TRUE},
               rpc : {FALSE}, output : {FALSE}, retired : {FALSE}, drained : {FALSE}]
BeginPre == /\ s.phase = "new" /\ ~s.stopped
            /\ s' = [s EXCEPT !.phase = "cold", !.cold = TRUE]
FinishPre == /\ s.phase = "cold" /\ s.cold
             /\ s' = [s EXCEPT !.cold = FALSE,
                       !.phase = IF (s.preOK \/ IgnorePre) /\ ~s.stopped THEN "pre" ELSE "failed",
                       !.stopped = s.stopped \/ ~(s.preOK \/ IgnorePre)]
Submit == /\ s.phase = "pre" /\ ~s.stopped
          /\ s' = [s EXCEPT !.phase = "hello", !.transport = TRUE, !.helloWrite = TRUE]
Hello == /\ s.phase = "hello" /\ ~s.stopped
         /\ s' = [s EXCEPT !.phase = IF s.helloOK \/ IgnoreHello THEN "post" ELSE "failed",
                   !.cold = s.helloOK \/ IgnoreHello, !.stopped = ~(s.helloOK \/ IgnoreHello)]
FinishPost == /\ s.phase = "post" /\ s.cold
              /\ s' = [s EXCEPT !.cold = FALSE,
                        !.ready = (s.postOK \/ IgnorePost) /\ ~s.stopped,
                        !.readyStopped = s.stopped,
                        !.phase = IF (s.postOK \/ IgnorePost) /\ ~s.stopped THEN "ready" ELSE "failed",
                        !.stopped = s.stopped \/ ~(s.postOK \/ IgnorePost)]
CheckCaller == /\ s.ready /\ ~s.stopped /\ ~s.checked /\ ~s.rpc
               /\ s' = [s EXCEPT !.checked = TRUE]
Acquire == /\ s.checked /\ s.available
           /\ s' = [s EXCEPT !.checked = FALSE, !.available = FALSE, !.retired = FALSE,
                     !.rpc = TRUE, !.output = TRUE]
Stop == /\ ~s.stopped
        /\ s' = [s EXCEPT !.stopped = TRUE, !.retired = s.available,
                  !.available = IF NoRetire THEN @ ELSE FALSE]
RPCDone == /\ s.rpc
           /\ s' = [s EXCEPT !.rpc = FALSE, !.output = FALSE, !.retired = s.stopped,
                     !.available = ~s.stopped \/ NoRetire]
HelloWriteDone == /\ s.helloWrite /\ s' = [s EXCEPT !.helloWrite = FALSE]
TransportDone == /\ s.stopped /\ s.transport /\ s' = [s EXCEPT !.transport = FALSE]
RetireUnused == /\ s.stopped /\ s.available /\ ~NoRetire
                /\ s' = [s EXCEPT !.available = FALSE, !.retired = TRUE]
Drain == /\ s.stopped /\ ~s.drained
         /\ (~s.cold \/ EarlyCold) /\ (~s.output \/ EarlyWrite) /\ (~s.helloWrite \/ EarlyHelloWrite)
         /\ (~s.transport \/ EarlyTransport) /\ (s.retired \/ EarlyWrite)
         /\ s' = [s EXCEPT !.drained = TRUE]
Next == BeginPre \/ FinishPre \/ Submit \/ Hello \/ FinishPost \/ CheckCaller \/ Acquire \/ Stop \/ RPCDone
        \/ HelloWriteDone \/ TransportDone \/ RetireUnused \/ Drain
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"new", "cold", "pre", "hello", "post", "ready", "failed"}
ReadyNeedsPre == s.ready => s.preOK
ReadyNeedsHello == s.ready => s.helloOK
ReadyNeedsPost == s.ready => s.postOK
ColdDrainsBeforeCapacity == s.drained => ~s.cold
OutputDrainsBeforeCapacity == s.drained => ~s.output
HelloWriteDrainsBeforeCapacity == s.drained => ~s.helloWrite
TransportDrainsBeforeCapacity == s.drained => ~s.transport
NoAcquisitionAfterDrain == s.drained => ~s.rpc
=============================================================================
