-------------------- MODULE ProfileServiceShutdown --------------------
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
CONSTANTS SelfStop, IgnoreJoin, IgnoreRepeated, EarlyManager, EarlySDK, IgnoreSignal, KeepSignal, JoinSelf, Reopen
VARIABLE s
vars == <<s>>
(* Actual child/thread and SDK-close observations are inputs. This does not prove
   remote abort, SDK internals, clocks, or liveness of an indefinitely blocked child. *)
Init == s \in [actual : {TRUE}, stop : {FALSE}, ended : {FALSE}, joined : {FALSE}, manager : {FALSE},
               sdkStarted : {FALSE}, sdkClosed : {FALSE}, metadataClosed : {FALSE}, sdkInterrupted : {FALSE},
               finished : {FALSE}, returned : {FALSE}, secondWaiting : {FALSE}, secondReturned : {FALSE},
               interrupted : {FALSE}, restored : {FALSE}, selfWaiting : {FALSE}, startedAfterStop : {FALSE}]
Stop == /\ ~s.stop /\ s' = [s EXCEPT !.stop = TRUE, !.interrupted = TRUE]
ActualDone == /\ s.actual /\ s' = [s EXCEPT !.actual = FALSE]
ThreadDone == /\ ~s.actual /\ ~s.ended /\ ~s.selfWaiting /\ s' = [s EXCEPT !.ended = TRUE]
Join == /\ s.stop /\ ~s.joined
        /\ (IF SelfStop THEN ~s.actual ELSE s.ended \/ IgnoreJoin)
        /\ s' = [s EXCEPT !.joined = TRUE, !.selfWaiting = SelfStop /\ JoinSelf]
SecondCaller == /\ s.stop /\ ~s.secondWaiting /\ ~s.secondReturned
                /\ s' = [s EXCEPT !.secondWaiting = ~IgnoreRepeated, !.secondReturned = IgnoreRepeated]
CloseManager == /\ s.stop /\ ~s.manager /\ (s.joined \/ EarlyManager)
                /\ s' = [s EXCEPT !.manager = TRUE]
StartSDK == /\ s.stop /\ ~s.sdkStarted /\ (s.joined \/ EarlySDK)
            /\ s' = [s EXCEPT !.sdkStarted = TRUE, !.sdkInterrupted = KeepSignal /\ s.interrupted]
CloseSDK == /\ s.sdkStarted /\ ~s.sdkClosed
            /\ s' = [s EXCEPT !.sdkClosed = TRUE, !.metadataClosed = ~s.sdkInterrupted]
Finish == /\ s.manager /\ s.sdkClosed /\ ~s.finished
          /\ s' = [s EXCEPT !.finished = TRUE, !.returned = TRUE, !.restored = ~IgnoreSignal]
FinishSecond == /\ s.secondWaiting /\ s.finished /\ s.ended
                /\ s' = [s EXCEPT !.secondWaiting = FALSE, !.secondReturned = TRUE]
StartAgain == /\ Reopen /\ s.stop /\ ~s.startedAfterStop /\ s' = [s EXCEPT !.startedAfterStop = TRUE]
Next == Stop \/ ActualDone \/ ThreadDone \/ Join \/ SecondCaller \/ CloseManager \/ StartSDK \/ CloseSDK
        \/ Finish \/ FinishSecond \/ StartAgain
Spec == Init /\ [][Next]_vars
TypeOK == s.actual \in BOOLEAN /\ s.ended \in BOOLEAN
ManagerNeedsActualDrain == s.manager => ~s.actual
SDKNeedsActualDrain == s.sdkStarted => ~s.actual
RepeatedCallerWaitsForCleanup == s.secondReturned => s.finished /\ s.ended
CallerRestoresInterrupt == s.returned => s.restored
SDKCloseKeepsMetadataCleanup == s.sdkClosed => s.metadataClosed
SelfStopNeverJoinsItself == ~s.selfWaiting
ShutdownNeverRestarts == ~s.startedAfterStop
=============================================================================
