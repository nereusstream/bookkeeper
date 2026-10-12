-------------------- MODULE ProfileDeleteAdoption --------------------
\* Licensed to the Apache Software Foundation (ASF) under one or more
\* contributor license agreements. See the NOTICE file distributed with
\* this work for additional information regarding copyright ownership.
\* The ASF licenses this file to you under the Apache License, Version 2.0
\* (the "License"); you may not use this file except in compliance
\* with the License. You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.
EXTENDS Integers
CONSTANTS IgnoreCut, IgnoreForce, ForgetCheckpoint, EarlyRelease
VARIABLE s
vars == <<s>>
(* Native PREPARED plan/birth, actual effect replay and I/O outcomes are Java facts.
   Models the local writer only: no native ACK, EFFECTIVE switch or route retirement. *)
Init == s \in [cut : {0,1}, local : {0}, written : {FALSE}, forced : {FALSE},
               adopted : {FALSE}, adoptedWithoutCut : {FALSE}, adoptedWithoutForce : {FALSE},
               recovered : {FALSE}, lostPlan : {FALSE}, terminal : {FALSE}, held : {TRUE},
               releasedEarly : {FALSE}, timedOut : {FALSE}, effectiveGeneration : {1}]
ApplyEffect == /\ ~s.written /\ s.cut = 1 /\ s.local = 0
               /\ s' = [s EXCEPT !.local = 1]
Append == /\ ~s.written /\ (s.local = s.cut \/ IgnoreCut)
          /\ s' = [s EXCEPT !.written = TRUE]
Force == /\ s.written /\ ~s.forced
         /\ s' = [s EXCEPT !.forced = TRUE]
Adopt == /\ s.written /\ ~s.adopted /\ (s.forced \/ IgnoreForce)
         /\ s' = [s EXCEPT !.adopted = TRUE, !.adoptedWithoutCut = (s.local # s.cut),
                   !.adoptedWithoutForce = ~s.forced]
OwnerTerminal == /\ s.adopted /\ ~s.terminal
                 /\ s' = [s EXCEPT !.terminal = TRUE]
Timeout == /\ ~s.timedOut /\ s' = [s EXCEPT !.timedOut = TRUE]
Release == /\ s.held /\ (s.terminal \/ EarlyRelease)
           /\ s' = [s EXCEPT !.held = FALSE, !.releasedEarly = ~s.terminal]
CheckpointRestart == /\ s.terminal /\ ~s.recovered
                     /\ s' = [s EXCEPT !.recovered = TRUE, !.adopted = ~ForgetCheckpoint,
                               !.lostPlan = ForgetCheckpoint]
Next == ApplyEffect \/ Append \/ Force \/ Adopt \/ OwnerTerminal \/ Timeout \/ Release \/ CheckpointRestart
Spec == Init /\ [][Next]_vars
TypeOK == s.local \in {0,1} /\ s.effectiveGeneration = 1
AdoptionHasCompleteCut == ~s.adoptedWithoutCut
AdoptionHasForce == ~s.adoptedWithoutForce
AdoptionReplaysCheckpoint == ~s.lostPlan
AdoptionReleaseWaitsOwnerTerminal == ~s.releasedEarly
=============================================================================
