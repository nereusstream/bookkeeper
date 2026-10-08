-------------------- MODULE ProfilePeerDiscovery --------------------
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
CONSTANTS IgnoreReady, IgnoreOwner, IgnorePair, IgnoreMarker, IgnoreGeneration, IgnoreBirth
VARIABLE s
vars == <<s>>
(* Strict codec/READY/ephemeral owner/paired envelopes/marker observations are inputs.
   This is a consumer observation cut, not AuthN/AuthZ, HELLO, mutation authority or liveness. *)
Init == s \in [phase : {"new"}, ready : BOOLEAN, owner : BOOLEAN, pair : BOOLEAN, marker : BOOLEAN,
               generation : {0}, birth : {0}, observedGeneration : {0}, observedBirth : {0},
               accepted : {FALSE}, finalGeneration : {0}, finalBirth : {0}]
Read == /\ s.phase = "new"
        /\ s' = [s EXCEPT !.phase = "read", !.observedGeneration = s.generation, !.observedBirth = s.birth]
ChangeGeneration == /\ s.phase = "read" /\ s.generation = 0
                    /\ s' = [s EXCEPT !.generation = 1]
RecreateSameBytes == /\ s.phase = "read" /\ s.birth = 0 /\ s' = [s EXCEPT !.birth = 1]
Finish == /\ s.phase = "read"
          /\ (s.ready \/ IgnoreReady) /\ (s.owner \/ IgnoreOwner)
          /\ (s.pair \/ IgnorePair) /\ (s.marker \/ IgnoreMarker)
          /\ (s.generation = s.observedGeneration \/ IgnoreGeneration)
          /\ (s.birth = s.observedBirth \/ IgnoreBirth)
          /\ s' = [s EXCEPT !.phase = "done", !.accepted = TRUE,
                    !.finalGeneration = s.generation, !.finalBirth = s.birth]
Next == Read \/ ChangeGeneration \/ RecreateSameBytes \/ Finish
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"new", "read", "done"}
ReadyRequired == s.accepted => s.ready
OwnerRequired == s.accepted => s.owner
PairRequired == s.accepted => s.pair
MarkerRequired == s.accepted => s.marker
GenerationUnchangedAtCut == s.accepted => s.finalGeneration = s.observedGeneration
BirthUnchangedAtCut == s.accepted => s.finalBirth = s.observedBirth
=============================================================================
