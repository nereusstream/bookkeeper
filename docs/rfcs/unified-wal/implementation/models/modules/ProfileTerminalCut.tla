-------------------- MODULE ProfileTerminalCut --------------------
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
CONSTANTS ApplyBeforeForce, SkipPrecut, AcceptNewAfterCut, ReturnExcluded,
          DropCutInCheckpoint, ReopenAfterReplay, FreePendingCut, FreeRequiredPrefix
VARIABLE s
vars == <<s>>
(* One old grant close upgraded under the same operation, an in-flight ticket
   and a read above selected P. I/O completion is an input, not generated here. *)
Init == s \in [phase : {"open"}, io : {0, 1}, forced : {FALSE}, cut : {FALSE},
               appliedWithIO : {FALSE}, exposed : {FALSE}, admitted : {FALSE}, image : {FALSE},
               freedSuffix : {FALSE}, freeHadCut : {FALSE}, freedPrefix : {FALSE}]
Prepare == /\ s.phase = "open" /\ s' = [s EXCEPT !.phase = "pending"]
FinishIO == /\ s.phase = "pending" /\ s.io = 1 /\ s' = [s EXCEPT !.io = 0]
Force == /\ s.phase = "pending" /\ (s.io = 0 \/ SkipPrecut)
         /\ s' = [s EXCEPT !.forced = TRUE]
Apply == /\ s.phase = "pending" /\ (s.forced \/ ApplyBeforeForce) /\ (s.io = 0 \/ SkipPrecut)
         /\ s' = [s EXCEPT !.phase = "applied", !.cut = TRUE, !.appliedWithIO = s.io # 0]
NewWrite == /\ s.phase \in {"pending", "applied", "replayed"} /\ AcceptNewAfterCut
            /\ s' = [s EXCEPT !.admitted = TRUE]
Read == /\ s.phase \in {"pending", "applied", "replayed"} /\ ReturnExcluded
        /\ s' = [s EXCEPT !.exposed = TRUE]
Checkpoint == /\ s.phase = "applied" /\ s' = [s EXCEPT !.phase = "checkpoint", !.image = ~DropCutInCheckpoint]
Replay == /\ s.phase = "checkpoint" /\ s' = [s EXCEPT !.phase = "replayed", !.cut = s.image /\ ~ReopenAfterReplay]
(* FREE consumes durable local exclusion, never pending authority or e <= P. *)
ReclaimSuffix == /\ s.phase \in {"pending", "applied", "checkpoint", "replayed"}
                 /\ (s.cut \/ FreePendingCut)
                 /\ s' = [s EXCEPT !.freedSuffix = TRUE, !.freeHadCut = s.cut]
ReclaimPrefix == /\ FreeRequiredPrefix /\ s.phase = "applied"
                 /\ s' = [s EXCEPT !.freedPrefix = TRUE]
Next == ReclaimSuffix \/ ReclaimPrefix \/ Prepare \/ FinishIO \/ Force \/ Apply \/ NewWrite \/ Read \/ Checkpoint \/ Replay
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"open", "pending", "applied", "checkpoint", "replayed"} /\ s.io \in 0..1
CutHasForce == s.cut => s.forced
CutWaitsForPrecut == ~s.appliedWithIO
NoNewAdmission == ~s.admitted
NoExcludedRead == ~s.exposed
CheckpointRetainsUpgrade == s.phase = "checkpoint" => s.image
ReplayKeepsCut == s.phase = "replayed" => s.cut
SuffixFreeHasDurableCut == s.freedSuffix => s.freeHadCut
RequiredPrefixIsNotDead == ~s.freedPrefix
=============================================================================
