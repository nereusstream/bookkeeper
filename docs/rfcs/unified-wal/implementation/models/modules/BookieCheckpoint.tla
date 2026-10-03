-------------------------- MODULE BookieCheckpoint --------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License. You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. *)
EXTENDS Naturals
CONSTANTS Snapshots, Crashes, DropTerminal, RetirePrefix
ASSUME /\ Snapshots >= 1 /\ Crashes >= 1 /\ DropTerminal \in BOOLEAN /\ RetirePrefix \in BOOLEAN
Empty == [installed |-> FALSE, active |-> FALSE, fenced |-> FALSE, tombstone |-> FALSE,
          grant |-> FALSE, closed |-> FALSE, lac |-> 0, version |-> 0]
AuthorityType == [installed : BOOLEAN, active : BOOLEAN, fenced : BOOLEAN, tombstone : BOOLEAN,
                  grant : BOOLEAN, closed : BOOLEAN, lac : 0..2, version : 0..4]
(* Fixed authenticated trace: install, activate, two LACs, grant, close, fence,
   tombstone, then retries of the old activate/install identities. *)
Step(s, command) ==
    CASE command = 1 -> [s EXCEPT !.installed = TRUE, !.version = 1]
      [] command = 2 -> [s EXCEPT !.active = TRUE, !.version = @ + 1]
      [] command = 3 -> [s EXCEPT !.lac = 1]
      [] command = 4 -> [s EXCEPT !.lac = 2]
      [] command = 5 -> [s EXCEPT !.grant = TRUE]
      [] command = 6 -> [s EXCEPT !.closed = TRUE]
      [] command = 7 -> [s EXCEPT !.active = FALSE, !.fenced = TRUE, !.version = @ + 1]
      [] command = 8 -> [s EXCEPT !.active = FALSE, !.tombstone = TRUE,
                         !.grant = FALSE, !.version = @ + 1]
      [] OTHER -> s
RECURSIVE Replay(_, _, _)
Replay(s, first, last) == IF first > last THEN s ELSE Replay(Step(s, first), first + 1, last)
Full(cut) == Replay(Empty, 1, cut)
VARIABLES length, durable, applied, authority, workCut, workAuthority, imageCut, imageAuthority,
          imageExists, generation, stage, life, crashCount, synced, retiredThrough
vars == <<length, durable, applied, authority, workCut, workAuthority, imageCut, imageAuthority,
          imageExists, generation, stage, life, crashCount, synced, retiredThrough>>
Init == /\ length = 0 /\ durable = 0 /\ applied = 0 /\ authority = Empty
        /\ workCut = 0 /\ workAuthority = Empty /\ imageCut = 0 /\ imageAuthority = Empty
        /\ imageExists = FALSE /\ generation = 0 /\ stage = "idle" /\ life = "live"
        /\ crashCount = 0 /\ synced = FALSE /\ retiredThrough = 0
Append == /\ life = "live" /\ length < 10 /\ length' = length + 1
          /\ UNCHANGED <<durable, applied, authority, workCut, workAuthority, imageCut, imageAuthority,
                         imageExists, generation, stage, life, crashCount, synced, retiredThrough>>
ForceLog == /\ life = "live" /\ durable < length /\ durable' = length
            /\ UNCHANGED <<length, applied, authority, workCut, workAuthority, imageCut, imageAuthority,
                           imageExists, generation, stage, life, crashCount, synced, retiredThrough>>
Apply == /\ life = "live" /\ applied < durable /\ applied' = applied + 1
         /\ authority' = Step(authority, applied + 1)
         /\ UNCHANGED <<length, durable, workCut, workAuthority, imageCut, imageAuthority,
                        imageExists, generation, stage, life, crashCount, synced, retiredThrough>>
Freeze == /\ life = "live" /\ stage = "idle" /\ generation < Snapshots
          /\ workCut' = applied /\ stage' = "frozen"
          /\ workAuthority' = (IF DropTerminal /\ (authority.fenced \/ authority.tombstone)
                                THEN [authority EXCEPT !.active = TRUE, !.fenced = FALSE, !.tombstone = FALSE]
                                ELSE authority)
          /\ UNCHANGED <<length, durable, applied, authority, imageCut, imageAuthority,
                         imageExists, generation, life, crashCount, synced, retiredThrough>>
(* This action assumes the payload/commit/publication barriers proven by the
   separate control-checkpoint Model C slice; it is not a composed A+C proof. *)
Publish == /\ life = "live" /\ stage = "frozen" /\ workCut <= durable
           /\ imageCut' = workCut /\ imageAuthority' = workAuthority /\ imageExists' = TRUE
           /\ generation' = generation + 1 /\ stage' = "idle"
           /\ UNCHANGED <<length, durable, applied, authority, workCut, workAuthority, life, crashCount, synced, retiredThrough>>
Crash == /\ life = "live" /\ crashCount < Crashes /\ life' = "recovery" /\ synced' = FALSE
         /\ crashCount' = crashCount + 1 /\ authority' = Empty /\ applied' = 0 /\ stage' = "idle"
         /\ UNCHANGED <<length, durable, workCut, workAuthority, imageCut, imageAuthority, imageExists, generation, retiredThrough>>
RecoverySync == /\ life = "recovery" /\ ~synced /\ durable' = length /\ synced' = TRUE
                /\ UNCHANGED <<length, applied, authority, workCut, workAuthority, imageCut, imageAuthority,
                               imageExists, generation, stage, life, crashCount, retiredThrough>>
Recover == /\ life = "recovery" /\ synced /\ (retiredThrough = 0 \/ imageExists)
           /\ life' = "live" /\ applied' = length
           /\ authority' = (IF imageExists THEN Replay(imageAuthority, imageCut + 1, length) ELSE Full(length))
           /\ UNCHANGED <<length, durable, workCut, workAuthority, imageCut, imageAuthority,
                          imageExists, generation, stage, crashCount, synced, retiredThrough>>
(* The shared physical-rotation slice separately checks dual roots and barriers.
   This permission projection retires only a committed image-covered prefix. *)
ReclaimPrefix == /\ RetirePrefix /\ life = "live" /\ stage = "idle" /\ imageExists
                 /\ imageCut > retiredThrough /\ retiredThrough' = imageCut
                 /\ UNCHANGED <<length, durable, applied, authority, workCut, workAuthority,
                                imageCut, imageAuthority, imageExists, generation, stage,
                                life, crashCount, synced>>
TypeOK == /\ length \in 0..10 /\ durable \in 0..length /\ applied \in 0..durable
          /\ authority \in AuthorityType /\ workAuthority \in AuthorityType /\ imageAuthority \in AuthorityType
          /\ workCut \in 0..10 /\ imageCut \in 0..durable /\ imageExists \in BOOLEAN
          /\ retiredThrough \in 0..durable
          /\ generation \in 0..Snapshots /\ crashCount \in 0..Crashes /\ synced \in BOOLEAN
          /\ life \in {"live", "recovery"} /\ stage \in {"idle", "frozen"}
BookieAuthorityMatchesAppliedPrefix == life # "live" \/ authority = Full(applied)
BookieTerminalPermissionsRemainDenied ==
    life # "live" \/ /\ ((Full(applied).fenced \/ Full(applied).tombstone) => ~authority.active)
                     /\ (Full(applied).tombstone => authority.tombstone)
BookieRetiredPrefixNeedsCheckpoint == retiredThrough = 0 \/ (imageExists /\ retiredThrough <= imageCut)
RetiredBookieTerminalPermissionsRemainDenied == retiredThrough = 0 \/ BookieTerminalPermissionsRemainDenied
Next == Append \/ ForceLog \/ Apply \/ Freeze \/ Publish \/ Crash \/ RecoverySync \/ Recover \/ ReclaimPrefix
Spec == Init /\ [][Next]_vars
=============================================================================
