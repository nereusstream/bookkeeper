-------------------------- MODULE ArenaCheckpoint --------------------------
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
EXTENDS Naturals, FiniteSets
CONSTANTS Commands, Generations, Crashes, RequireAppliedCut, RequireCommittedPublication
ASSUME /\ Commands >= 1 /\ Generations >= 1 /\ Crashes >= 1
       /\ RequireAppliedCut \in BOOLEAN /\ RequireCommittedPublication \in BOOLEAN
Slots == 1..2
AuthorityThrough(cut) == {i \in 1..cut : i % 2 = 1}
Maximum(values) == CHOOSE value \in values : \A other \in values : value >= other
Empty == [generation |-> 0, through |-> 0, capturedApplied |-> 0, authority |-> {},
          forced |-> FALSE, committed |-> FALSE, published |-> FALSE,
          publicationForced |-> FALSE, valid |-> FALSE]
ImageType == [generation : 0..Generations, through : 0..Commands, capturedApplied : 0..Commands,
              authority : SUBSET (1..Commands), forced : BOOLEAN, committed : BOOLEAN,
              published : BOOLEAN, publicationForced : BOOLEAN, valid : BOOLEAN]
VARIABLES length, durable, applied, authority, images, work, target, stage,
          generation, commitWritten, commitDurable, latest, life, crashCount, powerCount, synced
vars == <<length, durable, applied, authority, images, work, target, stage,
          generation, commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
Init == /\ length = 0 /\ durable = 0 /\ applied = 0 /\ authority = {}
        /\ images = [slot \in Slots |-> Empty] /\ work = Empty /\ target = 1 /\ stage = "idle"
        /\ generation = 0 /\ commitWritten = {} /\ commitDurable = {} /\ latest = 0
        /\ life = "live" /\ crashCount = 0 /\ powerCount = 0 /\ synced = FALSE
Append == /\ life = "live" /\ length < Commands
          /\ stage \in {"idle", "frozen", "written", "forced", "published"}
          /\ length' = length + 1
          /\ UNCHANGED <<durable, applied, authority, images, work, target, stage, generation,
                         commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
ForceLog == /\ life = "live" /\ durable < length
            /\ durable' = length
            /\ UNCHANGED <<length, applied, authority, images, work, target, stage, generation,
                           commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
Apply == /\ life = "live" /\ applied < durable
         /\ applied' = applied + 1 /\ authority' = AuthorityThrough(applied + 1)
         /\ UNCHANGED <<length, durable, images, work, target, stage, generation,
                        commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
Freeze == /\ life = "live" /\ stage = "idle" /\ generation < Generations
          /\ work' = [Empty EXCEPT !.generation = generation + 1,
                      !.through = IF RequireAppliedCut THEN applied ELSE durable,
                      !.capturedApplied = applied, !.authority = authority]
          /\ target' = (IF latest = 1 THEN 2 ELSE 1) /\ stage' = "frozen"
          /\ UNCHANGED <<length, durable, applied, authority, images, generation,
                         commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
WriteImage == /\ life = "live" /\ stage = "frozen"
              /\ images' = [images EXCEPT ![target] = [work EXCEPT !.valid = TRUE]]
              /\ stage' = "written"
              /\ UNCHANGED <<length, durable, applied, authority, work, target, generation,
                             commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
ForceImage == /\ life = "live" /\ stage = "written"
              /\ images' = [images EXCEPT ![target].forced = TRUE] /\ stage' = "forced"
              /\ UNCHANGED <<length, durable, applied, authority, work, target, generation,
                             commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
WriteCommit == /\ life = "live" /\ stage = "forced" /\ applied = length
               /\ commitWritten' = commitWritten \cup {work.generation} /\ stage' = "commitWritten"
               /\ UNCHANGED <<length, durable, applied, authority, images, work, target, generation,
                              commitDurable, latest, life, crashCount, powerCount, synced>>
ForceCommit == /\ life = "live" /\ stage = "commitWritten"
               /\ commitDurable' = commitWritten /\ generation' = work.generation
               /\ images' = [images EXCEPT ![target].committed = TRUE] /\ stage' = "committed"
               /\ UNCHANGED <<length, durable, applied, authority, work, target,
                              commitWritten, latest, life, crashCount, powerCount, synced>>
Publish == /\ life = "live" /\ stage \in {"forced", "commitWritten", "committed"}
           /\ images[target].forced
           /\ (~RequireCommittedPublication \/ images[target].committed)
           /\ images' = [images EXCEPT ![target].published = TRUE] /\ stage' = "publishing"
           /\ latest' = target
           /\ UNCHANGED <<length, durable, applied, authority, work, target, generation,
                          commitWritten, commitDurable, life, crashCount, powerCount, synced>>
ForcePublication == /\ life = "live" /\ stage = "publishing"
                    /\ images' = [images EXCEPT ![target].publicationForced = TRUE] /\ stage' = "published"
                    /\ UNCHANGED <<length, durable, applied, authority, work, target, generation,
                                   commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
Finish == /\ life = "live" /\ stage = "published" /\ stage' = "idle"
          /\ UNCHANGED <<length, durable, applied, authority, images, work, target, generation,
                         commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
Corrupt(slot) == /\ images[slot].valid
                /\ images' = [images EXCEPT ![slot].valid = FALSE]
                /\ UNCHANGED <<length, durable, applied, authority, work, target, stage, generation,
                               commitWritten, commitDurable, latest, life, crashCount, powerCount, synced>>
Crash == /\ life = "live" /\ crashCount < Crashes
         /\ life' = "crashed" /\ crashCount' = crashCount + 1 /\ synced' = FALSE
         /\ applied' = 0 /\ authority' = {} /\ stage' = "idle"
         /\ UNCHANGED <<length, durable, images, work, target, generation,
                        commitWritten, commitDurable, latest, powerCount>>
PowerLoss == /\ life = "crashed" /\ ~synced /\ powerCount < 1
             /\ length' = durable /\ commitWritten' = commitDurable /\ powerCount' = powerCount + 1
             /\ images' = [slot \in Slots |-> [images[slot] EXCEPT
                            !.valid = images[slot].valid /\ images[slot].forced,
                            !.published = images[slot].published /\ images[slot].publicationForced]]
             /\ UNCHANGED <<durable, applied, authority, work, target, stage, generation,
                            commitDurable, latest, life, crashCount, synced>>
RecoverySync == /\ life = "crashed" /\ ~synced
                /\ durable' = length /\ commitDurable' = commitWritten /\ synced' = TRUE
                /\ UNCHANGED <<length, applied, authority, images, work, target, stage, generation,
                               commitWritten, latest, life, crashCount, powerCount>>
Recover ==
    LET candidates == {slot \in Slots : images[slot].valid /\ images[slot].forced
                        /\ images[slot].published /\ images[slot].generation \in commitDurable}
        selected == IF candidates = {} THEN 0 ELSE
                    CHOOSE slot \in candidates : \A other \in candidates :
                           images[slot].generation >= images[other].generation
    IN /\ life = "crashed" /\ synced
       /\ authority' = IF selected = 0 THEN AuthorityThrough(length) ELSE
                       images[selected].authority \cup
                       {i \in (images[selected].through + 1)..length : i % 2 = 1}
       /\ applied' = length /\ life' = "recovered" /\ latest' = selected
       /\ images' = IF selected = 0 THEN images ELSE [images EXCEPT ![selected].publicationForced = TRUE]
       /\ generation' = Maximum(commitDurable \cup {0})
       /\ UNCHANGED <<length, durable, work, target, stage, commitWritten, commitDurable,
                      crashCount, powerCount, synced>>
Resume == /\ life = "recovered" /\ life' = "live"
          /\ UNCHANGED <<length, durable, applied, authority, images, work, target, stage, generation,
                         commitWritten, commitDurable, latest, crashCount, powerCount, synced>>
Next == Append \/ ForceLog \/ Apply \/ Freeze \/ WriteImage \/ ForceImage \/ WriteCommit \/ ForceCommit
        \/ Publish \/ ForcePublication \/ Finish \/ Crash \/ PowerLoss \/ RecoverySync \/ Recover \/ Resume
        \/ (\E slot \in Slots : Corrupt(slot))
TypeOK == /\ length \in 0..Commands /\ durable \in 0..length /\ applied \in 0..durable
          /\ authority \subseteq 1..Commands /\ generation \in 0..Generations
          /\ images \in [Slots -> ImageType] /\ work \in ImageType
          /\ commitWritten \subseteq 1..Generations /\ commitDurable \subseteq commitWritten
          /\ latest \in 0..2 /\ target \in Slots /\ crashCount \in 0..Crashes /\ powerCount \in 0..1
          /\ life \in {"live", "crashed", "recovered"} /\ synced \in BOOLEAN
          /\ stage \in {"idle", "frozen", "written", "forced", "commitWritten", "committed",
                        "publishing", "published"}
SnapshotCutWasApplied == (stage # "idle" => work.through <= work.capturedApplied)
                        /\ \A slot \in Slots : images[slot].through <= images[slot].capturedApplied
PublishedImageHasDurableCommit == \A slot \in Slots : images[slot].published =>
                                  images[slot].forced /\ images[slot].generation \in commitDurable
RecoveredAuthorityMatchesFullHistory == life = "recovered" => authority = AuthorityThrough(length)
RecoveredPublicationWasSynced == life = "recovered" /\ latest # 0 => images[latest].publicationForced
Spec == Init /\ [][Next]_vars
=============================================================================
