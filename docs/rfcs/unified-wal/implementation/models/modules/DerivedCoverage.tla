-------------------------- MODULE DerivedCoverage --------------------------
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
CONSTANTS Entries, Generations, Crashes, EarlyPublish
ASSUME /\ Entries >= 1 /\ Generations >= 2 /\ Crashes >= 1 /\ EarlyPublish \in BOOLEAN
Data == 1..Entries
None == [generation |-> 0, through |-> 0]
VARIABLES generation, binding, bindingDurable, visible, persisted, page, pageDurable, stage, life, crashes
vars == <<generation, binding, bindingDurable, visible, persisted, page, pageDurable, stage, life, crashes>>
Init == /\ generation = 1 /\ binding = 1 /\ bindingDurable = 1
        /\ visible = {} /\ persisted = {} /\ page = None /\ pageDurable = None
        /\ stage = "idle" /\ life = "live" /\ crashes = 0
(* One column-family WriteBatch changes the binding and deletes old locators atomically. *)
BeginRebuild == /\ life = "live" /\ stage = "idle" /\ generation < Generations
                /\ generation' = generation + 1 /\ binding' = generation + 1 /\ visible' = {}
                /\ stage' = "resetting"
                /\ UNCHANGED <<bindingDurable, persisted, page, pageDurable, life, crashes>>
ForceReset == /\ life = "live" /\ stage = "resetting"
              /\ bindingDurable' = binding /\ persisted' = visible /\ stage' = "loading"
              /\ UNCHANGED <<generation, binding, visible, page, pageDurable, life, crashes>>
Write == /\ life = "live" /\ stage = "loading" /\ visible # Data
         /\ \E entry \in Data \ visible : visible' = visible \cup {entry}
         /\ UNCHANGED <<generation, binding, bindingDurable, persisted, page, pageDurable, stage, life, crashes>>
Flush == /\ life = "live" /\ stage = "loading" /\ visible = Data
         /\ persisted' = visible /\ stage' = "flushed"
         /\ UNCHANGED <<generation, binding, bindingDurable, visible, page, pageDurable, life, crashes>>
Publish == /\ life = "live" /\ visible = Data
           /\ (stage = "flushed" \/ (EarlyPublish /\ stage = "loading"))
           /\ page' = [generation |-> generation, through |-> Entries] /\ stage' = "publishing"
           /\ UNCHANGED <<generation, binding, bindingDurable, visible, persisted, pageDurable, life, crashes>>
ForcePublication == /\ life = "live" /\ stage = "publishing"
                    /\ pageDurable' = page /\ stage' = "idle"
                    /\ UNCHANGED <<generation, binding, bindingDurable, visible, persisted, page, life, crashes>>
(* Model failure discards unflushed native contents and unforced pages; not a real fsync proof. *)
Crash == /\ life = "live" /\ crashes < Crashes /\ life' = "recovery" /\ crashes' = crashes + 1
         /\ binding' = bindingDurable /\ generation' = bindingDurable /\ visible' = persisted
         /\ page' = pageDurable /\ stage' = "idle"
         /\ UNCHANGED <<bindingDurable, persisted, pageDurable>>
Recover == /\ life = "recovery" /\ life' = "live"
           /\ UNCHANGED <<generation, binding, bindingDurable, visible, persisted, page, pageDurable, stage, crashes>>
Usable == pageDurable.generation = binding /\ pageDurable.generation > 0
CoverageWasPersisted == Usable => (persisted = Data /\ pageDurable.through = Entries)
RecoveredCoverageHasLocations == life # "live" \/ ~Usable \/ visible = Data
TypeOK == /\ generation \in 1..Generations /\ binding \in 1..Generations
          /\ bindingDurable \in 1..Generations /\ visible \subseteq Data /\ persisted \subseteq Data
          /\ page \in [generation : 0..Generations, through : 0..Entries]
          /\ pageDurable \in [generation : 0..Generations, through : 0..Entries]
          /\ stage \in {"idle", "resetting", "loading", "flushed", "publishing"}
          /\ life \in {"live", "recovery"} /\ crashes \in 0..Crashes
Next == BeginRebuild \/ ForceReset \/ Write \/ Flush \/ Publish \/ ForcePublication \/ Crash \/ Recover
Spec == Init /\ [][Next]_vars
=============================================================================
