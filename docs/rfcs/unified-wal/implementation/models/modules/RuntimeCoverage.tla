-------------------------- MODULE RuntimeCoverage --------------------------
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
CONSTANTS Entries, Publications, Crashes, EarlyPublish, ResetClock, LateCut
ASSUME /\ Entries >= 2 /\ Publications >= 3 /\ Crashes >= 1
       /\ EarlyPublish \in BOOLEAN /\ ResetClock \in BOOLEAN /\ LateCut \in BOOLEAN
None == [publication |-> 0, through |-> 0, contents |-> 0]
VARIABLES data, visible, persisted, cut, stage, pages, durablePages, life, crashes, lastPublication, monotone
vars == <<data, visible, persisted, cut, stage, pages, durablePages, life, crashes, lastPublication, monotone>>
Init == /\ data = 0 /\ visible = 0 /\ persisted = 0 /\ cut = 0 /\ stage = "idle"
        /\ pages = <<None, None>> /\ durablePages = <<None, None>>
        /\ life = "live" /\ crashes = 0 /\ lastPublication = 0 /\ monotone = TRUE
Latest(images) == IF images[1].publication >= images[2].publication THEN images[1] ELSE images[2]
Usable == Latest(durablePages).publication > 0 /\ Latest(durablePages).contents = visible
(* DATA force and original-owner contiguous publication are assumed from the DATA model. *)
Add == /\ life = "live" /\ data < Entries /\ data' = data + 1
       /\ UNCHANGED <<visible, persisted, cut, stage, pages, durablePages, life, crashes, lastPublication, monotone>>
Put == /\ life = "live" /\ stage = "idle" /\ visible < data /\ visible' = visible + 1
       /\ UNCHANGED <<data, persisted, cut, stage, pages, durablePages, life, crashes, lastPublication, monotone>>
Prepare == /\ life = "live" /\ stage = "idle" /\ visible = data
           /\ Latest(pages).publication < Publications /\ cut' = visible /\ stage' = "prepared"
           /\ UNCHANGED <<data, visible, persisted, pages, durablePages, life, crashes, lastPublication, monotone>>
Flush == /\ life = "live" /\ stage = "prepared" /\ persisted' = visible /\ stage' = "flushed"
         /\ UNCHANGED <<data, visible, cut, pages, durablePages, life, crashes, lastPublication, monotone>>
Publish == /\ life = "live" /\ (stage = "flushed" \/ (EarlyPublish /\ stage = "prepared"))
           /\ LET next == IF ResetClock /\ Latest(pages).contents # visible THEN 1
                          ELSE Latest(pages).publication + 1
                  slot == IF next % 2 = 1 THEN 1 ELSE 2
              IN /\ pages' = [pages EXCEPT ![slot] = [publication |-> next,
                             through |-> IF LateCut THEN data ELSE cut, contents |-> visible]]
                 /\ monotone' = (monotone /\ next > lastPublication)
                 /\ lastPublication' = next
           /\ stage' = "publishing"
           /\ UNCHANGED <<data, visible, persisted, cut, durablePages, life, crashes>>
ForcePublication == /\ life = "live" /\ stage = "publishing" /\ durablePages' = pages /\ stage' = "published"
                    /\ UNCHANGED <<data, visible, persisted, cut, pages, life, crashes, lastPublication, monotone>>
Resume == /\ life = "live" /\ stage = "published" /\ stage' = "idle"
          /\ UNCHANGED <<data, visible, persisted, cut, pages, durablePages, life, crashes, lastPublication, monotone>>
Abort == /\ life = "live" /\ stage \in {"prepared", "flushed", "publishing"} /\ stage' = "idle"
         /\ UNCHANGED <<data, visible, persisted, cut, pages, durablePages, life, crashes, lastPublication, monotone>>
BackgroundFlush == /\ life = "live" /\ stage = "idle" /\ persisted < visible /\ persisted' = visible
                   /\ UNCHANGED <<data, visible, cut, stage, pages, durablePages, life, crashes, lastPublication, monotone>>
Crash == /\ life = "live" /\ crashes < Crashes /\ life' = "recovery" /\ crashes' = crashes + 1
         /\ visible' = persisted /\ pages' = durablePages /\ stage' = "idle"
         /\ lastPublication' = Latest(durablePages).publication
         /\ UNCHANGED <<data, persisted, cut, durablePages, monotone>>
Recover == /\ life = "recovery" /\ life' = "live"
           /\ UNCHANGED <<data, visible, persisted, cut, stage, pages, durablePages, crashes, lastPublication, monotone>>
TypeOK == /\ data \in 0..Entries /\ visible \in 0..data /\ persisted \in 0..visible /\ cut \in 0..data
          /\ pages \in [1..2 -> [publication : 0..Publications, through : 0..Entries, contents : 0..Entries]]
          /\ durablePages \in [1..2 -> [publication : 0..Publications, through : 0..Entries, contents : 0..Entries]]
          /\ stage \in {"idle", "prepared", "flushed", "publishing", "published"}
          /\ life \in {"live", "recovery"} /\ crashes \in 0..Crashes
          /\ lastPublication \in 0..Publications /\ monotone \in BOOLEAN
RuntimeCoverageWasFlushed == Usable => Latest(durablePages).contents <= persisted
RuntimeCoverageStopsAtCapturedData == Usable =>
    (Latest(durablePages).through <= Latest(durablePages).contents /\ Latest(durablePages).through <= data)
PublicationClockAdvances == monotone
Next == Add \/ Put \/ Prepare \/ Flush \/ Publish \/ ForcePublication \/ Resume \/ Abort \/ BackgroundFlush \/ Crash \/ Recover
Spec == Init /\ [][Next]_vars
=============================================================================
