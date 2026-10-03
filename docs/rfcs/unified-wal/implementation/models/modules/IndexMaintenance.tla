-------------------------- MODULE IndexMaintenance --------------------------
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
CONSTANTS Entries, HotEntries, OwnerSlots, UseControlForIndex, EarlyHotEviction
ASSUME /\ Entries >= 2 /\ HotEntries >= 1 /\ OwnerSlots >= 2
       /\ UseControlForIndex \in BOOLEAN /\ EarlyHotEviction \in BOOLEAN
Keys == 1..Entries
VARIABLES admitted, dataPending, hot, durable, successes, rows, job, stage, failed, stalls, fenced
vars == <<admitted, dataPending, hot, durable, successes, rows, job, stage, failed, stalls, fenced>>
Init == /\ admitted = {} /\ dataPending = {} /\ hot = {} /\ durable = {}
        /\ successes = {} /\ rows = {} /\ job = {} /\ stage = "idle"
        /\ failed = FALSE /\ stalls = 0 /\ fenced = FALSE
IndexBusy == stage # "idle"
DataTickets == Cardinality(dataPending) + IF IndexBusy /\ ~UseControlForIndex THEN 1 ELSE 0
Submit == /\ ~failed /\ Cardinality(dataPending \cup hot) < HotEntries /\ DataTickets < OwnerSlots
          /\ \E entry \in Keys \ admitted :
               /\ admitted' = admitted \cup {entry} /\ dataPending' = dataPending \cup {entry}
          /\ UNCHANGED <<hot, durable, successes, rows, job, stage, failed, stalls, fenced>>
(* Payload barrier and hot publication are verified by the separate DATA model/Java path. *)
ForceData == /\ stage \notin {"putting", "stalled"} /\ dataPending # {}
             /\ \E entry \in dataPending :
                  /\ dataPending' = dataPending \ {entry} /\ hot' = hot \cup {entry}
                  /\ durable' = durable \cup {entry} /\ successes' = successes \cup {entry}
             /\ UNCHANGED <<admitted, rows, job, stage, failed, stalls, fenced>>
StartIndex == /\ stage = "idle" /\ ~failed /\ hot \ rows # {} /\ DataTickets < OwnerSlots
              /\ job' = hot \ rows /\ stage' = "queued"
              /\ hot' = IF EarlyHotEviction THEN hot \ (hot \ rows) ELSE hot
              /\ UNCHANGED <<admitted, dataPending, durable, successes, rows, failed, stalls, fenced>>
StartPut == /\ stage = "queued" /\ stage' = "putting"
            /\ UNCHANGED <<admitted, dataPending, hot, durable, successes, rows, job, failed, stalls, fenced>>
Stall == /\ stage = "putting" /\ stalls = 0 /\ stage' = "stalled" /\ stalls' = 1
         /\ UNCHANGED <<admitted, dataPending, hot, durable, successes, rows, job, failed, fenced>>
Resume == /\ stage = "stalled" /\ stage' = "putting"
          /\ UNCHANGED <<admitted, dataPending, hot, durable, successes, rows, job, failed, stalls, fenced>>
Put == /\ stage = "putting" /\ rows' = rows \cup job /\ stage' = "visible"
       /\ UNCHANGED <<admitted, dataPending, hot, durable, successes, job, failed, stalls, fenced>>
CompleteIndex == /\ stage = "visible" /\ job' = {} /\ stage' = "idle"
                 /\ hot' = hot \ job
                 /\ UNCHANGED <<admitted, dataPending, durable, successes, rows, failed, stalls, fenced>>
IndexFailure == /\ stage \in {"queued", "putting"} /\ failed' = TRUE /\ job' = {} /\ stage' = "idle"
                /\ UNCHANGED <<admitted, dataPending, hot, durable, successes, rows, stalls, fenced>>
(* This control target has no unresolved pre-cut DATA; the reserved lane must remain available. *)
Fence == /\ ~fenced /\ (~IndexBusy \/ ~UseControlForIndex) /\ fenced' = TRUE
         /\ UNCHANGED <<admitted, dataPending, hot, durable, successes, rows, job, stage, failed, stalls>>
TypeOK == /\ admitted \subseteq Keys /\ dataPending \subseteq admitted
          /\ hot \subseteq admitted /\ durable \subseteq admitted /\ successes \subseteq admitted
          /\ rows \subseteq Keys /\ job \subseteq admitted
          /\ stage \in {"idle", "queued", "putting", "stalled", "visible"}
          /\ failed \in BOOLEAN /\ fenced \in BOOLEAN /\ stalls \in 0..1
BacklogBudget == Cardinality(dataPending \cup hot) <= HotEntries /\ DataTickets <= OwnerSlots
LocalSuccessKeepsLocatorAuthority == successes \subseteq (hot \cup rows) /\ successes \subseteq durable
OccupiedCoordinatesSurviveEviction == durable \subseteq admitted /\ Cardinality(admitted) <= Entries
IndexRowsCameFromDurableData == rows \subseteq durable
FenceCanUseReservedLane == ~fenced => ENABLED Fence
Next == Submit \/ ForceData \/ StartIndex \/ StartPut \/ Stall \/ Resume \/ Put \/ CompleteIndex \/ IndexFailure \/ Fence
Spec == Init /\ [][Next]_vars
=============================================================================
