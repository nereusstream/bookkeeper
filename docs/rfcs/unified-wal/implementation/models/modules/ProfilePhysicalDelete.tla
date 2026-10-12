-------------------- MODULE ProfilePhysicalDelete --------------------
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
CONSTANTS IgnoreScope, IgnoreUnknown, IndexOnly, SelectorsOnly, NoForce, LoseLive,
          SkipDrain, EarlyRelease
VARIABLE s
vars == <<s>>
(* Native TOMBSTONE, scratch parse, I/O/owner terminal, COPY/MOVE/FREE barriers
   are real Java inputs; no physical fact, TLS, ZK, CRC or durable force is produced here. *)
Init == s \in [scope : BOOLEAN, known : BOOLEAN, oldIO : BOOLEAN,
               origin : {"DATA", "OLD_COPY"}, phase : {"new"}, held : {FALSE},
               indexEmpty : {FALSE}, inspected : {FALSE}, seen : {FALSE},
               oldAllocated : {TRUE}, copied : {FALSE}, moved : {FALSE},
               freeForced : {FALSE}, freed : {FALSE}, liveSafe : {TRUE},
               nativeDone : {FALSE}, ownerDone : {FALSE}, reusable : {FALSE},
               stopped : {FALSE}]
Start == /\ s.phase = "new" /\ (s.scope \/ IgnoreScope)
         /\ s' = [s EXCEPT !.phase = "running", !.held = TRUE]
OldDone == /\ s.oldIO /\ s' = [s EXCEPT !.oldIO = FALSE]
Cleanup == /\ s.phase = "running" /\ ~s.indexEmpty /\ (~s.oldIO \/ SkipDrain)
           /\ s' = [s EXCEPT !.indexEmpty = TRUE]
Inspect == /\ s.indexEmpty /\ ~s.inspected /\ (s.known \/ IgnoreUnknown)
           /\ s' = [s EXCEPT !.inspected = TRUE,
                     !.seen = ~IndexOnly /\ (~SelectorsOnly \/ s.origin = "DATA")]
Copy == /\ s.inspected /\ s.seen /\ ~s.copied /\ ~s.stopped
        /\ s' = [s EXCEPT !.copied = TRUE, !.liveSafe = ~LoseLive]
Move == /\ s.copied /\ ~s.moved /\ s' = [s EXCEPT !.moved = TRUE]
ForceFree == /\ s.moved /\ ~s.freeForced /\ s' = [s EXCEPT !.freeForced = TRUE]
Free == /\ s.moved /\ (s.freeForced \/ NoForce) /\ ~s.freed
        /\ s' = [s EXCEPT !.freed = TRUE, !.oldAllocated = FALSE]
NativeDone == /\ s.inspected /\ (~s.seen \/ s.freed \/ (s.stopped /\ ~s.copied)) /\ ~s.nativeDone
              /\ s' = [s EXCEPT !.nativeDone = TRUE]
OwnerDone == /\ s.nativeDone /\ ~s.ownerDone /\ s' = [s EXCEPT !.ownerDone = TRUE]
Finish == /\ s.phase = "running" /\ s.ownerDone /\ ~s.stopped
          /\ s' = [s EXCEPT !.phase = "done", !.held = FALSE, !.reusable = TRUE]
Stop == /\ s.phase = "running" /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
Abandon == /\ s.stopped /\ s.held /\ EarlyRelease /\ ~s.ownerDone
           /\ s' = [s EXCEPT !.held = FALSE]
Retire == /\ s.phase = "running" /\ s.stopped /\ s.ownerDone
          /\ s' = [s EXCEPT !.phase = "done", !.held = FALSE]
Next == Start \/ OldDone \/ Cleanup \/ Inspect \/ Copy \/ Move \/ ForceFree \/ Free
        \/ NativeDone \/ OwnerDone \/ Finish \/ Stop \/ Abandon \/ Retire
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"new", "running", "done"}
PhysicalHasScope == s.phase # "new" => s.scope
PhysicalHasKnownBytes == s.inspected => s.known
PhysicalScanIncludesAllOrigins == s.inspected => s.seen
PhysicalFreeNeedsForce == s.freed => s.freeForced
PhysicalKeepsOtherLedger == s.liveSafe
PhysicalWaitsOldIo == s.indexEmpty => ~s.oldIO
PhysicalCreditWaitsActualOwner == ~s.held /\ s.phase # "new" => s.ownerDone
PhysicalReusableHasNoAllocatedCopy == s.reusable => ~s.oldAllocated
=============================================================================
