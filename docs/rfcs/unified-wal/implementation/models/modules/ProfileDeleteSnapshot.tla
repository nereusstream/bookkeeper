-------------------- MODULE ProfileDeleteSnapshot --------------------
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
EXTENDS Integers, FiniteSets
CONSTANTS RootOnly, IgnoreScope, IgnoreBirth, SkipEffect, IgnoreSuffix, IgnoreStop, EarlyRelease
VARIABLE s
vars == <<s>>
Chunks == {1, 2}
Init == s \in [scope : BOOLEAN, birth : {TRUE}, chunks : {{}}, prepared : {FALSE},
               published : {FALSE}, stopped : {FALSE}, submittedAfterStop : {FALSE},
               held : {FALSE}, returned : {FALSE}, releasedEarly : {FALSE},
               cursor : {0}, tombstones : {{}}, booted : {FALSE},
               bootComplete : {FALSE}, bootScoped : {FALSE}]
(* CRC/ZK publication witness, complete chunks, exact target/incarnation and
   actual local TOMBSTONE force/apply are external Java inputs, not produced here.
   Prefix contains effects 1,2; actual journal suffix contains effect 3. No GC API. *)
Prepare == /\ ~s.prepared /\ ~s.stopped
           /\ s' = [s EXCEPT !.prepared = TRUE, !.held = TRUE]
Chunk(c) == /\ s.prepared /\ ~s.stopped /\ c \notin s.chunks
            /\ s' = [s EXCEPT !.chunks = @ \cup {c}]
Publish == /\ s.prepared /\ ~s.published /\ (s.chunks = Chunks \/ RootOnly)
           /\ (s.scope \/ IgnoreScope) /\ (~s.stopped \/ IgnoreStop)
           /\ s' = [s EXCEPT !.published = TRUE, !.submittedAfterStop = s.stopped]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
Return == /\ s.held /\ ~s.returned /\ s' = [s EXCEPT !.returned = TRUE]
Release == /\ s.held /\ (s.returned \/ EarlyRelease)
           /\ s' = [s EXCEPT !.held = FALSE, !.releasedEarly = @ \/ ~s.returned]
Recreate == /\ s.birth /\ s' = [s EXCEPT !.birth = FALSE]
ApplyPrefix == /\ s.published /\ s.cursor < 2 /\ s.chunks = Chunks
               /\ (s.scope \/ IgnoreScope) /\ (s.birth \/ IgnoreBirth)
               /\ s' = [s EXCEPT !.cursor = @ + 1,
                         !.tombstones = IF SkipEffect THEN @ ELSE @ \cup {s.cursor + 1}]
ApplySuffix == /\ s.cursor = 2
               /\ s' = [s EXCEPT !.cursor = 3,
                         !.tombstones = IF SkipEffect THEN @ ELSE @ \cup {3}]
Bootstrap == /\ ~s.booted /\ s.published /\ s.chunks = Chunks
             /\ (s.cursor = 3 \/ (IgnoreSuffix /\ s.cursor = 2))
             /\ (s.scope \/ IgnoreScope) /\ (s.birth \/ IgnoreBirth)
             /\ s' = [s EXCEPT !.booted = TRUE, !.bootComplete = s.cursor = 3,
                       !.bootScoped = s.scope /\ s.birth]
Next == Prepare \/ (\E c \in Chunks : Chunk(c)) \/ Publish \/ Stop \/ Return \/ Release
        \/ Recreate \/ ApplyPrefix \/ ApplySuffix \/ Bootstrap
Spec == Init /\ [][Next]_vars
TypeOK == s.cursor \in 0..3
SnapshotPublicationHasEveryChunk == s.published => s.chunks = Chunks
SnapshotPublicationHasScope == s.published => s.scope
SnapshotCursorHasEveryEffect == 1..s.cursor \subseteq s.tombstones
SnapshotBootstrapHasScopeAndBirth == s.booted => s.bootScoped
SnapshotBootstrapHasCompleteSuffix == s.booted => s.bootComplete
SnapshotStopStartsNoPublication == ~s.submittedAfterStop
SnapshotReleaseWaitsProcedure == ~s.releasedEarly
=============================================================================
