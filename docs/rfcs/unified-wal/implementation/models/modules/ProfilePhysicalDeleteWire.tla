-------------------- MODULE ProfilePhysicalDeleteWire --------------------
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
CONSTANTS IgnoreEvent, IgnorePeer, TrustCursor, IgnoreFresh, QuorumOnly, EarlyRelease
VARIABLE s
vars == <<s>>
(* TLS, committed ZK event, exact persisted local event and physical FREE are
   supplied native Java facts. This model produces none of them or GC authority. *)
Targets == {1, 2, 3}
Init == s \in [event : BOOLEAN, peer : BOOLEAN, applied : BOOLEAN,
               fresh : BOOLEAN, online : SUBSET Targets, reusable : SUBSET Targets,
               phase : {"new"}, started : {FALSE}, held : {FALSE},
               wireDone : {FALSE}, outputDone : {FALSE}, reported : {FALSE},
               aggregate : {FALSE}, stopped : {FALSE}]
Start == /\ s.phase = "new" /\ ~s.stopped
         /\ (s.event \/ IgnoreEvent) /\ (s.peer \/ IgnorePeer)
         /\ (s.applied \/ TrustCursor)
         /\ s' = [s EXCEPT !.phase = "running", !.started = TRUE, !.held = TRUE]
Report == /\ s.phase = "running" /\ ~s.stopped /\ ~s.reported
          /\ 1 \in s.reusable /\ (s.fresh \/ IgnoreFresh)
          /\ s' = [s EXCEPT !.reported = TRUE]
Wire == /\ s.started /\ ~s.wireDone /\ s' = [s EXCEPT !.wireDone = TRUE]
Output == /\ s.started /\ ~s.outputDone /\ s' = [s EXCEPT !.outputDone = TRUE]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
Release == /\ s.held /\ ((s.wireDone /\ s.outputDone) \/ EarlyRelease)
           /\ s' = [s EXCEPT !.held = FALSE, !.phase = "done"]
Aggregate == /\ ~s.aggregate /\ ~s.stopped /\ s.fresh
             /\ (IF QuorumOnly THEN Cardinality(s.reusable \cap s.online) >= 2
                 ELSE Targets \subseteq (s.reusable \cap s.online))
             /\ s' = [s EXCEPT !.aggregate = TRUE]
Next == Start \/ Report \/ Wire \/ Output \/ Stop \/ Release \/ Aggregate
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"new", "running", "done"}
PhysicalWireNeedsEvent == s.started => s.event
PhysicalWireNeedsPeer == s.started => s.peer
PhysicalWireNeedsAppliedEvent == s.started => s.applied
PhysicalWireReportNeedsFreshEvent == s.reported => s.fresh
PhysicalWireAggregateNeedsEveryTarget == s.aggregate => Targets \subseteq (s.reusable \cap s.online)
PhysicalWireReleaseNeedsActual == s.started /\ ~s.held => s.wireDone /\ s.outputDone
=============================================================================
