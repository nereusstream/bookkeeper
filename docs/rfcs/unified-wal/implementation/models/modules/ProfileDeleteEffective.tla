-------------------- MODULE ProfileDeleteEffective --------------------
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
CONSTANTS SkipCut, SkipAck, KeepOldRegistry, KeepOldGate, SkipForce, WrongEventGeneration,
          IgnoreHistory, ForgetCheckpoint
VARIABLE s
vars == <<s>>
(* Native physical witnesses, actual local force/owner returns and complete snapshot
   bytes are Java facts. This projection covers two effective shifts and old event identity. *)
Init == s = [meta |-> 1, local |-> 1, head |-> 1, cursor |-> 1, ready |-> 1, gate |-> TRUE,
             prepared |-> 0, cut |-> 0, adopted |-> 0, ack |-> 0, forced |-> 1,
             eventGeneration |-> 0, badCut |-> FALSE, badAck |-> FALSE,
             badGate |-> FALSE, badForce |-> FALSE, badHistory |-> FALSE,
             restarted |-> FALSE, lostEffective |-> FALSE]
Prepare == /\ s.prepared = 0 /\ s.meta < 3
           /\ s.head = IF s.meta = 1 THEN 1 ELSE 2
           /\ s' = [s EXCEPT !.prepared = s.meta + 1, !.cut = s.head, !.ack = 0]
Adopt == /\ s.prepared > 0 /\ s.adopted # s.prepared
         /\ (s.cursor = s.cut \/ SkipCut)
         /\ s' = [s EXCEPT !.adopted = s.prepared, !.badCut = (s.cursor # s.cut)]
Ack == /\ s.prepared > 0 /\ s.adopted = s.prepared /\ s.ack # s.prepared
       /\ s' = [s EXCEPT !.ack = s.prepared]
Retire == /\ s.prepared > 0 /\ s.gate
          /\ s' = [s EXCEPT !.gate = FALSE]
Switch == /\ s.prepared > 0 /\ (s.ack = s.prepared \/ SkipAck)
          /\ (~s.gate \/ KeepOldGate)
          /\ s' = [s EXCEPT !.meta = s.prepared, !.prepared = 0,
                    !.ready = IF KeepOldRegistry THEN s.ready ELSE 0,
                    !.badAck = (s.ack # s.prepared), !.badGate = s.gate]
ForceEffective == /\ s.local < s.meta /\ s.forced < s.meta
                  /\ s' = [s EXCEPT !.forced = s.meta]
ApplyEffective == /\ s.local < s.meta /\ (s.forced = s.meta \/ SkipForce)
                  /\ s' = [s EXCEPT !.local = s.meta, !.badForce = (s.forced # s.meta)]
Append == /\ s.meta = 2 /\ s.prepared = 0 /\ s.head = 1
          /\ s' = [s EXCEPT !.head = 2, !.eventGeneration = IF WrongEventGeneration THEN 1 ELSE 2]
ApplyEvent == /\ s.local = 2 /\ s.head = 2 /\ s.cursor = 1
              /\ s.eventGeneration = 2
              /\ s' = [s EXCEPT !.cursor = 2]
Register == /\ s.local = s.meta /\ s.cursor = s.head /\ s.forced = s.meta
            /\ s.ready # s.meta
            /\ s' = [s EXCEPT !.ready = s.meta, !.gate = TRUE]
ReadOld == /\ s.meta > 1 /\ ~s.badHistory /\ IgnoreHistory
           /\ s' = [s EXCEPT !.badHistory = TRUE]
Restart == /\ s.local = s.meta /\ s.local > 1 /\ ~s.restarted
           /\ s' = [s EXCEPT !.restarted = TRUE, !.gate = FALSE,
                     !.local = IF ForgetCheckpoint THEN 1 ELSE s.local,
                     !.lostEffective = ForgetCheckpoint]
Next == Prepare \/ Adopt \/ Ack \/ Retire \/ Switch \/ ForceEffective \/ ApplyEffective \/ Append
        \/ ApplyEvent \/ Register \/ ReadOld \/ Restart
Spec == Init /\ [][Next]_vars
TypeOK == s.meta \in 1..3 /\ s.local \in 1..3 /\ s.head \in 1..2 /\ s.cursor \in 1..2
EffectiveHasCompleteLocalCut == ~s.badCut
EffectiveHasOriginalAck == ~s.badAck
EffectiveRetiresOldRegistry == s.ready = 0 \/ s.ready = s.meta
EffectiveRetiresOldGate == ~s.badGate
EffectiveLocalProgressHasForce == ~s.badForce
EffectiveNewEventKeepsGeneration == s.head = 1 \/ s.eventGeneration = 2
EffectiveOldEventKeepsLineage == ~s.badHistory
EffectiveCheckpointKeepsProgress == ~s.lostEffective
=============================================================================
