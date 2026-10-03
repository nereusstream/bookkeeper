---------------------------- MODULE AllocationOrder ----------------------------
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
CONSTANTS Pools, Positions, UseFileOrder, ReturnOldTail
ASSUME /\ Pools = 3 /\ Positions = 2
       /\ UseFileOrder \in BOOLEAN /\ ReturnOldTail \in BOOLEAN
VARIABLES grantedThrough, cursor, used, abandoned, restartCut, restarted, lastKind
vars == <<grantedThrough, cursor, used, abandoned, restartCut, restarted, lastKind>>
(* Grants are already durable/applied and retain their original control order.
   Allocation 2 is below allocation 1 in the shared file; FREE is not modeled. *)
FileOrder(p) == CASE p = 1 -> 2 [] p = 2 -> 1 [] OTHER -> 3
CanReserve(p) ==
    /\ p \in 1..grantedThrough /\ used[p] < Positions /\ p > restartCut
    /\ (cursor = 0 \/ ReturnOldTail \/
        IF UseFileOrder THEN FileOrder(p) >= FileOrder(cursor) ELSE p >= cursor)
Init == /\ grantedThrough = 0 /\ cursor = 0
        /\ used = [p \in 1..Pools |-> 0] /\ abandoned = {}
        /\ restartCut = 0 /\ restarted = FALSE /\ lastKind = "initial"
Grant == /\ grantedThrough < Pools /\ grantedThrough' = grantedThrough + 1
         /\ UNCHANGED <<cursor, used, abandoned, restartCut, restarted, lastKind>>
Consume(kind) ==
    \E p \in 1..Pools :
       /\ CanReserve(p) /\ cursor' = p
       /\ used' = [used EXCEPT ![p] = @ + 1]
       /\ abandoned' = abandoned \cup (1..(p-1))
       /\ lastKind' = kind
       /\ UNCHANGED <<grantedThrough, restartCut, restarted>>
Reserve == Consume("reserved")
Cancel == Consume("cancelled")
Restart == /\ ~restarted /\ restarted' = TRUE
           /\ restartCut' = grantedThrough /\ cursor' = 0
           /\ abandoned' = abandoned \cup (1..grantedThrough)
           /\ lastKind' = "restarted"
           /\ UNCHANGED <<grantedThrough, used>>
TypeOK == /\ grantedThrough \in 0..Pools /\ cursor \in 0..Pools
          /\ used \in [1..Pools -> 0..Positions] /\ abandoned \subseteq 1..Pools
          /\ restartCut \in 0..Pools /\ restarted \in BOOLEAN
          /\ lastKind \in {"initial", "reserved", "cancelled", "restarted"}
NewerAllocationIsNotBlockedByFileOffset ==
    (cursor = 1 /\ grantedThrough >= 2 /\ restartCut < 2 /\ used[2] < Positions) => CanReserve(2)
OldAllocationsCannotBeReentered == \A p \in abandoned : ~CanReserve(p)
RestartDoesNotRestoreOldTail == \A p \in 1..restartCut : ~CanReserve(p)
Next == Grant \/ Reserve \/ Cancel \/ Restart
Spec == Init /\ [][Next]_vars
=============================================================================
