-------------------- MODULE CurrentCopyFacts --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS DropSuperseded, IndexRejected, KeepRetired, IgnoreRecordIdentity
VARIABLE s
vars == <<s>>
Copies == 1..3
Allocations == 1..2
Coordinates == 1..2
Allocation(id) == IF id = 1 THEN 1 ELSE 2
Coordinate(id) == IF id = 3 THEN 2 ELSE 1
CurrentFacts == {id \in s.successful : s.generation[Allocation(id)] = 1}
(* Durable verified COPY bytes, MOVE predecessor/Bookie predicates, and exact
   physical location are inputs. Each COPY has a distinct location and UUID;
   allocation generation, UUID and payload digest remain part of its proof. *)
Init == s = [successful |-> {}, indexed |-> {}, tried |-> {}, terminal |-> {}, reused |-> {},
             generation |-> [allocation \in Allocations |-> 1],
             selected |-> [key \in Coordinates |-> 0], queriesCorrect |-> TRUE]
ApplyMove == \E id \in Copies \ s.tried, feasible \in BOOLEAN :
    /\ s.generation[Allocation(id)] = 1
    /\ LET previous == IF DropSuperseded /\ feasible
                       THEN {old \in s.indexed : Coordinate(old) # Coordinate(id)} ELSE s.indexed
       IN s' = [s EXCEPT !.tried = @ \cup {id},
                !.successful = IF feasible THEN @ \cup {id} ELSE @,
                !.indexed = IF feasible \/ IndexRejected THEN previous \cup {id} ELSE previous,
                !.selected[Coordinate(id)] = IF feasible THEN id ELSE @]
Tombstone == \E key \in Coordinates \ s.terminal : s' = [s EXCEPT !.terminal = @ \cup {key}]
Free == \E allocation \in Allocations :
    /\ s.generation[allocation] # 0
    /\ \A key \in Coordinates : s.selected[key] = 0 \/ Allocation(s.selected[key]) # allocation
                                \/ key \in s.terminal
    /\ s' = [s EXCEPT !.generation[allocation] = 0,
             !.indexed = IF KeepRetired THEN @ ELSE {id \in @ : Allocation(id) # allocation},
             !.selected = [key \in Coordinates |->
                 IF s.selected[key] # 0 /\ Allocation(s.selected[key]) = allocation THEN 0 ELSE s.selected[key]]]
Reuse == \E allocation \in Allocations \ s.reused :
    /\ s.generation[allocation] = 0
    /\ s' = [s EXCEPT !.generation[allocation] = 2, !.reused = @ \cup {allocation}]
Query == \E id \in Copies, wrong \in 0..3 :
    (* wrong = 1/2/3 changes UUID/allocation generation/payload digest. *)
    LET actual == id \in s.indexed /\ (wrong = 0 \/ IgnoreRecordIdentity)
        expected == id \in CurrentFacts /\ wrong = 0
    IN s' = [s EXCEPT !.queriesCorrect = @ /\ (actual = expected)]
Next == ApplyMove \/ Tombstone \/ Free \/ Reuse \/ Query
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.successful \subseteq Copies /\ s.indexed \subseteq Copies /\ s.tried \subseteq Copies
          /\ s.terminal \subseteq Coordinates /\ s.reused \subseteq Allocations
          /\ s.generation \in [Allocations -> 0..2] /\ s.selected \in [Coordinates -> 0..3]
          /\ s.queriesCorrect \in BOOLEAN
CurrentCopyProofIsExact == s.indexed = CurrentFacts
CurrentSelectorHasCopyProof == \A key \in Coordinates : s.selected[key] = 0 \/ s.selected[key] \in CurrentFacts
QueriesBindCurrentIdentityAndDigest == s.queriesCorrect
=====================================================================
