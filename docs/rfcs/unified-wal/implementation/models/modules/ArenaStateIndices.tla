-------------------- MODULE ArenaStateIndices --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Integers, FiniteSets
CONSTANTS ForgetIssuedOnFree, RejectHoles, RejectedAdvancesHighWatermark, LoseGrantOrder
VARIABLE s
vars == <<s>>
Ids == 1..4
Covered(ranges) == UNION {r.first..r.through : r \in ranges}
Extend(ranges, id) ==
    LET left == {r \in ranges : r.through + 1 = id}
        right == {r \in ranges : r.first = id + 1}
        first == IF left = {} THEN id ELSE (CHOOSE r \in left : TRUE).first
        through == IF right = {} THEN id ELSE (CHOOSE r \in right : TRUE).through
    IN (ranges \ (left \cup right)) \cup {[first |-> first, through |-> through]}
Maximum(ids) == IF ids = {} THEN 0 ELSE CHOOSE id \in ids : \A other \in ids : other <= id
(* Slot/stream predicates and durable ordered apply are inputs. The immutable
   issued set and first-success orders are independent reference facts. *)
Init == s = [issued |-> {}, ranges |-> {}, high |-> 0, next |-> 1, correct |-> TRUE,
             current |-> [id \in Ids |-> -1], first |-> [id \in Ids |-> -1]]
Allocate == /\ s.next <= 6
            /\ \E id \in Ids, feasible \in BOOLEAN :
                LET expected == feasible /\ id \notin s.issued
                    blocked == IF RejectHoles THEN id <= s.high ELSE id \in Covered(s.ranges)
                    applied == feasible /\ ~blocked
                    high == IF applied \/ RejectedAdvancesHighWatermark
                            THEN IF id > s.high THEN id ELSE s.high ELSE s.high
                IN s' = [s EXCEPT !.next = @ + 1, !.correct = @ /\ (applied = expected), !.high = high,
                         !.ranges = IF applied THEN Extend(@, id) ELSE @,
                         !.issued = IF applied THEN @ \cup {id} ELSE @,
                         !.current[id] = IF applied THEN IF LoseGrantOrder THEN 0 ELSE s.next ELSE @,
                         !.first[id] = IF applied THEN s.next ELSE @]
Free == /\ s.next <= 6
        /\ \E id \in Ids :
            /\ s.current[id] # -1
            /\ s' = [s EXCEPT !.next = @ + 1, !.current[id] = -1,
                     !.ranges = IF ForgetIssuedOnFree
                                THEN {r \in @ : ~(r.first <= id /\ id <= r.through)} ELSE @]
Next == Allocate \/ Free
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.issued \subseteq Ids /\ s.next \in 1..7 /\ s.high \in 0..4
          /\ s.current \in [Ids -> (-1)..6] /\ s.first \in [Ids -> (-1)..6]
          /\ s.correct \in BOOLEAN
          /\ \A r \in s.ranges : r.first \in Ids /\ r.through \in Ids /\ r.first <= r.through
IssuedRangesAreExact == Covered(s.ranges) = s.issued
IssuedRangesAreDisjoint == \A a, b \in s.ranges : a = b \/ a.through < b.first \/ b.through < a.first
FeasibleUnissuedIdentityIsAccepted == s.correct
HighWatermarkTracksOnlySuccessfulIdentity == s.high = Maximum(s.issued)
CurrentGrantKeepsOriginalOrder ==
    \A id \in Ids : s.current[id] = -1 \/ s.current[id] = s.first[id]
=====================================================================
