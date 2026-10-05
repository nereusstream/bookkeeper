-------------------- MODULE AuthorityRead --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreKey, AllowInvalid, ErrorAsAbsent, MixVersion
VARIABLE s
vars == <<s>>
(* One cold sync/get. The atomic get callback carries bytes and Stat together.
   kind: 1 valid, 2 foreign key, 3 corrupt, 4 incomplete, 5 ephemeral,
         6 wrong state, 7 NONODE, 8 backend error.
   result: 0 pending, 1 found, 2 absent, 3 unavailable, 4 quarantined.
   Real codec/CRC/ACL/deadline and ZooKeeper atomicity are inputs; no lease,
   publication/CAS, installation, global sidecar, fanout or service model. *)
Init == \E kind \in 1..8 :
    s = [kind |-> kind, nodeKey |-> IF kind = 2 THEN 2 ELSE 1,
         generation |-> 1, version |-> 1, updated |-> FALSE, phase |-> 0,
         capturedKind |-> 0, capturedKey |-> 0, capturedGeneration |-> 0,
         capturedVersion |-> 0, returnedGeneration |-> 0, returnedVersion |-> 0,
         ioFailed |-> FALSE, result |-> 0]
SyncSuccess == /\ s.phase = 0
               /\ s' = [s EXCEPT !.phase = 1]
SyncFailure == /\ s.phase = 0
               /\ s' = [s EXCEPT !.phase = 3, !.ioFailed = TRUE,
                        !.result = IF ErrorAsAbsent THEN 2 ELSE 3]
Capture == /\ s.phase = 1
           /\ s' = [s EXCEPT !.phase = 2, !.capturedKind = s.kind,
                    !.capturedKey = s.nodeKey, !.capturedGeneration = s.generation,
                    !.capturedVersion = s.version]
ConcurrentUpdate == /\ ~s.updated
                    /\ s' = [s EXCEPT !.generation = 2, !.version = 2, !.updated = TRUE]
Reply == /\ s.phase = 2
         /\ LET valid == s.capturedKind \in {1, 2} \/ AllowInvalid
                keyMatches == s.capturedKey = 1 \/ IgnoreKey
                found == s.capturedKind \in 1..6 /\ valid /\ keyMatches
            IN s' = [s EXCEPT !.phase = 3,
                     !.result = IF s.capturedKind = 7 THEN 2
                                ELSE IF s.capturedKind = 8 THEN IF ErrorAsAbsent THEN 2 ELSE 3
                                ELSE IF found THEN 1 ELSE 4,
                     !.returnedGeneration = IF found THEN s.capturedGeneration ELSE 0,
                     !.returnedVersion = IF found THEN
                         IF MixVersion THEN s.version ELSE s.capturedVersion ELSE 0]
Timeout == /\ s.phase \in {1, 2}
           /\ s' = [s EXCEPT !.phase = 3, !.ioFailed = TRUE,
                    !.result = IF ErrorAsAbsent THEN 2 ELSE 3]
Next == SyncSuccess \/ SyncFailure \/ Capture \/ ConcurrentUpdate \/ Reply \/ Timeout
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.kind \in 1..8 /\ s.nodeKey \in 1..2 /\ s.generation \in 1..2 /\ s.version \in 1..2
          /\ s.updated \in BOOLEAN /\ s.phase \in 0..3 /\ s.ioFailed \in BOOLEAN /\ s.result \in 0..4
          /\ s.capturedKind \in 0..8 /\ s.capturedKey \in 0..2
          /\ s.capturedGeneration \in 0..2 /\ s.capturedVersion \in 0..2
          /\ s.returnedGeneration \in 0..2 /\ s.returnedVersion \in 0..2
FoundMatchesRequestedKey == s.result = 1 => s.capturedKey = 1
FoundHasValidPersistentCompleteRecord == s.result = 1 => s.capturedKind \in {1, 2}
AbsentOnlyFromActualNoNode == s.result = 2 => s.capturedKind = 7 /\ ~s.ioFailed
FoundKeepsOneAtomicSnapshot == s.result = 1 =>
    s.returnedGeneration = s.capturedGeneration /\ s.returnedVersion = s.capturedVersion
=====================================================================
