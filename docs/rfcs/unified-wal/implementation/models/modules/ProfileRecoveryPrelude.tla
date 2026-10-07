-------------------- MODULE ProfileRecoveryPrelude --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS AckQuorum, SkipMetadataBirth, SkipRootBirth, SkipPublication,
          DropCreator, ChangeOperation, CountUnknown, IgnorePeer, IgnoreScope, ClaimClose
VARIABLE s
vars == <<s>>
(* One captured OPEN predecessor, same-record context and E=W=3 A2/A3 fence
   fan-in. Physical births can change while versions/opaque UUIDs stay equal.
   Byte/CRC/digest, real ZK and actual force are produced by Java tests.
   No recovery Add, required frontier, CLOSED, clock or liveness proof. *)
Init == s \in [phase : {"captured"}, metaBirth : {1}, rootBirth : {1}, creator : {7},
               metaVersion : {0}, rootVersion : {1}, mg : {FALSE}, rg : {FALSE},
               published : BOOLEAN, mutable : BOOLEAN, token : {0}, written : {FALSE},
               current : {TRUE}, peer : [1..3 -> BOOLEAN],
               reply : [1..3 -> {"ok", "unknown", "none"}], fenced : {FALSE}, closed : {FALSE}]
Prepare == /\ s.phase = "captured"
           /\ s' = [s EXCEPT !.phase = IF s.mutable /\ (s.published \/ SkipPublication)
                                          THEN "prepared" ELSE "refused",
                    !.mg = TRUE, !.rg = TRUE]
RestoreMetadata == /\ s.phase = "prepared" /\ s.metaBirth = 1
                   /\ s' = [s EXCEPT !.metaBirth = 2, !.mg = FALSE]
RestoreRoot == /\ s.phase = "prepared" /\ s.rootBirth = 1
               /\ s' = [s EXCEPT !.rootBirth = 2, !.rg = FALSE]
Freeze == /\ s.phase = "prepared" /\ s.metaVersion = 0
          /\ s' = [s EXCEPT !.metaVersion = 1, !.mutable = FALSE]
Delete == /\ s.phase = "prepared" /\ s.rootVersion = 1
          /\ s' = [s EXCEPT !.rootVersion = 2, !.published = FALSE]
Cas == /\ s.phase = "prepared"
       /\ LET apply == s.metaVersion = 0 /\ s.rootVersion = 1
                        /\ (s.mg \/ SkipMetadataBirth) /\ (s.rg \/ SkipRootBirth)
          IN s' = [s EXCEPT !.phase = IF apply THEN "committed" ELSE "refused",
                   !.written = apply, !.metaVersion = IF apply THEN 1 ELSE @,
                   !.creator = IF apply /\ DropCreator THEN 0 ELSE @,
                   !.token = IF apply THEN IF ChangeOperation THEN 2 ELSE 1 ELSE @]
ScopeChanges == /\ s.phase = "committed" /\ s.current
                /\ s' = [s EXCEPT !.current = FALSE]
Votes == {i \in 1..3 : (s.peer[i] \/ IgnorePeer)
                          /\ (s.reply[i] = "ok" \/ (CountUnknown /\ s.reply[i] = "unknown"))}
RealVotes == {i \in 1..3 : s.peer[i] /\ s.reply[i] = "ok"}
FenceEnd == /\ s.phase = "committed"
            /\ LET enough == Cardinality(Votes) >= 4 - AckQuorum /\ (s.current \/ IgnoreScope)
               IN s' = [s EXCEPT !.phase = "done", !.fenced = enough,
                        !.closed = enough /\ ClaimClose]
Next == Prepare \/ RestoreMetadata \/ RestoreRoot \/ Freeze \/ Delete \/ Cas \/ ScopeChanges \/ FenceEnd
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"captured", "prepared", "refused", "committed", "done"}
WrittenContextHasObservedBirth == s.written => (s.metaBirth = 1 /\ s.rootBirth = 1)
WrittenContextHasPublishedAuthority == s.written => s.published
CreatorFieldsArePreserved == s.written => s.creator = 7
OperationIsFrozen == s.written => s.token = 1
FenceCoverageHasDurableVerifiedSources == s.fenced => Cardinality(RealVotes) >= 4 - AckQuorum
FenceScopeIsCurrent == s.fenced => s.current
PreludeNeverPublishesClosed == ~s.closed
=============================================================================
