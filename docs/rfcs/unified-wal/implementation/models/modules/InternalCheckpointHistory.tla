------------------- MODULE InternalCheckpointHistory -------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS KeepAllCommits, DropRootIdentities, AllowLegacyGap, DropBusinessResult, RejectOriginalGap
VARIABLE s
vars == <<s>>
Roles == {"grant", "credential", "tombstone"}
Latest(history) == {g \in history : \A other \in history : other <= g + 1}
Retained(history, roots) == history \cap (roots \cup Latest(history))
(* Eight internal generations, fixed business state and one retained image.
   Rotation has already satisfied its independent dual-root/barrier protocol.
   Framing, hashes, exact commit binding and business predicates are inputs;
   this projection does not prove full Model A+C or public history compaction. *)
Init == \E mode \in {"original", "rotated"} :
        s = [stage |-> "run", recoveryMode |-> mode, generation |-> 0, history |-> {}, roots |-> {},
             hasImage |-> FALSE, image |-> {}, imageRoots |-> {}, imageThrough |-> 0,
             imageRoles |-> Roles, version |-> 2, corrupted |-> FALSE, ready |-> FALSE]
Checkpoint == /\ s.stage = "run" /\ s.generation < 8
              /\ LET roots == IF DropRootIdentities THEN {} ELSE s.roots
                     kept == IF KeepAllCommits THEN s.history ELSE Retained(s.history, roots)
                 IN s' = [s EXCEPT !.generation = @ + 1, !.history = @ \cup {s.generation + 1},
                           !.hasImage = TRUE, !.image = kept, !.imageRoots = s.roots,
                           !.imageThrough = s.generation, !.version = 2, !.corrupted = FALSE,
                           !.imageRoles = IF DropBusinessResult THEN Roles \ {"tombstone"} ELSE Roles]
Rotate == /\ s.stage = "run" /\ s.generation >= 2
          /\ LET roots == {s.generation - 1, s.generation}
             IN s' = [s EXCEPT !.roots = roots, !.history = Retained(s.history, roots)]
MarkLegacy == /\ s.stage = "run" /\ s.hasImage /\ s.version = 2
              /\ s' = [s EXCEPT !.version = 1]
CorruptPredecessor == /\ s.stage = "run" /\ s.hasImage /\ s.imageThrough > 0 /\ ~s.corrupted
                      /\ s' = [s EXCEPT !.image = @ \ {s.imageThrough}, !.corrupted = TRUE]
Restore == /\ s.stage = "run" /\ s.hasImage
           /\ LET predecessor == s.imageThrough = 0 \/ s.imageThrough \in s.image
                  legacy == s.image = 1..s.imageThrough
              IN s' = [s EXCEPT !.stage = "done",
                        !.ready = predecessor /\ (s.version = 2 \/ legacy \/ AllowLegacyGap)
                                  /\ ~(RejectOriginalGap /\ s.recoveryMode = "original" /\ ~legacy)]
Next == Checkpoint \/ Rotate \/ MarkLegacy \/ CorruptPredecessor \/ Restore
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"run", "done"} /\ s.generation \in 0..8
          /\ s.recoveryMode \in {"original", "rotated"}
          /\ s.history \subseteq 1..8 /\ s.roots \subseteq s.history
          /\ s.image \subseteq 1..s.imageThrough /\ s.version \in {1, 2}
ImageHasBoundedInternalHistory == s.hasImage => Cardinality(s.image) <= 4
SnapshotRetainsCurrentRootIdentities ==
    s.hasImage /\ ~s.corrupted => s.imageRoots \subseteq s.image
SnapshotKeepsBusinessResults == s.hasImage => s.imageRoles = Roles
OriginalRestoresValidCompactedImage ==
    s.stage = "done" /\ s.version = 2 /\ ~s.corrupted /\ s.recoveryMode = "original" => s.ready
LegacyReadyHasCompleteCheckpointHistory == s.ready /\ s.version = 1 => s.image = 1..s.imageThrough
ReadyHasCheckpointPredecessor == s.ready => s.imageThrough = 0 \/ s.imageThrough \in s.image
=============================================================================
