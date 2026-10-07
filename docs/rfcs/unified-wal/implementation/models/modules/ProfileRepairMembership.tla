-------------------- MODULE ProfileRepairMembership --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipMetaBirth, SkipIntentBirth, IgnoreDelete, SkipAdmission, SplitPublish,
          WrongSlot, LoseContext, DropHistory, SkipProof, IgnoreFragmentEnd
VARIABLE s
vars == <<s>>
(* A caller-completed full copy is an input, not a proof produced by this cold primitive.
   Two finite historical-fragment CASs; exact hashes/bytes/force/TLS are Java inputs.
   No last-open-fragment frontier, strong reset, worker scheduling or CLOSED. *)
Init == s \in [phase : {"copy"}, n : {0}, metaBirth : {0}, intentBirth : {1},
               deleted : {FALSE}, checkedDelete : {FALSE}, admitted : BOOLEAN, bounded : BOOLEAN, context : {1},
               published : {FALSE}, membership : {FALSE}, head : {FALSE}, history : {0},
               checkedMeta : {0}, checkedIntent : {1}, checkedAdmission : {TRUE},
               checkedBound : {TRUE}, correctSlot : {TRUE}, proof : {TRUE}, adopted : {FALSE}]
RestoreMetadata == /\ s.phase = "copy" /\ s.metaBirth = 0
                   /\ s' = [s EXCEPT !.metaBirth = 2]
RestoreIntent == /\ s.phase = "copy" /\ s.intentBirth = 1
                 /\ s' = [s EXCEPT !.intentBirth = 2]
Delete == /\ s.phase \in {"copy", "published"} /\ ~s.deleted
          /\ s' = [s EXCEPT !.deleted = TRUE]
Cas == /\ s.phase = "copy" /\ s.n < 2
       /\ LET accepted == (s.metaBirth = 0 \/ SkipMetaBirth)
                            /\ (s.intentBirth = 1 \/ SkipIntentBirth)
                            /\ (~s.deleted \/ IgnoreDelete)
                            /\ (s.admitted \/ SkipAdmission) /\ (s.bounded \/ IgnoreFragmentEnd)
          IN s' = [s EXCEPT !.phase = IF accepted THEN "published" ELSE "rejected",
                   !.published = accepted, !.n = IF accepted THEN @ + 1 ELSE @,
                   !.membership = accepted, !.head = accepted /\ ~SplitPublish,
                   !.history = IF accepted /\ ~DropHistory THEN s.n + 1 ELSE @,
                   !.checkedDelete = s.deleted, !.checkedMeta = s.metaBirth, !.checkedIntent = s.intentBirth,
                   !.checkedAdmission = s.admitted, !.checkedBound = s.bounded,
                   !.correctSlot = ~WrongSlot, !.context = IF accepted /\ LoseContext THEN 2 ELSE @]
RemoveProof == /\ s.phase = "published" /\ s.proof
               /\ s' = [s EXCEPT !.proof = FALSE]
Adopt == /\ s.phase = "published"
         /\ s' = [s EXCEPT !.phase = "adopted", !.adopted = ~s.deleted /\ s.head /\ s.membership
                   /\ s.context = 1 /\ (s.proof \/ SkipProof)]
Continue == /\ s.phase = "adopted" /\ s.adopted /\ s.n = 1
            /\ s' = [s EXCEPT !.phase = "copy", !.published = FALSE, !.intentBirth = 1,
                     !.proof = TRUE, !.adopted = FALSE]
Next == RestoreMetadata \/ RestoreIntent \/ Delete \/ Cas \/ RemoveProof \/ Adopt \/ Continue
Spec == Init /\ [][Next]_vars
TypeOK == s.n \in 0..2 /\ s.phase \in {"copy", "published", "rejected", "adopted"}
PublicationHasMetaBirth == s.published => s.checkedMeta = 0
PublicationHasIntentBirth == s.published => s.checkedIntent = 1
DeletePreventsPublication == s.published => ~s.checkedDelete
PublicationHasAdmission == s.published => s.checkedAdmission
PublicationIsAtomic == s.membership = s.head
PublicationUsesPlannedSlot == s.published => s.correctSlot
ContextIsPreserved == s.context = 1
HistoryIsRetained == s.published => s.history = s.n
AdoptionRequiresProof == s.adopted => s.proof
PublicationHasBoundedFragment == s.published => s.checkedBound
=============================================================================
