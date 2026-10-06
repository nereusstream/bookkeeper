-------------------- MODULE ProfileMembershipCas --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipMarkerCheck, ForgetCapturedBirth, IgnoreFrozen, DropCreatorToken,
          DropUserEntry, PromoteUnchanged, PromoteLaterVersion
VARIABLE s
vars == <<s>>
(* One standard-metadata replacement and at most one retry, two physical
   parent births, one guard child, one additional version. The immutable
   snapshot includes physical birth; copied metadata bytes are adversarially
   allowed. No activation, writer exposure, target history, actual ZK quorum,
   ACL security, schema bytes, clocks, fairness or full A+C+D is proved. *)
Init == \E frozen, lost \in BOOLEAN :
    s = [phase |-> "raw", birth |-> 1, capturedBirth |-> 1, version |-> 0,
         creator |-> 1, userEntry |-> 7, mapping |-> "old", frozen |-> frozen,
         knownFrozen |-> frozen, lost |-> lost, guard |-> FALSE,
         pending |-> FALSE, committed |-> FALSE, result |-> "pending", retries |-> 0,
         foreignBirthWritten |-> FALSE, fieldsKept |-> TRUE,
         forbiddenWrite |-> FALSE, successVersion |-> 0, successMapping |-> "old"]
Allowed == ~s.frozen \/ IgnoreFrozen
SameBirth == s.birth = s.capturedBirth
Predecessor == s.version = 0 /\ s.mapping = "old" /\ s.creator = 1 /\ s.userEntry = 7
Successor == s.mapping = "new" /\ s.creator = 1 /\ s.userEntry = 7
             /\ (s.version = 1 \/ (PromoteLaterVersion /\ s.version = 2))
GuardCreate == /\ s.phase = "raw" /\ SameBirth /\ Predecessor /\ Allowed
               /\ s' = [s EXCEPT !.guard = TRUE, !.phase = "guarded"]
ReadAlready == /\ s.phase = "raw" /\ SameBirth /\ Successor /\ Allowed
               /\ s' = [s EXCEPT !.phase = "done", !.result = "already",
                        !.successVersion = s.version, !.successMapping = s.mapping]
RejectRead == /\ s.phase \in {"raw", "guarded"}
              /\ ~(SameBirth /\ Allowed /\ (Predecessor \/ (s.phase = "raw" /\ Successor)))
              /\ s' = [s EXCEPT !.phase = "done", !.result = "conflict"]
CheckAfterGuard == /\ s.phase = "guarded" /\ s.guard /\ SameBirth /\ Predecessor /\ Allowed
                   /\ s' = [s EXCEPT !.phase = "checked"]
Submit == /\ s.phase = "checked"
          /\ s' = [s EXCEPT !.phase = "submitted", !.pending = TRUE]
Commit == /\ s.pending
          /\ LET writes == s.version = 0 /\ (s.guard \/ SkipMarkerCheck)
             IN s' = [s EXCEPT !.pending = FALSE, !.committed = TRUE,
                 !.phase = IF s.phase = "done" THEN "done" ELSE "observing",
                 !.version = IF writes THEN 1 ELSE @,
                 !.mapping = IF writes THEN "new" ELSE @,
                 !.creator = IF writes /\ DropCreatorToken THEN 0 ELSE @,
                 !.userEntry = IF writes /\ DropUserEntry THEN 0 ELSE @,
                 !.foreignBirthWritten = @ \/ (writes /\ ~SameBirth),
                 !.fieldsKept = @ /\ ~(writes /\ (DropCreatorToken \/ DropUserEntry)),
                 !.forbiddenWrite = @ \/ (writes /\ s.frozen)]
Observe == /\ s.phase = "observing"
           /\ LET success == SameBirth /\ Allowed /\ Successor
              IN s' = [s EXCEPT !.phase = "done",
                    !.result = IF success THEN IF s.lost THEN "already" ELSE "applied"
                               ELSE IF SameBirth /\ Predecessor THEN "unknown" ELSE "conflict",
                    !.successVersion = IF success THEN s.version ELSE @,
                    !.successMapping = IF success THEN s.mapping ELSE @]
Timeout == /\ s.phase = "submitted"
           /\ s' = [s EXCEPT !.phase = "done",
                 !.result = IF PromoteUnchanged THEN "applied" ELSE "unknown",
                 !.successVersion = IF PromoteUnchanged THEN s.version ELSE @,
                 !.successMapping = IF PromoteUnchanged THEN s.mapping ELSE @]
Recreate == /\ s.birth = 1 /\ ~(s.phase = "done" /\ s.result \in {"applied", "already"})
            /\ s' = [s EXCEPT !.birth = 2, !.version = 0, !.guard = FALSE,
                     !.creator = 1, !.userEntry = 7, !.mapping = "old", !.frozen = FALSE]
Freeze == /\ ~s.frozen /\ s.version = 0 /\ s.phase \in {"guarded", "checked", "submitted"}
          /\ s' = [s EXCEPT !.frozen = TRUE, !.version = 1]
LaterSameMapping == /\ s.phase = "observing" /\ s.mapping = "new" /\ s.version = 1
                    /\ s' = [s EXCEPT !.version = 2]
RetryUnknown == /\ s.phase = "done" /\ s.result = "unknown" /\ ~s.pending /\ s.retries = 0
                /\ s' = [s EXCEPT !.phase = "raw", !.result = "pending", !.retries = 1,
                         !.capturedBirth = IF ForgetCapturedBirth THEN s.birth ELSE @]
Next == GuardCreate \/ ReadAlready \/ RejectRead \/ CheckAfterGuard \/ Submit \/ Commit \/ Observe
        \/ Timeout \/ Recreate \/ Freeze \/ LaterSameMapping \/ RetryUnknown
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"raw", "guarded", "checked", "submitted", "observing", "done"}
          /\ s.birth \in 1..2 /\ s.capturedBirth \in 1..2 /\ s.version \in 0..2
          /\ s.creator \in 0..1 /\ s.userEntry \in {0, 7} /\ s.mapping \in {"old", "new"}
          /\ s.retries \in 0..1 /\ s.successVersion \in 0..2 /\ s.successMapping \in {"old", "new"}
          /\ s.result \in {"pending", "applied", "already", "unknown", "conflict"}
          /\ \A f \in {"frozen", "knownFrozen", "lost", "guard", "pending", "committed",
                       "foreignBirthWritten", "fieldsKept", "forbiddenWrite"} : s[f] \in BOOLEAN
NewPhysicalBirthIsNeverOverwritten == ~s.foreignBirthWritten
CapturedBirthIsFrozen == s.capturedBirth = 1
FrozenMembershipIsNeverWritten == ~s.forbiddenWrite
MetadataFieldsArePreserved == s.fieldsKept
SuccessHasExactMappingAndVersion == s.result \in {"applied", "already"}
                                  => s.successMapping = "new" /\ s.successVersion = 1
==============================================================================
