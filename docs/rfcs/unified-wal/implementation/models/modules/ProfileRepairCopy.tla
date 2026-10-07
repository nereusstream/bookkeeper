-------------------- MODULE ProfileRepairCopy --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipSources, SkipGrant, SkipFullCopy, CountUnknown, ReleaseOnCancel,
          RecopyPublished, ParallelCopy, CancelContinues, ActivateNormal
VARIABLE s
vars == <<s>>
(* Two entry/RPCs, one coordinator permit. Published history is an input on restart.
   Source digest/fence/identity and actual network/force remain Java facts.
   No AutoRecovery scheduling, last-fragment frontier, strong reset or CLOSED. *)
Init == s \in [phase : {"ready"}, historical : BOOLEAN, sources : BOOLEAN, grant : BOOLEAN,
               copied : {0}, durable : {0}, lease : {TRUE}, pending : {0}, canceled : {FALSE},
               afterCancel : {FALSE}, published : {FALSE}, normal : {FALSE}]
Start == /\ s.phase = "ready"
         /\ LET copying == (~s.historical \/ RecopyPublished)
                              /\ (s.sources \/ SkipSources) /\ (s.grant \/ SkipGrant)
            IN s' = [s EXCEPT !.phase = IF ~copying THEN "done" ELSE "read",
                     !.lease = copying, !.pending = IF copying THEN 1 ELSE 0]
ReadEnd == /\ s.phase = "read"
           /\ s' = [s EXCEPT !.phase = IF s.canceled /\ ~CancelContinues THEN "done" ELSE "write",
                    !.afterCancel = s.canceled /\ CancelContinues,
                    !.pending = IF s.canceled /\ ~CancelContinues THEN 0 ELSE
                                   IF ParallelCopy THEN 2 ELSE 1,
                    !.lease = ~s.canceled \/ CancelContinues]
WriteEnd == /\ s.phase = "write"
            /\ \E success \in BOOLEAN :
                 s' = [s EXCEPT !.pending = 0,
                      !.copied = IF success \/ CountUnknown THEN @ + 1 ELSE @,
                      !.durable = IF success THEN @ + 1 ELSE @,
                      !.phase = IF success \/ CountUnknown THEN "next" ELSE "done",
                      !.lease = success \/ CountUnknown]
NextEntry == /\ s.phase = "next"
             /\ s' = [s EXCEPT !.phase = IF s.canceled /\ ~CancelContinues THEN "done"
                                         ELSE IF s.copied = 2 THEN "publish" ELSE "read",
                      !.afterCancel = s.canceled /\ CancelContinues,
                      !.pending = IF s.copied < 2 /\ (~s.canceled \/ CancelContinues) THEN 1 ELSE 0,
                      !.lease = ~s.canceled \/ CancelContinues]
Publish == /\ (s.phase = "publish" \/ (SkipFullCopy /\ s.phase = "next"))
           /\ s' = [s EXCEPT !.phase = "done", !.published = TRUE, !.normal = ActivateNormal, !.lease = FALSE]
Cancel == /\ ~s.canceled
          /\ s' = [s EXCEPT !.canceled = TRUE, !.lease = IF ReleaseOnCancel THEN FALSE ELSE @]
Next == Start \/ ReadEnd \/ WriteEnd \/ NextEntry \/ Publish \/ Cancel
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"ready", "read", "write", "next", "publish", "done"}
CopyHasSources == s.phase \in {"read", "write", "next", "publish"} => s.sources
CopyHasGrant == s.phase \in {"read", "write", "next", "publish"} => s.grant
PublicationHasAllCopies == s.published => s.copied = 2
OnlyDurableCounts == s.copied <= s.durable
InFlightHasPermit == s.pending > 0 => s.lease
PublishedResumeNeverCopies == s.historical => s.copied = 0
OnlyOneEntryInFlight == s.pending <= 1
CancelStopsFutureEntries == ~s.afterCancel
NormalNeverActivates == ~s.normal
=============================================================================
