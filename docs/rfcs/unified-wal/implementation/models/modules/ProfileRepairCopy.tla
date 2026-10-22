-------------------- MODULE ProfileRepairCopy --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipSources, SkipGrant, SkipFullCopy, CountUnknown, ReleaseOnCancel,
          RecopyPublished, ParallelCopy, CancelContinues, ActivateNormal
VARIABLES s, output
vars == <<s, output>>
(* Two entry/RPCs, one coordinator permit. Published history is an input on restart.
   Source digest/fence/identity and actual network/force remain Java facts.
   No AutoRecovery scheduling, last-fragment frontier, strong reset or CLOSED. *)
CoreInit == s \in [phase : {"ready"}, historical : BOOLEAN, sources : BOOLEAN, grant : BOOLEAN,
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
CoreNext == Start \/ ReadEnd \/ WriteEnd \/ NextEntry \/ Publish \/ Cancel
Init == CoreInit /\ output = 0
Next == CoreNext /\ UNCHANGED output
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

(* The original copy loop now separates the classified wire result and actual
   output/source terminal. Local DATA force, source identity/current CLOSED,
   native admission/delete CAS, digest and byte ownership remain Java inputs.
   This two-entry projection does not prove backup acceptance or full A+C+D. *)
OutputRequireReturn == TRUE
OutputReplyOnly == FALSE
OutputInit == CoreInit /\ output = [sent |-> FALSE, replied |-> FALSE,
                                  returned |-> FALSE, success |-> FALSE, terminals |-> 0]
OutputReadEnd ==
    /\ ReadEnd
    /\ output' = IF s'.phase = "write"
                 THEN [sent |-> TRUE, replied |-> FALSE, returned |-> FALSE,
                       success |-> FALSE, terminals |-> output.terminals]
                 ELSE output
OutputReply ==
    /\ s.phase = "write" /\ output.sent /\ ~output.replied
    /\ \E success \in BOOLEAN :
       output' = [output EXCEPT !.replied = TRUE, !.success = success,
                  !.terminals = IF output.returned /\ success THEN @ + 1 ELSE @]
    /\ UNCHANGED s
OutputReturn ==
    /\ output.sent /\ ~output.returned
    /\ output' = [output EXCEPT !.returned = TRUE,
                  !.terminals = IF output.replied /\ output.success THEN @ + 1 ELSE @]
    /\ UNCHANGED s
OutputWriteEnd ==
    /\ output.replied /\ (output.returned \/ ~OutputRequireReturn)
    /\ WriteEnd
    /\ s.durable' = s.durable + IF output.success THEN 1 ELSE 0
    /\ UNCHANGED output
OutputCoreStep == /\ (Start \/ NextEntry \/ Publish \/ Cancel)
                  /\ UNCHANGED output
OutputNext == OutputCoreStep \/ OutputReadEnd \/ OutputReply \/ OutputReturn \/ OutputWriteEnd
OutputSpec == OutputInit /\ [][OutputNext]_vars
OutputTypeOK == /\ TypeOK /\ output.terminals \in 0..2
                /\ \A key \in {"sent", "replied", "returned", "success"} : output[key] \in BOOLEAN
ActualCopyRetainsOutputCustody ==
    /\ s.copied <= output.terminals
    /\ (output.sent /\ ~(output.replied /\ output.returned) => s.lease)
=============================================================================
