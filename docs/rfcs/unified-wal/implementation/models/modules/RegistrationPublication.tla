-------------------- MODULE RegistrationPublication --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS SkipAuthorityCheck, SkipSessionGuard, NoServiceVersionBump, PromoteLostReply
VARIABLE s
vars == <<s>>
(* Atomic ZK multi, immutable unique nonce/session anchor, validated exact
   records and a closed local gate after demote/connection loss are inputs.
   Projection 1 is the captured old endpoint; 2 is refresh/replacement.
   Result: 0 pending, 1 applied/equivalent, 2 rejected, 3 unknown. *)
Init == s = [authority |-> 1, service |-> 0, writable |-> 0, marker |-> 0,
             version |-> 0, phase |-> 0, observedVersion |-> 0, observedProjection |-> 0,
             refreshCaptured |-> FALSE, refreshVersion |-> 0, refreshed |-> FALSE,
             demoteCaptured |-> FALSE, demoteVersion |-> 0, demoteWritable |-> FALSE, demoted |-> FALSE,
             changed |-> FALSE, replaced |-> FALSE, applied |-> FALSE, stale |-> FALSE,
             removedReplacement |-> FALSE, lost |-> FALSE, queried |-> FALSE, result |-> 0]
CaptureInfo == /\ s.phase = 0 /\ s' = [s EXCEPT !.phase = 1]
AuthorityChange == /\ s.phase \in 1..5 /\ ~s.changed
                   /\ s' = [s EXCEPT !.authority = 2, !.changed = TRUE]
PublishInfo == /\ s.phase = 1
               /\ LET allowed == s.authority = 1 \/ SkipAuthorityCheck
                  IN s' = [s EXCEPT !.service = IF allowed THEN 1 ELSE 0,
                           !.marker = IF allowed THEN 1 ELSE 0,
                           !.stale = allowed /\ s.authority # 1, !.phase = IF allowed THEN 2 ELSE 6]
CaptureWritable == /\ s.phase = 2 /\ s.marker = 1
                   /\ s' = [s EXCEPT !.observedVersion = s.version, !.observedProjection = s.service, !.phase = 3]
CaptureRefresh == /\ s.phase \in 2..3 /\ s.writable = 0 /\ s.marker = 1 /\ ~s.refreshCaptured
                  /\ s' = [s EXCEPT !.refreshCaptured = TRUE, !.refreshVersion = s.version]
Refresh == /\ s.refreshCaptured /\ ~s.refreshed
           /\ LET allowed == s.marker = 1 /\ s.version = s.refreshVersion
                             /\ (s.authority = 1 \/ SkipAuthorityCheck)
              IN s' = [s EXCEPT !.refreshed = TRUE, !.service = IF allowed THEN 2 ELSE @,
                       !.version = IF allowed THEN @ + 1 ELSE @,
                       !.stale = @ \/ (allowed /\ s.authority # 1)]
CreateWritable == /\ s.phase = 3
                  /\ LET allowed == s.writable = 0 /\ s.version = s.observedVersion
                                    /\ (s.marker = 1 \/ SkipSessionGuard)
                                    /\ (s.authority = 1 \/ SkipAuthorityCheck)
                     IN s' = [s EXCEPT !.writable = IF allowed THEN s.observedProjection ELSE @,
                              !.version = IF allowed /\ ~NoServiceVersionBump THEN @ + 1 ELSE @,
                              !.stale = @ \/ (allowed /\ s.authority # 1), !.applied = allowed, !.phase = 4]
Reply == /\ s.phase = 4 /\ \E lost \in BOOLEAN :
            s' = [s EXCEPT !.phase = 5, !.lost = lost,
                  !.result = IF lost THEN IF PromoteLostReply THEN 1 ELSE 3
                             ELSE IF s.applied THEN 1 ELSE 2]
Query == /\ s.phase = 5
         /\ s' = [s EXCEPT !.queried = TRUE, !.phase = 6,
                  !.result = IF s.marker = 1 /\ s.authority = 1 /\ s.writable = s.observedProjection
                               /\ s.service = s.writable THEN 1 ELSE 2]
CaptureDemote == /\ s.phase \in 2..5 /\ s.marker = 1 /\ ~s.demoteCaptured
                 /\ s' = [s EXCEPT !.demoteCaptured = TRUE, !.demoteVersion = s.version,
                          !.demoteWritable = s.writable # 0]
Replace == /\ s.phase \in 2..5 /\ ~s.replaced
           /\ s' = [s EXCEPT !.marker = 2, !.service = 2, !.writable = 2, !.version = 1, !.replaced = TRUE]
Demote == /\ s.demoteCaptured /\ ~s.demoted
          /\ LET allowed == s.version = s.demoteVersion /\ (s.marker = 1 \/ SkipSessionGuard)
             IN s' = [s EXCEPT !.demoted = TRUE, !.removedReplacement = allowed /\ s.marker = 2,
                      !.service = IF allowed THEN 0 ELSE @, !.marker = IF allowed THEN 0 ELSE @,
                      !.writable = IF allowed /\ s.demoteWritable THEN 0 ELSE @]
Next == CaptureInfo \/ AuthorityChange \/ PublishInfo \/ CaptureWritable \/ CaptureRefresh \/ Refresh
        \/ CreateWritable \/ Reply \/ Query \/ CaptureDemote \/ Replace \/ Demote
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.authority \in 1..2 /\ s.service \in 0..2 /\ s.writable \in 0..2 /\ s.marker \in 0..2
          /\ s.version \in 0..3 /\ s.phase \in 0..6 /\ s.result \in 0..3
PublishedOnlyAgainstMatchingAuthority == ~s.stale
OldDemotionCannotRemoveReplacement == ~s.removedReplacement
WritableProjectionMatchesService == s.writable # 0 => s.writable = s.service
UnresolvedPublicationNeverReportsApplied == ~(s.lost /\ ~s.queried /\ s.result = 1)
=====================================================================
