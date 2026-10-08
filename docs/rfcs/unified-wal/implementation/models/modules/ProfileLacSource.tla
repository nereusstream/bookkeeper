-------------------- MODULE ProfileLacSource --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Integers
CONSTANTS SkipAuthorization, ClaimCoordinateEmpty, IgnoreExplicitLac,
          ReuseSourceLength, IgnoreSourceBinding, ReturnUnvalidatedSource
VARIABLE s
vars == <<s>>
(* One source sample: its entry carries LAC 99 but length 1100 belongs to
   entry 100. Opt-in URL2 also returns an independent explicit witness, never length.
   No quorum, CRC, clocks or CLOSED protocol.
   Prior point-read and socket models cover actual custody/negotiation.
   Empty is a whole-ledger proof, never just absence of coordinate -1. *)
Init == s \in [phase : {"query"}, authorized : BOOLEAN, binding : BOOLEAN,
               occupied : BOOLEAN, selected : BOOLEAN, validated : BOOLEAN,
               coverage : BOOLEAN, fenced : BOOLEAN, stable : BOOLEAN,
               openRange : BOOLEAN, explicitLac : BOOLEAN, negotiated : BOOLEAN,
               returnedExplicit : {FALSE}, result : {"none"},
               source : {-1}, lac : {-1}, length : {0}, prefixLength : {-1}]
DataAbsenceProof == /\ ~s.occupied /\ s.coverage /\ s.fenced /\ s.stable /\ ~s.openRange
HasEmptyProof == DataAbsenceProof /\ ~s.explicitLac
Reply == /\ s.phase = "query"
         /\ LET authorized == s.authorized \/ SkipAuthorization
                source == s.selected /\ (s.validated \/ ReturnUnvalidatedSource)
                          /\ (s.binding \/ IgnoreSourceBinding)
                empty == (HasEmptyProof \/ ClaimCoordinateEmpty
                           \/ (IgnoreExplicitLac /\ ~s.occupied /\ s.coverage
                               /\ s.fenced /\ s.stable /\ ~s.openRange)) /\ s.binding
                result == IF ~authorized \/ s.openRange THEN "refused"
                          ELSE IF source THEN "source" ELSE IF empty THEN "empty"
                          ELSE IF s.negotiated /\ s.explicitLac /\ DataAbsenceProof /\ s.binding
                            THEN "explicit" ELSE "refused"
            IN s' = [s EXCEPT !.phase = "reply", !.result = result,
                     !.returnedExplicit = result \in {"source", "explicit"}
                                          /\ s.explicitLac /\ s.negotiated,
                     !.source = IF result = "source" THEN 100 ELSE -1,
                     !.lac = IF result = "source" THEN 99 ELSE -1,
                     !.length = IF result = "source" THEN 1100 ELSE 0,
                     !.prefixLength = IF result = "source" /\ ReuseSourceLength THEN 1100 ELSE -1]
Spec == Init /\ [][Reply]_vars
TypeOK == /\ s.phase \in {"query", "reply"}
          /\ s.result \in {"none", "source", "empty", "explicit", "refused"}
ReplyRequiresPermission == s.result \in {"source", "empty", "explicit"} => s.authorized
EmptyRequiresWholeLedgerProof == s.result = "empty" => HasEmptyProof
ExplicitRequiresStableDataAbsence == s.result = "explicit" =>
                                    (DataAbsenceProof /\ s.explicitLac /\ s.negotiated)
ExplicitWitnessIsSeparate == s.returnedExplicit =>
                            (s.explicitLac /\ s.negotiated /\ s.prefixLength = -1)
SourceHasValidatedData == s.result = "source" => (s.selected /\ s.validated)
ReplyRequiresSourceBinding == s.result \in {"source", "empty", "explicit"} => s.binding
SourceLengthIsNeverPrefixLength == /\ s.prefixLength = -1
                                  /\ (s.result = "source" =>
                                       (s.source = 100 /\ s.lac = 99 /\ s.length = 1100))
=============================================================================
