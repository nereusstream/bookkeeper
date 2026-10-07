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
   entry 100. No quorum, explicit-LAC output, CRC, clocks or CLOSED protocol.
   Prior point-read and socket models cover actual custody/negotiation.
   Empty is a whole-ledger proof, never just absence of coordinate -1. *)
Init == s \in [phase : {"query"}, authorized : BOOLEAN, binding : BOOLEAN,
               occupied : BOOLEAN, selected : BOOLEAN, validated : BOOLEAN,
               coverage : BOOLEAN, fenced : BOOLEAN, stable : BOOLEAN,
               openRange : BOOLEAN, explicitLac : BOOLEAN, result : {"none"},
               source : {-1}, lac : {-1}, length : {0}, prefixLength : {-1}]
HasEmptyProof == /\ ~s.occupied /\ s.coverage /\ s.fenced /\ s.stable
                 /\ ~s.openRange /\ ~s.explicitLac
Reply == /\ s.phase = "query"
         /\ LET authorized == s.authorized \/ SkipAuthorization
                source == s.selected /\ (s.validated \/ ReturnUnvalidatedSource)
                          /\ (s.binding \/ IgnoreSourceBinding)
                empty == (HasEmptyProof \/ ClaimCoordinateEmpty
                           \/ (IgnoreExplicitLac /\ ~s.occupied /\ s.coverage
                               /\ s.fenced /\ s.stable /\ ~s.openRange)) /\ s.binding
                result == IF ~authorized \/ s.openRange THEN "refused"
                          ELSE IF source THEN "source" ELSE IF empty THEN "empty" ELSE "refused"
            IN s' = [s EXCEPT !.phase = "reply", !.result = result,
                     !.source = IF result = "source" THEN 100 ELSE -1,
                     !.lac = IF result = "source" THEN 99 ELSE -1,
                     !.length = IF result = "source" THEN 1100 ELSE 0,
                     !.prefixLength = IF result = "source" /\ ReuseSourceLength THEN 1100 ELSE -1]
Spec == Init /\ [][Reply]_vars
TypeOK == /\ s.phase \in {"query", "reply"}
          /\ s.result \in {"none", "source", "empty", "refused"}
ReplyRequiresPermission == s.result \in {"source", "empty"} => s.authorized
EmptyRequiresWholeLedgerProof == s.result = "empty" => HasEmptyProof
SourceHasValidatedData == s.result = "source" => (s.selected /\ s.validated)
ReplyRequiresSourceBinding == s.result \in {"source", "empty"} => s.binding
SourceLengthIsNeverPrefixLength == /\ s.prefixLength = -1
                                  /\ (s.result = "source" =>
                                       (s.source = 100 /\ s.lac = 99 /\ s.length = 1100))
=============================================================================
