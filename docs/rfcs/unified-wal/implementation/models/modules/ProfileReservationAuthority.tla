-------------------- MODULE ProfileReservationAuthority --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreKey, IgnoreTarget, TrustCorrupt, AuthorizeNormal, MixVersion
VARIABLE s
vars == <<s>>
(* A committed PREPARING root cold read followed by an INSTALL projection.
   Kind: 0 absent, 1 matching key and planned local target, 2 wrong key,
   3 local target not planned, 4 corrupt/oversized/incomplete/ephemeral,
   5 unavailable. Purpose: 1 INSTALL, 2 ACTIVATE_INITIAL.
   Atomic bytes/Stat read and strict codec are inputs. Immutable root create
   result resolution uses the existing metadata-create projection plus actual
   ZK tests. No READY, all-E receipts, CAS/delete cut, lease or liveness. *)
Init == \E kind \in 0..5, purpose \in 1..2 :
    s = [kind |-> kind, purpose |-> purpose, version |-> 0, replaced |-> FALSE,
         phase |-> 0, capturedKind |-> 0, capturedVersion |-> 0,
         projectedVersion |-> 0, granted |-> FALSE]
Read == /\ s.phase = 0
        /\ s' = [s EXCEPT !.phase = 1, !.capturedKind = s.kind,
                 !.capturedVersion = s.version]
Replace == /\ ~s.replaced /\ s.phase \in 0..1
           /\ s' = [s EXCEPT !.replaced = TRUE, !.kind = 2, !.version = 1]
Project == /\ s.phase = 1
           /\ LET k == s.capturedKind
                  valid == k = 1 \/ (k = 2 /\ IgnoreKey) \/ (k = 3 /\ IgnoreTarget)
                           \/ (k = 4 /\ TrustCorrupt)
              IN s' = [s EXCEPT !.phase = 2,
                       !.granted = valid /\ (s.purpose = 1 \/ AuthorizeNormal),
                       !.projectedVersion = IF MixVersion THEN s.version ELSE s.capturedVersion]
Next == Read \/ Replace \/ Project
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.kind \in 0..5 /\ s.capturedKind \in 0..5 /\ s.purpose \in 1..2
          /\ s.version \in 0..1 /\ s.capturedVersion \in 0..1 /\ s.projectedVersion \in 0..1
          /\ s.phase \in 0..2 /\ s.replaced \in BOOLEAN /\ s.granted \in BOOLEAN
GrantedHasValidCommittedRoot == s.granted => ~(s.capturedKind \in {0, 4, 5})
GrantedMatchesRequestedKey == s.granted => s.capturedKind # 2
GrantedTargetIsPlanned == s.granted => s.capturedKind # 3
PreparingOnlyAuthorizesInstall == s.granted => s.purpose = 1
ProjectionKeepsReadVersion == s.granted => s.projectedVersion = s.capturedVersion
=====================================================================
