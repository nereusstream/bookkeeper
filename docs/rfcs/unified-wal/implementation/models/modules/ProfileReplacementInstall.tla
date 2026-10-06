-------------------- MODULE ProfileReplacementInstall --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnorePreparation, IgnoreDelete, IgnoreMetadata, InstallActivatesNormal,
          IgnoreCapability, IgnorePurposeSchema, AckBeforeForce
VARIABLE s
vars == <<s>>
Init == \E capability, schema, peer \in BOOLEAN :
    s = [prepared |-> FALSE, deleted |-> FALSE, metadata |-> 0, installed |-> FALSE,
         published |-> FALSE, capability |-> capability, schema |-> schema, peer |-> peer,
         submitted |-> FALSE, force |-> FALSE, applied |-> FALSE, ack |-> FALSE,
         normal |-> FALSE, validInstall |-> TRUE, validGrant |-> TRUE]
(* One frozen next-slot INSTALL and one ACTIVATE_REPLACEMENT. Metadata=0 is
   the frozen predecessor, 1 its exact committed mapping, 2 a changed/frozen
   record. Initial UIC1 and replacement UIC2 schemas are domain-separated.
   Real TLS, bytes, root/page birth checks, ownership and barriers remain in
   the existing projections and socket/file tests; no lease/liveness/full A+C+D. *)
Prepare == /\ ~s.prepared /\ ~s.deleted /\ s.metadata = 0
           /\ s' = [s EXCEPT !.prepared = TRUE]
Install == /\ ~s.installed /\ s.peer /\ (s.prepared \/ IgnorePreparation)
           /\ (~s.deleted \/ IgnoreDelete)
           /\ (s.metadata \in 0..1 \/ IgnoreMetadata)
           /\ s' = [s EXCEPT !.installed = TRUE,
                    !.normal = InstallActivatesNormal,
                    !.validInstall = s.prepared /\ ~s.deleted /\ s.metadata \in 0..1]
Membership == /\ s.installed /\ s.metadata = 0 /\ ~s.deleted
              /\ s' = [s EXCEPT !.metadata = 1]
Publish == /\ s.installed /\ s.prepared /\ s.metadata = 1 /\ ~s.deleted /\ ~s.published
           /\ s' = [s EXCEPT !.published = TRUE]
Delete == /\ ~s.deleted /\ s' = [s EXCEPT !.deleted = TRUE]
ChangeMetadata == /\ s.metadata < 2 /\ s' = [s EXCEPT !.metadata = 2]
SubmitActivation == /\ ~s.submitted /\ s.published /\ s.installed /\ s.peer /\ ~s.deleted
                    /\ s.metadata = 1 /\ (s.capability \/ IgnoreCapability)
                    /\ (s.schema \/ IgnorePurposeSchema)
                    /\ s' = [s EXCEPT !.submitted = TRUE,
                             !.validGrant = s.capability /\ s.schema /\ s.published /\ s.peer]
Force == /\ s.submitted /\ ~s.force /\ s' = [s EXCEPT !.force = TRUE]
Apply == /\ s.force /\ ~s.applied /\ s' = [s EXCEPT !.applied = TRUE, !.normal = TRUE]
Ack == /\ s.submitted /\ ~s.ack /\ (s.applied \/ AckBeforeForce)
       /\ s' = [s EXCEPT !.ack = TRUE]
Next == Prepare \/ Install \/ Membership \/ Publish \/ Delete \/ ChangeMetadata
        \/ SubmitActivation \/ Force \/ Apply \/ Ack
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.metadata \in 0..2
          /\ \A f \in {"prepared", "deleted", "installed", "published", "capability", "schema", "peer",
                       "submitted", "force", "applied", "ack", "normal", "validInstall", "validGrant"} :
                    s[f] \in BOOLEAN
InstallHasBoundLivePreparation == s.validInstall
InstallNeverActivatesNormal == s.normal => s.applied
ActivationHasNegotiatedBoundSchema == s.validGrant
AckHasActualForceAndOwnerApply == s.ack => s.force /\ s.applied
==============================================================================
