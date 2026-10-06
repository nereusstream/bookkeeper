-------------------- MODULE ProfileReadyLifecycle --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS TrustInert, SkipHeadBirth, SkipPageBirth, IgnoreMetadataBirth, IgnoreDeleteOrder,
          DropRetainedTargets, IgnoreAllE
VARIABLE s
vars == <<s>>
(* One initial page, PREPARING=1/READY=2/DELETE=3 head, one read/CAS per publisher.
   Root/page recreate resets dataVersion but changes immutable birth identity.
   Opaque head version and semantic control generation are separate. Metadata
   birth denotes creator token/reference with dataVersion again zero. Validation
   is a snapshot, not a lease: metadata may change after validation. Already
   granted local access is not instantaneously revoked by the global cut.
   Strict codecs, atomic backend transactions, ACLs and local I/O are inputs. *)
Init == \E complete \in BOOLEAN :
    s = [state |-> 1, version |-> 0, control |-> 1, rootBirth |-> 0, rootReset |-> FALSE,
         page |-> FALSE, pageBirth |-> 0, pageReset |-> FALSE, pageRoot |-> 0, pageAllE |-> complete,
         metadataBirth |-> 0, metadataReset |-> FALSE, readyRead |-> FALSE, deleteRead |-> FALSE,
         readyVersion |-> 0, readyRoot |-> 0, readyPage |-> 0, checkedMetadata |-> 0,
         deleteVersion |-> 0, deleteRoot |-> 0, reference |-> FALSE, referencedPage |-> 0,
         everReady |-> FALSE, everDelete |-> FALSE, mutation |-> FALSE, mutationRoot |-> 0,
         granted |-> FALSE, readyAtGrant |-> FALSE, readyCommitted |-> FALSE]
Prepare == /\ s.state = 1 /\ ~s.page /\ (s.pageAllE \/ IgnoreAllE)
           /\ s' = [s EXCEPT !.page = TRUE, !.pageRoot = s.rootBirth,
                    !.pageBirth = IF s.rootReset \/ s.pageReset THEN 1 ELSE 0]
ReadReady == /\ s.state = 1 /\ s.page /\ ~s.readyRead
             /\ (s.metadataBirth = 0 \/ IgnoreMetadataBirth)
             /\ s' = [s EXCEPT !.readyRead = TRUE, !.readyVersion = s.version,
                      !.readyRoot = s.rootBirth, !.readyPage = s.pageBirth,
                      !.checkedMetadata = s.metadataBirth]
ReadDelete == /\ s.state \in 1..2 /\ ~s.deleteRead
              /\ s' = [s EXCEPT !.deleteRead = TRUE, !.deleteVersion = s.version, !.deleteRoot = s.rootBirth]
ReadyCas == /\ s.readyRead /\ ~s.readyCommitted /\ s.page
            /\ (s.readyRoot = s.rootBirth \/ SkipHeadBirth)
            /\ (s.readyPage = s.pageBirth \/ SkipPageBirth) /\ s.pageRoot = s.rootBirth
            /\ ((s.readyVersion = s.version /\ s.state = 1) \/ IgnoreDeleteOrder)
            /\ s' = [s EXCEPT !.state = 2, !.version = s.version + 1, !.control = 1,
                     !.reference = TRUE, !.referencedPage = s.readyPage, !.everReady = TRUE,
                     !.mutation = TRUE, !.mutationRoot = s.readyRoot, !.readyCommitted = TRUE]
DeleteCas == /\ s.deleteRead /\ ~s.everDelete
             /\ (s.deleteRoot = s.rootBirth \/ SkipHeadBirth) /\ s.deleteVersion = s.version
             /\ s' = [s EXCEPT !.state = 3, !.version = s.version + 1, !.control = 2,
                      !.reference = IF DropRetainedTargets THEN FALSE ELSE s.reference,
                      !.everDelete = TRUE, !.mutation = TRUE, !.mutationRoot = s.deleteRoot]
RecreateRoot == /\ s.state = 1 /\ ~s.rootReset /\ ~s.everReady
                /\ s' = [s EXCEPT !.rootReset = TRUE, !.rootBirth = 1, !.version = 0,
                         !.page = FALSE, !.reference = FALSE, !.mutation = FALSE]
RecreatePage == /\ s.state = 1 /\ s.page /\ ~s.pageReset
                /\ s' = [s EXCEPT !.pageReset = TRUE, !.pageBirth = 1]
RecreateMetadata == /\ ~s.metadataReset
                    /\ s' = [s EXCEPT !.metadataReset = TRUE, !.metadataBirth = 1]
ReadActivation == /\ ~s.granted /\ s.page /\ s.metadataBirth = 0
                  /\ ((s.state = 2 /\ s.reference /\ s.referencedPage = s.pageBirth) \/ TrustInert)
                  /\ s' = [s EXCEPT !.granted = TRUE,
                           !.readyAtGrant = s.state = 2 /\ s.reference /\ s.referencedPage = s.pageBirth]
Next == Prepare \/ ReadReady \/ ReadDelete \/ ReadyCas \/ DeleteCas \/ RecreateRoot \/ RecreatePage
        \/ RecreateMetadata \/ ReadActivation
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.state \in 1..3 /\ s.version \in 0..3 /\ s.control \in 1..2
          /\ s.rootBirth \in 0..1 /\ s.pageBirth \in 0..1 /\ s.pageRoot \in 0..1
          /\ s.metadataBirth \in 0..1 /\ s.readyVersion \in 0..3 /\ s.readyRoot \in 0..1
          /\ s.readyPage \in 0..1 /\ s.checkedMetadata \in 0..1
          /\ s.deleteVersion \in 0..3 /\ s.deleteRoot \in 0..1 /\ s.referencedPage \in 0..1
          /\ s.mutationRoot \in 0..1
BirthChecksProtectTheCurrentRoot == s.mutation => s.mutationRoot = s.rootBirth
ReadyReferencesActualPageBirth == s.state = 2 => s.page /\ s.reference /\ s.referencedPage = s.pageBirth
ReadyRequiresAllEReceipts == s.readyCommitted => s.pageAllE
ReadyValidatedMetadataBirth == s.readyCommitted => s.checkedMetadata = 0
DeleteCutNeverRollsBack == s.everDelete => s.control = 2 /\ s.state = 3
PublishedTargetsSurviveDelete == s.everReady /\ s.state = 3 => s.reference
ActivationHasPublishedReady == s.granted => s.readyAtGrant
=====================================================================
