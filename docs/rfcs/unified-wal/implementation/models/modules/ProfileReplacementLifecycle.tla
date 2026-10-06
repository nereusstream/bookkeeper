-------------------- MODULE ProfileReplacementLifecycle --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS TrustCandidate, IgnoreDelete, SkipMetadata, SkipPageBirth, SkipRootBirth,
          DropHistory, DiscoverCurrentOnly, AcceptOldAuthority
VARIABLE s
vars == <<s>>
Pages == 1..2
Init == s = [state |-> "live", version |-> 1, rootBirth |-> 0, rootReset |-> FALSE,
             published |-> 0, metadata |-> 0, frozen |-> FALSE,
             candidates |-> {}, readPages |-> {}, pageBirth |-> [p \in Pages |-> 0],
             observedPage |-> [p \in Pages |-> 0], observedRoot |-> [p \in Pages |-> 0],
             observedVersion |-> [p \in Pages |-> 0], observedMetadata |-> [p \in Pages |-> 0],
             targets |-> {0}, everTargets |-> {0}, authorized |-> {},
             grantValid |-> TRUE, publicationValid |-> TRUE, rootMutationValid |-> TRUE, everDelete |-> FALSE,
             discovery |-> {}, discoveryExpected |-> {}, discoveryRead |-> FALSE]
(* Two sequential same-start membership changes, one candidate per next head
   slot, each referencing its predecessor. Metadata/root/page birth checks are
   one backend transaction. Grants and discovery are snapshot observations;
   DELETE does not instantly revoke prior local activation or DATA. No real
   bytes/ACL/quorum/clock/fsync, wire client, SDK resend, fairness or full A+C+D. *)
Membership(p) == /\ s.state = "live" /\ ~s.frozen /\ s.published = p - 1 /\ s.metadata = p - 1
                 /\ s' = [s EXCEPT !.metadata = p]
Prepare(p) == /\ s.state = "live" /\ ~s.frozen /\ s.metadata = p /\ s.published = p - 1
              /\ p \notin s.candidates
              /\ s' = [s EXCEPT !.candidates = @ \cup {p}]
ReadPublication(p) == /\ p \in s.candidates /\ p \notin s.readPages /\ s.published = p - 1
                      /\ s' = [s EXCEPT !.readPages = @ \cup {p}, !.observedPage[p] = s.pageBirth[p],
                               !.observedRoot[p] = s.rootBirth, !.observedVersion[p] = s.version,
                               !.observedMetadata[p] = s.metadata]
Publish(p) == /\ p \in s.readPages /\ s.published = p - 1
              /\ ((s.state = "live" /\ s.version = s.observedVersion[p]) \/ IgnoreDelete)
              /\ (s.rootBirth = s.observedRoot[p] \/ SkipRootBirth)
              /\ (s.pageBirth[p] = s.observedPage[p] \/ SkipPageBirth)
              /\ ((s.metadata = p /\ s.observedMetadata[p] = p /\ ~s.frozen) \/ SkipMetadata)
              /\ s' = [s EXCEPT !.state = "live", !.version = @ + 1, !.published = p,
                       !.targets = IF DropHistory THEN {p} ELSE @ \cup {p},
                       !.everTargets = @ \cup {p},
                       !.publicationValid = @ /\ s.metadata = p /\ ~s.frozen
                           /\ s.pageBirth[p] = s.observedPage[p],
                       !.rootMutationValid = @ /\ s.rootBirth = s.observedRoot[p]]
Delete == /\ s.state = "live"
          /\ s' = [s EXCEPT !.state = "delete", !.version = @ + 1, !.everDelete = TRUE]
FreezeMetadata == /\ ~s.frozen /\ s.metadata > 0
                  /\ s' = [s EXCEPT !.frozen = TRUE, !.metadata = 3]
RecreatePage(p) == /\ p \in s.readPages /\ s.pageBirth[p] = 0 /\ s.published < p
                   /\ s' = [s EXCEPT !.pageBirth[p] = 1]
RecreateRoot == /\ ~s.rootReset /\ s.published = 0
                /\ s' = [s EXCEPT !.rootReset = TRUE, !.rootBirth = 1, !.version = 1]
Grant(p) == /\ p \in s.candidates /\ p \notin s.authorized
            /\ ((s.state = "live" /\ s.published = p /\ s.metadata = p /\ ~s.frozen)
                \/ TrustCandidate \/ (AcceptOldAuthority /\ p <= s.published))
            /\ s' = [s EXCEPT !.authorized = @ \cup {p},
                     !.grantValid = @ /\ s.state = "live" /\ s.published = p
                         /\ s.metadata = p /\ ~s.frozen]
Discover == /\ ~s.discoveryRead
            /\ s' = [s EXCEPT !.discoveryRead = TRUE, !.discoveryExpected = s.everTargets,
                     !.discovery = IF DiscoverCurrentOnly THEN {s.published} ELSE s.targets]
Next == (\E p \in Pages : Membership(p) \/ Prepare(p) \/ ReadPublication(p) \/ Publish(p)
                          \/ RecreatePage(p) \/ Grant(p)) \/ Delete \/ FreezeMetadata \/ RecreateRoot \/ Discover
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.state \in {"live", "delete"} /\ s.published \in 0..2 /\ s.metadata \in 0..3
          /\ s.version \in 1..6 /\ s.rootBirth \in 0..1 /\ s.targets \subseteq 0..2
          /\ s.everTargets \subseteq 0..2 /\ s.authorized \subseteq Pages
          /\ s.candidates \subseteq Pages /\ s.readPages \subseteq Pages
          /\ \A p \in Pages : /\ s.pageBirth[p] \in 0..1 /\ s.observedPage[p] \in 0..1
                             /\ s.observedRoot[p] \in 0..1 /\ s.observedVersion[p] \in 0..6
                             /\ s.observedMetadata[p] \in 0..3
PublicationHasSameMetadataAndPageBirth == s.publicationValid
MutationUsesObservedRootBirth == s.rootMutationValid
DeleteCutNeverReopens == s.everDelete => s.state = "delete"
GrantedTargetsAreRetained == s.everTargets \subseteq s.targets
GrantRequiresCurrentPublishedAuthority == s.grantValid
DiscoveryIncludesPublishedTargets == s.discoveryRead => s.discoveryExpected \subseteq s.discovery
DiscoveryIsCompleteAtRead == s.discoveryRead => 0 \in s.discovery
==============================================================================
