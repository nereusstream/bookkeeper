-------------------- MODULE ProfileLacEvidence --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Integers, FiniteSets
CONSTANTS AckQuorum, CountBeforeDigest, ReuseSourceLength, IgnoreAnchorCoordinate,
          IgnoreLengthConflict, ReleaseOnCancel, IgnoreMetadataChange,
          IgnoreExplicitDigest, DiscardExplicitFrontier
VARIABLE s
vars == <<s>>
(* E=W=3 with A=2/3. Java reuses RRQuorumCoverageSet for all write sets.
   One all-terminal source fan-in and one all-terminal legal boundary read.
   Source 100 carries LAC 99/length 1100; only entry 99 anchors length 1000.
   URL2 includes independently verified explicit LAC 100 with no length.
   Invalid explicit digest excludes the whole endpoint, even with a source.
   No CRC producer, wire/timer, durable fence coverage or CLOSED authority. *)
Valid == {i \in 1..3 : s.replies[i] \in {"source", "empty", "source-explicit", "explicit"}}
Covered == Valid
           \cup (IF CountBeforeDigest THEN {i \in 1..3 : s.replies[i] = "corrupt"} ELSE {})
           \cup (IF IgnoreExplicitDigest
                    THEN {i \in 1..3 : s.replies[i] = "corrupt-explicit"} ELSE {})
HasSource == \E i \in 1..3 : s.replies[i] \in {"source", "source-explicit"}
HasExplicit == \E i \in 1..3 : s.replies[i] \in {"source-explicit", "explicit"}
HasPrefix == HasSource \/ HasExplicit
UseExplicit == HasExplicit /\ ~DiscardExplicitFrontier
SelectedPrefix == IF UseExplicit THEN 100 ELSE IF HasSource THEN 99 ELSE -1
MatchingLength == IF HasExplicit THEN 1100 ELSE 1000
Init == s \in [phase : {"sources"}, replies : [1..3 -> {"source", "empty", "corrupt", "unknown", "source-explicit", "explicit", "corrupt-explicit"}],
               anchor : {"matching", "wrong-coordinate", "conflict", "missing"},
               current : {TRUE}, waiting : {TRUE}, reserved : {TRUE}, covered : {FALSE},
               prefix : {-1}, length : {0}, outcome : {"none"}]
SourcesEnd == /\ s.phase = "sources"
              /\ s' = [s EXCEPT !.covered = Cardinality(Covered) >= 4 - AckQuorum,
                       !.phase = IF Cardinality(Covered) >= 4 - AckQuorum
                                      /\ (s.current \/ IgnoreMetadataChange)
                                    THEN IF HasSource \/ UseExplicit THEN "anchor" ELSE "empty"
                                    ELSE "refused",
                       !.prefix = SelectedPrefix]
AnchorEnd == /\ s.phase = "anchor"
             /\ s' = [s EXCEPT !.phase = IF (s.anchor = "matching"
                                           \/ (IgnoreAnchorCoordinate /\ s.anchor = "wrong-coordinate")
                                           \/ (IgnoreLengthConflict /\ s.anchor = "conflict"))
                                           /\ (s.current \/ IgnoreMetadataChange)
                                         THEN "ready" ELSE "refused",
                      !.length = IF ReuseSourceLength \/ s.anchor = "wrong-coordinate"
                                    THEN 1200 ELSE IF UseExplicit THEN 1100 ELSE 1000]
ChangeMetadata == /\ s.phase \in {"sources", "anchor"} /\ s.current
                  /\ s' = [s EXCEPT !.current = FALSE]
Cancel == /\ s.waiting /\ s.phase \in {"sources", "anchor"}
          /\ s' = [s EXCEPT !.waiting = FALSE,
                   !.reserved = IF ReleaseOnCancel THEN FALSE ELSE @]
Finish == /\ s.phase \in {"empty", "ready", "refused"}
          /\ s' = [s EXCEPT !.outcome = IF s.phase = "refused" THEN "refused" ELSE "success",
                   !.phase = "done", !.reserved = FALSE]
Next == SourcesEnd \/ AnchorEnd \/ ChangeMetadata \/ Cancel \/ Finish
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"sources", "anchor", "empty", "ready", "refused", "done"}
CoverageCountsOnlyVerified == s.covered => Cardinality(Valid) >= 4 - AckQuorum
SuccessHasMatchingLength == s.outcome = "success" =>
                            (IF HasPrefix THEN s.prefix = (IF HasExplicit THEN 100 ELSE 99)
                                             /\ s.length = MatchingLength
                                          ELSE s.prefix = -1 /\ s.length = 0)
SuccessHasCorrectAnchor == s.outcome = "success" /\ HasPrefix => s.anchor = "matching"
SuccessHasNoLengthConflict == s.outcome = "success" => (s.anchor # "conflict" \/ ~HasPrefix)
SuccessKeepsMetadata == s.outcome = "success" => s.current
InFlightOwnsReservation == s.phase \in {"sources", "anchor"} => s.reserved
=============================================================================
