-------------------- MODULE ProfileLacEvidence --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Integers, FiniteSets
CONSTANTS AckQuorum, CountBeforeDigest, ReuseSourceLength, IgnoreAnchorCoordinate,
          IgnoreLengthConflict, ReleaseOnCancel, IgnoreMetadataChange
VARIABLE s
vars == <<s>>
(* E=W=3 with A=2/3. Java reuses RRQuorumCoverageSet for all write sets.
   One all-terminal source fan-in and one all-terminal legal boundary read.
   Source 100 carries LAC 99/length 1100; only entry 99 anchors length 1000.
   No CRC producer, wire/timer, durable fence coverage or CLOSED authority. *)
Valid == {i \in 1..3 : s.replies[i] \in {"source", "empty"}}
Covered == IF CountBeforeDigest
              THEN Valid \cup {i \in 1..3 : s.replies[i] = "corrupt"} ELSE Valid
HasSource == \E i \in 1..3 : s.replies[i] = "source"
Init == s \in [phase : {"sources"}, replies : [1..3 -> {"source", "empty", "corrupt", "unknown"}],
               anchor : {"matching", "wrong-coordinate", "conflict", "missing"},
               current : {TRUE}, waiting : {TRUE}, reserved : {TRUE}, covered : {FALSE},
               prefix : {-1}, length : {0}, outcome : {"none"}]
SourcesEnd == /\ s.phase = "sources"
              /\ s' = [s EXCEPT !.covered = Cardinality(Covered) >= 4 - AckQuorum,
                       !.phase = IF Cardinality(Covered) >= 4 - AckQuorum
                                      /\ (s.current \/ IgnoreMetadataChange)
                                    THEN IF HasSource THEN "anchor" ELSE "empty"
                                    ELSE "refused",
                       !.prefix = IF HasSource THEN 99 ELSE -1]
AnchorEnd == /\ s.phase = "anchor"
             /\ s' = [s EXCEPT !.phase = IF (s.anchor = "matching"
                                           \/ (IgnoreAnchorCoordinate /\ s.anchor = "wrong-coordinate")
                                           \/ (IgnoreLengthConflict /\ s.anchor = "conflict"))
                                           /\ (s.current \/ IgnoreMetadataChange)
                                         THEN "ready" ELSE "refused",
                      !.length = IF ReuseSourceLength \/ s.anchor = "wrong-coordinate"
                                    THEN 1100 ELSE 1000]
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
                            (IF HasSource THEN s.prefix = 99 /\ s.length = 1000
                                          ELSE s.prefix = -1 /\ s.length = 0)
SuccessHasCorrectAnchor == s.outcome = "success" /\ HasSource => s.anchor = "matching"
SuccessHasNoLengthConflict == s.outcome = "success" => (s.anchor # "conflict" \/ ~HasSource)
SuccessKeepsMetadata == s.outcome = "success" => s.current
InFlightOwnsReservation == s.phase \in {"sources", "anchor"} => s.reserved
=============================================================================
