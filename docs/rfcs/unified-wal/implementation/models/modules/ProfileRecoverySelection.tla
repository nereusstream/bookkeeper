-------------------- MODULE ProfileRecoverySelection --------------------
(*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*)
EXTENDS Integers
CONSTANTS SkipMetaGuard, SkipMarkerGuard, SkipOrigin, IgnoreReadBirth,
          DropReference, WrongOffset, WrongRange, Reselect, PublishClosed
VARIABLE s
vars == <<s>>
(* One operation selection and two full-copy publications. Real read proof,
   physical node/metadata births and hashes are inputs; no CRC/TLS/I/O producer. *)
Init == s \in [phase : {"candidate"}, metaBirth : {0}, markerBirth : {1}, origin : BOOLEAN,
               n : {0, 1}, before : {0}, version : {0}, capturedN : {0}, prefix : {2},
               reference : {FALSE}, refBirth : {0}, published : {FALSE}, adopted : {FALSE},
               checkedMeta : {0}, checkedMarker : {1}, checkedOrigin : {TRUE}, range : {2}, closed : {FALSE}]
Prepare == /\ s.phase = "candidate"
           /\ s' = [s EXCEPT !.phase = "prepared", !.before = s.n, !.version = s.n, !.capturedN = s.n]
RestoreMeta == /\ s.phase = "prepared" /\ s.metaBirth = 0
               /\ s' = [s EXCEPT !.metaBirth = 1]
RestoreMarker == /\ s.phase \in {"prepared", "published"} /\ s.markerBirth = 1
                 /\ s' = [s EXCEPT !.markerBirth = 2, !.adopted = FALSE]
Publish == /\ s.phase = "prepared"
           /\ LET ok == (s.metaBirth = 0 \/ SkipMetaGuard)
                          /\ (s.markerBirth = 1 \/ SkipMarkerGuard) /\ (s.origin \/ SkipOrigin)
              IN s' = [s EXCEPT !.phase = IF ok THEN "published" ELSE "rejected",
                       !.published = ok, !.reference = ok /\ ~DropReference,
                       !.refBirth = 1, !.version = IF ok THEN s.n + 1 ELSE @,
                       !.checkedMeta = s.metaBirth, !.checkedMarker = s.markerBirth,
                       !.checkedOrigin = s.origin, !.closed = PublishClosed]
Read == /\ s.phase = "published"
        /\ s' = [s EXCEPT !.adopted = (s.markerBirth = s.refBirth \/ IgnoreReadBirth)
                  /\ s.reference /\ s.metaBirth = 0 /\ s.origin]
Copy == /\ s.phase = "published" /\ s.adopted /\ s.n < 3
        /\ s' = [s EXCEPT !.n = @ + 1, !.version = @ + (IF WrongOffset THEN 0 ELSE 1),
                  !.range = IF WrongRange THEN 3 ELSE s.prefix]
ChooseAgain == /\ s.phase = "published" /\ Reselect /\ s.prefix = 2
               /\ s' = [s EXCEPT !.prefix = 3]
Next == Prepare \/ RestoreMeta \/ RestoreMarker \/ Publish \/ Read \/ Copy \/ ChooseAgain
Spec == Init /\ [][Next]_vars
TypeOK == s.phase \in {"candidate", "prepared", "published", "rejected"} /\ s.n \in 0..3
PublicationHasMetaBirth == s.published => s.checkedMeta = 0
PublicationHasMarkerGuard == s.published => s.checkedMarker = 1
PublicationHasVerifiedOrigin == s.published => s.checkedOrigin
SelectionHasReference == s.published => s.reference
AdoptionHasPhysicalBirth == s.adopted => s.markerBirth = s.refBirth
VersionHasSingleSelectionOffset == s.phase = "published" => s.version = s.n + 1
CopyUsesSelectedRange == s.adopted => s.range = s.prefix
SelectionIsFixed == s.prefix = 2
SelectionNeverCloses == ~s.closed
=============================================================================
