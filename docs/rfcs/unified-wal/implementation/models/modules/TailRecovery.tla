-------------------------- MODULE TailRecovery --------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License. You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. *)
EXTENDS Naturals, FiniteSets
CONSTANTS Entries, TrustInvalidCoverage, EarlyExpose, IgnoreGap
ASSUME /\ Entries = 3 /\ TrustInvalidCoverage \in BOOLEAN
       /\ EarlyExpose \in BOOLEAN /\ IgnoreGap \in BOOLEAN
Prefix(cut) == 1..cut
VARIABLES present, dead, cut, db, position, expected, retained, used, broken, synced, unsafeSkip, stage
vars == <<present, dead, cut, db, position, expected, retained, used, broken, synced, unsafeSkip, stage>>
Init == /\ present \in {1..Entries, (1..Entries) \ {2}}
        /\ dead \in {{}, {1}} /\ cut \in 0..Entries
        /\ db \in {Prefix(cut), {}, Prefix(cut) \cup {3}}
        /\ position = 1 /\ expected = 1 /\ retained = {} /\ used = FALSE
        /\ broken = FALSE /\ synced = FALSE /\ unsafeSkip = FALSE /\ stage = "initial"
ValidProof == db = Prefix(cut)
(* Control replay, immutable grants/scope and valid headers are assumed; body validation is separate. *)
Start == /\ stage = "initial"
         /\ used' = (ValidProof \/ TrustInvalidCoverage)
         /\ retained' = (IF ValidProof \/ TrustInvalidCoverage THEN db ELSE {}) \ dead
         /\ stage' = "scan"
         /\ UNCHANGED <<present, dead, cut, db, position, expected, broken, synced, unsafeSkip>>
InspectHeader == /\ stage = "scan" /\ position <= Entries
                 /\ position' = position + 1
                 /\ IF position \in present
                     THEN /\ expected' = IF position = expected THEN expected + 1 ELSE expected
                          /\ broken' = (broken \/ position # expected)
                          /\ LET skip == used /\ position <= cut
                              IN /\ retained' = IF skip \/ position \in dead THEN retained
                                                  ELSE retained \cup {position}
                                 /\ unsafeSkip' = (unsafeSkip \/ (skip /\ ~ValidProof))
                     ELSE UNCHANGED <<expected, broken, retained, unsafeSkip>>
                 /\ UNCHANGED <<present, dead, cut, db, used, synced, stage>>
Finish == /\ stage = "scan" /\ position > Entries /\ stage' = "checked"
          /\ UNCHANGED <<present, dead, cut, db, position, expected, retained, used, broken, synced, unsafeSkip>>
ForceData == /\ stage = "checked" /\ (~broken \/ IgnoreGap)
             /\ synced' = TRUE /\ stage' = "synced"
             /\ UNCHANGED <<present, dead, cut, db, position, expected, retained, used, broken, unsafeSkip>>
Expose == /\ (stage = "synced" \/ (EarlyExpose /\ stage = "checked"))
          /\ (~broken \/ IgnoreGap) /\ stage' = "ready"
          /\ UNCHANGED <<present, dead, cut, db, position, expected, retained, used, broken, synced, unsafeSkip>>
TypeOK == /\ present \subseteq 1..Entries /\ dead \subseteq 1..Entries /\ cut \in 0..Entries
          /\ db \subseteq 1..Entries /\ retained \subseteq 1..Entries
          /\ position \in 1..(Entries+1) /\ expected \in 1..(Entries+1)
          /\ used \in BOOLEAN /\ broken \in BOOLEAN /\ synced \in BOOLEAN /\ unsafeSkip \in BOOLEAN
          /\ stage \in {"initial", "scan", "checked", "synced", "ready"}
SkippedBodyHasDurableMatchingIndex == ~unsafeSkip
ReadyHasRequiredDataSync == stage = "ready" => synced
ReadyHasContiguousPhysicalPrefix == stage = "ready" => ~broken
ReadyRetainsExactlyReadableCoordinates == stage = "ready" => retained = present \ dead
Next == Start \/ InspectHeader \/ Finish \/ ForceData \/ Expose
Spec == Init /\ [][Next]_vars
=============================================================================
