-------------------- MODULE ProfileDeleteHandoffAck --------------------
\* Licensed to the Apache Software Foundation (ASF) under one or more
\* contributor license agreements. See the NOTICE file distributed with
\* this work for additional information regarding copyright ownership.
\* The ASF licenses this file to you under the Apache License, Version 2.0
\* (the "License"); you may not use this file except in compliance
\* with the License. You may obtain a copy of the License at
\*
\*     http://www.apache.org/licenses/LICENSE-2.0
\*
\* Unless required by applicable law or agreed to in writing, software
\* distributed under the License is distributed on an "AS IS" BASIS,
\* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
\* See the License for the specific language governing permissions and
\* limitations under the License.
EXTENDS Integers
CONSTANTS IgnoreLocalProof, IgnoreSourceBirth, IgnoreRowBirth, OverwriteMatchingRow, EarlyRelease
VARIABLE s
vars == <<s>>
(* Original local force/apply capability and native Plan/snapshot verification are
   supplied Java facts. Persistent acknowledgement only; no EFFECTIVE or GC. *)
Init == s \in [local : BOOLEAN, sourceBirth : {1}, capturedSource : {1}, captured : {FALSE},
               row : {FALSE}, rowBirth : {1}, guardBirth : {1}, nonce : {1}, nativeReturned : {FALSE},
               confirmed : {FALSE}, invalidSourceConfirmed : {FALSE}, invalidRowConfirmed : {FALSE},
               coldReturned : {FALSE}, held : {FALSE}, stopped : {FALSE}, releasedEarly : {FALSE}]
Capture == /\ ~s.captured /\ ~s.stopped
           /\ s' = [s EXCEPT !.captured = TRUE, !.capturedSource = s.sourceBirth, !.held = TRUE]
SourceAba == /\ s.captured /\ s.sourceBirth = 1
             /\ s' = [s EXCEPT !.sourceBirth = 2]
Publish == /\ s.captured /\ ~s.row /\ ~s.stopped /\ (s.local \/ IgnoreLocalProof)
           /\ s' = [s EXCEPT !.row = TRUE]
NativeReturn == /\ s.captured /\ ~s.nativeReturned
                /\ s' = [s EXCEPT !.nativeReturned = TRUE]
RowAba == /\ s.row /\ s.rowBirth = 1
          /\ s' = [s EXCEPT !.rowBirth = 2]
Confirm == /\ s.row /\ s.nativeReturned /\ ~s.confirmed
           /\ (s.capturedSource = s.sourceBirth \/ IgnoreSourceBirth)
           /\ (s.rowBirth = s.guardBirth \/ IgnoreRowBirth)
           /\ s' = [s EXCEPT !.confirmed = TRUE,
                     !.invalidSourceConfirmed = (s.capturedSource # s.sourceBirth),
                     !.invalidRowConfirmed = (s.rowBirth # s.guardBirth)]
Retry == /\ s.row /\ s.nonce = 1 /\ OverwriteMatchingRow
         /\ s' = [s EXCEPT !.nonce = 2]
Stop == /\ ~s.stopped /\ s' = [s EXCEPT !.stopped = TRUE]
ColdReturn == /\ s.nativeReturned /\ ~s.coldReturned
              /\ s' = [s EXCEPT !.coldReturned = TRUE]
Release == /\ s.held /\ (s.coldReturned \/ EarlyRelease)
           /\ s' = [s EXCEPT !.held = FALSE, !.releasedEarly = ~s.coldReturned]
Next == Capture \/ SourceAba \/ Publish \/ NativeReturn \/ RowAba \/ Confirm \/ Retry \/ Stop \/ ColdReturn \/ Release
Spec == Init /\ [][Next]_vars
TypeOK == s.sourceBirth \in {1,2} /\ s.rowBirth \in {1,2}
AckHasActualLocalAdoption == s.row => s.local
AckConfirmsOriginalPreparation == ~s.invalidSourceConfirmed
AckConfirmsOriginalRowBirth == ~s.invalidRowConfirmed
AckMatchingRetryRetainsFirstRow == s.nonce = 1
AckReleaseWaitsActualColdReturn == ~s.releasedEarly
=============================================================================
