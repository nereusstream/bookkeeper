-------------------- MODULE StartupRequirements --------------------
(* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License. *)
EXTENDS Naturals
CONSTANTS SkipPreOpenCheck, SkipPublicationBirthCheck, AllowCallerSubstitution
VARIABLE s
vars == <<s>>
(* Complete codec validation, immutable unique birth markers, atomic ZooKeeper
   multi, and actual graph recovery/readiness are inputs. This model does not
   produce an old-binary fence, credential/OS proof, TLS, fsync, or recovery.
   A changed source is a different physical birth even with identical policy. *)
Init == s = [phase |-> 0, sourceBirth |-> 1, observedBirth |-> 0,
             supported |-> FALSE, callerChanged |-> FALSE, opened |-> FALSE,
             capturedBirth |-> 0, published |-> FALSE, changed |-> FALSE,
             unsafeOpen |-> FALSE, substituted |-> FALSE, stalePublication |-> FALSE]
ReadRequired == /\ s.phase = 0
                /\ \E supported \in BOOLEAN :
                   s' = [s EXCEPT !.phase = 1, !.observedBirth = s.sourceBirth, !.supported = supported]
SubstituteCaller == /\ s.phase = 2 /\ ~s.callerChanged
                    /\ s' = [s EXCEPT !.callerChanged = TRUE]
ConstructGraph == /\ s.phase = 1
                  /\ LET allowed == s.supported \/ SkipPreOpenCheck
                     IN s' = [s EXCEPT !.phase = IF allowed THEN 2 ELSE 5, !.opened = allowed,
                              !.unsafeOpen = allowed /\ ~s.supported]
RecreateSource == /\ s.phase \in 1..3 /\ ~s.changed
                  /\ s' = [s EXCEPT !.sourceBirth = 2, !.changed = TRUE]
CapturePublication == /\ s.phase = 2
                      /\ LET matching == s.sourceBirth = s.observedBirth
                                            /\ (~s.callerChanged \/ AllowCallerSubstitution)
                         IN s' = [s EXCEPT !.phase = IF matching THEN 3 ELSE 5,
                                  !.capturedBirth = s.observedBirth,
                                  !.substituted = matching /\ s.callerChanged]
PublishWritable == /\ s.phase = 3
                   /\ LET matching == s.sourceBirth = s.capturedBirth /\ s.capturedBirth = s.observedBirth
                          allowed == matching \/ SkipPublicationBirthCheck
                      IN s' = [s EXCEPT !.phase = 4, !.published = allowed,
                               !.stalePublication = allowed /\ ~matching]
Complete == /\ s.phase = 4 /\ s' = [s EXCEPT !.phase = 5]
Next == ReadRequired \/ SubstituteCaller \/ ConstructGraph \/ RecreateSource
        \/ CapturePublication \/ PublishWritable \/ Complete
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in 0..5 /\ s.sourceBirth \in 1..2
          /\ s.observedBirth \in 0..2 /\ s.capturedBirth \in 0..2
StorageRequiresSupportedPreOpenSource == ~s.unsafeOpen
CallerCannotSubstituteBoundRequirements == ~s.substituted
WritableUsesFrozenSourceAtPublication == ~s.stalePublication
=====================================================================
