-------------------------- MODULE SegmentAllocator -------------------------
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
EXTENDS UnifiedWalTypes, FiniteSets

(* Model C, current managed-session/physical-prefix/recovery scope only.
   Grants, identity, framing and immutable Bookie eligibility are validated
   inputs, not conclusions of this model. No MOVE/FREE/index/quorum model. *)
CONSTANTS Streams, Batches, Files, Epochs, Crashes, PowerLosses,
          RequireDataBarrier, RequireControlSync, RequireClosedSession, RequirePrefixIoReturn
ASSUME /\ Streams >= 1 /\ Batches >= 1 /\ Files >= 1
       /\ Epochs >= 1 /\ Crashes >= 1 /\ PowerLosses >= 1
       /\ RequireDataBarrier \in BOOLEAN
       /\ RequireControlSync \in BOOLEAN
       /\ RequireClosedSession \in BOOLEAN
       /\ RequirePrefixIoReturn \in BOOLEAN
BatchSet == (1..Streams) \X (1..Batches)
Phases == {"new", "opening", "live", "closing", "closed", "crashed", "scan", "ready"}

VARIABLES phase, controlLength, controlDurable, controlApplied,
          submitted, inflight, written, dataDurable, successes,
          runtimeFault, faultSeen, faultDurable, controlSynced, validated,
          dataSynced, crashCount, powerCount, ioReturned
vars == <<phase, controlLength, controlDurable, controlApplied,
          submitted, inflight, written, dataDurable, successes,
          runtimeFault, faultSeen, faultDurable, controlSynced, validated,
          dataSynced, crashCount, powerCount, ioReturned>>

Init == /\ phase = "new"
        /\ controlLength = 0 /\ controlDurable = 0 /\ controlApplied = 0
        /\ submitted = {} /\ inflight = {} /\ written = {}
        /\ dataDurable = {} /\ successes = {}
        /\ runtimeFault = {} /\ faultSeen = FALSE /\ faultDurable = {}
        /\ controlSynced = FALSE /\ validated = FALSE /\ dataSynced = FALSE
        /\ crashCount = 0 /\ powerCount = 0 /\ ioReturned = {}

AppendOpen ==
    /\ phase \in {"new", "ready"}
    /\ controlLength < 2 * Epochs
    /\ (controlLength = 0 \/ ClosedControl(controlApplied))
    /\ controlLength' = controlLength + 1 /\ phase' = "opening"
    /\ UNCHANGED <<controlDurable, controlApplied, submitted, inflight, written,
                   dataDurable, successes, runtimeFault, faultSeen, faultDurable,
                   controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

ForceControl ==
    /\ phase \in {"opening", "closing"} /\ controlDurable < controlLength
    /\ controlDurable' = controlLength
    /\ UNCHANGED <<phase, controlLength, controlApplied, submitted, inflight,
                   written, dataDurable, successes, runtimeFault, faultSeen,
                   faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

ApplyControl ==
    /\ phase \in {"opening", "closing"} /\ controlDurable = controlLength
    /\ controlApplied' = controlLength
    /\ phase' = IF OpenControl(controlLength) THEN "live" ELSE "closed"
    /\ UNCHANGED <<controlLength, controlDurable, submitted, inflight, written,
                   dataDurable, successes, runtimeFault, faultSeen, faultDurable,
                   controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

Submit(batch) ==
    /\ phase = "live" /\ FileOf(batch, Files) \notin runtimeFault
    /\ FileOf(batch, Files) \notin faultDurable
    /\ batch \notin submitted
    /\ (batch[2] = 1 \/ <<batch[1], batch[2] - 1>> \in submitted)
    /\ submitted' = submitted \cup {batch} /\ inflight' = inflight \cup {batch}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, written,
                   dataDurable, successes, runtimeFault, faultSeen, faultDurable,
                   controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

WriteComplete(batch) ==
    /\ phase = "live" /\ batch \in inflight /\ batch \notin written
    /\ written' = written \cup {batch}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, submitted,
                   inflight, dataDurable, successes, runtimeFault, faultSeen,
                   faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

CoverFileBarrier(file) ==
    LET covered == {b \in written \cap inflight : FileOf(b, Files) = file}
    IN /\ phase = "live" /\ covered \ dataDurable # {}
       /\ dataDurable' = dataDurable \cup covered
       /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied,
                      submitted, inflight, written, successes, runtimeFault, faultSeen,
                      faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

(* File coverage does not terminate another task's force or return its loan.
   CompleteIo and FailData are actual task-return events, independent from
   the file-level durability fact and later owner publication. *)
CompleteIo(batch) ==
    /\ phase = "live" /\ batch \in inflight
    /\ IF RequireDataBarrier THEN batch \in dataDurable ELSE batch \in written
    /\ ioReturned' = ioReturned \cup {batch}
    /\ inflight' = inflight \ {batch}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, submitted,
                   written, dataDurable, successes, runtimeFault, faultSeen,
                   faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount>>

PublishLive(batch) ==
    /\ phase = "live" /\ FileOf(batch, Files) \notin runtimeFault
    /\ FileOf(batch, Files) \notin faultDurable
    /\ batch \in written /\ batch \notin successes /\ batch \in ioReturned
    /\ (~RequirePrefixIoReturn \/ PrefixInSet(batch, ioReturned))
    /\ IF RequireDataBarrier THEN PrefixInSet(batch, dataDurable)
       ELSE PrefixInSet(batch, written)
    /\ successes' = successes \cup {batch}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied,
                   submitted, inflight, written, dataDurable, runtimeFault,
                   faultSeen, faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

FailData(batch) ==
    /\ phase = "live" /\ batch \in inflight
    /\ runtimeFault' = runtimeFault \cup {FileOf(batch, Files)} /\ faultSeen' = TRUE
    /\ inflight' = inflight \ {batch} /\ ioReturned' = ioReturned \cup {batch}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, submitted,
                   written, dataDurable, successes, faultDurable, controlSynced,
                   validated, dataSynced, crashCount, powerCount>>

PersistFault(file) ==
    /\ file \in runtimeFault \ faultDurable /\ phase = "live"
    /\ faultDurable' = faultDurable \cup {file}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, submitted,
                   inflight, written, dataDurable, successes, runtimeFault, faultSeen,
                   controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

AppendClose ==
    /\ phase = "live" /\ runtimeFault = {} /\ faultDurable = {} /\ inflight = {}
    /\ written \subseteq dataDurable /\ OpenControl(controlApplied)
    /\ controlLength' = controlLength + 1 /\ phase' = "closing"
    /\ UNCHANGED <<controlDurable, controlApplied, submitted, inflight, written,
                   dataDurable, successes, runtimeFault, faultSeen, faultDurable,
                   controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

CrashProcess ==
    /\ phase # "crashed" /\ phase # "new" /\ crashCount < Crashes
    /\ phase' = "crashed" /\ crashCount' = crashCount + 1
    /\ inflight' = {} /\ runtimeFault' = {} /\ controlApplied' = 0
    /\ controlSynced' = FALSE /\ validated' = FALSE /\ dataSynced' = FALSE
    (* faultSeen is a test oracle, unavailable to the new process. *)
    /\ UNCHANGED <<controlLength, controlDurable, submitted, written, dataDurable,
                   successes, faultSeen, faultDurable, powerCount, ioReturned>>

PowerLoss ==
    /\ phase = "crashed" /\ powerCount < PowerLosses
    /\ controlLength' = controlDurable /\ written' = dataDurable
    /\ powerCount' = powerCount + 1
    /\ UNCHANGED <<phase, controlDurable, controlApplied, submitted, inflight,
                   dataDurable, successes, runtimeFault, faultSeen, faultDurable,
                   controlSynced, validated, dataSynced, crashCount, ioReturned>>

StartRecovery ==
    /\ phase = "crashed" /\ controlLength > 0 /\ faultDurable = {}
    /\ phase' = "scan"
    /\ UNCHANGED <<controlLength, controlDurable, controlApplied, submitted,
                   inflight, written, dataDurable, successes, runtimeFault,
                   faultSeen, faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

RecoverControl ==
    /\ phase = "scan" /\ ~controlSynced
    /\ controlDurable' = IF RequireControlSync THEN controlLength ELSE controlDurable
    /\ controlApplied' = controlLength /\ controlSynced' = TRUE
    /\ UNCHANGED <<phase, controlLength, submitted, inflight, written, dataDurable,
                   successes, runtimeFault, faultSeen, faultDurable, validated,
                   dataSynced, crashCount, powerCount, ioReturned>>

ValidateData ==
    /\ phase = "scan" /\ controlSynced /\ ~validated
    /\ \A batch \in written : PrefixInSet(batch, written)
    /\ validated' = TRUE
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, submitted,
                   inflight, written, dataDurable, successes, runtimeFault, faultSeen,
                   faultDurable, controlSynced, dataSynced, crashCount, powerCount, ioReturned>>

RecoverDataBarrier ==
    /\ phase = "scan" /\ validated /\ ~dataSynced
    /\ dataDurable' = dataDurable \cup written /\ dataSynced' = TRUE
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied, submitted,
                   inflight, written, successes, runtimeFault, faultSeen, faultDurable,
                   controlSynced, validated, crashCount, powerCount, ioReturned>>

QualifyRecovered ==
    /\ phase = "scan" /\ controlSynced /\ validated /\ dataSynced /\ faultDurable = {}
    /\ (~RequireClosedSession \/ ClosedControl(controlApplied))
    /\ phase' = "ready"
    /\ UNCHANGED <<controlLength, controlDurable, controlApplied, submitted,
                   inflight, written, dataDurable, successes, runtimeFault,
                   faultSeen, faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

PublishRecovered(batch) ==
    /\ phase = "ready" /\ batch \in written /\ batch \notin successes
    /\ successes' = successes \cup {batch}
    /\ UNCHANGED <<phase, controlLength, controlDurable, controlApplied,
                   submitted, inflight, written, dataDurable, runtimeFault,
                   faultSeen, faultDurable, controlSynced, validated, dataSynced, crashCount, powerCount, ioReturned>>

Next == AppendOpen \/ ForceControl \/ ApplyControl \/ AppendClose
        \/ CrashProcess \/ PowerLoss \/ StartRecovery \/ RecoverControl
        \/ ValidateData \/ RecoverDataBarrier \/ QualifyRecovered
        \/ (\E b \in BatchSet : Submit(b) \/ WriteComplete(b) \/ CompleteIo(b) \/ PublishLive(b)
                               \/ FailData(b) \/ PublishRecovered(b))
        \/ (\E f \in 1..Files : CoverFileBarrier(f) \/ PersistFault(f))

TypeOK == /\ phase \in Phases
          /\ controlLength \in 0..(2 * Epochs)
          /\ controlDurable \in 0..(2 * Epochs) /\ controlApplied \in 0..(2 * Epochs)
          /\ \A set \in {submitted, inflight, written, dataDurable, successes, ioReturned} : set \subseteq BatchSet
          /\ runtimeFault \subseteq 1..Files /\ faultDurable \subseteq 1..Files
          /\ \A flag \in {faultSeen, controlSynced, validated, dataSynced} :
                    flag \in BOOLEAN
          /\ crashCount \in 0..Crashes /\ powerCount \in 0..PowerLosses
ControlHasBarrier == controlApplied <= controlDurable /\ controlDurable <= controlLength
DataSuccessHasBarrier == successes \subseteq dataDurable
PhysicalSuccessIsContiguous == \A batch \in successes : PrefixInSet(batch, dataDurable)
PhysicalSuccessWaitsIoReturn == \A batch \in successes : PrefixInSet(batch, ioReturned)
ProtectionWaitsActualIoReturn == crashCount = 0 => submitted \ ioReturned \subseteq inflight
KnownErrorRemainsIsolated == phase = "ready" => ~faultSeen
RecoveredDataIsDurable == phase = "ready" => written \subseteq dataDurable
LiveWriterHasOpenAuthority == phase = "live" =>
    OpenControl(controlApplied) /\ controlApplied = controlDurable /\ controlDurable = controlLength

Spec == Init /\ [][Next]_vars
=============================================================================
