------------------------- MODULE WholeAllocationFree -------------------------
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
CONSTANTS SkipLiveRecord, ReleasePendingGate, TrustRebootCounts, ApplyBeforeForce
ASSUME /\ SkipLiveRecord \in BOOLEAN /\ ReleasePendingGate \in BOOLEAN
       /\ TrustRebootCounts \in BOOLEAN /\ ApplyBeforeForce \in BOOLEAN
VARIABLES alive, actualIo, visibleIo, readers, gate, phase, healthy, crashed,
          verifiedDead, proofIo, generation, forceSucceeded, retired, waiting, cd, cdFirst, cdSecond
freeVars == <<alive, actualIo, visibleIo, readers, gate, phase, healthy, crashed,
          verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
vars == <<freeVars, cd, cdFirst, cdSecond>>
FreeInit == /\ alive = {1, 2} /\ actualIo = 0 /\ visibleIo = 0 /\ readers = 0
        /\ gate = FALSE /\ phase = "idle" /\ healthy = TRUE /\ crashed = FALSE
        /\ verifiedDead = FALSE /\ proofIo = 0 /\ generation = 1
        /\ forceSucceeded = FALSE /\ retired = FALSE /\ waiting = TRUE
Tombstone ==
    \E ledger \in alive :
       /\ alive' = alive \ {ledger}
       /\ UNCHANGED <<actualIo, visibleIo, readers, gate, phase, healthy, crashed,
                      verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
StartIo == /\ healthy /\ ~gate /\ generation = 1 /\ actualIo = 0
           /\ actualIo' = 1 /\ visibleIo' = 1
           /\ UNCHANGED <<alive, readers, gate, phase, healthy, crashed, verifiedDead,
                          proofIo, generation, forceSucceeded, retired, waiting>>
FinishIo == /\ actualIo = 1 /\ actualIo' = 0 /\ visibleIo' = 0
            /\ UNCHANGED <<alive, readers, gate, phase, healthy, crashed, verifiedDead,
                           proofIo, generation, forceSucceeded, retired, waiting>>
Pin == /\ healthy /\ ~gate /\ generation = 1 /\ readers = 0 /\ readers' = 1
       /\ UNCHANGED <<alive, actualIo, visibleIo, gate, phase, healthy, crashed,
                      verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
Unpin == /\ readers = 1 /\ readers' = 0
         /\ UNCHANGED <<alive, actualIo, visibleIo, gate, phase, healthy, crashed,
                        verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
Prepare == /\ phase = "idle" /\ generation = 1 /\ ~gate
           /\ (healthy \/ (TrustRebootCounts /\ crashed))
           /\ visibleIo = 0 /\ readers = 0
           /\ (actualIo = 0 \/ TrustRebootCounts)
           /\ (alive = {} \/ SkipLiveRecord)
           /\ gate' = TRUE /\ phase' = "prepared"
           /\ verifiedDead' = (alive = {}) /\ proofIo' = actualIo
           /\ UNCHANGED <<alive, actualIo, visibleIo, readers, healthy, crashed,
                          generation, forceSucceeded, retired, waiting>>
Submit == /\ phase = "prepared" /\ gate /\ phase' = "submitted"
          /\ UNCHANGED <<alive, actualIo, visibleIo, readers, gate, healthy, crashed,
                         verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
Force == /\ phase = "submitted" /\ phase' = "durable" /\ forceSucceeded' = TRUE
         /\ UNCHANGED <<alive, actualIo, visibleIo, readers, gate, healthy, crashed,
                        verifiedDead, proofIo, generation, retired, waiting>>
ForceUnknown == /\ phase = "submitted" /\ phase' = "failed" /\ healthy' = FALSE
                /\ UNCHANGED <<alive, actualIo, visibleIo, readers, gate, crashed,
                               verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
Apply == /\ (phase = "durable" \/ (ApplyBeforeForce /\ phase = "submitted"))
         /\ phase' = "applied" /\ generation' = 2 /\ retired' = TRUE /\ gate' = FALSE
         /\ UNCHANGED <<alive, actualIo, visibleIo, readers, healthy, crashed,
                        verifiedDead, proofIo, forceSucceeded, waiting>>
OwnerResolve == /\ phase \in {"applied", "failed"} /\ phase' = "resolved" /\ gate' = FALSE
                /\ UNCHANGED <<alive, actualIo, visibleIo, readers, healthy, crashed,
                               verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
Duplicate == /\ phase = "resolved" /\ generation = 2 /\ UNCHANGED vars
CancelWait == /\ waiting /\ waiting' = FALSE
              /\ UNCHANGED <<alive, actualIo, visibleIo, readers, gate, phase, healthy, crashed,
                             verifiedDead, proofIo, generation, forceSucceeded, retired>>
EarlyClose == /\ ReleasePendingGate /\ phase \in {"submitted", "durable"}
              /\ gate /\ gate' = FALSE
              /\ UNCHANGED <<alive, actualIo, visibleIo, readers, phase, healthy, crashed,
                             verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
Crash == /\ ~crashed /\ crashed' = TRUE /\ healthy' = FALSE /\ visibleIo' = 0
         /\ readers' = 0 /\ gate' = FALSE /\ phase' = "idle"
         /\ UNCHANGED <<alive, actualIo, verifiedDead, proofIo, generation, forceSucceeded, retired, waiting>>
TypeOK == /\ alive \subseteq {1, 2} /\ actualIo \in 0..1 /\ visibleIo \in 0..1
          /\ readers \in 0..1 /\ gate \in BOOLEAN /\ healthy \in BOOLEAN /\ crashed \in BOOLEAN
          /\ phase \in {"idle", "prepared", "submitted", "durable", "applied", "failed", "resolved"}
          /\ verifiedDead \in BOOLEAN /\ proofIo \in 0..1 /\ generation \in {1, 2}
          /\ forceSucceeded \in BOOLEAN /\ retired \in BOOLEAN /\ waiting \in BOOLEAN
EveryOccupantWasTerminal == generation = 2 => verifiedDead
SubmittedFreeRetainsProtection == phase \in {"submitted", "durable"} => gate
RebootCountsDoNotProveOldIoTerminal == generation = 2 => actualIo = 0 /\ proofIo = 0 /\ readers = 0
FreeAuthorityHasBarrier == generation = 2 => forceSucceeded /\ retired
FreeNext == Tombstone \/ StartIo \/ FinishIo \/ Pin \/ Unpin \/ Prepare \/ Submit \/ Force
        \/ ForceUnknown \/ Apply \/ OwnerResolve \/ Duplicate \/ CancelWait \/ EarlyClose \/ Crash
Init == FreeInit /\ cd = 0 /\ cdFirst = 0 /\ cdSecond = 0
Next == FreeNext /\ UNCHANGED <<cd, cdFirst, cdSecond>>
Spec == Init /\ [][Next]_vars


(* C+D composition: two logical deletions share one physical allocation at
   one retained target, whose offline state blocks local delivery but not
   logical success. The original logical and allocator actions execute
   together; a logical result supplies no local tombstone or FREE fact.
   Native record bytes, ZK birth, fsync and fencing remain Java inputs.
   Crash retires this local episode; no restart/rejoin or liveness claim. *)
CDMutation == "none"
CDEarlyDelivery == "early-delivery"
CDSkipLocalForce == "skip-local-force"
CDDropNeighbour == "drop-neighbour"
CDQuorumFree == "quorum-free"
CDFirst == INSTANCE ProfileLogicalDelete WITH
    s <- cdFirst, SkipClose <- FALSE, EarlyTargets <- FALSE,
    IgnoreHistory <- FALSE, EarlyLogical <- FALSE, IgnoreReference <- FALSE,
    ReleaseOnCancel <- FALSE, WaitOffline <- FALSE
CDSecond == INSTANCE ProfileLogicalDelete WITH
    s <- cdSecond, SkipClose <- FALSE, EarlyTargets <- FALSE,
    IgnoreHistory <- FALSE, EarlyLogical <- FALSE, IgnoreReference <- FALSE,
    ReleaseOnCancel <- FALSE, WaitOffline <- FALSE
CDDeletion(ledger) == IF ledger = 1 THEN cdFirst ELSE cdSecond
CDInit == /\ FreeInit /\ CDFirst!Init /\ CDSecond!Init
          /\ cdFirst.closed /\ cdFirst.fenced /\ cdSecond.closed /\ cdSecond.fenced
          /\ cdFirst.offline = cdSecond.offline
          /\ cd = [delivered |-> {}, forced |-> {}, applied |-> {}]
CDLogicalStep ==
    /\ (CDFirst!Next /\ UNCHANGED cdSecond)
       \/ (CDSecond!Next /\ UNCHANGED cdFirst)
    /\ UNCHANGED <<freeVars, cd>>
CDDeliver(ledger) ==
    /\ ledger \notin cd.delivered /\ CDDeletion(ledger).born
    /\ ~CDDeletion(ledger).offline
    /\ (CDDeletion(ledger).logical \/ CDMutation = "early-delivery")
    /\ cd' = [cd EXCEPT !.delivered = @ \cup {ledger}]
    /\ UNCHANGED <<freeVars, cdFirst, cdSecond>>
CDForceTombstone(ledger) ==
    /\ ledger \in cd.delivered \ cd.forced /\ healthy
    /\ cd' = [cd EXCEPT !.forced = @ \cup {ledger}]
    /\ UNCHANGED <<freeVars, cdFirst, cdSecond>>
CDFailTombstone(ledger) ==
    /\ ledger \in cd.delivered \ cd.forced /\ healthy
    /\ healthy' = FALSE
    /\ UNCHANGED <<alive, actualIo, visibleIo, readers, gate, phase, crashed,
                   verifiedDead, proofIo, generation, forceSucceeded, retired, waiting,
                   cd, cdFirst, cdSecond>>
CDApplyTombstone(ledger) ==
    /\ ledger \in cd.delivered \ cd.applied /\ healthy /\ generation = 1
    /\ (ledger \in cd.forced \/ CDMutation = "skip-local-force")
    /\ cd' = [cd EXCEPT !.applied = @ \cup {ledger}]
    /\ alive' = IF CDMutation = "drop-neighbour" THEN {} ELSE alive \ {ledger}
    /\ UNCHANGED <<actualIo, visibleIo, readers, gate, phase, healthy, crashed,
                   verifiedDead, proofIo, generation, forceSucceeded, retired, waiting,
                   cdFirst, cdSecond>>
CDPrepare ==
    /\ IF CDMutation = "quorum-free" /\ cd.applied # {}
       THEN /\ phase = "idle" /\ generation = 1 /\ healthy /\ ~gate
            /\ actualIo = 0 /\ readers = 0
            /\ gate' = TRUE /\ phase' = "prepared"
            /\ verifiedDead' = TRUE /\ proofIo' = 0
            /\ UNCHANGED <<alive, actualIo, visibleIo, readers, healthy, crashed,
                           generation, forceSucceeded, retired, waiting>>
       ELSE Prepare
    /\ UNCHANGED <<cd, cdFirst, cdSecond>>
CDFreeStep ==
    /\ StartIo \/ FinishIo \/ Pin \/ Unpin \/ Submit \/ Force \/ ForceUnknown
        \/ Apply \/ OwnerResolve \/ Duplicate \/ CancelWait \/ EarlyClose \/ Crash
    /\ UNCHANGED <<cd, cdFirst, cdSecond>>
CDNext == CDFreeStep \/ CDPrepare \/ CDLogicalStep
          \/ (\E ledger \in {1, 2} : CDDeliver(ledger)
                                     \/ CDForceTombstone(ledger) \/ CDFailTombstone(ledger)
                                     \/ CDApplyTombstone(ledger))
CDSpec == CDInit /\ [][CDNext]_vars
CDTypeOK == /\ TypeOK /\ CDFirst!TypeOK /\ CDSecond!TypeOK
            /\ \A set \in {cd.delivered, cd.forced, cd.applied} : set \subseteq {1, 2}
CDLogicalKeepsOriginalContract ==
    CDFirst!LogicalHasCompleteObligation /\ CDSecond!LogicalHasCompleteObligation
    /\ CDFirst!ReportRequiresManifestBirth /\ CDSecond!ReportRequiresManifestBirth
    /\ CDFirst!ColdOwnsGate /\ CDSecond!ColdOwnsGate
CDDeliveryHasLogicalObligation ==
    \A ledger \in cd.delivered : CDDeletion(ledger).logical /\ CDDeletion(ledger).seal
                               /\ CDDeletion(ledger).targets = CDFirst!AllTargets
CDLocalEffectHasForce == cd.applied \subseteq cd.forced
CDRemainingLedgerStaysAllocated == alive = {1, 2} \ cd.applied
CDFreeHasEveryLogicalAndLocalEffect == generation = 2 =>
    cd.applied = {1, 2} /\ cd.forced = {1, 2}
    /\ \A ledger \in {1, 2} : CDDeletion(ledger).logical
CDLogicalDoesNotNeedPhysicalFree ==
    CDFirst!OfflineDoesNotPreventLogical /\ CDSecond!OfflineDoesNotPreventLogical

=============================================================================
