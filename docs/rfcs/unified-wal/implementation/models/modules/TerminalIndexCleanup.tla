----------------------- MODULE TerminalIndexCleanup -----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS IgnoreInFlightPut, ReleaseBeforeDelete, UseLatestTombstones, ReleaseOnCancel
VARIABLE state
Init == state = [alive |-> {1, 2}, native |-> {1, 2}, occupied |-> {1, 2},
                  oldPut |-> 1, reader |-> 1, gate |-> FALSE, paused |-> FALSE,
                  captured |-> {}, deleteIo |-> 0, phase |-> "idle", waiting |-> TRUE,
                  failed |-> FALSE, newWritten |-> FALSE]
Tombstone == \E key \in state.alive : state' = [state EXCEPT !.alive = @ \ {key}]
FinishOldPut == /\ state.oldPut = 1
                /\ state' = [state EXCEPT !.oldPut = 0, !.native = @ \cup {1}]
FinishRead == /\ state.reader = 1 /\ state' = [state EXCEPT !.reader = 0]
Prepare == /\ state.phase = "idle" /\ state.reader = 0
           /\ (state.oldPut = 0 \/ IgnoreInFlightPut)
           /\ state' = [state EXCEPT !.gate = TRUE, !.paused = TRUE,
                         !.captured = {1, 2} \ state.alive, !.phase = "prepared"]
StartDelete == /\ state.phase = "prepared" /\ state.gate
               /\ state' = [state EXCEPT !.phase = "deleting", !.deleteIo = 1]
Delete == /\ state.phase = "deleting" /\ state.deleteIo = 1
          /\ state' = [state EXCEPT !.native = @ \ state.captured,
                        !.deleteIo = 0, !.phase = "deleted"]
DeleteUnknown == /\ state.phase = "deleting" /\ state.deleteIo = 1
                 /\ \E partial \in SUBSET state.captured :
                      state' = [state EXCEPT !.native = @ \ partial, !.deleteIo = 0,
                                !.phase = "failed", !.failed = TRUE]
Prune == /\ (state.phase = "deleted" \/ (ReleaseBeforeDelete /\ state.phase = "prepared"))
         /\ state' = [state EXCEPT !.occupied = @ \ (IF UseLatestTombstones
                         THEN {1, 2} \ state.alive ELSE state.captured), !.phase = "pruned"]
Publish == /\ state.phase = "pruned" /\ state.deleteIo = 0
           /\ state' = [state EXCEPT !.phase = "published"]
PublicationUnknown == /\ state.phase = "pruned" /\ state.deleteIo = 0
                      /\ state' = [state EXCEPT !.phase = "failed", !.failed = TRUE]
Finish == /\ state.phase \in {"published", "failed"} /\ state.deleteIo = 0
          /\ state' = [state EXCEPT !.gate = FALSE, !.paused = FALSE, !.phase = "done"]
CancelWait == /\ state.waiting
              /\ state' = [state EXCEPT !.waiting = FALSE,
                            !.gate = IF ReleaseOnCancel /\ state.deleteIo = 1 THEN FALSE ELSE @]
NewWrite == /\ state.phase = "done" /\ ~state.gate /\ ~state.paused /\ ~state.newWritten
            /\ Cardinality(state.occupied) < 2
            /\ state' = [state EXCEPT !.occupied = @ \cup {3}, !.native = @ \cup {3}, !.newWritten = TRUE]
TypeOK == state \in [alive : SUBSET {1, 2}, native : SUBSET {1, 2, 3}, occupied : SUBSET {1, 2, 3},
                     oldPut : 0..1, reader : 0..1, gate : BOOLEAN, paused : BOOLEAN,
                     captured : SUBSET {1, 2}, deleteIo : 0..1,
                     phase : {"idle", "prepared", "deleting", "deleted", "pruned", "published", "failed", "done"},
                     waiting : BOOLEAN, failed : BOOLEAN, newWritten : BOOLEAN]
OnlyCapturedTerminalCoordinatesRetire == ({1, 2} \ state.occupied) \subseteq state.captured
                                        /\ state.captured \cap state.alive = {}
ReleasedCoordinatesHaveNoNativeRows == ({1, 2} \ state.occupied) \cap state.native = {}
MaintenanceGateHeldThroughActualDelete == state.deleteIo = 1 => state.gate /\ state.paused
CapacityBounded == Cardinality(state.native) <= 2 /\ Cardinality(state.occupied) <= 2
Next == Tombstone \/ FinishOldPut \/ FinishRead \/ Prepare \/ StartDelete \/ Delete \/ DeleteUnknown
        \/ Prune \/ Publish \/ PublicationUnknown \/ Finish \/ CancelWait \/ NewWrite
Spec == Init /\ [][Next]_state
=============================================================================
