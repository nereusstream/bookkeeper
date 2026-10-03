----------------------- MODULE WholeAllocationReuse -----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 *)
EXTENDS Naturals
CONSTANTS ReuseBeforeFree, CloseBeforeIo, TrustOldBytes
VARIABLE state
Init == state = [phase |-> "old", oldIo |-> 1, newIo |-> 0,
                  free |-> FALSE, grant |-> FALSE, used |-> 0, barrier |-> FALSE,
                  failed |-> FALSE, closed |-> FALSE, crashed |-> FALSE, result |-> "none"]
FinishOld == /\ state.oldIo = 1
             /\ state' = [state EXCEPT !.oldIo = 0]
Free == /\ state.phase = "old" /\ state.oldIo = 0 /\ ~state.crashed
        /\ state' = [state EXCEPT !.free = TRUE, !.phase = "free"]
Reuse == /\ (state.phase = "free" \/ (ReuseBeforeFree /\ state.phase = "old"))
         /\ ~state.crashed
         /\ state' = [state EXCEPT !.grant = TRUE, !.phase = "ready"]
StartWrite == /\ state.phase = "ready" /\ ~state.crashed /\ ~state.closed
              /\ state' = [state EXCEPT !.newIo = 1, !.phase = "writing", !.used = 1]
Force == /\ state.phase = "writing" /\ state.newIo = 1
         /\ state' = [state EXCEPT !.newIo = 0, !.barrier = TRUE, !.phase = "written"]
WriteUnknown == /\ state.phase = "writing" /\ state.newIo = 1
                /\ state' = [state EXCEPT !.newIo = 0, !.failed = TRUE, !.phase = "failed"]
Close == /\ state.grant /\ ~state.closed /\ ~state.crashed /\ ~state.failed
         /\ (state.newIo = 0 \/ CloseBeforeIo)
         /\ state' = [state EXCEPT !.closed = TRUE]
Crash == /\ ~state.crashed
         /\ state' = [state EXCEPT !.crashed = TRUE]
Recover == /\ state.crashed /\ state.closed /\ state.phase # "recovered"
           /\ state' = [state EXCEPT !.phase = "recovered",
                        !.result = IF TrustOldBytes THEN "old"
                                   ELSE IF state.used = 1 THEN "current" ELSE "none"]
TypeOK == state \in [phase : {"old", "free", "ready", "writing", "written", "failed", "recovered"},
                     oldIo : 0..1, newIo : 0..1, free : BOOLEAN, grant : BOOLEAN,
                     used : 0..1, barrier : BOOLEAN, failed : BOOLEAN, closed : BOOLEAN,
                     crashed : BOOLEAN, result : {"none", "old", "current"}]
ReuseHasTerminalOldIoAndFree == state.grant => state.free /\ state.oldIo = 0
ClosedTailHasActualDurableIo == state.closed => state.newIo = 0 /\ ~state.failed
                                              /\ (state.used = 0 \/ state.barrier)
OldBytesNeverBecomeCurrent == state.result # "old"
Next == FinishOld \/ Free \/ Reuse \/ StartWrite \/ Force \/ WriteUnknown \/ Close \/ Crash \/ Recover
Spec == Init /\ [][Next]_state
=============================================================================
