---------------------- MODULE PooledCompactionCapture ----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS ReleaseOnCancel, IgnoreMeasurement
VARIABLE s
vars == <<s>>
(* Two finite COPY chunks. The original coordinate limit bounds plan metadata;
   source payload uses no SegmentEntryInputs. A real guard and source identity
   check are represented by valid. Actual CRC, SHA, native coverage, whole-block
   proof, scheduling and fsync are implementation premises, not modeled bytes. *)
Init == \E rows \in 2..4, pool \in 1..2 :
          s = [stage |-> "measure", rows |-> rows, planned |-> 0, pool |-> pool,
               owned |-> 0, inputs |-> 0, valid |-> TRUE, capturedValid |-> TRUE,
               cancelled |-> FALSE, ioDone |-> FALSE, forced |-> FALSE,
               copies |-> 0, moves |-> 0, freed |-> FALSE]
Measure == /\ s.stage = "measure"
           /\ s' = [s EXCEPT !.planned = IF s.rows > 3 THEN 0 ELSE s.rows,
                    !.stage = IF s.rows > 3 THEN "refused" ELSE "ready"]
AcquireScratch == /\ s.stage = "ready"
                  /\ s' = [s EXCEPT !.owned = 1, !.stage = "scratch"]
AcquireCopy == /\ s.stage = "scratch"
               /\ s' = [s EXCEPT !.owned = IF s.pool = 1 THEN 0 ELSE 2,
                        !.ioDone = FALSE, !.stage = IF s.pool = 1 THEN "refused" ELSE "capture"]
ChangeSource == /\ s.stage \in {"ready", "scratch", "capture"} /\ s.valid
                /\ s' = [s EXCEPT !.valid = FALSE]
CaptureIoEnd == /\ s.stage = "capture" /\ ~s.ioDone
                /\ s' = [s EXCEPT !.ioDone = TRUE, !.capturedValid = s.valid]
CaptureOwnerEnd == /\ s.stage = "capture" /\ s.ioDone
                   /\ LET accept == s.capturedValid \/ IgnoreMeasurement IN
                        s' = [s EXCEPT !.owned = IF accept THEN 1 ELSE 0,
                              !.stage = IF accept THEN "write" ELSE "refused",
                              !.ioDone = FALSE, !.forced = FALSE]
WriteIoEnd == /\ s.stage = "write" /\ ~s.ioDone
              /\ \E barrier \in BOOLEAN :
                   s' = [s EXCEPT !.ioDone = TRUE, !.forced = barrier]
WriteOwnerEnd == /\ s.stage = "write" /\ s.ioDone
                 /\ s' = [s EXCEPT !.owned = 0,
                          !.copies = IF s.forced THEN @ + 1 ELSE @,
                          !.stage = IF s.forced THEN "move" ELSE "refused"]
Move == /\ s.stage = "move"
        /\ s' = [s EXCEPT !.moves = @ + 1, !.stage = IF s.moves = 1 THEN "proof" ELSE "ready"]
Free == /\ s.stage = "proof" /\ s.moves = 2
        /\ s' = [s EXCEPT !.freed = TRUE, !.stage = "done"]
Cancel == /\ s.stage \in {"capture", "write"} /\ ~s.cancelled
          /\ s' = [s EXCEPT !.cancelled = TRUE, !.owned = IF ReleaseOnCancel THEN 0 ELSE @]
Next == Measure \/ AcquireScratch \/ AcquireCopy \/ ChangeSource \/ CaptureIoEnd
        \/ CaptureOwnerEnd \/ WriteIoEnd \/ WriteOwnerEnd \/ Move \/ Free \/ Cancel
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.stage \in {"measure", "ready", "scratch", "capture", "write", "move", "proof", "refused", "done"}
          /\ s.owned \in 0..s.pool /\ s.planned \in 0..3 /\ s.inputs = 0
          /\ s.copies \in 0..2 /\ s.moves \in 0..s.copies
CaptureAndCopyKeepOwnedBuffers == /\ (s.stage = "capture" => s.owned = 2)
                                 /\ (s.stage = "write" => s.owned = 1)
CopyMatchesMeasurement == s.stage \in {"write", "move", "proof", "done"} => s.capturedValid
FreeHasAllDurableMoves == s.freed => s.moves = 2 /\ s.copies = 2 /\ s.owned = 0
=============================================================================
