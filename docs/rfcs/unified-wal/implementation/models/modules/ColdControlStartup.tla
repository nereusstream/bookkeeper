------------------------ MODULE ColdControlStartup ------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS RotateBeforeRecovery, OpenBeforeRotation, ReadyAfterFailure
VARIABLE s
vars == <<s>>
(* One cold restart. Capacity, genuine CLOSED, writer exclusion, validation,
   sync and rotation outcomes are finite inputs. Existing ArenaRotation owns
   dual-root publication; this projection does not prove filesystem durability. *)
Init == \E closed \in BOOLEAN, protected \in BOOLEAN, valid \in BOOLEAN,
           needsRotation \in BOOLEAN, fits \in BOOLEAN :
          s = [stage |-> "recover", closed |-> closed, protected |-> protected,
               valid |-> valid, needsRotation |-> needsRotation, fits |-> fits,
               synced |-> FALSE, rotationStarted |-> FALSE, rotated |-> FALSE,
               openStarted |-> FALSE, opened |-> FALSE, ready |-> FALSE, failed |-> FALSE]
Recover == /\ s.stage = "recover"
           /\ \E syncSucceeded \in BOOLEAN :
                 LET valid == s.closed /\ s.protected /\ s.valid /\ syncSucceeded
                 IN s' = [s EXCEPT !.stage = IF valid THEN "capacity" ELSE "failed",
                           !.synced = valid, !.failed = ~valid]
PrematureRotation == /\ RotateBeforeRecovery /\ s.stage = "recover" /\ s.needsRotation /\ s.fits
                     /\ s' = [s EXCEPT !.stage = "rotation", !.rotationStarted = TRUE]
Capacity == /\ s.stage = "capacity"
            /\ IF ~s.needsRotation \/ OpenBeforeRotation
                  THEN s' = [s EXCEPT !.stage = "open"]
                  ELSE IF s.fits
                          THEN s' = [s EXCEPT !.stage = "rotation", !.rotationStarted = TRUE]
                          ELSE s' = [s EXCEPT !.stage = "failed", !.failed = TRUE]
Rotation == /\ s.stage = "rotation"
            /\ \E success \in BOOLEAN :
                  s' = [s EXCEPT !.stage = IF success THEN "open" ELSE "failed",
                            !.rotated = success, !.failed = ~success]
Open == /\ s.stage = "open"
        /\ \E durable \in BOOLEAN :
              s' = [s EXCEPT !.stage = IF durable THEN "owner" ELSE "failed",
                        !.openStarted = TRUE, !.opened = durable, !.failed = ~durable]
Publish == /\ s.stage = "owner" /\ s' = [s EXCEPT !.stage = "done", !.ready = TRUE]
Fail == /\ s.stage = "failed" /\ s' = [s EXCEPT !.stage = "done", !.ready = ReadyAfterFailure]
Next == Recover \/ PrematureRotation \/ Capacity \/ Rotation \/ Open \/ Publish \/ Fail
Spec == Init /\ [][Next]_vars
TypeOK == s.stage \in {"recover", "capacity", "rotation", "open", "owner", "failed", "done"}
RotationHasValidatedProtectedClosedData ==
    s.rotationStarted => s.closed /\ s.protected /\ s.valid /\ s.synced /\ s.fits
OpenHasRecoveredCapacity ==
    s.openStarted => s.synced /\ (~s.needsRotation \/ s.rotated)
ReadyHasDurableOpenAndNoFailure == s.ready => s.opened /\ ~s.failed
FreshJournalTooSmallNeverRotates == ~s.fits => ~s.rotationStarted
=============================================================================
