-------------------- MODULE StartupDataExclusion --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals
CONSTANTS IgnoreDataLock, ReleaseAtHandoff, LeakOwnedOnCleanup, DropForeignOnCleanup
VARIABLE s
vars == <<s>>
(* One graph restart and one optional foreign DATA writer. Native exclusion,
   validated storage identity, successful recovery barriers and actual terminal
   startup cleanup are inputs. No compatibility/delete facts or service readiness
   are inferred; accepted runtime DATA/drain is covered by the separate slices. *)
Init == \E foreign \in BOOLEAN :
    s = [phase |-> "new", scope |-> IF foreign THEN "foreign" ELSE "none",
         initialForeign |-> foreign, foreignReleased |-> FALSE, unsafeRecovery |-> FALSE]
Acquire == /\ s.phase = "new" /\ s.scope = "none"
           /\ s' = [s EXCEPT !.phase = "locked", !.scope = "attempt"]
RejectBusy == /\ s.phase = "new" /\ s.scope = "foreign"
              /\ s' = [s EXCEPT !.phase = "failed"]
Recover == /\ (s.phase = "locked" \/ (IgnoreDataLock /\ s.phase = "new"))
           /\ s' = [s EXCEPT !.phase = "controls", !.unsafeRecovery = s.scope # "attempt"]
AdoptFiles == /\ s.phase = "controls" /\ s.scope = "attempt"
              /\ s' = [s EXCEPT !.phase = "files",
                       !.scope = IF ReleaseAtHandoff THEN "none" ELSE "files"]
Bootstrap == /\ s.phase = "files" /\ s' = [s EXCEPT !.phase = "ready"]
Fail == /\ s.phase \in {"locked", "controls", "files"}
        /\ s' = [s EXCEPT !.phase = "failed"]
Cleanup == /\ s.phase = "failed"
           /\ s' = [s EXCEPT !.phase = "closed",
                    !.scope = IF (s.scope \in {"attempt", "files"} /\ ~LeakOwnedOnCleanup)
                              \/ (s.scope = "foreign" /\ DropForeignOnCleanup)
                              THEN "none" ELSE @]
Close == /\ s.phase = "ready"
         /\ s' = [s EXCEPT !.phase = "closed", !.scope = "none"]
ForeignRelease == /\ s.scope = "foreign" /\ ~s.foreignReleased
                  /\ s' = [s EXCEPT !.scope = "none", !.foreignReleased = TRUE]
Next == Acquire \/ RejectBusy \/ Recover \/ AdoptFiles \/ Bootstrap \/ Fail
        \/ Cleanup \/ Close \/ ForeignRelease
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.phase \in {"new", "locked", "controls", "files", "ready", "failed", "closed"}
          /\ s.scope \in {"none", "foreign", "attempt", "files"}
          /\ s.initialForeign \in BOOLEAN /\ s.foreignReleased \in BOOLEAN
          /\ s.unsafeRecovery \in BOOLEAN
RecoveryStartsWithOwnExclusion == ~s.unsafeRecovery
FileHandoffKeepsExclusion == s.phase \in {"files", "ready"} => s.scope = "files"
ClosedAttemptHasNoOwnedScope == s.phase = "closed" => s.scope \notin {"attempt", "files"}
ForeignWriterKeepsItsScope == s.initialForeign /\ ~s.foreignReleased => s.scope = "foreign"
=====================================================================
