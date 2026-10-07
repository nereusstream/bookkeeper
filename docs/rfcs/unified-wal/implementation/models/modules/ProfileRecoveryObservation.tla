-------------------- MODULE ProfileRecoveryObservation --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS AckQuorum, CountUnfencedMissing, CountUnknownMissing, IgnoreContext,
          ClaimBudgetTail, AllowAdd, ReleaseOnCancel, ClaimClose
VARIABLE s
vars == <<s>>
(* One readonly entry probe after a LAC/length anchor. Original Java recovery
   reads compute per-write-set W-A+1; this finite projection uses E=W=3.
   Missing is a sample, never full required-frontier or CLOSED authority. *)
Init == s \in [phase : {"probe"}, reply : [1..3 -> {"found", "missing", "unknown"}],
               fenced : [1..3 -> BOOLEAN], bound : [1..3 -> BOOLEAN], current : {TRUE},
               budget : BOOLEAN, waiting : {TRUE}, reserved : {TRUE},
               result : {"none"}, appended : {FALSE}, closed : {FALSE}]
ActualMissing == {i \in 1..3 : s.reply[i] = "missing" /\ s.fenced[i] /\ s.bound[i]}
ClaimedMissing == {i \in 1..3 : s.bound[i] /\ (s.fenced[i] \/ CountUnfencedMissing)
                   /\ (s.reply[i] = "missing" \/ (CountUnknownMissing /\ s.reply[i] = "unknown"))}
ProbeEnd == /\ s.phase = "probe"
            /\ LET absent == Cardinality(ClaimedMissing) >= 4 - AckQuorum
                    candidate == \E i \in 1..3 : s.reply[i] = "found" /\ s.bound[i]
                    outcome == IF ~s.current /\ ~IgnoreContext THEN "refused"
                               ELSE IF ~s.budget /\ ~ClaimBudgetTail THEN "deferred"
                               ELSE IF absent \/ (~s.budget /\ ClaimBudgetTail) THEN "absent"
                               ELSE IF candidate THEN "candidate" ELSE "refused"
                IN s' = [s EXCEPT !.phase = "done", !.reserved = FALSE, !.result = outcome,
                         !.closed = ClaimClose /\ outcome = "absent"]
ContextChanges == /\ s.phase = "probe" /\ s.current
                  /\ s' = [s EXCEPT !.current = FALSE]
Cancel == /\ s.phase = "probe" /\ s.waiting
          /\ s' = [s EXCEPT !.waiting = FALSE,
                   !.reserved = IF ReleaseOnCancel THEN FALSE ELSE @]
Append == /\ ~s.appended /\ s' = [s EXCEPT !.appended = AllowAdd]
Next == ProbeEnd \/ ContextChanges \/ Cancel \/ Append
Spec == Init /\ [][Next]_vars
TypeOK == s.result \in {"none", "absent", "candidate", "deferred", "refused"}
AbsenceHasFencedBoundCoverage == s.result = "absent" => Cardinality(ActualMissing) >= 4 - AckQuorum
EvidenceHasCurrentContext == s.result \in {"absent", "candidate"} => s.current
BudgetNeverMeansAbsence == s.result = "absent" => s.budget
ReadonlyNeverAdds == ~s.appended
CallerCancelKeepsReservation == s.phase = "probe" => s.reserved
ObservationNeverCloses == ~s.closed
=============================================================================
