------------------------- MODULE ConditionalMove -------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS ApplyBeforeForce, IgnorePredecessor, ReleaseOnCancel
VARIABLE state
Ops == {1, 2, 3}
Keys == {1, 2}
Pred(op) == IF op = 3 THEN 1 ELSE 0
Empty == [key \in Keys |-> 0]
RECURSIVE Replay(_)
Replay(history) == IF Len(history) = 0 THEN Empty
                   ELSE LET prior == Replay(SubSeq(history, 1, Len(history) - 1))
                            op == history[Len(history)]
                        IN IF \A key \in Keys : prior[key] = Pred(op)
                           THEN [key \in Keys |-> op] ELSE prior
Init == state = [copies |-> {}, log |-> <<>>, durable |-> 0, applied |-> 0,
                  selector |-> Empty, guard |-> 0, reading |-> FALSE,
                  readSelector |-> Empty, waiting |-> TRUE]
Copy(op) == /\ op \notin state.copies /\ state.guard = 0
            /\ state' = [state EXCEPT !.copies = @ \cup {op}]
ReadStart == /\ ~state.reading /\ state.guard = 0
             /\ state' = [state EXCEPT !.reading = TRUE, !.readSelector = state.selector]
ReadFinish == /\ state.reading /\ state' = [state EXCEPT !.reading = FALSE]
Prepare(op) == /\ op \in state.copies /\ state.guard = 0 /\ ~state.reading
               /\ op \notin {state.log[i] : i \in 1..Len(state.log)}
               /\ (op # 3 \/ 1 \in {state.log[i] : i \in 1..state.applied})
               /\ state' = [state EXCEPT !.guard = op]
AppendControl == /\ state.guard # 0
          /\ state.guard \notin {state.log[i] : i \in 1..Len(state.log)}
          /\ state' = [state EXCEPT !.log = Append(@, state.guard)]
Force == /\ state.durable < Len(state.log)
         /\ state' = [state EXCEPT !.durable = @ + 1]
Apply == /\ state.applied < state.durable \/ (ApplyBeforeForce /\ state.applied < Len(state.log))
         /\ LET op == state.log[state.applied + 1]
                wins == IgnorePredecessor \/ (\A key \in Keys : state.selector[key] = Pred(op))
            IN state' = [state EXCEPT !.applied = @ + 1,
                           !.selector = IF wins THEN [key \in Keys |-> op] ELSE @]
Finish == /\ state.guard # 0 /\ state.guard \in {state.log[i] : i \in 1..Len(state.log)}
          /\ state.applied = Len(state.log) /\ state.durable = Len(state.log)
          /\ state' = [state EXCEPT !.guard = 0]
CrashReplay == /\ state.applied < state.durable
               /\ state' = [state EXCEPT !.log = SubSeq(@, 1, state.durable),
                             !.applied = state.durable,
                             !.selector = Replay(SubSeq(state.log, 1, state.durable)),
                             !.guard = 0, !.reading = FALSE]
CancelWait == /\ state.waiting
              /\ state' = [state EXCEPT !.waiting = FALSE,
                            !.guard = IF ReleaseOnCancel THEN 0 ELSE @]
TypeOK == /\ state.copies \subseteq Ops /\ state.log \in Seq(Ops) /\ Len(state.log) <= 3
          /\ state.durable \in 0..Len(state.log) /\ state.applied \in 0..Len(state.log)
          /\ state.selector \in [Keys -> 0..3] /\ state.guard \in 0..3
SelectorHasBarrier == state.applied <= state.durable
SelectorMatchesReplay == state.selector = Replay(SubSeq(state.log, 1, state.applied))
PendingMoveKeepsProtection == Len(state.log) > state.applied => state.guard # 0
ReadPinKeepsSelector == state.reading => state.selector = state.readSelector
CopyNeverSelects == state.applied = 0 => state.selector = Empty
Next == (\E op \in Ops : Copy(op) \/ Prepare(op)) \/ ReadStart \/ ReadFinish \/ AppendControl
        \/ Force \/ Apply \/ Finish \/ CrashReplay \/ CancelWait
Spec == Init /\ [][Next]_state
=============================================================================
