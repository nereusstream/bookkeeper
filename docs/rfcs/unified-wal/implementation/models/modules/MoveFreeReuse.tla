-------------------------- MODULE MoveFreeReuse --------------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS FreeCurrent, IgnoreDestinationGrant, IgnoreSelectorCut, RecoverOldBytes
VARIABLE state
Locations == {"A", "B", "C"}
Victims == {"A", "B"}
Pred(dest) == IF dest = "B" THEN "A" ELSE "B"
Init == state = [copies |-> {}, selected |-> "A", alive |-> Locations,
                 freed |-> {}, reused |-> {}, attempted |-> {}, cut |-> 0,
                 pending |-> "NONE", target |-> "A", phase |-> 0, proofCut |-> 0,
                 advanced |-> FALSE, badCut |-> FALSE, ready |-> FALSE, recovered |-> "A"]
Copy(dest) == /\ dest \in {"B", "C"} \cap state.alive /\ dest \notin state.copies
              /\ state.pending = "NONE"
              /\ state' = [state EXCEPT !.copies = @ \cup {dest}]
PrepareMove(dest) == /\ dest \in state.copies /\ dest \notin state.attempted
                     /\ state.pending = "NONE"
                     /\ state' = [state EXCEPT !.pending = "MOVE", !.target = dest, !.phase = 0]
PrepareFree(victim) == /\ victim \in Victims \cap state.alive /\ victim \notin state.reused
                       /\ victim # state.selected \/ FreeCurrent
                       /\ state.pending = "NONE"
                       /\ state' = [state EXCEPT !.pending = "FREE", !.target = victim,
                                     !.phase = 0, !.proofCut = state.cut]
PrepareReuse(victim) == /\ victim \in state.freed /\ victim \notin state.reused
                        /\ state.pending = "NONE"
                        /\ state' = [state EXCEPT !.pending = "REUSE", !.target = victim, !.phase = 0]
AdvanceControl == /\ ~state.advanced /\ state.phase = 0
                   /\ state' = [state EXCEPT !.advanced = TRUE, !.cut = @ + 1]
AppendControl == /\ state.pending # "NONE" /\ state.phase = 0
                 /\ state' = [state EXCEPT !.phase = 1]
Force == /\ state.pending # "NONE" /\ state.phase = 1
         /\ state' = [state EXCEPT !.phase = 2]
ApplyMove == /\ state.pending = "MOVE" /\ state.phase = 2
             /\ LET dest == state.target
                    wins == state.selected = Pred(dest)
                            /\ ((dest \in state.alive /\ dest \notin state.reused) \/ IgnoreDestinationGrant)
                            /\ Pred(dest) \in state.alive /\ Pred(dest) \notin state.reused
                IN state' = [state EXCEPT !.selected = IF wins THEN dest ELSE @,
                               !.attempted = @ \cup {dest}, !.cut = @ + 1,
                               !.pending = "NONE", !.ready = FALSE]
ApplyFree == /\ state.pending = "FREE" /\ state.phase = 2
             /\ LET victim == state.target
                    wins == (victim # state.selected \/ FreeCurrent)
                            /\ (state.proofCut = state.cut \/ IgnoreSelectorCut)
                IN state' = [state EXCEPT !.alive = IF wins THEN @ \ {victim} ELSE @,
                               !.freed = IF wins THEN @ \cup {victim} ELSE @,
                               !.badCut = @ \/ (wins /\ state.proofCut # state.cut),
                               !.cut = @ + 1, !.pending = "NONE", !.ready = FALSE]
ApplyReuse == /\ state.pending = "REUSE" /\ state.phase = 2
              /\ state' = [state EXCEPT !.alive = @ \cup {state.target},
                            !.reused = @ \cup {state.target}, !.cut = @ + 1,
                            !.pending = "NONE", !.ready = FALSE]
Restart == /\ state.pending = "NONE" /\ ~state.ready
           /\ state' = [state EXCEPT !.ready = TRUE,
                         !.recovered = IF RecoverOldBytes /\ "A" \in state.reused
                                       THEN "A" ELSE state.selected]
TypeOK == /\ state.selected \in Locations /\ state.alive \subseteq Locations
          /\ state.freed \subseteq Victims /\ state.reused \subseteq state.freed
          /\ state.pending \in {"NONE", "MOVE", "FREE", "REUSE"} /\ state.phase \in 0..2
SelectedAllocationRemainsLive == state.selected \in state.alive
SelectorNeverReentersReusedGeneration == state.selected \notin state.reused
AppliedFreeHadExactSelectorCut == ~state.badCut
RestartKeepsControlSelector == state.ready => state.recovered = state.selected
Next == (\E dest \in {"B", "C"} : Copy(dest) \/ PrepareMove(dest))
        \/ (\E victim \in Victims : PrepareFree(victim) \/ PrepareReuse(victim))
        \/ AdvanceControl \/ AppendControl \/ Force \/ ApplyMove \/ ApplyFree \/ ApplyReuse \/ Restart
Spec == Init /\ [][Next]_state
=============================================================================
