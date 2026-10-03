----------------------- MODULE RelocationCompletion -----------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS CertifyOnForce, PublishOrdinary, ReleaseOnCancel
VARIABLE state
Copies == {1, 2}
Init == state = [io |-> Copies, held |-> Copies, durable |-> {}, failed |-> {},
                  received |-> {}, ownerTerminal |-> {}, certified |-> {}, prefix |-> 0,
                  ordinary |-> FALSE, waiting |-> TRUE]
Force(seq) == /\ seq \in state.io
              /\ state' = [state EXCEPT !.io = @ \ {seq}, !.durable = @ \cup {seq},
                            !.certified = IF CertifyOnForce THEN @ \cup {seq} ELSE @]
ForceUnknown(seq) == /\ seq \in state.io
                     /\ state' = [state EXCEPT !.io = @ \ {seq}, !.failed = @ \cup {seq}]
Prefix(done) == IF Copies \subseteq done THEN 2 ELSE IF 1 \in done THEN 1 ELSE 0
Receive(seq) == /\ seq \notin state.io /\ seq \notin state.received
                /\ LET done == state.received \cup {seq}
                       cut == IF state.failed = {} THEN Prefix(done \cap state.durable) ELSE state.prefix
                   IN state' = [state EXCEPT !.received = done, !.prefix = cut,
                                 !.certified = @ \cup (1..cut),
                                 !.ordinary = @ \/ (PublishOrdinary /\ cut > state.prefix)]
OwnerTerminal(seq) == /\ seq \notin state.ownerTerminal
                      /\ (seq \in state.certified \/ state.failed \cap state.received # {})
                      /\ state' = [state EXCEPT !.ownerTerminal = @ \cup {seq}]
Release(seq) == /\ seq \in state.held /\ seq \notin state.io /\ seq \in state.ownerTerminal
                /\ state' = [state EXCEPT !.held = @ \ {seq}]
CancelWait == /\ state.waiting
              /\ state' = [state EXCEPT !.waiting = FALSE,
                            !.held = IF ReleaseOnCancel THEN {} ELSE @]
TypeOK == state \in [io : SUBSET Copies, held : SUBSET Copies, durable : SUBSET Copies,
                     failed : SUBSET Copies, received : SUBSET Copies, ownerTerminal : SUBSET Copies,
                     certified : SUBSET Copies, prefix : 0..2, ordinary : BOOLEAN, waiting : BOOLEAN]
CopyFactsHaveBarrierAndPrefix == state.certified \subseteq state.durable \cap (1..state.prefix)
CopyDoesNotPublishOrdinary == ~state.ordinary
BuffersWaitForBothTerminals == (Copies \ state.held) \subseteq (state.ownerTerminal \ state.io)
Next == (\E seq \in Copies : Force(seq) \/ ForceUnknown(seq) \/ Receive(seq)
                              \/ OwnerTerminal(seq) \/ Release(seq)) \/ CancelWait
Spec == Init /\ [][Next]_state
=============================================================================
