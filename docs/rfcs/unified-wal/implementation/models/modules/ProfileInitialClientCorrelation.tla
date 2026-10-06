-------------------- MODULE ProfileInitialClientCorrelation --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS SkipPeer, AcceptBadReply, ReleaseOnCancel, ReleaseOnReply,
          ReuseFrameId, TimeoutAsNone, ReviveCancelled
VARIABLE s
vars == <<s>>
Requests == 1..2
(* Two requests in one borrowed event loop. Exact TLS/HELLO bookie/incarnation/
   engine/capability and full frame/body binding are inputs. Server successful
   reply is assumed to have the force/owner evidence of existing dispatch models.
   No byte/crypto/fsync/ZK/all-E/quorum, timing, fairness or full A/C/D proof. *)
Init == \E peerBound \in BOOLEAN, capacity \in 1..2 :
 s = [peerBound |-> peerBound, capacity |-> capacity, connection |-> "hello",
      request |-> [i \in Requests |-> [phase |-> "absent", frameId |-> i,
        held |-> FALSE, writeDone |-> FALSE, wireDone |-> FALSE,
        submitted |-> FALSE, cancelled |-> FALSE, boundReply |-> FALSE,
        uncertain |-> FALSE, result |-> "pending"]]]
Used == Cardinality({i \in Requests : s.request[i].held})
Hello == /\ s.connection = "hello"
         /\ s' = [s EXCEPT !.connection = IF s.peerBound \/ SkipPeer THEN "live" ELSE "closed"]
Enqueue(i) == /\ s.connection = "live" /\ s.request[i].phase = "absent" /\ Used < s.capacity
              /\ s' = [s EXCEPT !.request[i].phase = "queued", !.request[i].held = TRUE,
                       !.request[i].frameId = IF ReuseFrameId THEN 1 ELSE i]
Refuse(i) == /\ s.connection = "live" /\ s.request[i].phase = "absent" /\ Used = s.capacity
             /\ s' = [s EXCEPT !.request[i].phase = "refused", !.request[i].result = "none",
                      !.request[i].writeDone = TRUE, !.request[i].wireDone = TRUE]
Write(i) == /\ s.connection = "live" /\ s.request[i].phase = "queued" /\ ~s.request[i].cancelled
            /\ s' = [s EXCEPT !.request[i].phase = "written", !.request[i].submitted = TRUE]
Cancel(i) == /\ s.request[i].held /\ s.request[i].result = "pending"
             /\ s' = [s EXCEPT !.request[i].cancelled = TRUE, !.request[i].result = "cancelled",
                      !.request[i].held = IF ReleaseOnCancel THEN FALSE ELSE @]
AbortQueued(i) == /\ s.request[i].phase = "queued" /\ s.request[i].cancelled
                  /\ s' = [s EXCEPT !.request[i].phase = "terminal",
                           !.request[i].writeDone = TRUE, !.request[i].wireDone = TRUE]
WriteTerminal(i) == /\ s.request[i].submitted /\ ~s.request[i].writeDone
                    /\ s' = [s EXCEPT !.request[i].writeDone = TRUE]
Reply(i) == /\ s.connection = "live" /\ s.request[i].submitted /\ ~s.request[i].wireDone
            /\ \E bound \in BOOLEAN :
                s' = [s EXCEPT !.request[i].wireDone = TRUE, !.request[i].boundReply = bound,
                     !.request[i].uncertain = ~bound /\ ~AcceptBadReply,
                     !.request[i].result = IF s.request[i].cancelled /\ ~ReviveCancelled THEN "cancelled"
                       ELSE IF bound \/ AcceptBadReply THEN "success" ELSE "unknown",
                     !.request[i].held = IF ReleaseOnReply THEN FALSE ELSE @,
                     !.connection = IF bound \/ AcceptBadReply THEN @ ELSE "closed"]
Disconnect == /\ s.connection = "live"
              /\ s' = [s EXCEPT !.connection = "closed"]
FailClosed(i) == /\ s.connection = "closed" /\ s.request[i].held /\ ~s.request[i].wireDone
                /\ s' = [s EXCEPT !.request[i].wireDone = TRUE, !.request[i].uncertain = TRUE,
                         !.request[i].writeDone = IF ~s.request[i].submitted THEN TRUE ELSE @,
                         !.request[i].result = IF s.request[i].cancelled THEN "cancelled"
                           ELSE IF TimeoutAsNone THEN "none" ELSE "unknown"]
Release(i) == /\ s.request[i].held /\ s.request[i].writeDone /\ s.request[i].wireDone
              /\ s' = [s EXCEPT !.request[i].held = FALSE, !.request[i].phase = "terminal"]
Next == Hello \/ Disconnect \/ \E i \in Requests : Enqueue(i) \/ Refuse(i) \/ Write(i) \/ Cancel(i)
        \/ AbortQueued(i) \/ WriteTerminal(i) \/ Reply(i) \/ FailClosed(i) \/ Release(i)
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.peerBound \in BOOLEAN /\ s.capacity \in 1..2
          /\ s.connection \in {"hello", "live", "closed"}
          /\ \A i \in Requests :
             /\ s.request[i].phase \in {"absent", "queued", "written", "refused", "terminal"}
             /\ s.request[i].frameId \in 1..2
             /\ s.request[i].result \in {"pending", "success", "unknown", "none", "cancelled"}
             /\ \A field \in {"held", "writeDone", "wireDone", "submitted", "cancelled", "boundReply", "uncertain"} :
                 s.request[i][field] \in BOOLEAN
RequestRequiresVerifiedPeer == \A i \in Requests : s.request[i].submitted => s.peerBound
SuccessHasBoundReply == \A i \in Requests : s.request[i].result = "success" => s.request[i].boundReply
SlotsWaitForBothTerminals == \A i \in Requests :
    s.request[i].phase \in {"queued", "written"} /\ ~(s.request[i].writeDone /\ s.request[i].wireDone)
      => s.request[i].held
FrameIdsNeverAlias == \A i,j \in Requests : i # j
    /\ s.request[i].phase \in {"queued", "written", "terminal"}
    /\ s.request[j].phase \in {"queued", "written", "terminal"}
      => s.request[i].frameId # s.request[j].frameId
NetworkUncertaintyNeverMeansNone == \A i \in Requests : s.request[i].uncertain => s.request[i].result # "none"
CancelledFutureIsNotRevived == \A i \in Requests : s.request[i].cancelled => s.request[i].result # "success"
CapacityIsBounded == Used <= s.capacity
=============================================================================
