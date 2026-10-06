-------------------- MODULE ProfileSdkBridge --------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, FiniteSets
CONSTANTS ForgetUnknown, DropRetrySource, ChangeRetryCoordinate, RepeatCallback, PublishClassicClose
VARIABLE s
vars == <<s>>
Targets == 0..2
MaxAttempts == 2
(* One admitted original SDK entry, three original slots, A=2. DATA futures
   supply full classified statuses; TLS/write/wire ownership is verified by
   the existing source/dispatch projections and actual socket tests. Time is
   an explicit expiry event, not a real clock. No replacement, recovery,
   metadata lease, fairness, byte/kernel totals, or full A+C+D claim. *)
Init == s = [phase |-> [t \in Targets |-> "running"],
             attempts |-> [t \in Targets |-> 1], source |-> [t \in Targets |-> TRUE],
             unknown |-> [t \in Targets |-> FALSE], everUnknown |-> [t \in Targets |-> FALSE],
             coordinate |-> [t \in Targets |-> 0], callbacks |-> 0,
             user |-> "pending", stopped |-> FALSE, metadata |-> "OPEN"]
Durable(t) == /\ s.phase[t] = "running" /\ ~s.stopped
              /\ s' = [s EXCEPT !.phase[t] = "durable", !.source[t] = FALSE]
Refused(t, result) ==
    /\ s.phase[t] = "running" /\ ~s.stopped /\ result \in {"NONE", "UNKNOWN"}
    /\ LET retrying == s.attempts[t] < MaxAttempts
           observed == s.unknown[t] \/ result = "UNKNOWN"
       IN s' = [s EXCEPT !.phase[t] = IF retrying THEN "backoff" ELSE "failed",
                !.source[t] = retrying /\ ~DropRetrySource,
                !.unknown[t] = IF ForgetUnknown THEN result = "UNKNOWN" ELSE observed,
                !.everUnknown[t] = @ \/ result = "UNKNOWN"]
Retry(t) == /\ s.phase[t] = "backoff" /\ ~s.stopped
            /\ s' = [s EXCEPT !.phase[t] = "running", !.attempts[t] = @ + 1,
                     !.coordinate[t] = IF ChangeRetryCoordinate THEN @ + 1 ELSE @]
Expire(t) == /\ s.phase[t] \in {"running", "backoff"}
             /\ s' = [s EXCEPT !.phase[t] = "failed", !.source[t] = FALSE,
                      !.unknown[t] = @ \/ s.phase[t] = "running",
                      !.everUnknown[t] = @ \/ s.phase[t] = "running"]
Quorum == /\ (s.user = "pending" \/ RepeatCallback) /\ ~s.stopped
          /\ Cardinality({t \in Targets : s.phase[t] = "durable"}) >= 2
          /\ s' = [s EXCEPT !.user = "success", !.callbacks = IF @ < 2 THEN @ + 1 ELSE @]
Stop == /\ ~s.stopped
        /\ s' = [s EXCEPT !.stopped = TRUE,
                 !.user = IF @ = "pending" THEN "failed" ELSE @,
                 !.callbacks = IF s.user = "pending" THEN @ + 1 ELSE @,
                 !.unknown = [t \in Targets |-> s.unknown[t] \/ s.phase[t] = "running"],
                 !.everUnknown = [t \in Targets |-> s.everUnknown[t] \/ s.phase[t] = "running"],
                 !.phase = [t \in Targets |-> IF s.phase[t] = "durable" THEN "durable" ELSE "failed"],
                 !.source = [t \in Targets |-> FALSE],
                 !.metadata = IF PublishClassicClose THEN "CLOSED" ELSE @]
Next == (\E t \in Targets : Durable(t) \/ Retry(t) \/ Expire(t)
                           \/ (\E result \in {"NONE", "UNKNOWN"} : Refused(t, result))) \/ Quorum \/ Stop
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.user \in {"pending", "success", "failed"} /\ s.callbacks \in 0..2
          /\ s.metadata \in {"OPEN", "CLOSED"} /\ s.stopped \in BOOLEAN
          /\ \A t \in Targets : /\ s.phase[t] \in {"running", "backoff", "durable", "failed"}
                                 /\ s.attempts[t] \in 1..MaxAttempts
                                 /\ s.source[t] \in BOOLEAN /\ s.unknown[t] \in BOOLEAN
                                 /\ s.everUnknown[t] \in BOOLEAN /\ s.coordinate[t] \in 0..1
KnownNoneNeverClearsUnknown == \A t \in Targets : s.everUnknown[t] => s.unknown[t]
RetryKeepsSource == \A t \in Targets : s.phase[t] \in {"running", "backoff"} => s.source[t]
RetryKeepsCoordinate == \A t \in Targets : s.coordinate[t] = 0
OneOriginalCallback == s.callbacks <= 1
SuccessRequiresOriginalQuorum == s.user = "success" => Cardinality({t \in Targets : s.phase[t] = "durable"}) >= 2
LocalStopNeverPublishesClosed == s.metadata = "OPEN"
==============================================================================
