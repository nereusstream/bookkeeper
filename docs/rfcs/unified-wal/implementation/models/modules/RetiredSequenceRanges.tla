------------------- MODULE RetiredSequenceRanges -------------------
(* Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements. See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0. *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS IgnoreSequenceGap, IgnoreLiveBarrier, DropBookieCut, DropEmptyBookieCut, SkipEmptyBarrierRebuild
VARIABLE s
vars == <<s>>
RangeSet(ranges) == {ranges[i] : i \in 1..Len(ranges)}
Coverage(ranges) == UNION {{<<r.stream, n>> : n \in r.first..r.through} : r \in RangeSet(ranges)}
(* A stream abstracts exact logical shard and generation. Durable FREE facts,
   current allocation order and the Bookie cut are inputs; no new FREE or
   physical byte/durability authority is proved by merging their projection. *)
Init == \E gap \in BOOLEAN, otherStream \in {0, 1}, liveOrders \in SUBSET {2, 4}, emptyCut \in {0, 4},
           emptyOrder \in {2, 4} :
    s = [input |-> <<[stream |-> 0, order |-> 1, last |-> 1, first |-> 1, through |-> 1, cut |-> 1],
                    [stream |-> 0, order |-> 3, last |-> 3,
                     first |-> IF gap THEN 3 ELSE 2, through |-> IF gap THEN 3 ELSE 2, cut |-> 2],
                    [stream |-> otherStream, order |-> 5, last |-> 5,
                     first |-> IF otherStream = 1 THEN 1 ELSE IF gap THEN 4 ELSE 3,
                     through |-> IF otherStream = 1 THEN 1 ELSE IF gap THEN 4 ELSE 3, cut |-> 3]>>,
         live |-> liveOrders, emptyCut |-> emptyCut, checked |-> FALSE, bookieCut |-> 0,
         emptyOrder |-> emptyOrder, phase |-> 0,
         index |-> 1, output |-> <<>>, done |-> FALSE]
CheckBookieCut == /\ ~s.checked
                 /\ s' = [s EXCEPT !.checked = TRUE,
                          !.bookieCut = IF DropEmptyBookieCut \/ s.emptyCut = 0 THEN 3 ELSE s.emptyCut]
Advance == /\ s.checked /\ ~s.done /\ s.index <= Len(s.input)
           /\ LET next == s.input[s.index]
                  previous == IF Len(s.output) = 0 THEN next ELSE s.output[Len(s.output)]
                  barrier == previous.stream = 0 /\
                             \E order \in s.live : previous.order < order /\ order < next.last
                  join == Len(s.output) > 0 /\ previous.stream = next.stream
                          /\ previous.last < next.order
                          /\ (IgnoreSequenceGap \/ previous.through + 1 = next.first)
                          /\ (IgnoreLiveBarrier \/ ~barrier)
                  merged == [previous EXCEPT !.last = next.last, !.through = next.through,
                             !.cut = IF DropBookieCut THEN @ ELSE IF @ > next.cut THEN @ ELSE next.cut]
                  output == IF join THEN Append(SubSeq(s.output, 1, Len(s.output) - 1), merged)
                            ELSE Append(s.output, next)
              IN s' = [s EXCEPT !.output = output, !.index = @ + 1]
Finish == /\ s.checked /\ ~s.done /\ s.index > Len(s.input) /\ s' = [s EXCEPT !.done = TRUE]
RetireEmpty == /\ s.done /\ s.phase = 0 /\ s.emptyOrder \in s.live
               /\ s' = [s EXCEPT !.live = @ \ {s.emptyOrder}, !.phase = 1, !.emptyCut = 4,
                        !.checked = FALSE, !.bookieCut = 0, !.input = s.output,
                        !.index = IF SkipEmptyBarrierRebuild THEN Len(s.output) + 1 ELSE 1,
                        !.output = IF SkipEmptyBarrierRebuild THEN @ ELSE <<>>,
                        !.done = SkipEmptyBarrierRebuild]
Next == CheckBookieCut \/ Advance \/ Finish \/ RetireEmpty
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.index \in 1..4 /\ Len(s.output) <= 3 /\ s.done \in BOOLEAN
          /\ s.live \subseteq {2, 4} /\ s.bookieCut \in {0, 3, 4} /\ s.checked \in BOOLEAN
          /\ s.phase \in {0, 1} /\ s.emptyOrder \in {2, 4}
RetiredCoverageIsExact == Coverage(s.output) = Coverage(SubSeq(s.input, 1, s.index - 1))
CurrentAllocationOrderIsNotBridged ==
    \A r \in RangeSet(s.output) : r.stream # 0 \/
        \A order \in s.live : ~(r.order < order /\ order < r.last)
RequiredBookieCutIsRetained ==
    \A r \in RangeSet(s.output), original \in RangeSet(SubSeq(s.input, 1, s.index - 1)) :
        r.stream = original.stream /\ r.first <= original.first /\ original.through <= r.through
            => r.cut >= original.cut
EmptyRetirementKeepsRequiredBookieCut == ~s.checked \/ s.bookieCut >= s.emptyCut
EmptyBarrierRemovalIsFolded == s.done =>
    \A i \in 1..(Len(s.output) - 1) :
        LET previous == s.output[i]
            next == s.output[i + 1]
        IN previous.stream # next.stream \/ previous.through + 1 # next.first \/
           previous.last >= next.order \/
           (previous.stream = 0 /\ \E order \in s.live : previous.order < order /\ order < next.last)
=====================================================================
