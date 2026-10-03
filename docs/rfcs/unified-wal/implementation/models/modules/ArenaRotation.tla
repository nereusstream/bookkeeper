--------------------------- MODULE ArenaRotation ---------------------------
EXTENDS Naturals, FiniteSets
CONSTANT RequireBothRoots
VARIABLES oldPresent, fileWritten, fileForced, directoryForced, images, imageForced,
          writtenRoots, forcedRoots, switched, tailWritten, tailForced, acknowledged,
          life, crashes, corruptions, recovered
vars == <<oldPresent, fileWritten, fileForced, directoryForced, images, imageForced,
          writtenRoots, forcedRoots, switched, tailWritten, tailForced, acknowledged,
          life, crashes, corruptions, recovered>>
Slots == {1, 2}
(* Two cuts retain the same ordinary authority; the intervening internal checkpoint
   commit and byte binding are checked by Java. One physical rotation, one power
   loss and one image corruption. No claim of full allocator/selector composition. *)
Init == /\ oldPresent = TRUE /\ fileWritten = FALSE /\ fileForced = FALSE
        /\ directoryForced = FALSE /\ images = {} /\ imageForced = {}
        /\ writtenRoots = {} /\ forcedRoots = {} /\ switched = FALSE
        /\ tailWritten = FALSE /\ tailForced = FALSE /\ acknowledged = 1
        /\ life = "live" /\ crashes = 0 /\ corruptions = 0 /\ recovered = 0
WriteImages == /\ life = "live" /\ images = {} /\ images' = Slots
               /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, imageForced,
                              writtenRoots, forcedRoots, switched, tailWritten, tailForced,
                              acknowledged, life, crashes, corruptions, recovered>>
ForceImages == /\ life = "live" /\ images = Slots /\ imageForced = {}
               /\ imageForced' = Slots
               /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                              writtenRoots, forcedRoots, switched, tailWritten, tailForced,
                              acknowledged, life, crashes, corruptions, recovered>>
WriteFile == /\ life = "live" /\ imageForced = Slots /\ ~fileWritten
             /\ fileWritten' = TRUE
             /\ UNCHANGED <<oldPresent, fileForced, directoryForced, images, imageForced,
                            writtenRoots, forcedRoots, switched, tailWritten, tailForced,
                            acknowledged, life, crashes, corruptions, recovered>>
ForceFile == /\ life = "live" /\ fileWritten /\ ~fileForced /\ fileForced' = TRUE
             /\ UNCHANGED <<oldPresent, fileWritten, directoryForced, images, imageForced,
                            writtenRoots, forcedRoots, switched, tailWritten, tailForced,
                            acknowledged, life, crashes, corruptions, recovered>>
ForceDirectory == /\ life = "live" /\ fileForced /\ ~directoryForced
                  /\ directoryForced' = TRUE
                  /\ UNCHANGED <<oldPresent, fileWritten, fileForced, images, imageForced,
                                 writtenRoots, forcedRoots, switched, tailWritten, tailForced,
                                 acknowledged, life, crashes, corruptions, recovered>>
WriteRoot(slot) == /\ life = "live" /\ directoryForced /\ slot \notin writtenRoots
                   /\ writtenRoots' = writtenRoots \cup {slot}
                   /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                                  imageForced, forcedRoots, switched, tailWritten, tailForced,
                                  acknowledged, life, crashes, corruptions, recovered>>
ForceRoot(slot) == /\ life = "live" /\ slot \in writtenRoots /\ slot \notin forcedRoots
                   /\ forcedRoots' = forcedRoots \cup {slot}
                   /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                                  imageForced, writtenRoots, switched, tailWritten, tailForced,
                                  acknowledged, life, crashes, corruptions, recovered>>
Reclaim == /\ life = "live" /\ oldPresent
           /\ (IF RequireBothRoots THEN forcedRoots = Slots ELSE forcedRoots # {})
           /\ oldPresent' = FALSE /\ switched' = TRUE
           /\ UNCHANGED <<fileWritten, fileForced, directoryForced, images, imageForced,
                          writtenRoots, forcedRoots, tailWritten, tailForced,
                          acknowledged, life, crashes, corruptions, recovered>>
WriteTail == /\ life = "live" /\ switched /\ forcedRoots = Slots /\ ~tailWritten
             /\ tailWritten' = TRUE
             /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                            imageForced, writtenRoots, forcedRoots, switched, tailForced,
                            acknowledged, life, crashes, corruptions, recovered>>
ForceTail == /\ life = "live" /\ tailWritten /\ ~tailForced /\ tailForced' = TRUE
             /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                            imageForced, writtenRoots, forcedRoots, switched, tailWritten,
                            acknowledged, life, crashes, corruptions, recovered>>
Acknowledge == /\ life = "live" /\ tailForced /\ acknowledged = 1 /\ acknowledged' = 2
               /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                              imageForced, writtenRoots, forcedRoots, switched, tailWritten,
                              tailForced, life, crashes, corruptions, recovered>>
Corrupt(slot) == /\ corruptions = 0 /\ slot \in imageForced
                 /\ images' = images \ {slot} /\ imageForced' = imageForced \ {slot}
                 /\ corruptions' = 1
                 /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced,
                                writtenRoots, forcedRoots, switched, tailWritten, tailForced,
                                acknowledged, life, crashes, recovered>>
CorruptRoot(slot) == /\ corruptions = 0 /\ slot \in forcedRoots
                     /\ writtenRoots' = writtenRoots \ {slot}
                     /\ forcedRoots' = forcedRoots \ {slot} /\ corruptions' = 1
                     /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced,
                                    images, imageForced, switched, tailWritten, tailForced,
                                    acknowledged, life, crashes, recovered>>
Crash == /\ life = "live" /\ crashes = 0 /\ life' = "down" /\ crashes' = 1
         /\ fileWritten' = fileForced /\ images' = imageForced
         /\ writtenRoots' = forcedRoots /\ tailWritten' = tailForced
         /\ UNCHANGED <<oldPresent, fileForced, directoryForced, imageForced,
                        forcedRoots, switched, tailForced, acknowledged, corruptions, recovered>>
NewRoot == fileForced /\ directoryForced /\ forcedRoots # {} /\ imageForced # {}
Recover == /\ life = "down" /\ (oldPresent \/ NewRoot) /\ life' = "recovered"
           /\ recovered' = IF NewRoot /\ tailWritten THEN 2 ELSE 1
           /\ UNCHANGED <<oldPresent, fileWritten, fileForced, directoryForced, images,
                          imageForced, writtenRoots, forcedRoots, switched, tailWritten,
                          tailForced, acknowledged, crashes, corruptions>>
Next == WriteImages \/ ForceImages \/ WriteFile \/ ForceFile \/ ForceDirectory
        \/ (\E slot \in Slots: WriteRoot(slot) \/ ForceRoot(slot) \/ Corrupt(slot) \/ CorruptRoot(slot))
        \/ Reclaim \/ WriteTail \/ ForceTail \/ Acknowledge \/ Crash \/ Recover
TypeOK == /\ oldPresent \in BOOLEAN /\ fileWritten \in BOOLEAN /\ fileForced \in BOOLEAN
          /\ directoryForced \in BOOLEAN /\ images \subseteq Slots /\ imageForced \subseteq Slots
          /\ writtenRoots \subseteq Slots /\ forcedRoots \subseteq Slots /\ switched \in BOOLEAN
          /\ tailWritten \in BOOLEAN /\ tailForced \in BOOLEAN /\ acknowledged \in {1, 2}
          /\ life \in {"live", "down", "recovered"} /\ crashes \in 0..1
          /\ corruptions \in 0..1 /\ recovered \in 0..2
SurvivingAuthority == oldPresent \/ NewRoot
AcknowledgedTailSurvives == acknowledged = 2 => fileForced /\ directoryForced /\ tailForced
RecoveryKeepsAcknowledgements == life = "recovered" => recovered >= acknowledged
Spec == Init /\ [][Next]_vars
=============================================================================
