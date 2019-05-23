def higher_slot_scoring(block, _db) -> int:
    """
    A ``block`` with a higher slot has a higher score.
    """
    return block.slot
