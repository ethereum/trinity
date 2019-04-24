from .base import BaseForkChoice


class HighestSlotForkChoice(BaseForkChoice):
    """
    A block with a higher slot has a higher score.
    """
    def __init__(self):
        pass

    def _score(self, block) -> int:
        return block.slot

    def _set_score(self, scores, block):
        scores[block.root] = self._score(block)

    def run(self, store, block) -> Dict[Hash32, int]:
        scores = {}
        for child in store.get_children(block):
            scores.update(self.run(store, child))
        self._set_score(scores, block)
        return scores
