from .base import BaseForkChoice


class LMDGHOSTForkChoice(BaseForkChoice):
    """
    Use LMD GHOST to select the tip of the chain.
    """
    def __init__(self):
        pass

    def run(store, block) -> Dict[Hash32, int]:
        """
        Starting from ``block``, use the information in ``store`` to
        compute the score of every block according to LMD GHOST.

        Return a mapping of block root to the computed score.
        """
        pass
