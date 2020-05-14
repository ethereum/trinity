from eth_utils import ValidationError

from eth2.beacon.types.blocks import SignedBeaconBlock


class ParentNotFoundError(ValidationError):
    """
    Raise if the parent for a block is not found.
    """


class SlashableBlockError(ValidationError):
    """
    Raise if a slashable block is found, i.e.
    a block for a slot we already have a canonical block for.
    """

    def __init__(self, slashable_block: SignedBeaconBlock, message: str) -> None:
        self.block = slashable_block
        self.message = message
