from eth_utils import encode_hex
import ssz

from eth2.beacon.types.checkpoints import Checkpoint, default_checkpoint


class EpochInfo(ssz.Serializable):

    fields = [
        ("previous_justified_checkpoint", Checkpoint),
        ("current_justified_checkpoint", Checkpoint),
        ("finalized_checkpoint", Checkpoint),
    ]

    def __init__(
        self,
        previous_justified_checkpoint: Checkpoint = default_checkpoint,
        current_justified_checkpoint: Checkpoint = default_checkpoint,
        finalized_checkpoint: Checkpoint = default_checkpoint,
    ) -> None:
        super().__init__(
            previous_justified_checkpoint=previous_justified_checkpoint,
            current_justified_checkpoint=current_justified_checkpoint,
            finalized_checkpoint=finalized_checkpoint,
        )

    def __str__(self) -> str:
        return (
            f"({encode_hex(self.previous_justified_checkpoint.root)[0:8]}) "
            f"({encode_hex(self.current_justified_checkpoint.root)[0:8]}) "
            f"({encode_hex(self.finalized_checkpoint.root)[0:8]})"
        )


default_epoch_info = EpochInfo()
