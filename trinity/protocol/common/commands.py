from p2p.protocol import Command


class SnappyCommand(Command):
    def __init__(self, cmd_id_offset: int) -> None:
        super().__init__(cmd_id_offset, snappy_support=True)
