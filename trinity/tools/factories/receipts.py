try:
    import factory
    from faker import Faker
except ImportError:
    raise ImportError("The p2p.tools.factories module requires the `factory_boy` library.")

from typing import (
    Any,
    List,
    Type,
)

from eth.constants import BLANK_ROOT_HASH
from eth.rlp.receipts import Receipt
import rlp

from trinity.rlp.sedes import (
    UninterpretedReceipt,
    strip_interpretation,
)


class UninterpretedReceiptFactory(factory.Factory):
    class Meta:
        model = list

    __faker = Faker()
    @classmethod
    def _create(cls,
                model_class: Type[List[bytes]],
                *args: Any,
                **kwargs: Any) -> UninterpretedReceipt:

        if cls.__faker.boolean():
            return b'\x01' + rlp.encode(LegacyReceiptFactory(*args, **kwargs))
        else:
            return strip_interpretation(LegacyReceiptFactory(*args, **kwargs))


class LegacyReceiptFactory(factory.Factory):
    class Meta:
        model = Receipt

    state_root = BLANK_ROOT_HASH
    gas_used = 0
    bloom = 0
    logs = ()
