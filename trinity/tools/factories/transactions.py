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

from eth.constants import ZERO_ADDRESS
from eth.rlp.transactions import BaseTransactionFields
from eth.vm.forks.frontier.transactions import FrontierUnsignedTransaction
import rlp

from p2p.tools.factories import PrivateKeyFactory

from trinity.rlp.sedes import (
    UninterpretedTransaction,
    strip_interpretation,
)


class UninterpretedTransactionFactory(factory.Factory):
    class Meta:
        model = list

    __faker = Faker()
    @classmethod
    def _create(cls,
                model_class: Type[List[bytes]],
                *args: Any,
                **kwargs: Any) -> UninterpretedTransaction:

        if cls.__faker.boolean():
            return b'\x01' + rlp.encode(LegacyTransactionFactory(*args, **kwargs))
        else:
            return strip_interpretation(LegacyTransactionFactory(*args, **kwargs))


class _FakeTransaction(BaseTransactionFields):
    chain_id: int = None

    def encode(self) -> bytes:
        return rlp.encode(self)


class LegacyTransactionFactory(factory.Factory):
    class Meta:
        model = _FakeTransaction

    nonce = factory.Sequence(lambda n: n)
    gas_price = 1
    gas = 21000
    to = ZERO_ADDRESS
    value = 0
    data = b''

    @classmethod
    def _create(cls,
                model_class: Type[BaseTransactionFields],
                *args: Any,
                chain_id: int = None,
                **kwargs: Any) -> BaseTransactionFields:
        if 'vrs' in kwargs:
            v, r, s = kwargs.pop('vrs')
        else:
            if 'private_key' in kwargs:
                private_key = kwargs.pop('private_key')
            else:
                private_key = PrivateKeyFactory()

            tx_for_signing = FrontierUnsignedTransaction(**kwargs)
            signed_tx = tx_for_signing.as_signed_transaction(private_key)

            v = signed_tx.v
            r = signed_tx.r
            s = signed_tx.s

        return model_class(**kwargs, v=v, r=r, s=s)
