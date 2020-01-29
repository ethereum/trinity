from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    UserDict,
)
import coincurve
import secrets
from typing import (
    Tuple,
    Type,
    TYPE_CHECKING,
)

from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend as cryptography_default_backend

from hashlib import sha256

from eth_keys.datatypes import (
    PrivateKey,
    PublicKey,
    NonRecoverableSignature,
)
from eth_keys.exceptions import (
    BadSignature,
    ValidationError as EthKeysValidationError,
)

from eth_utils import (
    encode_hex,
    keccak,
    ValidationError,
)

from p2p.discv5.typing import (
    AES128Key,
    IDNonce,
    NodeID,
    SessionKeys,
)
from p2p.discv5.constants import (
    AES128_KEY_SIZE,
    HKDF_INFO,
    ID_NONCE_SIGNATURE_PREFIX,
)


if TYPE_CHECKING:
    from p2p.discv5.enr import (  # noqa: F401
        BaseENR,
        ENR,
    )

# https://github.com/python/mypy/issues/5264#issuecomment-399407428
if TYPE_CHECKING:
    IdentitySchemeRegistryBaseType = UserDict[bytes, Type["IdentityScheme"]]
else:
    IdentitySchemeRegistryBaseType = UserDict


class IdentitySchemeRegistry(IdentitySchemeRegistryBaseType):

    def register(self,
                 identity_scheme_class: Type["IdentityScheme"]
                 ) -> Type["IdentityScheme"]:
        """Class decorator to register identity schemes."""
        if identity_scheme_class.id is None:
            raise ValueError("Identity schemes must define ID")

        if identity_scheme_class.id in self:
            raise ValueError(
                f"Identity scheme with id {identity_scheme_class.id!r} is already registered",
            )

        self[identity_scheme_class.id] = identity_scheme_class

        return identity_scheme_class


default_identity_scheme_registry = IdentitySchemeRegistry()
discv4_identity_scheme_registry = IdentitySchemeRegistry()


class IdentityScheme(ABC):

    id: bytes = None

    #
    # ENR
    #
    @classmethod
    @abstractmethod
    def create_enr_signature(cls, enr: "BaseENR", private_key: bytes) -> bytes:
        """Create and return the signature for an ENR."""
        ...

    @classmethod
    @abstractmethod
    def validate_enr_structure(cls, enr: "BaseENR") -> None:
        """Validate that the data required by the identity scheme is present and valid in an ENR."""
        ...

    @classmethod
    @abstractmethod
    def validate_enr_signature(cls, enr: "ENR") -> None:
        """Validate the signature of an ENR."""
        ...

    @classmethod
    @abstractmethod
    def extract_public_key(cls, enr: "BaseENR") -> bytes:
        """Retrieve the public key from an ENR."""
        ...

    @classmethod
    @abstractmethod
    def extract_node_id(cls, enr: "BaseENR") -> NodeID:
        """Retrieve the node id from an ENR."""
        ...

    #
    # Handshake
    #
    @classmethod
    @abstractmethod
    def create_handshake_key_pair(cls) -> Tuple[bytes, bytes]:
        """Create a random private/public key pair used for performing a handshake."""
        ...

    @classmethod
    @abstractmethod
    def validate_handshake_public_key(cls, public_key: bytes) -> None:
        """Validate that a public key received during handshake is valid."""
        ...

    @classmethod
    @abstractmethod
    def compute_session_keys(cls,
                             *,
                             local_private_key: bytes,
                             remote_public_key: bytes,
                             local_node_id: NodeID,
                             remote_node_id: NodeID,
                             id_nonce: IDNonce,
                             is_locally_initiated: bool,
                             ) -> SessionKeys:
        """Compute the symmetric session keys."""
        ...

    @classmethod
    @abstractmethod
    def create_id_nonce_signature(cls,
                                  *,
                                  id_nonce: IDNonce,
                                  ephemeral_public_key: bytes,
                                  private_key: bytes,
                                  ) -> bytes:
        """Sign an id nonce received during handshake."""
        ...

    @classmethod
    @abstractmethod
    def validate_id_nonce_signature(cls,
                                    *,
                                    id_nonce: IDNonce,
                                    ephemeral_public_key: bytes,
                                    signature: bytes,
                                    public_key: bytes,
                                    ) -> None:
        """Validate the id nonce signature received from a peer."""
        ...


def ecdh_agree(private_key: bytes, public_key: bytes) -> bytes:
    """Perform the ECDH key agreement.

    The public key is expected in uncompressed format and the resulting secret point will be
    formatted as a 0x02 or 0x03 prefix (depending on the sign of the secret's y component)
    followed by 32 bytes of the x component.
    """
    # We cannot use `cryptography.hazmat.primitives.asymmetric.ec.ECDH only gives us the x
    # component of the shared secret point, but we need both x and y.
    public_key_eth_keys = PublicKey(public_key)
    public_key_compressed = public_key_eth_keys.to_compressed_bytes()
    public_key_coincurve = coincurve.keys.PublicKey(public_key_compressed)
    secret_coincurve = public_key_coincurve.multiply(private_key)
    return secret_coincurve.format()


def hkdf_expand_and_extract(secret: bytes,
                            initiator_node_id: NodeID,
                            recipient_node_id: NodeID,
                            id_nonce: IDNonce,
                            ) -> Tuple[bytes, bytes, bytes]:
    info = b"".join((
        HKDF_INFO,
        initiator_node_id,
        recipient_node_id,
    ))

    hkdf = HKDF(
        algorithm=SHA256(),
        length=3 * AES128_KEY_SIZE,
        salt=id_nonce,
        info=info,
        backend=cryptography_default_backend(),
    )
    expanded_key = hkdf.derive(secret)

    if len(expanded_key) != 3 * AES128_KEY_SIZE:
        raise Exception("Invariant: Secret is expanded to three AES128 keys")

    initiator_key = expanded_key[:AES128_KEY_SIZE]
    recipient_key = expanded_key[AES128_KEY_SIZE:2 * AES128_KEY_SIZE]
    auth_response_key = expanded_key[2 * AES128_KEY_SIZE:3 * AES128_KEY_SIZE]

    return initiator_key, recipient_key, auth_response_key


@default_identity_scheme_registry.register
@discv4_identity_scheme_registry.register
class V4IdentityScheme(IdentityScheme):

    id = b"v4"
    public_key_enr_key = b"secp256k1"

    private_key_size = 32

    #
    # ENR
    #
    @classmethod
    def create_enr_signature(cls, enr: "BaseENR", private_key: bytes) -> bytes:
        message = enr.get_signing_message()
        private_key_object = PrivateKey(private_key)
        signature = private_key_object.sign_msg_non_recoverable(message)
        return bytes(signature)

    @classmethod
    def validate_enr_structure(cls, enr: "BaseENR") -> None:
        if cls.public_key_enr_key not in enr:
            raise ValidationError(f"ENR is missing required key {cls.public_key_enr_key!r}")

        public_key = cls.extract_public_key(enr)
        cls.validate_compressed_public_key(public_key)

    @classmethod
    def validate_enr_signature(cls, enr: "ENR") -> None:
        message_hash = keccak(enr.get_signing_message())
        cls.validate_signature(
            message_hash=message_hash,
            signature=enr.signature,
            public_key=enr.public_key,
        )

    @classmethod
    def extract_public_key(cls, enr: "BaseENR") -> bytes:
        try:
            return enr[cls.public_key_enr_key]
        except KeyError as error:
            raise KeyError("ENR does not contain public key") from error

    @classmethod
    def extract_node_id(cls, enr: "BaseENR") -> NodeID:
        public_key_object = PublicKey.from_compressed_bytes(enr.public_key)
        uncompressed_bytes = public_key_object.to_bytes()
        return NodeID(keccak(uncompressed_bytes))

    #
    # Handshake
    #
    @classmethod
    def create_handshake_key_pair(cls) -> Tuple[bytes, bytes]:
        private_key = secrets.token_bytes(cls.private_key_size)
        public_key = PrivateKey(private_key).public_key.to_bytes()
        return private_key, public_key

    @classmethod
    def validate_handshake_public_key(cls, public_key: bytes) -> None:
        cls.validate_uncompressed_public_key(public_key)

    @classmethod
    def compute_session_keys(cls,
                             *,
                             local_private_key: bytes,
                             remote_public_key: bytes,
                             local_node_id: NodeID,
                             remote_node_id: NodeID,
                             id_nonce: IDNonce,
                             is_locally_initiated: bool
                             ) -> SessionKeys:
        secret = ecdh_agree(local_private_key, remote_public_key)

        if is_locally_initiated:
            initiator_node_id, recipient_node_id = local_node_id, remote_node_id
        else:
            initiator_node_id, recipient_node_id = remote_node_id, local_node_id

        initiator_key, recipient_key, auth_response_key = hkdf_expand_and_extract(
            secret,
            initiator_node_id,
            recipient_node_id,
            id_nonce,
        )

        if is_locally_initiated:
            encryption_key, decryption_key = initiator_key, recipient_key
        else:
            encryption_key, decryption_key = recipient_key, initiator_key

        return SessionKeys(
            encryption_key=AES128Key(encryption_key),
            decryption_key=AES128Key(decryption_key),
            auth_response_key=AES128Key(auth_response_key),
        )

    @classmethod
    def create_id_nonce_signature(cls,
                                  *,
                                  id_nonce: IDNonce,
                                  ephemeral_public_key: bytes,
                                  private_key: bytes,
                                  ) -> bytes:
        private_key_object = PrivateKey(private_key)
        signature_input = cls.create_id_nonce_signature_input(
            id_nonce=id_nonce,
            ephemeral_public_key=ephemeral_public_key,
        )
        signature = private_key_object.sign_msg_hash_non_recoverable(signature_input)
        return bytes(signature)

    @classmethod
    def validate_id_nonce_signature(cls,
                                    *,
                                    id_nonce: IDNonce,
                                    ephemeral_public_key: bytes,
                                    signature: bytes,
                                    public_key: bytes,
                                    ) -> None:
        signature_input = cls.create_id_nonce_signature_input(
            id_nonce=id_nonce,
            ephemeral_public_key=ephemeral_public_key,
        )
        cls.validate_signature(
            message_hash=signature_input,
            signature=signature,
            public_key=public_key,
        )

    #
    # Helpers
    #
    @classmethod
    def validate_compressed_public_key(cls, public_key: bytes) -> None:
        try:
            PublicKey.from_compressed_bytes(public_key)
        except (EthKeysValidationError, ValueError) as error:
            raise ValidationError(
                f"Public key {encode_hex(public_key)} is invalid compressed public key: {error}"
            ) from error

    @classmethod
    def validate_uncompressed_public_key(cls, public_key: bytes) -> None:
        try:
            PublicKey(public_key)
        except EthKeysValidationError as error:
            raise ValidationError(
                f"Public key {encode_hex(public_key)} is invalid uncompressed public key: {error}"
            ) from error

    @classmethod
    def validate_signature(cls,
                           *,
                           message_hash: bytes,
                           signature: bytes,
                           public_key: bytes,
                           ) -> None:
        public_key_object = PublicKey.from_compressed_bytes(public_key)

        try:
            signature_object = NonRecoverableSignature(signature)
        except BadSignature:
            is_valid = False
        else:
            is_valid = signature_object.verify_msg_hash(message_hash, public_key_object)

        if not is_valid:
            raise ValidationError(
                f"Signature {encode_hex(signature)} is not valid for message hash "
                f"{encode_hex(message_hash)} and public key {encode_hex(public_key)}"
            )

    @classmethod
    def create_id_nonce_signature_input(cls,
                                        *,
                                        id_nonce: IDNonce,
                                        ephemeral_public_key: bytes,
                                        ) -> bytes:
        preimage = b"".join((
            ID_NONCE_SIGNATURE_PREFIX,
            id_nonce,
            ephemeral_public_key,
        ))
        return sha256(preimage).digest()


@default_identity_scheme_registry.register
@discv4_identity_scheme_registry.register
class V4CompatIdentityScheme(V4IdentityScheme):
    """
    An identity scheme to be used for locally crafted ENRs representing remote nodes that don't
    support the ENR extension.

    ENRs using this identity scheme have a zero-length signature.
    """

    # The spec says all nodes should use the v4 id scheme, but the ENRs using this are forged
    # and meant to be used only internally, so we use a different ID to be able to easily
    # distinguish them.
    id = b"v4-compat"

    @classmethod
    def validate_enr_signature(cls, enr: "ENR") -> None:
        pass

    @classmethod
    def create_enr_signature(cls, enr: "BaseENR", private_key: bytes) -> bytes:
        raise NotImplementedError()
