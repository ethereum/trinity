import argparse
import json
from socket import (
    inet_aton,
)
import textwrap

from eth_keys.datatypes import (
    PrivateKey,
)
from eth_utils import (
    decode_hex,
    encode_hex,
)

from p2p.tools.factories.keys import (
    PrivateKeyFactory,
)

from p2p.discv5.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    UDP_PORT_ENR_KEY,
)
from p2p.discv5.enr import (
    ENR,
    UnsignedENR,
)
from p2p.discv5.identity_schemes import (
    V4IdentityScheme,
)


output_format = textwrap.dedent("""\
    Private key: {private_key}
    Node ID:     {node_id}
    ENR:         {enr!r}
""")


def print_output(private_key, enr):
    print(output_format.format(
        private_key=encode_hex(private_key) if private_key else "unknown",
        node_id=encode_hex(enr.node_id),
        enr=enr,
    ))


def create_identity(args):
    if args.private_key is not None:
        private_key = decode_hex(args.private_key)
        if len(private_key) != 32:
            raise ValueError(f"Private key must be 32 bytes, but is {len(private_key)}")
    else:
        private_key = PrivateKeyFactory().to_bytes()

    public_key = PrivateKey(private_key).public_key.to_compressed_bytes()
    v4_identity_scheme_entries = {
        b"id": V4IdentityScheme.id,
        V4IdentityScheme.public_key_enr_key: public_key,
    }

    if args.ip_address is not None:
        ip_address_entry = {IP_V4_ADDRESS_ENR_KEY: inet_aton(args.ip_address)}
    else:
        ip_address_entry = {}

    if args.udp_port is not None:
        if not isinstance(args.udp_port, int) or args.udp_port <= 0:
            raise ValueError(f"UDP port must be a positive integer, but is {args.udp_port}")
        udp_port_entry = {UDP_PORT_ENR_KEY: args.udp_port}
    else:
        udp_port_entry = {}

    if args.data is not None:
        data_dict = json.loads(args.data)
        if not isinstance(data_dict, dict):
            raise ValueError("Data must be JSON encoded dictionary")
        for key, value in data_dict.items():
            if not isinstance(key, str):
                raise ValueError(f"Data keys must be strings, got {key}")
            if not isinstance(value, (str, int)):
                raise ValueError(f"Data values must be strings or integers, got {value}")

        additional_kv_pairs = {
            key.encode("ascii"): value.encode("ascii") if isinstance(value, str) else value
            for key, value in data_dict.items()
        }
    else:
        additional_kv_pairs = {}

    if args.sequence_number is not None:
        sequence_number = args.sequence_number
    else:
        sequence_number = 1

    kv_pairs = {
        **v4_identity_scheme_entries,
        **ip_address_entry,
        **udp_port_entry,
        **additional_kv_pairs,
    }
    unsigned_enr = UnsignedENR(
        sequence_number=sequence_number,
        kv_pairs=kv_pairs,
    )
    enr = unsigned_enr.to_signed_enr(private_key)
    return private_key, enr


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tool to create DiscV5 node identities"
    )
    parser.add_argument(
        "-p", "--private-key",
        help="The node's hex encoded private key",
    )
    parser.add_argument(
        "-a", "--ip-address",
        help="The IP V4 address of the endpoint the node is reachable at",
    )
    parser.add_argument(
        "-u", "--udp-port",
        type=int,
        help="The node's UDP port of the endpoint the node is reachable at"
    )
    parser.add_argument(
        "-d", "--data",
        help="JSON encoded key-value pairs to include in the ENR",
    )
    parser.add_argument(
        "-s", "--sequence_number",
        type=int,
        help="The ENR's sequence number",
    )
    parser.add_argument(
        "-e", "--enr",
        help="The node's ENR in its canonical representation format. If given, it must be the "
             "only argument and only the node ID will be extracted",
    )
    args = parser.parse_args()

    if args.enr is not None:
        for key, value in vars(args).items():
            all_args = set(key for key, value in vars(args).items() if value is not None)
            if all_args - {"enr"}:
                print(all_args)
                raise ValueError("ENR is given, but it's not the only argument")

    if args.enr is not None:
        enr = ENR.from_repr(args.enr)
        private_key = None
    else:
        private_key, enr = create_identity(args)

    print_output(private_key, enr=enr)
