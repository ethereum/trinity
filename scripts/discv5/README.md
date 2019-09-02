# DiscV5 Tests

This directory contains two scripts: `discv5.py` and `create_identity.py`

## discv5.py

`discv5.py` starts a Trinity DiscV5 node. No other components of Trinity are run. There is no persistent state, so it always starts from zero. It should be configured via two YAML files whose filename are given as CLI parameters:

- `--config`, `-c`: Required
- `--logging`, `-l`: Optional

### config

The config file defines the node's identity, initial routing table and ENR db, and the UDP socket hostname and port it should use. Here's an example:

```YAML
local_private_key: "0x96ae6a02fe9685dc45e20d4f04390d7fdaba405137bb44b97a2db46330d80dbc"
enrs:
  - enr:-IW4QGSCA9OZjOk14p2uDXDABw_GyCcc2ko6Z9PrkS66sJ23E65smTb08GrS_yJ1ipN8OpExDY1zAlHpz9D4ZSnrbAEBgmlkgnY0gmlwhH8AAAGJc2VjcDI1NmsxoQPoUBRAhF9QakPlj7ZK_5lqEHC_jZJ3kEWZ4iUIPIg254N1ZHCDBipI
  - enr:-IW4QI21ZNk9LThfR8suvr4sr05Job0HaAucpp-XpYaA2NNoYRkJi2DdFZ4i5VBW7axPK7MVg63zxw_qm4TClAXogLYBgmlkgnY0gmlwhH8AAAGJc2VjcDI1NmsxoQOpw6TAKiZq1HaZdL-MBJgGJ6L9V7ntwzk5JbTJ8luwf4N1ZHCDBipJ

routing_table:
  - "0xd5ae289621a449e9a1d1611dee733454ffa71340e81da0727e064ad370207ec0"
host: "127.0.0.1"
port: 202020
```

The local private key should be a hex encoded 32 byte string (don't forget the quotes, otherwise YAML will interpret it as an integer).

The ENR section is a list of ENRs in their canonical base64 representation. It should at least contain the node's own ENR, otherwise it will crash.

The routing table should contain hex encoded 32 byte node ids. It can be empty, but then it won't be able to do anything until another node connects to it (even if there are ENRs in the ENR DB). The ENR DB should contain an ENR for each entry in the routing table and the ENR should contain an IP address and a port (otherwise we can't ping them).

Host and port define the address the socket should bind to. Initially I tried `404040` which kind of worked, but apparently these high port numbers are reassigned by the OS, so it's probably better to use something lower.

### logging

This can be used to granularly configure the log output. It's especially useful to silence some components, but still be able to see the output of others. It is passed directly to `logging.config.dictConfig`. Example:

```YAML
formatters:
  default:
    format: '%(levelname)-8s %(name)-15s %(message)s'

handlers:
  console:
    level: DEBUG
    class: logging.StreamHandler
    formatter: default

loggers:
  "":
    level: INFO
    handlers: [console]

  "p2p.trio_service.Manager":
    level: INFO

  "p2p.discv5.channel_services":
    level: INFO

  "p2p.discv5.packer":
    level: INFO

  "p2p.discv5.message_dispatcher":
    level: INFO

  "p2p.discv5.routing_table_manager":
    level: DEBUG

  "p2p.discv5.routing_table":
    level: DEBUG

  "p2p.discv5.enr_db":
    level: DEBUG
```


## create_identity.py

This script creates random private keys as well as corresponding node ids and ENRs. There are a few CLI options, most of which define the key value pairs that will end up in the ENR:

- `--ip-address`, `-a`: The (human readable representation of) the endpoint's IP address
- `--udp-port`, `-u`: The UDP port number of the node's endpoint
- `--data`, `-d`: JSON encoded key value pairs to include in addition (both keys and values must be ASCII encoded)
- `--sequence-number`, `-s`: The ENRs sequence number (better not use 0)

In addition, there is:

- `--private-key`, `-p`: The hex encoded private key to use for signing the ENR (if not given, a random one will be generated)
- `--enr`, `-e`: Kind of special, an ENR in canonical base64 representation. If given, no other arguments should be given. This can be used to extract the node id of the signer.

Example:

```
> python create_identity.py -a 127.0.0.1 -u 404041 -d '{"eth": "2"}'
Private key: 0x1ad846dcb39667f16a56a435b1b28de1467f72369026158dfc3bbb1615528381
Node ID:     0xf60a226cd66251cd09916687350472c139598937b007b3019a104449c044039f
ENR:         enr:-Iq4QAHg5XNlaQrR3B592-NEAoF4ZOR-H89rJNyI6V2efobqQ6N7sxw4gfEHDnLe3_q6eALd4zujJIMgrwb2Xg-X4xMBg2V0aDKCaWSCdjSCaXCEfwAAAYlzZWNwMjU2azGhA-Zazsz1gGZjsSNSB6B3RZCtYp0agJCt3mBI_-EVjATig3VkcIMGKkk
```
