Release Notes 
=============

Trinity is moving fast. Read up on all the latest improvements.

.. towncrier release notes start

Trinity 0.1.0-alpha.30 (2019-11-13)
-----------------------------------

Features
~~~~~~~~

- Upgrade to Py-EVM ``0.3.0a8`` adding the planned Istanbul block for mainnet. See all the
  `other changes in the latest py-evm <https://py-evm.readthedocs.io/en/latest/release_notes.html#py-evm-0-3-0-alpha-8-2019-11-05>`_ (`#1255 <https://github.com/ethereum/trinity/issues/1255>`__)


Bugfixes
~~~~~~~~

- Fix JSON-RPC eth_syncing endpoint that was accidentally removed in v0.1.0-alpha.23 (`#765 <https://github.com/ethereum/trinity/issues/765>`__)


Internal Changes - for Trinity Contributors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Refactor handling and code organization of pre-configured networks to make it easier to
  add support for new networks. (`#1260 <https://github.com/ethereum/trinity/issues/1260>`__)


Trinity 0.1.0-alpha.29 (2019-09-30)
-----------------------------------

Features
~~~~~~~~

- Added Istanbul block number to default Ropsten configuration: 6485846 (`#907 <https://github.com/ethereum/trinity/issues/907>`__)
- Upgrade `ipython` shell to `7.8.0` which supports `async` / `await` hence improves
  the UI/UX of `trinity attach` and `trinity db-shell`. (`#1203 <https://github.com/ethereum/trinity/issues/1203>`__)


Bugfixes
~~~~~~~~

- Fixed handshake bug that caused all inbound connections to fail with: ``"AttributeError: 'Session' object has no attribute 'address'"`` (`#1129 <https://github.com/ethereum/trinity/issues/1129>`__)
- Ensure EthStatsService properly handles lost connections to the server (`#1139 <https://github.com/ethereum/trinity/issues/1139>`__)


Improved Documentation
~~~~~~~~~~~~~~~~~~~~~~

- Document how to install, run and develop with the Trinity DappNode package. (`#1082 <https://github.com/ethereum/trinity/issues/1082>`__)


Internal Changes - for Trinity Contributors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Add a ``make create-dappnode-image`` command to expose Trinity as a DappNode package. The
  package can be found as ``trinity.public.dappnode.eth``. (`#1082 <https://github.com/ethereum/trinity/issues/1082>`__)
- ``Plugins`` are now called ``Components``. We've found ``Components`` to be a
  better term for the provided functionality, especially since it is less loaded
  with the assumption of being something that *optionally extends* functionality
  when in reality Trinity's core functionality is built out of ``Components``. (`#1140 <https://github.com/ethereum/trinity/issues/1140>`__)
- Add ``ConnectionAPI.get_protocol_for_command_type`` (`#1145 <https://github.com/ethereum/trinity/issues/1145>`__)
- Add ``ConnectionAPI.get_receipt_by_type(receipt_type: Type[ReceiptAPI])`` API (`#1148 <https://github.com/ethereum/trinity/issues/1148>`__)
- The ``ConnectionAPI`` now has a mirrored version of ``MultiplexerAPI.has_protocol`` via ``ConnectionAPI.has_protocol`` (`#1181 <https://github.com/ethereum/trinity/issues/1181>`__)


Miscellaneous changes
~~~~~~~~~~~~~~~~~~~~~

- `#1135 <https://github.com/ethereum/trinity/issues/1135>`__, `#1142 <https://github.com/ethereum/trinity/issues/1142>`__, `#1150 <https://github.com/ethereum/trinity/issues/1150>`__


Trinity 0.1.0-alpha.28 (2019-09-12)
-----------------------------------

Features
~~~~~~~~

- Remove Trinity specific subclass of the ``lahja`` endpoint in favor of using the core ``EndpointAPI`` everywhere.  The previous functionality from the ``TrinityEventBusEndpoint`` is now handled by a special service designed to manage the endpoint lifecycle. (`#672 <https://github.com/ethereum/trinity/issues/672>`__)
- Allow trinity db-shell to inspect the beacon node (`#809 <https://github.com/ethereum/trinity/issues/809>`__)
- Expose ``NewBlockEvent`` on the event bus. (`#822 <https://github.com/ethereum/trinity/issues/822>`__)
- Add ``p2p.p2p_proto.P2PProtocol.send_ping`` and ``p2p.p2p_proto.P2PProtocol.send_hello`` methods. (`#826 <https://github.com/ethereum/trinity/issues/826>`__)
- Add ``p2p.peer.receive_handshake`` to encapsulate the logic for handling incoming connections. (`#828 <https://github.com/ethereum/trinity/issues/828>`__)
- The ``p2p.p2p_proto.P2PProtocol`` class now requires that handshake parameters be passed into the ``send_handshake`` method.  These parameters are now part of the ``p2p.peer.BasePeerContext`` class. (`#829 <https://github.com/ethereum/trinity/issues/829>`__)
- Add a new ``p2p.tools.factories.TransportPairFactory`` for generating directly connected ``p2p.transport.Transport`` objects. (`#830 <https://github.com/ethereum/trinity/issues/830>`__)
- Add ``p2p.multiplexer.Multiplexer`` for combining the commands from different devp2p sub-protocols into a single network write stream, and split the incoming network stream into individually retrievable sub-protocol commands. (`#835 <https://github.com/ethereum/trinity/issues/835>`__)
- Adds ``p2p.protocol.get_cmd_offsets`` helper function for computing the command id offsets for devp2p protocols (`#836 <https://github.com/ethereum/trinity/issues/836>`__)
- Use the ``p2p.multiplexer.Multiplexer`` within the ``BasePeer`` to handle the incoming message stream. (`#847 <https://github.com/ethereum/trinity/issues/847>`__)
- Add factories for creating devp2p protocols and commands for testing. (`#850 <https://github.com/ethereum/trinity/issues/850>`__)
- Beam Sync: parallel execution of blocks. When connected to a peer on a local network, can now
  keep up with mainnet (assuming a beefy machine). Also added beam stats in the logs. (`#855 <https://github.com/ethereum/trinity/issues/855>`__)
- Replace ``multiprocessing`` based database access with a custom implementation that increases database access performance by 1.5-2x (`#859 <https://github.com/ethereum/trinity/issues/859>`__)
- Implement ``p2p.handshake`` API.  This provides a generic interface for
  performing proper DevP2p handshakes using multiple sub-protocols without
  needing involvement of the ``BasePeer``. (`#869 <https://github.com/ethereum/trinity/issues/869>`__)
- Use the new ``p2p.handshake`` APIs in the ``p2p.peer.BasePeer`` handshake logic. (`#887 <https://github.com/ethereum/trinity/issues/887>`__)
- If Trinity is beam syncing and a call to `eth_getBalance` requests data which is not in
  the local database, Trinity asks for the data over the network. (`#894 <https://github.com/ethereum/trinity/issues/894>`__)
- Speculative Execution in Beam Sync: split block transactions to run them in parallel, for speedup. (`#899 <https://github.com/ethereum/trinity/issues/899>`__)
- Allow beam sync to start from a trusted checkpoint.
  Specify a checkpoint via CLI parameter such as:

  ``--beam-from-checkpoint="eth://block/byhash/<hash>?score=<score>"``

  When given, beam sync will use this as a checkpoint
  to avoid having to download the entire chain of headers
  first. (`#921 <https://github.com/ethereum/trinity/issues/921>`__)
- Expose the `force-beam-block-number` config as a command line parameter.
  The config is useful for testing to force beam sync to activate on a given block number. (`#923 <https://github.com/ethereum/trinity/issues/923>`__)
- Add ``p2p_version`` to ``p2p.peer.BasePeerContext`` properties and use for handshake. (`#931 <https://github.com/ethereum/trinity/issues/931>`__)
- If `eth_getCode` is called during beam sync but the requested data is not available
  locally trinity will attempt to fetch the requested data from remote peers. (`#944 <https://github.com/ethereum/trinity/issues/944>`__)
- Beam Sync: start backfilling data, especially as a way to gather performance data about peers, and
  improve the performance of beam sync importing. (`#951 <https://github.com/ethereum/trinity/issues/951>`__)
- Add ``p2p.service.run_service`` which implements a context manager API for running a ``p2p.service.BaseService``. (`#955 <https://github.com/ethereum/trinity/issues/955>`__)
- Add ``p2p.connection.Connection`` service which actively manages the ``p2p.multiplexer.Multiplexer`` exposing an API for registering handler callbacks for individuall protocol commands or entire protocols, as well as access to general metadata about the p2p connection. (`#956 <https://github.com/ethereum/trinity/issues/956>`__)
- If `eth_getStorageAt` is called during beam sync but the requested data is not available
  locally trinity will attempt to fetch the requested data from remote peers. (`#957 <https://github.com/ethereum/trinity/issues/957>`__)
- ``p2p.peer.BasePeer`` now uses ``ConnectionAPI`` for underlying protocol interactions. (`#962 <https://github.com/ethereum/trinity/issues/962>`__)
- Allow Trinity to automatically resolve a checkpoint through the etherscan API
  using this syntax: ``--beam-from-checkpoint="eth://block/byetherscan/latest"`` (`#963 <https://github.com/ethereum/trinity/issues/963>`__)
- Fetch missing data from remote peers, if requested over json-rpc during beam sync.
  Requests for data at an old block will fail; remote peers probably don't have it. (`#975 <https://github.com/ethereum/trinity/issues/975>`__)
- Expose the ``MiningChain`` on the `db-shell` REPL to allow creating blocks on a REPL (`#977 <https://github.com/ethereum/trinity/issues/977>`__)
- Add ``ConnectionAPI.get_p2p_receipt`` for fetching the ``HandshakeReceipt`` for the base ``p2p`` protocol. (`#986 <https://github.com/ethereum/trinity/issues/986>`__)
- ``p2p.protocol.Protocol.supports_command`` is now a ``classmethod`` (`#987 <https://github.com/ethereum/trinity/issues/987>`__)
- The ``HandlerSubscriptionAPI`` now supports a context manager interface, removing/cancelling the subscription when the context exits (`#989 <https://github.com/ethereum/trinity/issues/989>`__)
- Handler functions for ``Connection.add_protocol_handler`` and ``Connection.add_command_handler`` now expect the ``Connection`` instance as the first argument. (`#990 <https://github.com/ethereum/trinity/issues/990>`__)
- Introduce ``p2p.session.Session`` which is now used in place of the ``remote`` to identify peers in the peer pool. (`#1054 <https://github.com/ethereum/trinity/issues/1054>`__)
- Add ``HTTPServer`` for JSON-RPC over HTTP APIs. (`#1078 <https://github.com/ethereum/trinity/issues/1078>`__)
- Make `beam` the default sync strategy and remove `fast` sync. (`#1084 <https://github.com/ethereum/trinity/issues/1084>`__)
- Detect if a checkpoint is too close to the tip and delay sync until we have reached a minimum
  distance to the tip. (`#1107 <https://github.com/ethereum/trinity/issues/1107>`__)


Bugfixes
~~~~~~~~

- Proper cancellation of subtasks upon cancellation of ``p2p.service.BaseService`` (`#809 <https://github.com/ethereum/trinity/issues/809>`__)
- The recently introduced fix that ensures we do not run multiple concurrent
  handshakes to the same peer accidentially introduced a (rarely exposed) memory
  leak. This fix introduces a ``ResourceLock`` and refactores the code to use it
  to also fix the previously introduced memory leak. (`#811 <https://github.com/ethereum/trinity/issues/811>`__)
- Fix issue where test state was leaking between tests in ``tests/p2p/test_discovery.py`` (`#839 <https://github.com/ethereum/trinity/issues/839>`__)
- Beam Sync: Serve node data requests in parallel, instead of series (`#857 <https://github.com/ethereum/trinity/issues/857>`__)
- Fix for ``DEBUG2`` logs always being shown irrespective of log level. (`#860 <https://github.com/ethereum/trinity/issues/860>`__)
- Beam Sync stats: Count the extra single node that is sometimes required when downloading the nodes
  needed to look up an account or storage. (Usually because of a trie reorg) (`#877 <https://github.com/ethereum/trinity/issues/877>`__)
- Fixes issue with Trinity not shutting down when issues a ``CTRL+C``. (`#878 <https://github.com/ethereum/trinity/issues/878>`__)
- Fix ``__str__`` implementation of ``BaseProxyPeer`` to properly represent the ``p2p.kademlia.Node`` URI. (`#881 <https://github.com/ethereum/trinity/issues/881>`__)
- Add missing field `from` to the response of `RPC` calls `eth_getTransactionByBlockHashAndIndex` and `eth_getTransactionByBlockNumberAndIndex`. (`#889 <https://github.com/ethereum/trinity/issues/889>`__)
- Ensure ``--profile`` parameter takes profiles of every process (`#891 <https://github.com/ethereum/trinity/issues/891>`__)
- Handle escaping ``PeerConnectionLost`` exception from ``Multiplexer`` in ``BasePeer`` (`#895 <https://github.com/ethereum/trinity/issues/895>`__)
- Fix JSON-RPC call `eth_getBalance(address, block_number)` to return balance at the requested block_number.
  Earlier it would always return balance at `block(0)`. (`#900 <https://github.com/ethereum/trinity/issues/900>`__)
- Fix a MissingTrieNode exception when the first imported block has an uncle (`#909 <https://github.com/ethereum/trinity/issues/909>`__)
- Handles ``MalformedMessage`` and ``TimeoutError`` exceptions that can occur while multiplexing the devp2p connection (`#916 <https://github.com/ethereum/trinity/issues/916>`__)
- Fix type hints so that ``max_headers`` is recognized as keyword argument
  to ``get_block_headers``. (`#921 <https://github.com/ethereum/trinity/issues/921>`__)
- ``BootManager`` now uses the ``BasePeer.loop`` as well as their cancel token. (`#926 <https://github.com/ethereum/trinity/issues/926>`__)
- Fix a deadlock bug: if you request data from a peer at just the wrong moment, the request would hang
  forever. Now, it correctly raises an ``OperationCancelled``. (`#932 <https://github.com/ethereum/trinity/issues/932>`__)
- ``ETHHandshakeReceipt`` and ``LESHandshakeReceipt`` now properly accept their protocol instances in their constructors. (`#934 <https://github.com/ethereum/trinity/issues/934>`__)
- Pin ``lahja==0.14.0`` until connection timeout issue is resolved. (`#936 <https://github.com/ethereum/trinity/issues/936>`__)
- Beam Sync: catch the TimeoutError that was escaping, and retry (`#939 <https://github.com/ethereum/trinity/issues/939>`__)
- Ensure the ``BasePeer`` negotiates the proper base protocol. (`#942 <https://github.com/ethereum/trinity/issues/942>`__)
- Capture :class:`PeerConnectionLost` in more places, especially sync. (`#943 <https://github.com/ethereum/trinity/issues/943>`__)
- Beam Sync: Sometimes we would get stuck using a bad peer for node retrieval, fixed. Sometimes we
  would stop asking for predicted trie nodes when we don't have any immediate nodes to ask for, fixed. (`#958 <https://github.com/ethereum/trinity/issues/958>`__)
- Fix ``p2p.tools.factories.MultiplexerPairFactory`` negotiation of ``p2p`` protocol version. (`#964 <https://github.com/ethereum/trinity/issues/964>`__)
- Add missing exception handling inside of ``Connection.run`` for ``PeerConnectionLost`` exception that bubbles from multiplexer.  ``Connection`` is now responsible for calling ``Multiplexer.close`` on shutdown.  Detect a closed connection during handshake. (`#992 <https://github.com/ethereum/trinity/issues/992>`__)
- Fix ``P2PProtocol.send_disconnect`` to accept enum values from ``p2p.disconnect.DisconnectReason`` (`#994 <https://github.com/ethereum/trinity/issues/994>`__)
- Instead of the ``ProcessPoolExecutor`` use a ``ThreadPoolExecutor`` to normalize
  expensive messages. This fixes a bug where Trinity would leave idle processes
  from the ``ProcessPoolExecutor`` behind every time it shuts down after a sync.

  Performance wise, both methods should be roughly compareable and since many
  task have already been moved to their own managed processes over time, using
  a ``ThreadPoolExecutor`` strikes as a simple solution to fix that bug. (`#1004 <https://github.com/ethereum/trinity/issues/1004>`__)
- Fix a bug where trying to start beam sync from a checkpoint would throw an error
  due to an uninitialized var if a request to a peer would raise an error while
  we are trying to resolve a header from it. (`#1005 <https://github.com/ethereum/trinity/issues/1005>`__)
- Fix for ``TrioService.run_task`` to ensure that when a background task throws an unhandled exception that it causes full service cancellation and that the exception is propagated. (`#1040 <https://github.com/ethereum/trinity/issues/1040>`__)
- Fix issue where Trinity does not recognize and disconnect from ETC peers
  when it is being used as an ETH client (`#1050 <https://github.com/ethereum/trinity/issues/1050>`__)
- Handle ``MalformedMessage`` rising out of the ``Transport`` in the ``Connection``. (`#1051 <https://github.com/ethereum/trinity/issues/1051>`__)
- Ensure discovery V4 handles invalid command ids gracefully (`#1063 <https://github.com/ethereum/trinity/issues/1063>`__)
- Fix issue where attempts to establish new peer connections would halt shortly after startup due to missing timeout when attempting to dial a peer. (`#1069 <https://github.com/ethereum/trinity/issues/1069>`__)
- An exception while serving peer requests would crash out the peer pool event server.
  Now it doesn't crash, but logs a big red error (and catches innocuous exceptions, early on). (`#1074 <https://github.com/ethereum/trinity/issues/1074>`__)
- An occasional warning "ValidationError: Duplicate tasks detected" was crashing the node. It's
  recoverable, so log it, but don't crash. (`#1083 <https://github.com/ethereum/trinity/issues/1083>`__)
- Fix warning on ethstats.net due to incorrectly reported API version number. (`#1094 <https://github.com/ethereum/trinity/issues/1094>`__)
- Fix warning caused by inappropriate call to ``cancel_nowait``. (`#99999 <https://github.com/ethereum/trinity/issues/99999>`__)


Performance improvements
~~~~~~~~~~~~~~~~~~~~~~~~

- Immediately insert Beam Sync nodes that are "predicted" (soon to be used during parallel execution)
  This saves a round trip on live execution, when parallel execution already downloaded a node.
  Also, more aggressively make predictive requests if no urgent requests are waiting in the queue. (`#877 <https://github.com/ethereum/trinity/issues/877>`__)
- Previously, we gave up on predicted nodes that were not returned by a peer. Now we retry them,
  which helps make sure we aren't missing any nodes at block import time. (`#932 <https://github.com/ethereum/trinity/issues/932>`__)
- During Beam Sync previews, be sure to collect the nodes required to generate the new state root,
  rather than wait until it's time to import the block. (`#933 <https://github.com/ethereum/trinity/issues/933>`__)


Improved Documentation
~~~~~~~~~~~~~~~~~~~~~~

- Add a "Performance improvements" section to the release notes (`#884 <https://github.com/ethereum/trinity/issues/884>`__)
- Cleanup Quickstart and start a Cookbook with small recipes (`#890 <https://github.com/ethereum/trinity/issues/890>`__)
- Cover ``--profile`` parameter in Cookbook (`#891 <https://github.com/ethereum/trinity/issues/891>`__)
- Add a guide on how to create a custom developer testnet using a genesis configuration file (`#1037 <https://github.com/ethereum/trinity/issues/1037>`__)


Deprecations and Removals
~~~~~~~~~~~~~~~~~~~~~~~~~

- Remove ``p2p._utils.clamp`` in favor of the one from ``eth-utils>=1.5.2`` (`#832 <https://github.com/ethereum/trinity/issues/832>`__)
- Remove unused ``token`` argument from ``p2p.tools.memory_transport.MemoryTransport`` constructor (`#838 <https://github.com/ethereum/trinity/issues/838>`__)
- Remove legacy tests from core application code. (`#882 <https://github.com/ethereum/trinity/issues/882>`__)
- Remove the ``FakeAsync...`` classes from tests in favor of using the real versions for things like chain and database objects (`#949 <https://github.com/ethereum/trinity/issues/949>`__)


Miscellaneous internal changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- `#818 <https://github.com/ethereum/trinity/issues/818>`__, `#879 <https://github.com/ethereum/trinity/issues/879>`__, `#880 <https://github.com/ethereum/trinity/issues/880>`__, `#915 <https://github.com/ethereum/trinity/issues/915>`__, `#917 <https://github.com/ethereum/trinity/issues/917>`__, `#927 <https://github.com/ethereum/trinity/issues/927>`__, `#928 <https://github.com/ethereum/trinity/issues/928>`__, `#929 <https://github.com/ethereum/trinity/issues/929>`__, `#930 <https://github.com/ethereum/trinity/issues/930>`__, `#932 <https://github.com/ethereum/trinity/issues/932>`__, `#935 <https://github.com/ethereum/trinity/issues/935>`__, `#938 <https://github.com/ethereum/trinity/issues/938>`__, `#950 <https://github.com/ethereum/trinity/issues/950>`__, `#965 <https://github.com/ethereum/trinity/issues/965>`__, `#983 <https://github.com/ethereum/trinity/issues/983>`__, `#985 <https://github.com/ethereum/trinity/issues/985>`__, `#988 <https://github.com/ethereum/trinity/issues/988>`__, `#991 <https://github.com/ethereum/trinity/issues/991>`__, `#993 <https://github.com/ethereum/trinity/issues/993>`__, `#995 <https://github.com/ethereum/trinity/issues/995>`__, `#997 <https://github.com/ethereum/trinity/issues/997>`__, `#1021 <https://github.com/ethereum/trinity/issues/1021>`__, `#1043 <https://github.com/ethereum/trinity/issues/1043>`__, `#1045 <https://github.com/ethereum/trinity/issues/1045>`__, `#1052 <https://github.com/ethereum/trinity/issues/1052>`__, `#1055 <https://github.com/ethereum/trinity/issues/1055>`__, `#1066 <https://github.com/ethereum/trinity/issues/1066>`__, `#1075 <https://github.com/ethereum/trinity/issues/1075>`__


Trinity 0.1.0-alpha.27 (2019-07-17)
-----------------------------------

Bugfixes
~~~~~~~~

- Don't require blspy by default, which was breaking doc builds and making standard installs more
  difficult (by requiring cmake) (`#805 <https://github.com/ethereum/trinity/issues/805>`__)


Misc
~~~~

- `#806 <https://github.com/ethereum/trinity/issues/806>`__


Trinity 0.1.0-alpha.26 (2019-07-16)
-----------------------------------

Features
~~~~~~~~

- Expose certain peer pool events and move RequestServer into an isolated plugin (`#617 <https://github.com/ethereum/trinity/issues/617>`__)
- Run UPnP Service as an isolated plugin (plus `#730 <https://github.com/ethereum/trinity/pull/730>`_ fixup) (`#636 <https://github.com/ethereum/trinity/issues/636>`__)
- Log the gap time between the imported header and now; don't blast screen with logs when regular syncing a lot (`#646 <https://github.com/ethereum/trinity/issues/646>`__)
- Make logging config more ergonomic, flexible and consistent (`#682 <https://github.com/ethereum/trinity/issues/682>`__)
- In-memory ``Transport`` for use in testing. (`#693 <https://github.com/ethereum/trinity/issues/693>`__)
- Expose events for Transaction and NewBlockHashes commands on the EventBus (`#696 <https://github.com/ethereum/trinity/issues/696>`__)
- Use ``trinity db-shell`` to connect to a running process and inspect its database (`#728 <https://github.com/ethereum/trinity/issues/728>`__)
- Pool management upgrades

  - Move TransactionPool into its own process
  - Create ProxyPeerPool that partially exposes peer pool functionality to any process (`#734 <https://github.com/ethereum/trinity/issues/734>`__)

- Move responsibility for receiving handshake into ``p2p.transport.Transport`` class. (`#755 <https://github.com/ethereum/trinity/issues/755>`__)
- Trinity can now autocomplete CLI parameters on ``<tab>``.
  Learn how to activate autocomplete in the :doc:`docs</api/api.cli>`. (`#768 <https://github.com/ethereum/trinity/issues/768>`__)
- Implement ``p2p.trio_service.Service`` abstraction using ``trio`` as a loose
  replacement for the existing ``asyncio`` based ``p2p.service.BaseService``. (`#790 <https://github.com/ethereum/trinity/issues/790>`__)
- trinity attach can now accept path to ipc as parameter
  Learn more :doc:`docs</api/api.cli>`. (`#796 <https://github.com/ethereum/trinity/issues/796>`__)


Bugfixes
~~~~~~~~

- Header syncing is now limited in how far ahead of block sync it will go (`#704 <https://github.com/ethereum/trinity/issues/704>`__)
- Prevent ``KeyError`` exception raised at ``del self._dependencies[prune_task_id]`` during syncing (`#731 <https://github.com/ethereum/trinity/issues/731>`__)
- Fix a race condition in Trinity's event bus announcement ceremony (`#763 <https://github.com/ethereum/trinity/issues/763>`__)
- Several very uncommon issues during syncing, more likely during beam sync (`#772 <https://github.com/ethereum/trinity/issues/772>`__)
- Squashed bug that redownloads block bodies and logs this warning:
  ``ValidationError: Cannot finish prereq BlockImportPrereqs.StoreBlockBodies of task`` (`#780 <https://github.com/ethereum/trinity/issues/780>`__)
- When starting beam sync, download previous six block bodies, so that uncle validation can succeed.
  Import needs to verify that new block imports don't add uncles that were already added. (`#803 <https://github.com/ethereum/trinity/issues/803>`__)


Improved Documentation
~~~~~~~~~~~~~~~~~~~~~~

- Setup towncrier to generate release notes from fragment files to  ensure a higher standard
  for release notes. (`#754 <https://github.com/ethereum/trinity/issues/754>`__)
- Cover ``trinity.protocol`` events in API docs (`#766 <https://github.com/ethereum/trinity/issues/766>`__)
- Cover :class:`~trinity.config.TrinityConfig`, :class:`~trinity.config.Eth1AppConfig` and
  :class:`~trinity.config.BeaconAppConfig` in API docs. (`#775 <https://github.com/ethereum/trinity/issues/775>`__)
- Improve layout of API docs by grouping classmethods, methods and attributes. (`#778 <https://github.com/ethereum/trinity/issues/778>`__)
- In the API docs display class methods, static methods and methods as one group "methods".
  While we ideally wish to separate these, Sphinx keeps them all as one group which we'll
  be following until we find a better option. (`#794 <https://github.com/ethereum/trinity/issues/794>`__)


Deprecations and Removals
~~~~~~~~~~~~~~~~~~~~~~~~~

- Plugin removals

  - Remove ``BaseAsyncStopPlugin`` which isn't needed anymore now that there is no
    hardwired ``networking`` process anymore.
  - Remove plugin manager scopes which aren't needed anymore now that there is only
    a single ``PluginManager``. (`#763 <https://github.com/ethereum/trinity/issues/763>`__)

- The ``get_chain_config`` API was moved from the ``TrinityConfig`` to the ``Eth1AppConfig`` (`#771 <https://github.com/ethereum/trinity/issues/771>`__)


v0.1.0-alpha.25
--------------------------

Released 2019-06-05

- Upgraded py-evm to deal with eth-keys v0.3.0 dependency issue --
  `see commit <https://github.com/ethereum/trinity/commit/55d70bafb6e8d6918fee91ad54da721bdc5ed185>`_

v0.1.0-alpha.24
--------------------------

Released 2019-05-21

- `#637 <https://github.com/ethereum/trinity/pull/637>`_: EVM upgrade: py-evm upgraded to v0.2.0-alpha.43, changes copied here, from `the py-evm changelog <https://py-evm.readthedocs.io/en/latest/release_notes/index.html#alpha-43>`_

  - `#1778 <https://github.com/ethereum/py-evm/pull/1778>`_: Feature: Raise custom decorated exceptions when a trie node is missing from the database (plus some bonus logging and performance improvements)
  - `#1732 <https://github.com/ethereum/py-evm/pull/1732>`_: Bugfix: squashed an occasional "mix hash mismatch" while syncing
  - `#1716 <https://github.com/ethereum/py-evm/pull/1716>`_: Performance: only calculate & persist state root at end of block (post-Byzantium)
  - `#1735 <https://github.com/ethereum/py-evm/pull/1735>`_:

    - Performance: only calculate & persist storage roots at end of block (post-Byzantium)
    - Performance: batch all account trie writes to the database once per block
  - `#1747 <https://github.com/ethereum/py-evm/pull/1747>`_:

    - Maintenance: Lazily generate VM.block on first access. Enables loading the VM when you don't have its block body.
    - Performance: Fewer DB reads when block is never accessed.
  - Performance: speedups on ``chain.import_block()``:

    - `#1764 <https://github.com/ethereum/py-evm/pull/1764>`_: Speed up ``is_valid_opcode`` check, formerly 7% of total import time! (now less than 1%)
    - `#1765 <https://github.com/ethereum/py-evm/pull/1765>`_: Reduce logging overhead, ~15% speedup
    - `#1766 <https://github.com/ethereum/py-evm/pull/1766>`_: Cache transaction sender, ~3% speedup
    - `#1770 <https://github.com/ethereum/py-evm/pull/1770>`_: Faster bytecode iteration, ~2.5% speedup
    - `#1771 <https://github.com/ethereum/py-evm/pull/1771>`_: Faster opcode lookup in apply_computation, ~1.5% speedup
    - `#1772 <https://github.com/ethereum/py-evm/pull/1772>`_: Faster Journal access of latest data, ~6% speedup
    - `#1773 <https://github.com/ethereum/py-evm/pull/1773>`_: Faster stack operations, ~9% speedup
    - `#1776 <https://github.com/ethereum/py-evm/pull/1776>`_: Faster Journal record & commit checkpoints, ~7% speedup
    - `#1777 <https://github.com/ethereum/py-evm/pull/1777>`_: Faster bytecode navigation, ~7% speedup
  - `#1751 <https://github.com/ethereum/py-evm/pull/1751>`_: Maintenance: Add placeholder for Istanbul fork
- `#629 <https://github.com/ethereum/trinity/pull/629>`_: Feature: Peers which disconnect from us too quickly are blacklisted for a short period of time.
- `#625 <https://github.com/ethereum/trinity/pull/625>`_: Feature: Peer backend system is now sent full list of connected remotes
- `#624 <https://github.com/ethereum/trinity/pull/624>`_: Feature: Better logging and tracking of the reason a peer disconnection occured.
- `#612 <https://github.com/ethereum/trinity/pull/612>`_: Feature: Make Python 3.7 the environment of the ethereum/trinity docker images
- `#596 <https://github.com/ethereum/trinity/pull/596>`_: Feature: ``p2p.PeerPool`` now sources peer candidates using an extendable backend system.
- `#519 <https://github.com/ethereum/trinity/pull/519>`_: Feature: Retain disconnect reason on ``BasePeer`` when we disconnect.
- `#555 <https://github.com/ethereum/trinity/pull/555>`_: Feature: Peers who timeout too often in the Request/Response API will be disconnected from and blacklisted for 5 minutes.
- `#558 <https://github.com/ethereum/trinity/pull/558>`_: Feature: Peers who are disconnected due to a ``bad_protocol`` are blacklisted for 10 minutes.
- `#559 <https://github.com/ethereum/trinity/pull/559>`_: Feature: Peers who send invalid responses are disconnected from using ``bad_protocol``.
- `#569 <https://github.com/ethereum/trinity/pull/569>`_: Feature: Log messages with sequences of block numbers now use a concise representation to reduce message size.
- `#571 <https://github.com/ethereum/trinity/pull/571>`_: Feature: ``BaseService.uptime`` property now exposes integer number of seconds since service was started.
- `#441 <https://github.com/ethereum/trinity/pull/441>`_: Feature: Run with any custom network ID, as long as you specify a genesis file
- `#436 <https://github.com/ethereum/trinity/pull/436>`_: Feature: Connect to preferred nodes even when discovery is disabled
- `#518 <https://github.com/ethereum/trinity/pull/518>`_: Feature: Create log directory for you, if data dir is empty
- `#630 <https://github.com/ethereum/trinity/pull/630>`_: Bugfix: Proper shutdown of the whole trinity process if the network database is corrupt.
- `#618 <https://github.com/ethereum/trinity/pull/618>`_: Bugfix: Can actually connect to other trinity peers now (and syncing peers).
- `#595 <https://github.com/ethereum/trinity/pull/595>`_: Bugfix: Error handling for corrupt snappy data
- `#591 <https://github.com/ethereum/trinity/pull/591>`_: Bugfix: Catch ``RuntimeError`` in handshake to prevent crashing the entire node
- `#469 <https://github.com/ethereum/trinity/pull/469>`_: Bugfix: Fix deprecation warnings from ``p2p.ecies`` module.
- `#527 <https://github.com/ethereum/trinity/pull/527>`_: Bugfix: ``LESPeer`` class now raises proper exceptions for mismatched genesis hash or network id.
- `#531 <https://github.com/ethereum/trinity/pull/431>`_: Bugfix: ``p2p.kademlia.Node`` class is now pickleable.
- `#564 <https://github.com/ethereum/trinity/pull/464>`_: Bugfix: Sub-protocol compatibility matching extracted from ``p2p.BasePeer`` to make it easier to test.
- `#565 <https://github.com/ethereum/trinity/pull/565>`_: Bugfix: ``p2p.Protocol`` and ``p2p.Command`` classes no longer use mutable data structures for class-level properties.
- `#568 <https://github.com/ethereum/trinity/pull/568>`_: Bugfix: Revert to fixed timeout for Request/Response cycle with peer to mitigate incorrect timeouts when networking conditions change.
- `#570 <https://github.com/ethereum/trinity/pull/570>`_: Bugfix: Remove local implementations of humanize utils in favor of ``eth-utils`` library implementations.
- `#485 <https://github.com/ethereum/trinity/pull/485>`_: Bugfix: Ensure Trinity shuts down if Discovery crashes unexpectedly
- `#400 <https://github.com/ethereum/trinity/pull/400>`_: Bugfix: Respect configuration of individual logger (e.g -l p2p.discovery=ERROR)
- `#336 <https://github.com/ethereum/trinity/pull/336>`_: Bugfix: Ensure Trinity shuts down if the process pool dies (fatal error)
- `#347 <https://github.com/ethereum/trinity/pull/347>`_: Bugfix: Don't crash during sync pruning when switching peers
- `#446 <https://github.com/ethereum/trinity/pull/446>`_: Bugfix(es): Several reliability improvements to regular sync
- `#389 <https://github.com/ethereum/trinity/pull/389>`_: Bugfix: Always return contiguous headers from header syncer
- `#493 <https://github.com/ethereum/trinity/pull/493>`_: Performance: Establish peer connections concurrently rather than sequentially.
- `#528 <https://github.com/ethereum/trinity/pull/528>`_: Performance: Limit number of concurrent attempts to establish new peer connections.
- `#536 <https://github.com/ethereum/trinity/pull/536>`_: Performance: Peer connection tracking is now a plugin in the ``trinity`` codebase.
- `#389 <https://github.com/ethereum/trinity/pull/389>`_: Performance: When switching sync to a new lead peer, don't backtrack to importing old headers
- `#556 <https://github.com/ethereum/trinity/pull/556>`_: Performance: Upgrade to lahja 0.13.0 which performs less inter-process communication
- `#386 <https://github.com/ethereum/trinity/pull/386>`_: Performance: Slightly reduce eventbus traffic that the peer pool causes
- `#483 <https://github.com/ethereum/trinity/pull/483>`_: Performance: Speed up normalization of peer messages
- `#608 <https://github.com/ethereum/trinity/pull/608>`_: Maintenance: Enable tests for Constantinople and Petersburg
- `#623 <https://github.com/ethereum/trinity/pull/623>`_: Maintenance: Optimise for faster test runs

0.1.0-alpha.23
--------------------------

Released 2019-02-28

- `#337 <https://github.com/ethereum/trinity/pull/337>`_: Feature: Support for ConstantinopleV2 aka Petersburg aka ConstantinopleFix
- `#270 <https://github.com/ethereum/trinity/pull/270>`_: Performance: Persist information on peers between runs
- `#268 <https://github.com/ethereum/trinity/pull/268>`_: Maintenance: Add more bootnodes, use all the Geth and Parity bootnodes
- `#263 <https://github.com/ethereum/trinity/pull/263>`_: Performance: Upgrade to lahja 0.11.0 and get rid of EventBus coordinator process
- `#227 <https://github.com/ethereum/trinity/pull/227>`_: Bugfix: Do not accidentially create many processes that sit idle
- `#227 <https://github.com/ethereum/trinity/pull/227>`_: Tests: Cover APIs that also hit the database in `trinity attach` tests
- `#155 <https://github.com/ethereum/trinity/pull/155>`_: Feature: Disable syncing entirely with `--sync-mode none`
- `#155 <https://github.com/ethereum/trinity/pull/155>`_: Feature: Allow running `--sync-mode full` directly
- `#155 <https://github.com/ethereum/trinity/pull/155>`_: Feature: Allow plugins to extend `--sync-mode` with different strategies
- `#236 <https://github.com/ethereum/trinity/pull/236>`_: Performance: Quicker pruning of in-memory headers, was a leading asyncio bottleneck
- `#236 <https://github.com/ethereum/trinity/pull/236>`_: Bugfix: Several reliability improvements during sync

0.1.0-alpha.22
--------------

Released Jan 15, 2019

- `#176 <https://github.com/ethereum/trinity/pull/176>`_: Delay Constantinople upgrade

0.1.0-alpha.20
--------------

Released December 13, 2018

- `#1579 <https://github.com/ethereum/py-evm/pull/1579>`_: Feature: Full Constantinople support, with `all* <https://github.com/ethereum/py-evm/blob/fd537be45bafb2041c45a92f3d5240db2bc7f517/tests/json-fixtures/test_blockchain.py#L135-L158>`_ tests passing
- `#1590 <https://github.com/ethereum/py-evm/pull/1590>`_: Performance: CodeStream speedup
- `#1576 <https://github.com/ethereum/py-evm/pull/1576>`_: Bugfix: require recent enough py-ecc to avoid busted py-ecc release (see `#1572 <https://github.com/ethereum/py-evm/pull/1572>`_)
- `#1577 <https://github.com/ethereum/py-evm/pull/1577>`_: Maintenance: Show state diffs on all state failures (see #1573)
- `#1570 <https://github.com/ethereum/py-evm/pull/1570>`_: Maintenance: Cleanup sporadic unclean shutdown of peer request
- `#1580 <https://github.com/ethereum/py-evm/pull/1580>`_: Maintenance: The logged delta in expected vs actual account balance was backwards
- `#1573 <https://github.com/ethereum/py-evm/pull/1573>`_: Maintenance: Display state diffs on failing tests, for much easier EVM debugging
- `#1567 <https://github.com/ethereum/py-evm/pull/1567>`_: Performance: Reduce event bus traffic by enabling point-to-point communication
- `#1569 <https://github.com/ethereum/py-evm/pull/1569>`_: Bugfix: Increase Kademlia timeouts to work on high-latency networks
- `#1530 <https://github.com/ethereum/py-evm/pull/1530>`_: Maintenance: Rename logging level from ``trace`` (reserved for EVM tracing) to ``debug2``
- `#1553 <https://github.com/ethereum/py-evm/pull/1553>`_: Maintenance: Dynamically tune peer timeouts with historical latency (also `#1583 <https://github.com/ethereum/py-evm/pull/1583>`_)
- `#1560 <https://github.com/ethereum/py-evm/pull/1560>`_: Bugfix: Constantinople CREATE2 gas usage
- `#1559 <https://github.com/ethereum/py-evm/pull/1559>`_: Feature: Mainnet configuration now defaults to Constantinople rules at 7080000
- `#1557 <https://github.com/ethereum/py-evm/pull/1557>`_: Docs: Clarify that local plugins must be installed with ``-e``
- `#1538 <https://github.com/ethereum/py-evm/pull/1538>`_: Maintenance: Variety of dependency resolution warning cleanups
- `#1549 <https://github.com/ethereum/py-evm/pull/1549>`_: Maintenance: Separate Plugin space for ``trinity`` and ``trinity-beacon``
- `#1554 <https://github.com/ethereum/py-evm/pull/1554>`_: Maintenance: Enable asynchronous iterators that can be cancelled by a service
- `#1523 <https://github.com/ethereum/py-evm/pull/1523>`_: Maintenance: Much faster testing of valid PoW chains
- `#1536 <https://github.com/ethereum/py-evm/pull/1536>`_: Maintenance: Add ``trinity-beacon`` command as a placeholder for future Beacon Chain
- `#1500 <https://github.com/ethereum/py-evm/pull/1500>`_: Performance: Be smarter about validating the bloom filter, to avoid duplicate hashing
- `#1537 <https://github.com/ethereum/py-evm/pull/1537>`_: Maintenance: Use new event bus feature to avoid the old hack for clean shutdown
- `#1544 <https://github.com/ethereum/py-evm/pull/1544>`_: Docs: Quickstart fix -- use ``trinity attach`` instead of console
- `#1541 <https://github.com/ethereum/py-evm/pull/1541>`_: Docs: Simplify and de-duplicate readme
- `#1533 <https://github.com/ethereum/py-evm/pull/1533>`_: Bugfix: Light chain data lookups regressed during genesis file feature. Fixed
- `#1524 <https://github.com/ethereum/py-evm/pull/1524>`_: Bugfix: Validate header chain continuity during light sync
- `#1528 <https://github.com/ethereum/py-evm/pull/1528>`_: Maintenance: Computation code reorg and gas logging bugfix
- `#1522 <https://github.com/ethereum/py-evm/pull/1522>`_: Bugfix: Increase the system recursion limit for EVM requirements, but never decrease it
- `#1519 <https://github.com/ethereum/py-evm/pull/1519>`_: Docs: Document why we must spawn instead of fork on linux (spoiler: asyncio)
- `#1516 <https://github.com/ethereum/py-evm/pull/1516>`_: Maintenance: Add test for ``trinity attach``
- `#1299 <https://github.com/ethereum/py-evm/pull/1299>`_: Feature: Launch via custom genesis file (See `EIP proposal <https://github.com/ethereum/EIPs/issues/1085>`_)
- `#1496 <https://github.com/ethereum/py-evm/pull/1496>`_: Bugfix: Regular chain sync crash
- The research team has started adding Beacon Chain code to the underlying py-evm repo. It's all a work in progress, but for those who like to follow along:

  - `#1508 <https://github.com/ethereum/py-evm/pull/1508>`_: Rework Eth2.0 Types
  - `#1543 <https://github.com/ethereum/py-evm/pull/1543>`_: Beacon Chain network commands and protocol scaffolding
  - `#1521 <https://github.com/ethereum/py-evm/pull/1521>`_: Rework helper functions - part 1
  - `#1552 <https://github.com/ethereum/py-evm/pull/1552>`_: Beacon Chain protocol class and handshake
  - `#1555 <https://github.com/ethereum/py-evm/pull/1555>`_: Rename data structures and constants
  - `#1563 <https://github.com/ethereum/py-evm/pull/1563>`_: Rework helper functions - part 2
  - `#1574 <https://github.com/ethereum/py-evm/pull/1574>`_: Beacon block request handler

0.1.0-alpha.18,19
-----------------

That sound you make when you burp in the middle of a hiccup. Hiccurp?

0.1.0-alpha.17
--------------

Released November 20, 2018

- `#1488 <https://github.com/ethereum/py-evm/pull/1488>`_: Bugfix: Bugfix for state sync to limit the number of open files.
- `#1478 <https://github.com/ethereum/py-evm/pull/1478>`_: Maintenance: Improve logging messages during fast sync to include performance metrics
- `#1476 <https://github.com/ethereum/py-evm/pull/1476>`_: Bugfix: Ensure that network connections are properly close when a peer doesn't successfully complete the handshake.
- `#1474 <https://github.com/ethereum/py-evm/pull/1474>`_: Bugfix: EthStats fix for displaying correct uptime metrics
- `#1471 <https://github.com/ethereum/py-evm/pull/1471>`_: Maintenance: Upgrade ``mypy`` to ``0.641``
- `#1469 <https://github.com/ethereum/py-evm/pull/1469>`_: Maintenance: Add logging to show when fast sync has completed.
- `#1467 <https://github.com/ethereum/py-evm/pull/1467>`_: Bugfix: Don't add peers which disconnect during the boot process to the peer pool.
- `#1465 <https://github.com/ethereum/py-evm/pull/1465>`_: Bugfix: Proper handling for when ``SIGTERM`` is sent to the main Trinity process.
- `#1463 <https://github.com/ethereum/py-evm/pull/1463>`_: Bugfix: Better handling for bad server responses by EthStats client.
- `#1443 <https://github.com/ethereum/py-evm/pull/1443>`_: Maintenance: Merge the ``--nodekey`` and ``--nodekey-path`` flags.
- `#1438 <https://github.com/ethereum/py-evm/pull/1438>`_: Bugfix: Remove warnings when printing the ASCII Trinity header
- `#1437 <https://github.com/ethereum/py-evm/pull/1437>`_: Maintenance: Update to use f-strings for string formatting
- `#1435 <https://github.com/ethereum/py-evm/pull/1435>`_: Maintenance: Enable Constantinople fork on Ropsten chain
- `#1434 <https://github.com/ethereum/py-evm/pull/1434>`_: Bugfix: Fix incorrect mainnet genesis parameters.
- `#1421 <https://github.com/ethereum/py-evm/pull/1421>`_: Maintenance: Implement ``eth_syncing`` JSON-RPC endpoint
- `#1410 <https://github.com/ethereum/py-evm/pull/1410>`_: Maintenance: Implement EIP1283 for updated logic for ``SSTORE`` opcode gas costs.
- `#1395 <https://github.com/ethereum/py-evm/pull/1395>`_: Bugfix: Fix gas cost calculations for ``CREATE2`` opcode
- `#1386 <https://github.com/ethereum/py-evm/pull/1386>`_: Maintenance: Trinity now prints a message to make it more clear why Trinity was shutdown.
- `#1387 <https://github.com/ethereum/py-evm/pull/1387>`_: Maintenance: Use colorized output for ``WARNING`` and ``ERROR`` level logging messages.
- `#1378 <https://github.com/ethereum/py-evm/pull/1378>`_: Bugfix: Fix address generation for ``CREATE2`` opcode.
- `#1374 <https://github.com/ethereum/py-evm/pull/1374>`_: Maintenance: New ``ChainTipMonitor`` service to keep track of the highest TD chain tip.
- `#1371 <https://github.com/ethereum/py-evm/pull/1371>`_: Maintenance: Upgrade ``mypy`` to ``0.630``
- `#1367 <https://github.com/ethereum/py-evm/pull/1367>`_: Maintenance: Improve logging output to include more contextual information
- `#1361 <https://github.com/ethereum/py-evm/pull/1361>`_: Maintenance: Remove ``HeaderRequestingPeer`` in favor of ``BaseChainPeer``
- `#1353 <https://github.com/ethereum/py-evm/pull/1353>`_: Maintenance: Decouple peer message handling from syncing.
- `#1351 <https://github.com/ethereum/py-evm/pull/1351>`_: Bugfix: Unhandled ``DecryptionError``
- `#1348 <https://github.com/ethereum/py-evm/pull/1348>`_: Maintenance: Add default server URIs for mainnet and ropsten.
- `#1347 <https://github.com/ethereum/py-evm/pull/1347>`_: Maintenance: Improve code organization within ``trinity`` module
- `#1343 <https://github.com/ethereum/py-evm/pull/1343>`_: Bugfix: Rename ``Chain.network_id`` to be ``Chain.chain_id``
- `#1342 <https://github.com/ethereum/py-evm/pull/1342>`_: Maintenance: Internal rename of ``ChainConfig`` to ``TrinityConfig``
- `#1336 <https://github.com/ethereum/py-evm/pull/1336>`_: Maintenance: Implement plugin for EthStats reporting.
- `#1335 <https://github.com/ethereum/py-evm/pull/1335>`_: Maintenance: Relax some constraints on the ordered task management constructs.
- `#1332 <https://github.com/ethereum/py-evm/pull/1332>`_: Maintenance: Upgrade ``pyrlp`` to ``1.0.3``
- `#1317 <https://github.com/ethereum/py-evm/pull/1317>`_: Maintenance: Extract peer selection from the header sync.
- `#1312 <https://github.com/ethereum/py-evm/pull/1312>`_: Maintenance: Turn on warnings by default if in a prerelease

0.1.0-alpha.16
--------------

Released September 27, 2018

- `#1332 <https://github.com/ethereum/py-evm/pull/1332>`_: Bugfix: Comparing rlp objects across processes used to fail sporadically, because of a changing object hash (fixed by upgrading pyrlp to 1.0.3)
- `#1326 <https://github.com/ethereum/py-evm/pull/1326>`_: Maintenance: Squash a stack trace in the logs when a peer sends us an invalid public key during handshake
- `#1325 <https://github.com/ethereum/py-evm/pull/1325>`_: Bugfix: When switching to a new peer to sync headers, it might have started from too far behind the tip, and get stuck
- `#1327 <https://github.com/ethereum/py-evm/pull/1327>`_: Maintenance: Squash some log warnings from trying to make a request to a peer (or receive a response) while it is shutting down
- `#1321 <https://github.com/ethereum/py-evm/pull/1321>`_: Bugfix: Address a couple race condition exceptions when syncing headers from a new peer, and other downstream processing is in progress
- `#1316 <https://github.com/ethereum/py-evm/pull/1316>`_: Maintenance: Reduce size of images in documentation
- `#1313 <https://github.com/ethereum/py-evm/pull/1313>`_: Maintenance: Remove miscellaneous things that are generating python warnings (eg~ using deprecated methods)
- `#1279 <https://github.com/ethereum/py-evm/pull/1279>`_: Reliability: Atomically persist when storing: a block, a chain of headers, or a cluster of trie nodes
- `#1304 <https://github.com/ethereum/py-evm/pull/1304>`_: Maintenance: Refactor AtomicDB to return an explict database instance to write into
- `#1296 <https://github.com/ethereum/py-evm/pull/1296>`_: Maintenance: Require new AtomicDB in chain and header DB layers
- `#1295 <https://github.com/ethereum/py-evm/pull/1295>`_: Maintenance: New AtomicDB interface to enable a batch of atomic writes (all succeed or all fail)
- `#1290 <https://github.com/ethereum/py-evm/pull/1290>`_: Bugfix: more graceful recovery when re-launching sync on a fork
- `#1277 <https://github.com/ethereum/py-evm/pull/1277>`_: Maintenance: add a cancellable ``call_later`` to all services
- `#1226 <https://github.com/ethereum/py-evm/pull/1226>`_: Performance: enable multiple peer requests to a single fast peer when other peers are slow
- `#1254 <https://github.com/ethereum/py-evm/pull/1254>`_: Bugfix: peer selection when two peers have exactly the same throughput
- `#1253 <https://github.com/ethereum/py-evm/pull/1253>`_: Maintenance: prefer f-string formatting in p2p, trinity code

0.1.0-alpha.15
--------------

- `#1249 <https://github.com/ethereum/py-evm/pull/1249>`_: Misc bugfixes for fast sync reliability.
- `#1245 <https://github.com/ethereum/py-evm/pull/1245>`_: Improved exception messaging for ``BaseService``
- `#1244 <https://github.com/ethereum/py-evm/pull/1244>`_: Use ``time.perf_counter`` or ``time.monotonic`` over ``time.time``
- `#1242 <https://github.com/ethereum/py-evm/pull/1242>`_: Bugfix: Unhandled ``MalformedMessage``.
- `#1235 <https://github.com/ethereum/py-evm/pull/1235>`_: Typo cleanup.
- `#1236 <https://github.com/ethereum/py-evm/pull/1236>`_: Documentation cleanup
- `#1237 <https://github.com/ethereum/py-evm/pull/1237>`_: Code cleanup
- `#1232 <https://github.com/ethereum/py-evm/pull/1232>`_: Bugfix: Correctly enforce timeouts on peer requests and add lock mechanism to support concurrency.
- `#1229 <https://github.com/ethereum/py-evm/pull/1229>`_: CI cleanup
- `#1228 <https://github.com/ethereum/py-evm/pull/1228>`_: Merge ``KademliaProtocol`` and ``DiscoveryProtocol``
- `#1225 <https://github.com/ethereum/py-evm/pull/1225>`_: Expand peer stats tracking
- `#1221 <https://github.com/ethereum/py-evm/pull/1221>`_: Implement Discovery V5 Protocol
- `#1219 <https://github.com/ethereum/py-evm/pull/1219>`_: Re-organize and document fixture filler tools
- `#1214 <https://github.com/ethereum/py-evm/pull/1214>`_: Implement ``BaseService.is_operational``.
- `#1210 <https://github.com/ethereum/py-evm/pull/1210>`_: Convert sync to use streaming queue instead of batches.
- `#1209 <https://github.com/ethereum/py-evm/pull/1209>`_: Chain Builder tool
- `#1205 <https://github.com/ethereum/py-evm/pull/1205>`_: Bugfix: ExchangeHandler stats crash
- `#1204 <https://github.com/ethereum/py-evm/pull/1204>`_: Consensus bugfix for uncle validation
- `#1151 <https://github.com/ethereum/py-evm/pull/1151>`_: Change to ``import_block`` to return chain re-organization data.
- `#1197 <https://github.com/ethereum/py-evm/pull/1197>`_: Increase wait time for database IPC socket.
- `#1194 <https://github.com/ethereum/py-evm/pull/1194>`_: Unify ``ValidationError`` to use ``eth-utils`` exception class.
- `#1190 <https://github.com/ethereum/py-evm/pull/1190>`_: Improved testing for peer authentication
- `#1189 <https://github.com/ethereum/py-evm/pull/1189>`_: Detect crashed sub-services and exit
- `#1179 <https://github.com/ethereum/py-evm/pull/1179>`_: ``LightNode`` now uses ``Server`` for incoming peer connections.
- `#1182 <https://github.com/ethereum/py-evm/pull/1182>`_: Convert ``fix-unclean-shutdown`` CLI command to be a plugin


0.1.0-alpha.14
--------------

- `#1081 <https://github.com/ethereum/py-evm/pull/1081>`_ `#1115 <https://github.com/ethereum/py-evm/pull/1115>`_ `#1116 <https://github.com/ethereum/py-evm/pull/1116>`_: Reduce logging output during state sync.
- `#1063 <https://github.com/ethereum/py-evm/pull/1063>`_ `#1035 <https://github.com/ethereum/py-evm/pull/1035>`_ `#1089 <https://github.com/ethereum/py-evm/pull/1089>`_ `#1131 <https://github.com/ethereum/py-evm/pull/1131>`_ `#1132 <https://github.com/ethereum/py-evm/pull/1132>`_ `#1138 <https://github.com/ethereum/py-evm/pull/1138>`_ `#1149 <https://github.com/ethereum/py-evm/pull/1149>`_ `#1159 <https://github.com/ethereum/py-evm/pull/1159>`_: Implement round trip request/response API.
- `#1094 <https://github.com/ethereum/py-evm/pull/1094>`_ `#1124 <https://github.com/ethereum/py-evm/pull/1124>`_: Make the node processing during state sync more async friendly.
- `#1097 <https://github.com/ethereum/py-evm/pull/1097>`_: Keep track of which peers are missing trie nodes during state sync.
- `#1109 <https://github.com/ethereum/py-evm/pull/1109>`_ `#1135 <https://github.com/ethereum/py-evm/pull/1135>`_: Python 3.7 testing and experimental support.
- `#1136 <https://github.com/ethereum/py-evm/pull/1136>`_ `#1120 <https://github.com/ethereum/py-evm/pull/1120>`_: Module re-organization in preparation of extracting ``p2p`` and ``trinity`` modules.
- `#1137 <https://github.com/ethereum/py-evm/pull/1137>`_: Peer subscriber API now supports specifying specific msg types to reduce msg queue traffic.
- `#1142 <https://github.com/ethereum/py-evm/pull/1142>`_ `#1165 <https://github.com/ethereum/py-evm/pull/1165>`_: Implement JSON-RPC endpoints for: ``eth_estimateGas``, ``eth_accounts``, ``eth_call``
- `#1150 <https://github.com/ethereum/py-evm/pull/1150>`_ `#1176 <https://github.com/ethereum/py-evm/pull/1176>`_: Better handling of malformed messages from peers.
- `#1157 <https://github.com/ethereum/py-evm/pull/1157>`_: Use shared pool of workers across all services.
- `#1158 <https://github.com/ethereum/py-evm/pull/1158>`_: Support specifying granular logging levels via CLI.
- `#1161 <https://github.com/ethereum/py-evm/pull/1161>`_: Use a tmpfile based LevelDB database for cache during state sync to reduce memory footprint.
- `#1166 <https://github.com/ethereum/py-evm/pull/1166>`_: Latency and performance tracking for peer requests.
- `#1173 <https://github.com/ethereum/py-evm/pull/1173>`_: Better APIs for background task running for ``Service`` classes.
- `#1182 <https://github.com/ethereum/py-evm/pull/1182>`_: Convert ``fix-unclean-shutdown`` command to be a plugin.


0.1.0-alpha.13
--------------

- Remove specified ``eth-account`` dependency in favor of allowing ``web3.py`` specify the correct version.


0.1.0-alpha.12
--------------

- `#1058 <https://github.com/ethereum/py-evm/pull/1058>`_  `#1044 <https://github.com/ethereum/py-evm/pull/1044>`_: Add ``fix-unclean-shutdown`` CLI command for cleaning up after a dirty shutdown of the ``trinity`` CLI process.
- `#1041 <https://github.com/ethereum/py-evm/pull/1041>`_: Bugfix for ensuring CPU count for process pool is always greater than ``0``
- `#1010 <https://github.com/ethereum/py-evm/pull/1010>`_: Performance tuning during fast sync.  Only check POW on a subset of the received headers.
- `#996 <https://github.com/ethereum/py-evm/pull/996>`_ Experimental new Plugin API:  Both the transaction pool and the ``console`` and ``attach`` commands are now written as plugins.
- `#898 <https://github.com/ethereum/py-evm/pull/898>`_: New experimental transaction pool.  Disabled by default.  Enable with ``--tx-pool``.  (**warning**: has known issues that effect sync performance)
- `#935 <https://github.com/ethereum/py-evm/pull/935>`_: Protection against eclipse attacks.
- `#869 <https://github.com/ethereum/py-evm/pull/869>`_: Ensure connected peers are on the same side of the DAO fork.

Minor Changes

- `#1081 <https://github.com/ethereum/py-evm/pull/1081>`_: Reduce ``DEBUG`` log output during state sync.
- `#1071 <https://github.com/ethereum/py-evm/pull/1071>`_: Minor fix for how version string is generated for trinity
- `#1070 <https://github.com/ethereum/py-evm/pull/1070>`_: Easier profiling of ``ChainSyncer``
- `#1068 <https://github.com/ethereum/py-evm/pull/1068>`_: Optimize ``evm.db.chain.ChainDB.persist_block`` for common case.
- `#1057 <https://github.com/ethereum/py-evm/pull/1057>`_: Additional ``DEBUG`` logging of peer uptime and msg stats.
- `#1049 <https://github.com/ethereum/py-evm/pull/1049>`_: New integration test suite for trinity CLI
- `#1045 <https://github.com/ethereum/py-evm/pull/1045>`_ `#1051 <https://github.com/ethereum/py-evm/pull/1051>`_: Bugfix for generation of block numbers for ``GetBlockHeaders`` requests.
- `#1011 <https://github.com/ethereum/py-evm/pull/1011>`_: Workaround for parity bug `parity #8038 <https://github.com/paritytech/parity-ethereum/issues/8038>`_
- `#987 <https://github.com/ethereum/py-evm/pull/987>`_: Now serving requests from peers during fast sync.
- `#971 <https://github.com/ethereum/py-evm/pull/971>`_ `#909 <https://github.com/ethereum/py-evm/pull/909>`_ `#650 <https://github.com/ethereum/py-evm/pull/650>`_: Benchmarking test suite.
- `#968 <https://github.com/ethereum/py-evm/pull/968>`_: When launching ``console`` and ``attach`` commands, check for presence of IPC socket and log informative message if not found.
- `#934 <https://github.com/ethereum/py-evm/pull/934>`_: Decouple the ``Discovery`` and ``PeerPool`` services.
- `#913 <https://github.com/ethereum/py-evm/pull/913>`_: Add validation of retrieved contract code when operating in ``--light`` mode.
- `#908 <https://github.com/ethereum/py-evm/pull/908>`_: Bugfix for transitioning from syncing chain data to state data during fast sync.
- `#905 <https://github.com/ethereum/py-evm/pull/905>`_: Support for multiple UPNP devices.


0.1.0-alpha.11
--------------

- Bugfix for ``PreferredNodePeerPool`` to respect ``max_peers``


0.1.0-alpha.10
--------------

- More bugfixes to enforce ``--max-peers`` in ``PeerPool._connect_to_nodes``


0.1.0-alpha.9
-------------

- Bugfix to enforce ``--max-peers`` for incoming connections.


0.1.0-alpha.7
-------------

- Remove ``min_peers`` concept from ``PeerPool``
- Add ``--max-peers`` and enforcement of maximum peer connections maintained by
  the ``PeerPool``.


0.1.0-alpha.6
-------------

- Respond to ``GetBlockHeaders`` message during fast sync to prevent being disconnected as a *useless peer*.
- Add ``--profile`` CLI flag to Trinity to enable profiling via ``cProfile``
- Better error messaging with Trinity cannot determine the appropriate location for the data directory.
- Handle ``ListDeserializationError`` during handshake.
- Add ``net_version`` JSON-RPC endpoint.
- Add ``web3_clientVersion`` JSON-RPC endpoint.
- Handle ``rlp.DecodingError`` during handshake.
