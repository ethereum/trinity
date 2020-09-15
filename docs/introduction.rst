Introduction
============

Trinity is an implementation of an Ethereum network node. It is built on top of Py-EVM which is
an implementation of the Ethereum Virtual Machine (EVM) written in Python.

Trinity and Py-EVM are the successors to the deprecated py-ethereum project and are the current
go-to implementations for the Python ecosystem.

If none of this makes sense to you yet we recommend to checkout the
`Ethereum <https://ethereum.org>`_ website as well as a
`higher level description <http://www.ethdocs.org/en/latest/introduction/what-is-ethereum.html>`_
of the Ethereum project.

Goals
------------

The main focus is to enrich the Ethereum ecosystem with a Python implementation that:

* Supports Ethereum 1.0 as well as 2.0 / Serenity
* Runs as a "full" node using a
  novel `beam sync <https://github.com/ethereum/stateless-ethereum-specs/blob/master/beam-sync-phase0.md>`_
  strategy
* Is well documented
* Is easy to understand
* Has clear APIs
* Runs fast and resource friendly
* Is highly flexible to support:

  * Public chains (including Mainnet, Görli, Ropsten and other networks)
  * Private chains
  * Consortium chains
  * Advanced research


.. note::

  Trinity is currently in **public alpha** and can connect and sync to the main Ethereum network.
  While it isn't meant for production use yet, we encourage the adventurous to try it out.
  Follow along the :doc:`Quickstart </guides/quickstart>` to get things going.

Further reading
---------------

Here are a couple more useful links to check out.

* :doc:`Quickstart </guides/quickstart>`
* `Source Code on GitHub <https://github.com/ethereum/trinity>`_
* `Public Gitter Chat <https://gitter.im/ethereum/trinity>`_
* :doc:`Get involved </contributing>`