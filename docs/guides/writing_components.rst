Writing Components
==================

Trinity aims to be a highly flexible Ethereum node to support lots of different use cases
beyond just participating in the regular networking traffic.

To support this goal, Trinity allows developers to create components that hook into the system to
extend its functionality. In fact, Trinity dogfoods its Component API in the sense that several
built-in features are written as components that just happen to be shipped among the rest of the core
modules. For instance, the JSON-RPC API, the Transaction Pool as well as the ``trinity attach``
command that provides an interactive REPL with `Web3` integration are all built as components.

Trinity tries to follow the practice: If something can be written as a component, it should be written
as a component.


What can components do?
~~~~~~~~~~~~~~~~~~~~~~~

Component support in Trinity is still very new and the API hasn't stabilized yet. That said, components
are already pretty powerful and are only becoming more so as the APIs of the underlying services
improve over time.

Here's a list of functionality that is currently provided by components:

- JSON-RPC API
- Transaction Pool
- EthStats Reporting
- Interactive REPL with Web3 integration
- Crash Recovery Command


Understanding the different component categories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are currently three different types of components that we'll all cover in this guide.

- Components that overtake and redefine the entire ``trinity`` command
- Components that spawn their own new isolated process


Components that redefine the Trinity process
--------------------------------------------

This is the simplest category of components as it doesn't really *hook* into the Trinity process but 
hijacks it entirely instead. We may be left wonderering: Why would one want to do that?

The only reason to write such a component is to execute some code that we want to group under the
``trinity`` command. A great example for such a component is the ``trinity attach`` command that gives
us a REPL attached to a running Trinity instance. This component could have easily be written as a
standalone program and associated with a command such as ``trinity-attach``. However, using a
subcommand ``attach`` is the more idiomatic approach and this type of component gives us simple way
to develop exactly that.

We build this kind of component by subclassing from
:class:`~trinity.extensibility.component.Application`. A detailed example will follow soon.


Components that spawn their own new isolated process
----------------------------------------------------

Of course, if all what components could do is to hijack the `trinity` command, there wouldn't be
much room to actually extend the *runtime functionality* of Trinity. If we want to create components
that boot with and run alongside the main node activity, we need to write a different kind of
component. These type of components can respond to events such as a peers connecting/disconnecting and
can access information that is only available within the running application.

The JSON-RPC API is a great example as it exposes information such as the current count
of connected peers which is live information that can only be accessed by talking to other parts
of the application at runtime.

This is the default type of component we want to build if:

- we want to execute logic **together** with the command that boots Trinity (as opposed
  to executing it in a separate command)
- we want to execute logic that integrates with parts of Trinity that can only be accessed at
  runtime (as opposed to e.g. just reading things from the database)

We build this kind of component subclassing from
:class:`~trinity.extensibility.asyncio.AsyncioIsolatedComponent`.  A detailed example will follow soon.


The component lifecycle
~~~~~~~~~~~~~~~~~~~~~~~

Components are run by the
:class:`~trinity.extensibility.component_manager.ComponentManager` which is
responsible for running and stopping components.

Each component is expected to implement
:meth:`~trinity.extensibility.component.ComponentAPI.run` which must be a
coroutine.


Defining components
~~~~~~~~~~~~~~~~~~~

We define a component by deriving from either
:class:`~trinity.extensibility.component.Application` or
:class:`~trinity.extensibility.asyncio.AsyncioIsolatedComponent` depending on the kind of component that we
intend to write. For now, we'll stick to :class:`~trinity.extensibility.asyncio.AsyncioIsolatedComponent`
which is the most commonly used component category.

Every component needs to overwrite ``name`` so voilà, here's our first component!

.. literalinclude:: ../../trinity-external-components/examples/peer_count_reporter/peer_count_reporter_component/component.py
   :language: python
   :pyobject: PeerCountReporterComponent
   :end-before: def configure_parser

Of course that doesn't do anything useful yet, bear with us.

Configuring Command Line Arguments
----------------------------------

More often than not we want to have control over if or when a component should start. Adding
command-line arguments that are specific to such a component, which we then check, validate, and act
on, is a good way to deal with that. Implementing
:meth:`~trinity.extensibility.component.ComponentAPI.configure_parser` enables us to do exactly that.

This method is called when Trinity starts and bootstraps the component system, in other words,
**before** the start of any component. It is passed an :class:`~argparse.ArgumentParser` as well as a
:class:`~argparse._SubParsersAction` which allows it to amend the configuration of Trinity's
command line arguments in many different ways.

For example, here we are adding a boolean flag ``--report-peer-count`` to Trinity.

.. literalinclude:: ../../trinity-external-components/examples/peer_count_reporter/peer_count_reporter_component/component.py
   :language: python
   :pyobject: PeerCountReporterComponent.configure_parser

To be clear, this does not yet cause our component to automatically start if ``--report-peer-count``
is passed, it simply changes the parser to be aware of such flag and hence allows us to check for
its existence later.

.. note::

  For a more advanced example, that also configures a subcommand, checkout the ``trinity attach``
  component.

Most CLI argument validation can happen within the standard library APIs
exposed by ``argparse``.  If a component needs to do runtime validation it can
do so via
:meth:`~trinity.extensibility.component.BaseComponentAPI.validate_cli`.
Convention here is to raise ``eth_utils.ValidationError`` if an error is
encountered.


Communication Patterns
----------------------

For most components to be useful they need to be able to communicate with the rest of the application
as well as other components. In addition to that, this kind of communication needs to work across
process boundaries as components will often operate in independent processes.

To achieve this, Trinity uses the
`Lahja project <https://github.com/ethereum/lahja>`_, which enables us to operate
a lightweight event bus that works across processes. An event bus is a software dedicated to the
transmission of events from a broadcaster to interested parties.

This kind of architecture allows for efficient and decoupled communication between different parts
of Trinity including components.

For instance, a component may be interested to perform some action every time that a new peer connects
to our node. These kind of events get exposed on the EventBus and hence allow a wide range of
components to make use of them.

For an event to be usable across processes it needs to be pickleable and in general should be a
shallow Data Transfer Object (`DTO <https://en.wikipedia.org/wiki/Data_transfer_object>`_)

.. note::
  This guide will soon cover communication through the event bus in more detail. For now, the
  `Lahja documentation <https://github.com/ethereum/lahja/blob/master/README.md>`_ gives us some
  more information about the available APIs and how to use them.


Distributing components
~~~~~~~~~~~~~~~~~~~~~~~

Of course, components are more fun if we can share them and anyone can simply install them through
``pip``. The good news is, it's not hard at all!

In this guide, we won't go into details about how to create Python packages as this is already
`covered in the official Python docs <https://packaging.python.org/tutorials/packaging-projects/>`_
.

Once we have a ``setup.py`` file, all we have to do is to expose our component under
``trinity.components`` via the ``entry_points`` section.

.. literalinclude:: ../../trinity-external-components/examples/peer_count_reporter/setup.py
   :language: python

Check out the `official documentation on entry points <https://packaging.python.org/guides/creating-and-discovering-components/#using-package-metadata>`_
for a deeper explanation.

A component where the ``setup.py`` file is configured as described can be installed by
``pip install <package-name>`` and immediately becomes available as a component in Trinity.

.. note::

  Components installed from a local directory (instead of the pypi registry), such as the sample
  component described in this article, must be installed with the ``-e`` parameter (Example:
  ``pip install -e ./trinity-external-components/examples/peer_count_reporter``)
