Measuring runtime sync performance
==================================

Trinity comes with a built-in plugin that allows us to measure its runtime performance.
The plugin is currently limited to a single use case which is measuring the time it takes to
sync from a blank slate to some target block.


Measuring runtime performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start performance testing with default settings simply run ``trinity track-perf``. This will
kick off a series of tasks:

1. Boot the performance runner
2. Rename Trinity's currently used database with a timestamp suffix
3. Run ``trinity`` and monitor its performance until block ``100000`` is hit
4. Append (create if it doesn't exist) performance stats to the ``performance_report.txt`` file
5. Repeat steps 2 - 4 until ``3`` runs are completed.


When the performance tracking has finished we can look at the ``performance_report.txt`` to
find out about the:

- names of all archived databases
- reached block numbers
- total duration (including pre-sync time)
- actual syncing duration
- synced blocks per second


We can also customize the target block number as well as the number of individual runs. Here is
how we can start ``10`` runs, each syncing up until block ``20000``.

.. code-block:: shell

  trinity track-perf --track-perf-target-block 20000 --track-perf-run-count 10


Passing parameters to trinity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Chances are that we would like to pass certain parameters to trinity for each run. For instance,
we may want to enable/disable certain plugins or ensure to only connect to a specific peer.
The performance runner allows this by using the ``--track-perf-trinity-arg`` parameter as seen in
the following example.

.. code-block:: shell

  trinity track-perf --track-perf-trinity-args='--disable-discovery --preferred-node="enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e3bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303" --max-peers 1'


A/B testing different branches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If we would like to profile two different versions against each other, we can do so by continously
toggling between different branches. The runner doesn't support this specifically but it allows us
to run any command in between the individual runs using the ``--track-perf-pre-exec-cmd`` parmeter.

We can rely on git toggling between branches if we configure `--track-perf-pre-exec-cmd` as
follows:

.. code-block:: shell

  trinity track-perf --track-perf-pre-exec-cmd 'git checkout -'

This feature can also come handy if we are profiling branches that may not shut down cleanly as we
can arbitrary chain commands e.g.

.. code-block:: shell

  trinity track-perf --track-perf-pre-exec-cmd 'trinity fix-unclean-shutdown && git checkout -'

Notice that any output that this command may generate will end up in the ``performance_report.txt``
which also means that the output of ``git checkout -`` ensures we'll be able to identify the branches
of each run.

Special cases
~~~~~~~~~~~~~

The performance runner lets us configure the path to the actual trinity executable which may be
useful if the host system does not support a global ``trinity`` command.

.. code-block:: shell

  trinity track-perf --track-perf-trinity-exec '/home/ubuntu/trinity/venv/bin/trinity'

Similarly, the path to the ``performance_report.txt`` can be set, too.

.. code-block:: shell

  trinity track-perf --track-perf-report-path '/home/ubuntu/trinity/performance_report.txt'

