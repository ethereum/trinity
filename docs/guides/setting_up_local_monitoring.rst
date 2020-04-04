Setting up local monitoring
===========================

Trinity comes with a system that allows a wide range of metrics to be recorded at runtime to be
written to an InfluxDB database. The data can be visualized in different ways but we chose Grafana
to visualize the data through various charts.

Although the Trinity team maintains its own central metrics infrastructure, everyone can setup their
own local InfluxDB and Grafana instances to work with the metrics system and display results.


.. warning:: This guide is only intended to setup a local development system. It may not follow
  best practices for production systems.


Setup InfluxDB
~~~~~~~~~~~~~~

First, follow the official
`documentation <https://docs.influxdata.com/influxdb/v1.4/introduction/installation/>`_ to
install InfluxDB.

Then point the InfluxDB service to its configuration file with
``influxd -config [Path to config file]``, in our case that is ``/etc/influxdb/influxdb.conf``

Once that is done, we start the service and check that it is running.

.. note:: The specific commands differ for various operating systems but can be found
  in the official documentation

.. note:: 
  The default port for using InfluxDB is ``8086``. 

Make the following modifications to the config file in ``/etc/influxdb/influxdb.conf``:

At the top, we uncomment ``reporting-disabled = false`` and set it to ``true``. 

Under "http", we uncomment ``enabled = true``, ``bind-address = ":8086"`` and
``auth-enabled = false`` and set it to ``true``.

Save the changes and exit.

Influx CLI
----------

Once the InfluxDB service is running, we can open the Influx CLI using the ``influx`` command
inside our terminal. We can leave the Influx CLI by executing ``exit``.

Manage Databases
----------------

Enter the Influx CLI to create a new database using ``create database trinity``.

.. note:: While Trinity supports any database and user name via the parameters
  ``--metrics-influx-user``  and ``--metrics-influx-database``, we can save us these configurations
  if we choose the name ``trinity`` for both the user and the database.

Run ``show databases`` to confirm the database was created successfully.

Manage Users
------------

Now that we have a database, we need to add a users to it. First we have to select the database
that we want to add the user to. This can be done via the ``use [db-name]`` command, ``use trinity``
in our case. After we specified which database to add users to, we run
``CREATE USER "trinity" WITH PASSWORD 'trinity' WITH ALL PRIVILEGES``.

We can also create a regular user with ``CREATE USER "user" WITH PASSWORD '12345'``. 

To check whether our changes have been applied correctly, we can use ``show users`` command.
For the changes to be applied to the current running service, we need to restart it using
``service influxdb restart`` inside the regular terminal.

Setup Grafana
~~~~~~~~~~~~~

Next, we follow the `official documentation <https://grafana.com/grafana/download/6.7.1/>`_
to install Grafana.


Start Grafana-Server
--------------------
After the installation is done, we start the service and check that it is running.

.. note:: The specific commands differ for various operating systems but can be found in the
  official documentation (e.g. it is ``sudo systemctl start grafana-server`` and
  ``sudo systemctl start influxd`` on Linux).

If Grafana is running correctly, we can go to `http://localhost:3000`.
For the first login we use "admin" as username and "admin" as password. It is recommended to change
these credentials after the first login.

Add Datasource to Grafana
-------------------------

Now that Grafana is running, we still need to connect our previously installed InfluxDB to it.
We can do this by clicking on the "Create a data source" icon in the home screen at
``http://localhost:3000``.

The parameters are:

- NAME: InfluxDB Trinity
- HTTP URL: http://localhost:8086
- ACCESS: SERVER(default)

- Check: Basic Auth
- Basic Auth Details: input User name + password, in our case "trinity" and "trinity"

InfluxDB Details: 

- Database: trinity
- User: trinity
- Password: trinity

We confirm our inputs by clicking "Save & Test" Button.

Start monitoring with Trinity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start monitoring our data, we have to use several flags when starting up Trinity: 

.. code:: sh

  trinity --enable-metrics --metrics-host "test" --metrics-influx-server "localhost" --metrics-influx-password "trinity" --metrics-influx-protocol http --metrics-influx-port 8086

Let us go through all those flags one by one: 

- ``trinity`` is the basic command to start the client.
- ``--enable-metrics`` enables the collection of metrics
- ``--metrics-host boot-node-asia-1`` tags each metrics with a host label such as ``boot-node-asia-1``. This
  allows us to filter the dashboard in Grafana by the host machine that produced the data
- ``--metrics-influx-server localhost`` the host where the InfluxDB is running.
- ``--metrics-influx-password trinity`` the password that is used to access InfluxDB
- ``--metrics-influx-protocol`` the protocol to access the InfluxDB can either be ``http`` or
  ``https``. We use ``http`` in our development environment.
- ``--metrics-influx-port 8086`` The port being used to access the InfluxDB

If everything went right so far, at this point data should get fed into the InfluxDB and we can
visualize them on Grafana dashboards.

Create Dashboards
~~~~~~~~~~~~~~~~~

Will follow soon! 
