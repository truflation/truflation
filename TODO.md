Please come up with some ideas to make this easier to write connectors
and ingestors

In the class truflation/data/connectors/kwil.py

   Look at the method query_tx_wait and suggest better methods for
   dealing with asynchonous calls to blockchain.

Look over the class structure for connector and make suggestions for
improvement.

Write an example class for the constructor that pulls data from a rest
interface and then saves it to CSV

Please write a simple test case that tests a piece of functionality
that is currently not implemented.

Please describe what the missing parts that are necessary to submit
the package to pypi

Please write a skeleton for sphinx documentation

Look at the function pipeline_run_server.py that takes a pipeline and
comment on async issues.

Please explain how to integrate this package with asyncio

Please suggest some ideas for how to make this package work with low
latency systems.

Please write some performance benchmark systems
