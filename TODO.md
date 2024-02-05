Projects.  Pull requests welcome

* The logging is broken.  There is no flexible way of logging errors,
  and the current mechanism of sending out a telegram message with an
  error is not thread-safe and causes errors when one process tries to
  load a file while another process is writing.  Look into ways that
  we can do alerts in case the ingesting throws an exception.  This is
  fairly high priority since right now the ingestion can fail silently.

* The pipelines are not using python classes, and so it makes it
  difficult to compose the objects together.  Look at redesigning the
  pipelines in using proper python classes.  This also creates
  problems for parallel processing and memory management

* Right now there are memory leaks because the objects are taking a
  data frame from one function storing in a cache and then reading
  from the cache.  Look at ingest and then try to smooth out the
  internal logic so that it is uses only local variables and that
  everything is garbage collected during the ingest

* Use async to parallized calls.  This is particularly important for
  database calls.  However the system should be such so that some
  calls are done in order while others are done in parallel

* look into using numba for pandas calculations

* Try to see what it would take to use OpenLlama to generate ingestors
  using AI.

* The truflation-data framework has internal cron schedule, which we are
  not using.  This can be removed.


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
