Parallel processing
===================
FancyPipe analyses the serial/parallel alignment of your tasks on the 
fly in a bottom-up fashion. It starts at the output of the module whose 
run() method is called. Typically, the outputs contain dependencies on 
tasks defined inside main(). 
These tasks themselves may have dependencies on other tasks etc. etc. 
FancyPipe works its way up through the dependency graph until it 
encounters a task for which the input is fully specified. 
It submits this task to the *job queue* and continues to find
other ready-to-run tasks. Meanwhile, an array of worker nodes is waiting
for jobs to appear in the queue. Upon completion, the worker node 
submits the result to a *result queue*. The main process listens to this 
queue, and feeds the result to tasks that need it as input. 
This continues until all requested outputs of the outer module are 
computed.

Multi-threading, multi-processing, and distributed processing
-------------------------------------------------------------
FancyPipe supports three modes of parallel operations, which can be
specified as a command line option. For the code it does not 
matter which parallelization option is chosen: for debugging you can
use multi-threading or multi-processing on a laptop, and for production 
you can setup a distributed processing cluster.

Multi-threading
...............
The simplest way to run jobs in parallel is to setup a pool of 
*worker threads* on a multi-processor workstation. This is a good 
solution if the tasks consist mainly of calls to external programs. 
If the tasks run mainly python code, then multi-threading will suffer 
from python's *Global Interpreter Lock*, which allows only one thread to 
run the python interpreter at a time.

The option "-workertype=T" invokes multi-threading.

The option "-numworkers=X" creates X worker threads. By default, one 
worker per cpu is created.

Multi-processing
................
To run tasks that run pure python code in parallel, the better solution 
is to create a separate proces (=python interpreter) for each parallel 
task. This requires more overhead and memory than multi-threading, as it
involves serializing and copying the input and output data of the tasks. 
This all happens behind the scene.

The option "-workertype P" invokes multi-processing.

The option "-numworkers=X" creates X worker threads. By default, one 
worker per cpu is created.

Distributed processing
......................
The previous options are restricted to a single multi-core workstation.
Distributed processing involved multiple workstations, grid nodes, or
virtual machine instances in a cloud environment. In this setup, there
are three players: 

* the *client*, who submits a large job to the *manager*.
* the *manager*, who distributes the job to the *worker nodes* and 
  returns the result to the client.
* the *worker node*, who takes jobs and returns results to the *manager*.

Each player can reside on the same or on separate machines, as long as:

* the client and worker nodes have internet (tcp) access to the
  manager.
* the client and worker nodes have access to a shared filesystem, in
  which the working directory (-tempdir) and all other files used
  by the pipeline must reside.
* the same version of python and libraries used by the pipeline are
  available on the client and worker nodes.
* the manager's machine has a tcp-port available that is not 
  blocked by a firewall.

First test distributed processing on a single machine, this does not 
require a shared filesystem:

1. Start the manager by running

  ::

    python fancymanager.py &

2. Start the workers by running

  ::

    python fancyworker.py -jobmanager=localhost &

  Optionally you can set -numworkers=X if you want fewer workers than the cpu-count.
  
3. Start the client by running your pipeline with the option: -jobmanager=localhost

To terminate the manager and workers, use (on Linux): 

::

  killall python

Next, setup distributed processing on multiple machines. As an example, let's use three machines 
with ip addresses ipA, ipB and ipC, and available port 5432 on ipA:

1. Setup the shared filesystem on machines A, B and C.

2. Start the manager by running on machine A

  ::
  
    python fancymanager.py -jobmanager=ipA:5432 &

3. Start the workers by running

  ::

    On machine A: python fancyworker.py -jobmanager=ipA:5432 &
    On machine B: python fancyworker.py -jobmanager=ipA:5432 &
    On machine C: python fancyworker.py -jobmanager=ipA:5432 &

4. On machine A (or B or C), start the client by running your pipeline with the option: -jobmanager=ipA:5432

The manager machine needs no python modules or shared filesystem, since
all that the manager does is to relay jobs and results between
client and workers.

Be aware that this setup is only suitable for a private network, because 
anyone who has access to the manager can submit jobs to it. You can add a 
bit of security by adding the parameter -jobauth=SECRET to all
commands that use the -jobmanager option, whereby SECRET must be replaced
by a hard-to-guess string.
