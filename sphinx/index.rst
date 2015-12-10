.. FancyPipe documentation master file, created by
   sphinx-quickstart on Thu Apr  2 22:08:18 2015.

FancyPipe
=========
FancyPipe is an open `source <https://github.com/rbakker/FancyPipe>`_ 
python package for running cpu-intensive data processing pipelines. 
It covers those aspects that would otherwise distract you from your 
main job: the design of the processing steps.

FancyPipe takes care of:

* parallel and distributed execution of code
* html-based, hierarchical logging
* pipeline parameter management
* command line interface
* temporary files and garbage collection
* error handling


Programming: serial vs. parallel
--------------------------------
In FancyPipe, you build your pipeline as a (nested) set of FancyTasks.
When you run() a FancyTask, three methods are called successively:
init(...), main(...), and done(...), thereby using the result of each 
method as input to the next. By default, all three just copy input to
output, so to have the Task do something useful, you need to overwrite
at least one of these methods. Only main() should contain cpu-intensive 
code, because it is run in parallel mode whenever possible.
If the task is not cpu-intensive at all, then you can save parallel 
processing overhead by leaving main() undefined and putting all code in 
init(). 

Once the Task is implemented, there are two ways to run it. 
The simplest way is to use this chain of commands:

::

  myTask.setInput( ... )
  myTask.run()
  myTask.getOutput( key )

With myTask.setInput() you set the variables that will be passed to 
myTask.init(). myTask.run() stores the output of myTask.done()
inside the myTask object. myTask.getOutput( key ) retrieves the key-th 
element of this stored output.

The above code does not allow parallel execution. For that you need to use:

::

  task1a.setInput( ... )
  task1b.setInput( ... )
  task2.setInput(
    task1a.requestOutput(key),
    task1b.requestOutput(key),
    ... 
  )
  task2.run()

When task2 is run, FancyPipe will automatically detect that its inputs 
contain output-requests to task1a and task1b, and it will run these 
tasks first, in parallel. This illustrates the basic idea of parallel 
programming in FancyPipe. 
The `parallel programming page <parallelprogramming.html>`_ describes 
more complex cases.

When you call a FancyPipe program from the command line, you can specify
the resources that are available for parallel processing. Three 
`parallel execution modes <parallelprocessing.html>`_ are supported: 
multi-threading, multi-processing, and distributed processing (experimental).


HTML logging
------------
Whenever a task is run, a json-encoded entry is added to a log file,
shared by all parallel processing nodes. There is no need to
explicitly add entries to the log file. When the task finishes, its
console output is attached to the log entry.
The page 'fancylog.html' contains scripts to disentangle this log file,
and to present the results in a hierarchical tree, which can already
be viewed while the job is running.


Parameter management
--------------------
Data processing pipelines deal with two sets of parameters:

1. Parameters that control the environment in which the pipeline runs.
2. Parameters used by the pipeline tasks.

In FancyPipe, the environment parameters need to be passed only to the
outer task, typically from the command line. They are propagated 
automatically to all child tasks.

Task parameters can be provided at three levels.

1. Default values are specified in the task code.
2. A configuration file may override these defaults, by specifying context-value pairs. The context specifies under which conditions the parameter should be used.
3. The parent task may override the configuration file values.


Command line interface
----------------------
Each task for which input types are defined can be run from the
command line, by the name of the source file. If the source file 
contains multiple tasks, then the generic command 'runtask.py' can
be used to specify the sourcefile and task name.


Temporary files and garbage collection
--------------------------------------
The outer task uses an input parameter 'tempdir' to manage storage of 
intermediate results. Child tasks create subdirectories in the parent 
folder if they need to save temporary files. Directory names start with 
a digit, which represents the order at which the child was created. 

Garbage collection is the removal of intermediate results when they are 
no longer needed. When a task has completed, all temporary files created 
by task.tempdir() and task.tempfile() are removed, unless these files 
are returned as output of the task. In that case, the parent task inherits 
the files and either removes them later, or returns them as output. 
In the end of this recursive scheme, all temporary files except for 
those returned by the outer task are removed.

The command line option '-nocleanup' disables garbage collection.


Error handling
--------------
When an error occurs in any of the processing nodes, execution halts and
control is returned to the main process, triggering a second error that
terminates all active processing nodes. Both error messages and a full 
traceback report are prominently displayed both in the console and in 
the html log file.


Documentation tree
------------------

.. toctree::
  :maxdepth: 2
   
  installation
  parallelprogramming
  parallelprocessing
  classesmethods

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
