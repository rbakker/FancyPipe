.. FancyPipe documentation master file, created by
   sphinx-quickstart on Thu Apr  2 22:08:18 2015.

FancyPipe
=========
FancyPipe is an open `source <https://github.com/rbakker/FancyPipe>`_ 
python package for running cpu-intensive data processing pipelines. 
It covers those aspects that would otherwise distract you from your 
main job: the design of the processing steps.

FancyPipe takes care of:

* parallel execution of code
* html-based, hierarchical logging
* pipeline parameter management
* command line interface
* temporary files and garbage collection
* error handling


Parallel execution
------------------
In FancyPipe, you build your pipeline as a (nested) set of FancyModules.
Each FancyModule has a main( ... ) method, which contains the code
to transform inputs to outputs. This code may call external programs 
and/or other FancyModules.
The main() method should not be called directly. Instead, there are two
proper ways to run modules. The simplest way is to use this chain of 
commands:

::

  myModule.setInput( ... )
  myModule.run()
  myModule.getOutput( key )

This chain does not allow parallel execution. For that you need to 
use:

::

  module1a.setInput( ... )
  module1b.setInput( ... )
  module2.setInput(
    module1a.requestOutput(key),
    module1b.requestOutput(key),
    ... 
  )
  module2.run()

When module2 is run, FancyPipe will automatically detect that its inputs 
contain output-requests to module1a and module1b, and it will run these 
modules in parallel. This illustrates the basic idea, the `parallel 
programming page` describes more complex cases.


HTML logging
------------
Whenever a module is run, a json-encoded entry is added to a log file,
shared by all parallel processing nodes. There is no need to
explicitly add entries to the log file. When the module finishes, its
console output is attached to the log entry.
The page 'fancylog.html' contains scripts to disentangle this log file,
and to present the results in a hierarchical tree, which can already
be viewed while the job is running.


Parameter management
--------------------
Data processing pipelines deal with two sets of parameters:

1. Parameters that control the environment in which the pipeline runs.
2. Parameters used by the pipeline modules.

In FancyPipe, the environment parameters need to be passed only to the
outer module, typically from the command line. They are propagated 
automatically to all child modules.

Module parameters can be provided at three levels.

1. Default values are specified in the module code.
2. A configuration file may override these defaults, by specifying context-value pairs. The context specifies under which conditions the parameter should be used.
3. The parent module may override the configuration file values.


Command line interface
----------------------
Each module for which input types are defined can be run from the
command line, by the name of the source file. If the source file 
contains multiple modules, then the generic command 'runmodule.py' can
be used to specify the sourcefile and module name.


Temporary files and garbage collection
--------------------------------------
The outer module uses an input parameter 'tempdir' to manage storage 
of intermediate results. Child modules can specify a 'tempsubdir' to
have their output directed to tempdir/tempsubdir.
The removal of intermediate results when they are no longer needed
is not yet activated.


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
   
  classesmethods

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`