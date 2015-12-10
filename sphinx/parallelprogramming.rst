Creating a FancyPipe program
============================

A FancyPipe program consists of a set of tasks that are connected by
setInput and getOutput or requestOutput statements.
Before you read on, please be aware that in Python you can use two 
types of notation to create a dictionary. The first:

::

  A = { 'key1':'value1' }

The second:

::

  A = dict( key1='value1' )

A single task
-------------
To get started, have a look at a very basic FancyPipe program that 
consists of just one task (../examples/onetask.py):

::

  import sys
  sys.path.append('../fancypipe')
  from fancypipe import *

  class DoSquare(FancyTask):
    inputs = odict(
      x = dict(
        type = int,
        help = 'Input variable "x"'
      )
    )
    def main(self,x):
      y = x*x
      return FancyOutput(y=y)

  if __name__ == '__main__':
    DoSquare.fromCommandLine().run()

The onetask.py code contains the following essential elements:

* from fancypipe import * => this imports the public elements of the fancypipe task, after the system path has been updated to include the fancypipe folder.

* class DoSquare(FancyTask) => this is how a task is created; it must be derived from the *FancyTask* class. Of course, in practice your tasks should do more complex operations to offset the overhead introduced by the task definitions.

* inputs = odict( ... ) => This defines the inputs of the task, whereby *odict* is defined in fancypipe as an alias to OrderedDictionary.

* x = dict( ... ) => Defines the input *x*.

* def main(self,x) => Every user-defined task must contain a function main(self, ...), whereby the inputs correspond to the keys of the 'inputs' variable. It is mandatory that main() returns a FancyOutput object. This is just a container to store positional and keyword arguments. For example FancyOutput(y=y) has *y* as an output with key 'y'. 

* DoSquare.fromCommandLine().run() => Although you must define the *main()* method, you must never call it directly. Instead, use the *run()* method, which calls *main()* with inputs from either:

  - A previous call to setInput(), as illustrated in the next example.
  
  - A configuration file (not documented yet).
  
  - A default value (not documented yet).
  
  - The command line. This is invoked with the *fromCommandLine()* constructor, as used here.

Running the task
----------------
From the commandline, in the directory that contains the onetask.py, run
::

  python onetask.py -h

You will find out that the program needs the parameter -x,
but also accepts additional parameters that control how FanyPipe runs the script.

Two tasks, serial programming
-----------------------------
The script below adds a second task, which can in principle be run in
parallel with the first task. However, we first show code that does not
allow parallel execution  (../examples/twotasks_serial.py):

::

  import sys
  sys.path.append('../fancypipe')
  from fancypipe import *

  class DoSquare(FancyTask):
    def main(self,x):
      y = x*x
      return FancyOutput(y=y)
      
  class MainTask(FancyTask):
    inputs = {
      'x1':{'default':3, 'help':'input x1'},
      'x2':{'default':4, 'help':'input x2'}
    }
    def main(self,x1,x2):
      task1 = DoSquare.fromParent(self).setInput(
        x = x1
      ).run()
      task2 = DoSquare.fromParent(self).setInput(
        x = x2
      ).run()
      return FancyOutput(
        sum = task1.getOutput('y') + task2.getOutput('y')
      )
      
  if __name__ == '__main__':
    MainTask.fromCommandLine().run()

This program returns the sum of squares of its two inputs x1 and x2.
It contains the following essential elements, in addition to the previous script onetask.py:

* In *MainTask*, two subtasks are defined to square the two inputs: *co1* and *co2*. The 'fancy' settings of these tasks, such as tempdir, logging, configuration file etc. are inherited from the parent task by using the 'fromParent(self)' constructor.

* Each task is immediately run after its inputs are set. This effectively disables parallel execution, but makes the task output immediately available.

* In the return statement, the output of each task is obtained with the getOutput() method, which can only be used *after a task has run*.

Two tasks, parallel programming
-------------------------------
The script below performs the same computation as the previous example,
but this time the code allows for parallel execution  (../examples/twotasks.py):

::

  import sys
  sys.path.append('../fancypipe')
  from fancypipe import *

  class DoSquare(FancyTask):
    def main(self,x):
      y = x*x
      return FancyOutput(y=y)
      
  class DoSum(FancyTask):
    def main(self,x1,x2):
      y = x1+x2
      return FancyOutput(y=y)

  class MainTask(FancyTask):
    inputs = {
      'x1':{'default':3, 'help':'input x1'},
      'x2':{'default':4, 'help':'input x2'}
    }
    def main(self,x1,x2):
      task1 = DoSquare.fromParent(self).setInput(
        x = x1
      )
      task2 = DoSquare.fromParent(self).setInput(
        x = x2
      )
      task3 = DoSum.fromParent(self).setInput(
        x1 = task1.requestOutput('y'),
        x2 = task2.requestOutput('y')
      )      
      return FancyOutput(
        y = task3.requestOutput('y')
      )
      
  if __name__ == '__main__':
    MainTask.fromCommandLine().run()

This program again returns the sum of squares of its two inputs x1 and x2.
To allow for parallel execution, it is slightly more complex than the
previous script twotasks_serial.py:

* In MainTask.main, task1 and task2 are defined, their input is set, but they are not run.

* A third task is created that takes the sum of its two inputs. In *task3.setInput()*, the outputs of *task1* and *task2* are obtained with the *requestOutput()* method. This method can be called on tasks before they are run. It returns a request object (instance of FancyLink). It is important to remember that request objects can only be used for two purposes:

  1. To serve as an input for another task. This is the case in the above code, where *requestOutput()* is called inside a *setInput()* context.
  
  2. To serve as an output of a task. This is also used in the above code, where task3.requestOutput('y') is called inside a *return FancyOutput()* context.
  
It is a common error to use request objects in other places. For example, it is tempting to write, as in the serial code:

::

  return FancyOutput(
    sum = task1.requestOutput('y') + task2.requestOutput('y')
  )

This is an error, because request objects cannot be summed. A separate task must be created for the summing operation, so that the request objects can be used as inputs.

Internally, FancyPipe will make sure that before the *main()* method of a task is called, all its arguments are resolved. That means, if any argument is a request object, the task that it links to is run first and the request object is replaced by the actual value.

Calling external programs
-------------------------
Pipelining is all about glueing together different pieces of software. FancyPipe has a special class to run external executables: FancyExec. This class can be either used on its own, or you can use it as a base class instead of FancyTask.
We first look at using FancyExec directly. In the example the executable is *python*, with the execute option -c set to "print(1+2)".

::
  cmd = ['python','-c' '"print(1+2)"']
  doExec = FancyExec.fromParent(self).setInput(*cmd)
  ans = doExec.run()

Normally you would not call python as an external executable, here it is chosen because you have it installed for sure.
As you can see, FancyExec is used directly to submit the command obtained by concatenating the elements of *cmd*, and it will return the output of the program (in this case: '2').
Recall that the notation (*cmd) instructs python to convert the elements of *cmd* into separate inputs. All elements of *cmd* must be either strings or request objects that become strings when they are resolved.

An issue with FancyExec is that you cannot directly connect it to other tasks, because its inputs cannot contain request objects, they must all be strings.

Parallel processing paradigms
-----------------------------
After you have programmed a parallel pipeline, choose a
`parallel processing paradigm <parallelprocessing.html>`_ to run it.
