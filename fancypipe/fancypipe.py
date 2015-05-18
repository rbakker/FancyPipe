# FancyPipe
# ---------
# The FancyPipe Engine is a python package that takes care of those
# aspects of complex data processing pipelines that would otherwise 
# distract you from your main job: the design of the processing steps.
#
# FancyPipe takes care of
# * command line parsing
# * logging to dynamic html-file and console
# * parallel execution
# * temporary file management
# * error handling
# * reading parameters from a configuration file
#
# To use FancyPipe effectively, you need to wrap your processing steps 
# as FancyTask or FancyModule classes, and define input-output 
# connections between tasks.
#
# FancyPipe works in both Python 2.7 and Python 3
#
# Useful links:
# https://pythonhosted.org/joblib/parallel.html
# http://eli.thegreenplace.net/2012/01/24/distributed-computing-in-python-with-multiprocessing
# http://bugs.python.org/issue7503
#
## Use Processes to execute compute-intensive tasks written in
## python. Data will be serialized and copied, so that multiple 
## instances of Python run in parallel.
## Use Threads if the bottleneck in your tasks is waiting for external 
## processes to finish or for data to become available.
## Threads avoid serializing and copying data, because they use 
## shared memory. However, CPython does not run threads in parallel,
## it switches between them.

# These functions and classes get imported after "from fancypipe import *"
__all__ = [
  'assertPassword','assertBool','assertFile','assertDir','assertList','assertDict','assertExec','assertToken',
  'assertMatch','assertInstance','assertType',
  'odict',
  'FancyOutputFile','FancyPassword','FancyLink','FancyArgs','FancyOutput','FancyInput','FancyList','FancyDict',
  'Task','FancyTask','FancyModule','FancyExec','FancyNode','FancyLog'
]

import __future__
import os,sys
import os.path as op
import argparse, subprocess
import tempfile, datetime, json
from collections import OrderedDict as odict
import re
import threading, multiprocessing, multiprocessing.managers
try:
    import queue
except ImportError:
    import Queue as queue
try:
    import cPickle as pickle
except ImportError:
    import pickle


class FancyPassword:
  def __init__(self,pwd):
    self.value = pwd
  def __repr__(self):
    return '***'

def assertPassword(s):
  return FancyPassword(s)

def assertBool(s):
  if (s is True or s.lower() == 'true' or s == '1'): return True
  elif (s is False or s.lower() == 'false' or s == '0'): ans = False
  else: raise AssertionError('Option "{}" does not represent a boolean value.'.format(s))

def assertFile(s):
  """ Assert that the input is an existing file. """
  if not op.isfile(s): raise AssertionError('String "{}" is not an existing file.'.format(s))
  return s

def assertDir(s):
  """ Assert that the input is an existing directory. """
  if not op.isdir(s): raise AssertionError('String "{}" is not an existing directory.'.format(s))
  return s

def assertList(s):
  """ Assert that the input is a list or tuple. """
  if isinstance(s,str): 
    try:
      s = json.loads(s)
    except:
      raise AssertionError('String "{}" cannot be json-decoded.'.format(s))
  if not isinstance(s,(list,tuple)): raise AssertionError('Variable "{}" is not a list.'.format(s))
  return s

def assertDict(s):
  """ Assert that the input is a dictionary. """
  if isinstance(s,str): 
    try:
      s = json.loads(args['execute'])
    except:
      raise AssertionError('String "{}" cannot be json-decoded.'.format(s))
  if not isinstance(s,dict): raise AssertionError('Variable "{}" is not a dictionary.'.format(s))
  return s

def assertExec(s):
  """ Assert that the input can be executed and return the full path to the executable. """
  import os
  def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

  fpath,fname = op.split(s)
  if fpath:
    if is_exe(s):
      return s
  else:
    for path in os.environ["PATH"].split(os.pathsep):
      path = path.strip('"')
      exe_file = op.join(path,s)
      if is_exe(exe_file):
        return exe_file
  raise AssertionError('Cannot find executable command "{}".'.format(s))

def assertToken(s):
  return s.strip()

# call as assertMatch(...)
def assertMatch(regexp):
  def f(s):
    matches = re.match(regexp,s)
    if matches: return matches.groups()
    raise AssertionError('String "{}" has no match for regular expression "{}".'.format(s,regexp))
  return f

# call as assertInstance(...)
def assertInstance(cls):
  def f(v):
    if isinstance(v,cls): return v
    raise AssertionError('Variable "{}" is not an instance of "{}".'.format(v,cls))
  return f

# call as assertType(...)
def assertType(tp):
  def f(v):
    try: 
      return tp(v)
    except ValueError:
      raise AssertionError('Value "{}" cannot be converted to type "{}".'.format(v,tp))
  return f

## Deprecated: just use FancyOutputFile(...).
def flagOutfile(v):
  return FancyOutputFile(v)

## Use this class to indicate that an input represents an outputfile.
## In recycling mode, fancypipe will check whether the file can be recycled.
class FancyOutputFile(str):
  def __init__(self,s):
    if hasattr(s,'autoclean'): self.autoclean = s.autoclean
#endclass
  
class _FancyAutoclean:
  def __init__(self,f):
    self.f = f
  def __del__(self):
    print('Future: Removing file {}'.format(self.f))
    #os.remove(self.f)      

## Use this class to indicate that a string represents a temporary file,
## that can be removed when it is no longer referenced.
class FancyAutoclean(str):
  def __init__(self,f):
    self.autoclean = _FancyAutoclean(f)
#endclass


## Used for output and error reporting
class FancyReport:
  import traceback as tb
  @staticmethod
  def warning(s): print('\033[1;30m{}\033[0m'.format(s))
  @staticmethod
  def error(s): sys.stderr.write('\033[1;31m{}\033[0m'.format(s))
  @staticmethod
  def success(*out,**kwout):
    if len(out):
      result = FancyLog.stringifyList(out)
      print(json.dumps(result))
    if kwout:
      result = FancyLog.stringifyDict(kwout)
      print(json.dumps(result,indent=2))
  @staticmethod
  def traceback():
    exc_type, exc_value, exc_traceback = sys.exc_info() 
    return FancyReport.tb.format_exception(exc_type, exc_value, exc_traceback)
  @staticmethod
  def fail(msg='Error',code=1):
    FancyReport.error(msg+'\n')
    FancyReport.error(json.dumps(FancyReport.traceback(),indent=2)+'\n')
    print()
    sys.exit(code)
#endclass


## Used for logging to console and html-file.
class FancyLog:
  def __init__(self, logdir=None,loglevel=3):
    self.logdir = logdir
    self.logfile = None
    if logdir:
      if not op.isdir(logdir):
        print('Creating new log directory "{}".'.format(logdir))
        os.makedirs(logdir)
      logfilesdir = op.join(logdir,'fancylog_files')
      if not op.isdir(logfilesdir):
        print('Creating new log_files directory "{}".'.format(logfilesdir))
        os.mkdir(logfilesdir)
      self.logdir = logdir
      self.logfile = op.join(logdir,'fancylog.js')
      if not op.isfile(self.logfile):
        self.reset()

    self.loglevel = loglevel
  
  @classmethod
  def fromParent(cls,parentTask):
    try:
      return parentTask.fancyLog
    except:
      return cls()
    
  @staticmethod
  def stringifyValue(v):
    if hasattr(v,'tolist'): v = v.tolist()
    if isinstance(v,(tuple, list)): return FancyLog.stringifyList(v)
    if isinstance(v,(dict)): return FancyLog.stringifyDict(v)
    if isinstance(v,(int,bool,float)) or v is None: return v
    return '{}'.format(v)

  @staticmethod
  def stringifyList(args):
    return [FancyLog.stringifyValue(v) for v in args]

  @staticmethod
  def stringifyDict(kwargs):
    return { k:FancyLog.stringifyValue(v) for k,v in kwargs.items() }
    
  def appendLog(self,s):
    lock = threading.Lock()
    lock.acquire()
    with open(self.logfile,'a') as log:
      log.write(s)
    lock.release()
    
  # Add an entry to the logfile
  def addEntry(self, logId,cmd,args,kwargs, title=None,commandLine=None):
    print('___\nLog({}) {}'.format(logId,cmd+': '+title if title else cmd))
    # prepare args for json-dumping
    if args:
      args = self.stringifyList(args)
    if kwargs:
      kwargs = self.stringifyDict(kwargs)
    if self.loglevel > 0:
      if commandLine: 
        print('Command line: {}'.format(commandLine))
      print('Arguments: {},{}'.format(json.dumps(args,indent=2),json.dumps(kwargs,indent=2)))
    
    # write to logfile
    if not self.logfile: return
    starttime = datetime.datetime.now()
    parentLogId = '.'.join(logId.split('.')[:-1])
    params = {
      'id': logId,
      'parentId': parentLogId,
      'cmd': cmd,
      'args': args,
      'kwargs': kwargs,
      'timeStamp': starttime.isoformat()
    }
    if title:
      params['title'] = title
    if commandLine:
      params['commandLine'] = commandLine
    self.appendLog('LOG.push('+json.dumps(params,indent=2)+')\n')
    
  def attachMessage(self,logId,msg, type=None):
    params = {
      'attachTo':logId,
      'message':msg
    }  
    print('Task {} says: {}'.format(logId,msg))
    # write to logfile
    if not self.logfile: return
    self.appendLog('LOG.push('+json.dumps(params,indent=2)+')\n')

  def attachResult(self,logId,name,args=None,kwargs=None, tp=None):
    # write to logfile
    if not self.logfile: return
    params = {
      'attachTo':logId,
      'name':name,
      'type':tp
    }
    if tp=='longtext':
      lineCount = len(args)
      if lineCount>12:
        logfilesdir = op.join(self.logdir,'fancylog_files')
        outfile = op.join(logfilesdir,'stdout_{}.txt'.format(logId))
        with open(outfile,"w") as fp:
          fp.write('\n'.join(args))      
        args = args[0:6] + ['... {} more lines ...'.format(lineCount-12)] + args[-6:0]
        kwargs = {
          'name':outfile,
          'href':'file:///{}'.format(outfile)
        }
    if args: params['args'] = FancyLog.stringifyList(args)
    if kwargs: params['kwargs'] = FancyLog.stringifyDict(kwargs)
    params_json = json.dumps(params,indent=2);
    self.appendLog('LOG.push('+params_json+')\n')

  def reset(self,overwrite=False):
    if self.logfile:
      if op.isfile(self.logfile) and not overwrite:
        raise Exception('Logfile "{}" already exists, use RESET to overwrite.'.format(self.logfile))
      with open(self.logfile,'w') as log:
        log.write('var LOG = [];\n\n')
      htmlsrc = op.join(op.dirname(__file__),'fancylog.html')
      htmltgt = op.join(self.logdir,'fancylog.html')
      if not op.isfile(htmltgt):
        import shutil
        shutil.copyfile(htmlsrc,htmltgt)
#endclass


## Used to keep track of temporary files. Status: scratch.
class FancyClean:
  def __init__(self, noCleanup=False):
    self.noCleanup = noCleanup
    self.files = set()
    self.dirs = set()
    
  @classmethod
  def fromParent(cls,parentTask):
    try:
      noCleanup = parentTask.fancyClean.noCleanup
      return cls(noCleanup)
    except:
      return cls()
    
  def _addFile(self,ff):
    pass
    #self.files.add(ff)

  def addNewFile(self,f):
    f = op.realpath(f)
    if not op.isfile(f): self._addFile(f)
        
  def _addDir(self,dd):
    pass
    #self.dirs.add(dd)
  
  def addCreateDir(self,d):
    d = op.realpath(d)
    create = []
    while not op.exists(d):
      create.append(d)
      head,tail = op.split(d)
      d = head
    for d in reversed(create):
      os.mkdir(d)
      self._addDir(d)

  def cleanup(self,exclude=None):
    if self.noCleanup: return
    E = set()
    if exclude: 
      for e in exclude:
        E.add(op.realpath(path))
    for f in self.files:
      if f not in E:
        try:
          print('DELETING FILE "{}"'.format(f))
          #os.remove(f)
        except:
          pass
    for d in reversed(sorted(self.dirs)):
      if d not in E:
        try:
          print('REMOVING DIR "{}"'.format(d))
          #os.rmdir(d)
        except:
          pass
#endclass


## Used to load and keep track of configuration parameters.
class FancyConfig:
  def __init__(self,config={}):
    self.config = config

  @staticmethod
  def etree_to_odict(t):
    if len(t):
      od = odict()
      for ch in t:
        od[ch.tag] = FancyConfig.etree_to_odict(ch)
      return od
    else:
      return t.text

  @classmethod
  def fromFile(cls,configFile):
    if configFile is None:
      return cls()
    name,ext = op.splitext(configFile)
    if ext.lower() == '.xml':
      from lxml import etree
      tree = etree.parse(configFile)
      root = tree.getroot()
      config = odict()
      config[root.tag] = FancyConfig.etree_to_odict(root)
      return cls(config)
    elif ext.lower() == '.json':
      return cls(json.load(configFile, object_pairs_hook=odict))
    else:
      raise RuntimeError('Configuration file "{}" has an unrecognized format.'.format(configFile))

  @classmethod
  def fromParent(cls,parentTask):
    try:
      config = parentTask.fancyConfig.config.copy()
      parentClass = parentTask.__class__.__name__
      if parentClass in config:
        # overwrite defaults with className-specific defaults
        for k,v in config[parentClass].items(): config[k] = v;
        config.pop(parentClass)
      return cls(config)
    except:
      return cls()
    
  def classDefaults(self,taskClass):
    return self.config[taskClass] if taskClass in self.config else {}
#endclass


## Indicates an outgoing link of output <outKey> of task <task>.
class FancyLink:
  def __init__(self,task,outKey):
    self.task = task # list of FancyTasks
    self.outKey = outKey # output key
  
  def __repr__(self):
    return 'FancyLink<{}[{}]>'.format(self.task,self.outKey)
#endclass
  

## Base class for task input/output, consisting of list and keyword arguments
class FancyArgs:
  def __init__(self,*args,**kwargs):
    self.args = list(args)
    self.kwargs = kwargs

  @staticmethod
  def listReady(d):
    for i,v in enumerate(d):
      if isinstance(v,FancyLink): return False
      elif isinstance(v,FancyArgs) and not v.ready(): return False
    return True
    
  @staticmethod
  def dictReady(d):
    for k,v in d.items():
      if isinstance(v,FancyLink): return False
      elif isinstance(v,FancyArgs) and not v.ready(): return False
    return True

  def ready(self):
    return self.listReady(self.args) and self.dictReady(self.kwargs)

  @staticmethod
  def listSources(d):
    ans = set()
    for i,v in enumerate(d):
      if isinstance(v,FancyLink): ans.add(v.task)
      elif isinstance(v,FancyArgs): ans.update( v.sources() )
    return ans

  @staticmethod
  def dictSources(d):
    ans = set()
    for k,v in self.items():
      if isinstance(v,FancyLink): ans.add(v.task)
      elif isinstance(v,FancyArgs): ans.update( v.sources() )
    return ans

  def sources(self):
    return self.listSources(self.args) | self.dictSources(self.kwargs)

  def __getitem__(self,key):
    return self.args[key] if isinstance(key,int) else self.kwargs[key]

  def __setitem__(self,key,val):
    if isinstance(key,(int)): self.args[key] = val
    else: self.kwargs[key] = val
#endclass
    

## Special case of FancyArgs, consisting of only a list.
class FancyList(list,FancyArgs):  
  def __init__(self,args):
    list.__init__(self,args)
    self.args = self
    self.kwargs = {}

  def ready(self):
    return FancyArgs.listReady(self)

  def sources(self):
    return FancyArgs.listSources(self)
#endclass


## Special case of FancyArgs, consisting of only keyword arguments.
class FancyDict(dict,FancyArgs):
  def __init__(self,kwargs):
    dict.__init__(self,kwargs)
    self.kwargs = self
    self.args = []

  def ready(self):
    return FancyArgs.dictReady(self)

  def sources(self):
    return FancyArgs.dictSources(self)
#endclass

## FancyInput and FancyOutput have no special meaning, but improve
## readability when used to represent the input and output of a task.
FancyInput  = FancyArgs
FancyOutput = FancyArgs

## Constants to indicate the run-status of a task
Task_Inactive = 0
Task_ResolvingInput = 1
Task_Submitted = 2
Task_ResolvingOutput = 3
Task_Completed = 4

## No-frills Task class, supports parallel execution, but no logging, 
## no tempdir, no cleanup, no config file
class Task():
  name = None
  numChildren = 0
  taskId = '0'
  runStatus = Task_Inactive
  numWorkers = False
  workerType = 'P'
  jobManager = False
  jobAuth = None
  
  def __init__(self,taskId=None):
    if taskId: self.taskId = taskId
    self.myInput     = FancyInput()
    self.myOutput    = FancyOutput()
    self.requests    = {} # for each sourceKey, a list of target task ids to send the result to
    self.sourceLinks = {} # for each sourceTask.sourceKey a list of (targetArgs,targetKey) pairs that this task receives data from

  def getName(self):
    return self.name if self.name else self.__class__.__name__
    
  def __repr__(self):
    return "%s[%s]" % (self.getName(), self.taskId)

  @classmethod
  def fromParent(cls,parent):
    taskId = parent.newChildId()
    return cls(taskId)

  def newChildId(self):
    self.numChildren += 1
    return self.taskId+'.'+str(self.numChildren)

  # Get an input.
  def getInput(self,key=None):
    return self.myInput[key]

  # Set (extend/update) the input for a subsequent call to run().
  def prepareInput(self,args,kwargs):
    self.myInput.args.extend(args)
    self.myInput.kwargs.update(kwargs)

  # Set the input for a subsequent call to run(); register output files for recycling.
  def setInput(self,*args,**kwargs):
    self.prepareInput(args,kwargs)
    return self

  # Generate a link that requests output, use this link only 
  # to return as output of a task, or to set as input to another task.
  def requestOutput(self,key):
    try:
      return self.myOutput[key]
    except:
      return FancyLink(self,key)

  # Get an output, after calling run().
  def getOutput(self,key=None):
    if not self.myOutput.ready():
      raise RuntimeError('The output of task {} contains unresolved values. Use requestOutput() instead, or run the task first.'.format(self))
    return self.myOutput[key]

  # Initialize requests for output from upstream tasks, called when 
  # myInput or myOutput of a task becomes available.
  def initRequests(self,myArgs):
    def addRequest(tk,sl): # target key, source link
      sn,sk = (sl.task,sl.outKey)
      print('Task {} requests output "{}" from task {}'.format(self,sk,sn))
      if not sk in sn.requests: sn.requests[sk] = set()
      sn.requests[sk].add(str(self))
      srcId = '{}.{}'.format(sn,sk)
      if not srcId in self.sourceLinks: self.sourceLinks[srcId] = []
      self.sourceLinks[srcId].append((myArgs,tk))

    unresolvedTasks = { str(self):self }
    mySources = set()
    for i,v in enumerate(myArgs.args):
      if isinstance(v,FancyLink): 
        addRequest(i,v)
        mySources.add(v.task)
      elif isinstance(v,FancyArgs): 
        unresolvedTasks.update( self.initRequests(v) )
    for k,v in myArgs.kwargs.items():
      if isinstance(v,FancyLink): 
        addRequest(k,v)
        mySources.add(v.task)
      elif isinstance(v,FancyArgs): 
        unresolvedTasks.update( self.initRequests(v) )

    for src in mySources:
      if src.runStatus == Task_Inactive:
        src.runStatus = Task_ResolvingInput
        unresolvedTasks.update( src.initRequests(src.myInput) )
    
    return unresolvedTasks

  # Fulfill requests after running (some of) the unresolved tasks.
  # sn: source task, tn: target task
  def fulfillRequests(self,taskCache):
    affectedTasks = {}
    for sk,targets in self.requests.items():      
      srcId = '{}.{}'.format(self,sk)
      try:
        val = self.myOutput if sk is None else self.myOutput.args[sk] if isinstance(sk,int) else self.myOutput.kwargs[sk]
      except KeyError: 
        raise KeyError('Task "{}" does not have the requested output "{}"'.format(self,sk))
      for tnId in targets:
        tn = taskCache[tnId]
        affectedTasks[tnId] = tn
        for (targetArgs,tk) in tn.sourceLinks[srcId]:
          targetArgs[tk] = val
    return affectedTasks
        
  def resolveAffected(self,affectedTasks,taskCache,jobQueue,fancyPool):
    taskCache.update(affectedTasks)
    for taskName,task in affectedTasks.items():
      if task.runStatus == Task_ResolvingInput and task.myInput.ready():
        task.runStatus = Task_Submitted
        if fancyPool:
          if isinstance(fancyPool,RemotePoolClient):
            jobQueue.put(pickle.dumps((task,fancyPool.runId)))
          else: # local pool
            jobQueue.put(task)
          fancyPool.jobCount += 1
        else:
          jobQueue.put(str(task))
      elif task.runStatus == Task_ResolvingOutput and task.myOutput.ready():
        task.runStatus = Task_Completed
        taskCache.pop(taskName)
        affectedChildren = task.fulfillRequests(taskCache)
        task.resolveAffected(affectedChildren,taskCache,jobQueue,fancyPool)
    
  def resolveOutput(self,jobQueue,fancyPool=None):
    # Deal with unresolved links in the output.
    taskCache = {}
    affectedTasks = self.initRequests(self.myOutput)
    self.resolveAffected(affectedTasks,taskCache,jobQueue,fancyPool)
    while self.runStatus is not Task_Completed:
      if fancyPool:
        # Parallel execution, using local or remote pool
        if fancyPool.jobCount == 0:
          raise RuntimeError('Waiting for output but no jobs are running.')
        # Wait for result to appear in result queue.
        (srcId,myOutput) = fancyPool.getResult()
        fancyPool.jobCount -= 1
        if srcId is None:
          workerName,srcId = myOutput
          raise RuntimeError('Worker {} encountered an error while running task {}.'.format(workerName,srcId))
        src = taskCache[srcId]
        src.myOutput = myOutput
      else:
        # serial execution
        srcId = jobQueue.get()
        src = taskCache[srcId]
        src.myOutput = src._run()
      
      src.runStatus = Task_ResolvingOutput
      affectedTasks = src.initRequests(src.myOutput)
      self.resolveAffected(affectedTasks,taskCache,jobQueue,fancyPool)

  # Main processing step, to be implemented by the derived class.
  def main(self,*args,**kwargs):
    raise NotImplementedError

  def _main(self,myInput):
    # Main processing step.
    return self.main(*myInput.args,**myInput.kwargs)

  def logEntry(self):
    pass
    
  def logOutput(self):
    pass

  def logError(self):
    FancyReport.fail('Fatal error in task {}.'.format(self))

  def _run(self):
    # Add entry to log/console.
    self.logEntry()
    myOutput = self._main(self.myInput)
    if not isinstance(myOutput,FancyArgs):
      raise RuntimeError('The function main() of task {} must return a FancyOutput(...) object.'.format(self))
    return myOutput

  # Run this task and return its output: outer loop.
  def run(self):
    try:
      self.myOutput = self._run()
      self.runStatus = Task_ResolvingOutput
      if not self.myOutput.ready():
        # resolve output dependencies
        isWorker = isinstance(threading.current_thread(),Worker) | isinstance(multiprocessing.current_process(),Worker)
        if self.jobManager:
          print('Running task {} in distributed mode.'.format(self))
          fancyPool = RemotePoolClient(self.jobManager,self.jobAuth)
          self.resolveOutput(fancyPool.jobQueue,fancyPool)          
        elif self.numWorkers and not isWorker:
          print('Running task {} in parallel mode.'.format(self))
          fancyPool = LocalPool(self.numWorkers,self.workerType)
          self.resolveOutput(fancyPool.jobQueue,fancyPool)
        else:
          print('Running task {} in serial mode.'.format(self))
          self.resolveOutput(queue.Queue())

      self.logOutput()
      FancyReport.success(*self.myOutput.args,**self.myOutput.kwargs)
      return self
    except:
      self.logError()
#endclass


## Fancy task class, has all features except calling the task from the command line.
class FancyTask(Task):
  title = None
  outfiles     = None # outputs that are returned as files
  
  def __init__(self,tempdir,fancyClean,fancyConfig,fancyLog,taskId,cmdArgs={}):
    Task.__init__(self,taskId)
    self._tempdir     = tempdir
    self.fancyLog     = assertInstance(FancyLog)(fancyLog)
    self.fancyConfig  = assertInstance(FancyConfig)(fancyConfig)
    self.fancyClean   = assertInstance(FancyClean)(fancyClean)
    self.doRecycle    = fancyClean.noCleanup
    
    # Prepare inputs from defaults and configuration file.
    self.prepareInput([],self._parseInputs(cmdArgs,self.fancyConfig.classDefaults(self.__class__.__name__)))
  
  @classmethod
  def fromParent(cls,parent,title=None):
    taskId = parent.newChildId()
    tempdir = op.join(parent._tempdir,cls.tempsubdir(parent.numChildren))
    fancyLog = FancyLog.fromParent(parent)
    fancyConfig = FancyConfig.fromParent(parent)
    fancyClean = FancyClean.fromParent(parent)
    
    args = {
      'taskId': taskId,
      'tempdir': tempdir,
      'fancyLog': fancyLog,
      'fancyConfig': fancyConfig,
      'fancyClean': fancyClean
    }
    # Instantiate.
    self = cls(**args)
    if title:
      self.title = title
    return self

  # Parse external inputs, from commandline or configfile.
  @classmethod
  def _parseInputs(cls,raw,cfg):
    if 'inputs' not in cls.__dict__: return {}
    kwargs = {}
    # first loop: command line arguments
    inputs = cls.inputs.copy()
    for key,inp in inputs.items():
      if inp:
        if key in raw:
          # typecast
          tp = inp['type'] if 'type' in inp else str
          kwargs[key] = tp(raw[key])
          inputs[key] = False
    # second loop: configuration file arguments
    for key,inp in inputs.items():
      if inp:
        if key in cfg:
          # typecast
          tp = inp['type'] if 'type' in inp else str
          kwargs[key] = tp(cfg[key])
          inputs[key] = False
    # final loop: default arguments
    for key,inp in inputs.items():
      if inp:
        if 'default' in inp:
          if hasattr(inp['default'],'__call__'):
            kwargs[key] = inp['default'](kwargs)
          else:
            kwargs[key] = inp['default']
        #else:
        #  raise ValueError('No value supplied for required input "{}" of module "{}"'.format(key,cls))
    return kwargs
  
  @classmethod
  def tempsubdir(cls,taskId):
    return '{}_{}'.format(taskId,cls.__name__)

  # tempdir(subdir) returns tempdir/subdir, and registers d for cleanup after running main()
  def tempdir(self,subdir=None):
    td = self._tempdir
    if subdir: 
      td = op.join(td,subdir)
    self.fancyClean.addCreateDir(td)
    return td

  # tempfile(f) returns tempdir/f, and registers f for cleanup after running main()
  def tempfile(self,f=None,ext='',autoclean=None):
    if f is None:
      import string
      import random
      rand8 = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(8))      
      f = '{}_{}{}'.format(self.__class__.__name__.lower(),rand8,ext)
    f = op.join(self.tempdir(),f)
    # DEPRECATED: self.fancyClean.addNewFile(f)
    if autoclean is None: autoclean = not self.fancyClean.noCleanup
    return FancyAutoclean(f) if autoclean and f and not op.isfile(f) else f
  
  def getCommandLine(self):
    return 'python runmodule.py {} [...]'.format(self.__class__.__name__)

  # Flag outputs that will be returned as files, so that they can be recycled.
  def outputFiles(self):
    return FancyOutput(
      *[ v for v in self.myInput.args if isinstance(v,FancyOutputFile) ],
      **{ k:v for k,v in self.myInput.kwargs.items() if isinstance(v,FancyOutputFile) }
    )

  # Set the input for a subsequent call to run(); register output files for recycling.
  def setInput(self,*args,**kwargs):
    Task.prepareInput(self,args,kwargs)
    self.outfiles = self.outputFiles()
    if not isinstance(self.outfiles,FancyArgs):
      raise TypeError('The method outputFiles() of task {} must return a FancyOutput(...) object.'.format(self))
    return self

  # Generate a link that requests output, use this link only 
  # to return as output of a task, or to set as input to another task.
  def requestOutput(self,key):
    try:
      return self.myOutput[key]
    except:
      if self.doRecycle:
        try:
          f = self.outfiles[key]
          if op.isfile(f): 
            print('Recycling file "{}" in task {}'.format(f,self))
            return f
        except (KeyError,IndexError):
          pass          
      return FancyLink(self,key)

  def logEntry(self):
    name = self.getName()
    commandLine = self.getCommandLine()
    self.fancyLog.addEntry(self.taskId, name,self.myInput.args,self.myInput.kwargs, title=self.title,commandLine=commandLine)

  def logOutput(self):
    self.fancyLog.attachResult(self.taskId, 'output',self.myOutput.args,self.myOutput.kwargs)

  def logError(self):
    msg = FancyReport.traceback()
    title = 'Fatal error in task {}.'.format(self)
    self.fancyLog.attachResult(self.taskId,title,msg,{}, tp='error')
    FancyReport.fail(title)
#endclass

## Deprecated
FancyNode = FancyTask

## Extended class for pipeline tasks, adds support for calling the task
## from the command line, and adds methods beforeMain() and afterMain().
class FancyModule(FancyTask):
  description = None
  inputs = odict([
    ('tempdir', dict( default=None,
      help='Temp directory, to store intermediate results. Default: system tempdir.'
    )),
    ('logdir', dict( default=None,
      help='Log directory, to store logfile (fancylog.js + fancylog.html) and attachments. Default: tempdir.'
    )),
    ('loglevel', dict( type=int, default=3,
      help='Log level, 1 for entries, 2 for standard output, 4 for data, 8 for extra. Default: 3.'
    )),
    ('taskid', dict( default='0',
      help='Hierarchical identifier of the task (e.g. "3.4.7"). Use taskid=INIT to create a new empty log (error if it exists), and taskid=RESET to reset (overwrite) an existing log.'
    )),
    ('configfile', dict( default=None,
      help='Configuration file (XML or JSON), to read default parameters from. Default: None.'
    )),
    ('nocleanup', dict( action='store_true', default=False,
      help='Whether to cleanup intermediate results. Default: Do cleanup'
    )),
    ('workertype', dict( type=assertMatch('([pPtT])'), default=('P'),
      help='Either "T" or "P": T uses multi-threading while P uses multi-processing. Use T when the pipeline mainly involves calls to external programs; use P when Python itself is used for number-crunching.'
    )),
    ('numworkers', dict( type=assertType(int), default='auto',
      help='Number of parallel workers. Default: number of CPUs.'
    )),
    ('jobmanager', dict( default=False,
      help='Address (ip-address:port) of job manager started with "python fancymanager.py-auth=abracadabra"'
    )),
    ('jobauth', dict( default='abracadabra',
      help='Authorization key for submitting jobs to the job manager. Default: abracadabra.'
    ))
  ])
  
  @classmethod
  def fromCommandLine(cls):
    cmdArgs = cls._getParser().parse_args().__dict__;
    kwargs = FancyModule._parseInputs(cmdArgs,{})
    taskId = kwargs['taskid'] if kwargs['taskid'] else '0'
    logReset = False
    if taskId == 'INIT' or taskId == 'RESET': 
      logReset =True
      logOverwrite = (taskId == 'RESET')
      taskId = '0'
    tempdir = kwargs['tempdir'] if kwargs['tempdir'] else op.join(tempfile.gettempdir(),cls.tempsubdir(taskId))
    logdir = kwargs['logdir'] if kwargs['logdir'] else tempdir
    logLevel = kwargs['loglevel']
    fancyLog = FancyLog(logdir,logLevel)
    if logReset:
      fancyLog.reset(overwrite=logOverwrite)
    configFile = kwargs['configfile']
    # Instantiate.
    self = cls(**{
      'tempdir': tempdir,
      'fancyClean': FancyClean(kwargs['nocleanup']),
      'fancyConfig': FancyConfig.fromFile(configFile),
      'fancyLog': fancyLog,
      'taskId': taskId,
      'cmdArgs': cmdArgs
    })
    # Worker settings
    self.workerType = kwargs['workertype'][0]
    try: nw = int(kwargs['numworkers'])
    except: nw = multiprocessing.cpu_count()
    self.numWorkers = nw
    jm = kwargs['jobmanager']
    if jm:
      addr = jm.split(':')
      port = int(addr[1]) if len(addr)>1 else 51423
      jm = (addr[0],port)
    self.jobManager = jm
    self.jobAuth = kwargs['jobauth']
    return self

  @classmethod
  def _getParser(cls):
    p = argparse.ArgumentParser(
      description=cls.description if cls.description else cls.title,
      formatter_class=argparse.RawTextHelpFormatter,
      argument_default=argparse.SUPPRESS
    )
    cls._extendParser(p,FancyModule.inputs)
    cls._extendParser(p,cls.inputs)
    return p

  @classmethod
  def _extendParser(cls,p,inputs):
    for key in inputs:
      inp = inputs[key].copy()
      short = inp.pop('short') if 'short' in inp else key
      positional = inp.pop('positional') if 'positional' in inp else False
      dest = [key] if positional else ['-{}'.format(short),'--{}'.format(key)]
      if not positional: 
        inp['required'] = False if 'default' in inp else True
      # ignore argument default, defer to parseInputs
      if 'default' in inp: del inp['default']
      # overwrite argument type, defer to parseInputs
      if 'type' in inp: inp['type'] = str
      p.add_argument(*dest,**inp)

  def _main(self,myInput):
    # Create mainInput from myInput (default: same).
    mainInput = self.beforeMain(*myInput.args,**myInput.kwargs)
    # Main processing step.
    mainOutput = FancyTask._main(self,mainInput)
    # Create myOutput from mainOutput (default: same).
    return self.afterMain(*mainOutput.args,**mainOutput.kwargs)

  # Convert run() input to main() input
  def beforeMain(self,*args,**kwargs):
    return FancyInput(*args,**kwargs)
  
  # Convert main() output to run() output
  def afterMain(self,*args,**kwargs):
    return FancyOutput(*args,**kwargs)
#endclass


class FancyExec(FancyModule):
  """ 
  Extended class for pipeline tasks that execute jobs outside of Python,
  with a pre-defined main method.
  """
  def getName(self):
    mainInput = self.beforeMain(*self.myInput.args,**self.myInput.kwargs)
    cmd = list(mainInput.args);
    return '<{}>'.format(cmd[0])
    
  def getCommandLine(self):
    mainInput = self.beforeMain(*self.myInput.args,**self.myInput.kwargs)
    cmd = list(mainInput.args);
    for k,v in mainInput.kwargs.items(): cmd.extend([k,v])
    return ' '.join(['{}'.format(v) for v in cmd])

  def main(self,*args,**kwargs):
    cmd = list(args);
    for k,v in kwargs.items(): cmd.extend([k,v])
    stdout = subprocess.check_output(cmd, shell=False, stderr=subprocess.STDOUT).decode('utf-8')
    self.fancyLog.attachResult(self.taskId, 'stdout',stdout.split('\n'),{}, tp='longtext')
    return FancyOutput(stdout)
    
  def afterMain(self,stdout):
    if 'outputFiles' in self.__class__.__dict__: return self.outfiles
    # If no output files have been defined, then assume that
    # outputs are stored in files with the name given by the corresponding input
    kwargs = {}
    for k,_ in self.requests.items():
      kwargs[k] = self.getInput(k)
    return FancyOutput(**kwargs)
#endclass

## Worker class used for parallel task execution.
class Worker():
  def __init__(self, jobQueue, resultQueue):
    self.jobQueue = jobQueue
    self.resultQueue = resultQueue

  def getTask(self):
    return (self.jobQueue.get(),self.resultQueue)

  @staticmethod
  def putResult(result,resultQueue):
    resultQueue.put(result)

  def run(self):
    task = False
    resultQueue = False
    while True:
      try:
        (task,resultQueue) = self.getTask()
        if task is None:
          # Poison pill means shutdown
          print('Exiting worker {}'.format(self.name))
          break
        result = (str(task),task._run())
        self.putResult(result,resultQueue)
        task = False
      except:
        msg = FancyReport.traceback()
        if task:
          title = 'Fatal error in worker {} while running task {}.'.format(self.name,task)
          task.fancyLog.attachResult(task.taskId,title,msg,{}, tp='error')
          if resultQueue:
            result = (None,(self.name,str(task)))
            self.putResult(result,resultQueue)
        else:
          title = 'Fatal error in worker {}:\n{}'.format(self.name,msg)
        FancyReport.fail(title)
#endclass

class RemoteWorker(Worker,multiprocessing.Process):
  def __init__(self, managerAddr,managerAuth):
    multiprocessing.Process.__init__(self)
    from multiprocessing.managers import BaseManager
    class ManagerProxy(BaseManager): pass
    ManagerProxy.register('getJobQueue')
    ManagerProxy.register('getResultQueue')
    print('Connecting to manager {}:{}'.format(managerAddr[0],managerAddr[1]))
    self.manager = ManagerProxy(address=managerAddr,authkey=managerAuth)
    self.manager.connect()
    self.jobQueue = self.manager.getJobQueue()

  def getTask(self):
    (task,runId) = pickle.loads(self.jobQueue.get())
    resultQueue = self.manager.getResultQueue(runId)
    return (task,resultQueue)

  @staticmethod
  def putResult(result,resultQueue):
    resultQueue.put(pickle.dumps(result))
#endclass

class WorkerThread(Worker,threading.Thread):
  def __init__(self, jobQueue, resultQueue):
    threading.Thread.__init__(self)
    Worker.__init__(self,jobQueue,resultQueue)
#endclass

class WorkerProcess(Worker,multiprocessing.Process):
  def __init__(self, jobQueue, resultQueue):
    multiprocessing.Process.__init__(self)
    Worker.__init__(self,jobQueue,resultQueue)
#endclass

## LocalPool maintains a pool of workers, either implemented as 
## processes (workerType 'P'), or threads (workerType 'T').
class LocalPool:
  def __init__(self,numWorkers,workerType='P'):
    if workerType == 'T':
      self.jobQueue = queue.Queue()
      self.resultQueue = queue.Queue()
    elif workerType == 'P':
      from multiprocessing.queues import Queue
      self.jobQueue = multiprocessing.Queue()
      self.resultQueue = multiprocessing.Queue()
    else:
      raise TypeError('Invalid workerType "{}"'.format(workerType))
    self.jobCount = 0
    self.resultCount = 0
    for w in range(numWorkers):
      if workerType == 'T':
        worker = WorkerThread(self.jobQueue, self.resultQueue)
      else:
        worker = WorkerProcess(self.jobQueue, self.resultQueue)
      worker.daemon = True
      worker.start()
      
  def getResult(self):
    return self.resultQueue.get()
#endclasss

## As an alternative to maintaining a local pool of workers, this class 
## connects to a manager that distributes jobs to remote workers.
class RemotePoolClient:
  def __init__(self,jobManager,jobAuth):
    class ManagerProxy(multiprocessing.managers.BaseManager): pass
    ManagerProxy.register('getJobQueue')
    ManagerProxy.register('getResultQueue')
    ManagerProxy.register('popResultQueue')
    self.runId = datetime.datetime.now().isoformat()
    print('Connecting to job manager {}:{} to run job {}'.format(jobManager[0],jobManager[1],self.runId))
    self.manager = ManagerProxy(address=jobManager,authkey=jobAuth)
    self.manager.connect()
    multiprocessing.current_process().authkey = jobAuth
    self.jobQueue = self.manager.getJobQueue()
    self.resultQueue = self.manager.getResultQueue(self.runId)
    self.jobCount = 0
    self.resultCount = 0
    
  def getResult(self):
    return pickle.loads(self.resultQueue.get())

  def __del__(self):
    self.manager.popResultQueue(self.runId)
#endclasss
