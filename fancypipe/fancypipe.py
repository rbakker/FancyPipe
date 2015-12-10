# FancyPipe
# ---------
# FancyPipe is a pure python package that takes care of those
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
# * running external commands
#
# To use FancyPipe effectively, you need to wrap your processing steps 
# as FancyTask classes, and define input-output connections between tasks.
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

from __future__ import print_function
from __future__ import unicode_literals

# These functions and classes get imported after "from fancypipe import *"
__all__ = [
  'assertPassword','assertBool','assertFile','assertDir','assertList','assertMultiSelect','assertDict','assertExec','assertToken',
  'assertMatch','assertInstance','assertType',
  'odict',
  'FANCYDEBUG',
  'FancyOutputFile','FancyTempFile','FancyPassword','FancyLink','FancyArgs','FancyOutput','FancyInput','FancyList','FancyDict',
  'Task','FancyTask','FancyFunc','FancyExec','FancyLog','FancyReport'
]

import os,sys
import os.path as op
import argparse, subprocess
import tempfile, datetime, json
import codecs
from collections import OrderedDict
import inspect
import re,uuid
import threading, multiprocessing, multiprocessing.queues, multiprocessing.managers
try:
  import queue
except ImportError:
  import Queue as queue
try:
  import cPickle as pickle
except ImportError:
  import pickle
import traceback
import string,StringIO, random

class odict(OrderedDict):
  def __init__(self, *keyvals):
    try:
      OrderedDict.__init__(self,*keyvals)
    except:
      OrderedDict.__init__(self)
      if keyvals and keyvals[0]:    
        if isinstance(keyvals[0],(tuple,list)):
          OrderedDict.__init__(self,keyvals)
        else:
          for i in range(0,len(keyvals),2):
            self[keyvals[i]] = keyvals[i+1]

  def extend(self,keyvals,*keys):
    for k in keys:
      self[k] = keyvals[k]
#endclass

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

def assertMultiSelect(s):
  if isinstance(s,str):
    parts = s.lstrip('[').rstrip(']').split(',')
    s = []
    for p in parts:
      p = [int(v) for v in p.split('-')]
      if len(p)>1: p = range(p[0],p[1]+1)
      s.extend(p)
    s = list(set(s))
  return assertList(s)
  

def assertDict(s):
  """ Assert that the input is a dictionary. """
  if isinstance(s,str): 
    try:
      s = json.loads(s)
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
def assertMatch(regexp,fromStart=True,decompose=True):
  def f(s):
    if fromStart: matches = re.match(regexp,s)
    else: matches = re.search(regexp,s)
    if matches: 
      if decompose: return matches.groups()
      else: return s
    raise AssertionError('String "{}" has no match for regular expression "{}".'.format(s,regexp))
  return f

# call as assertInstance(...)
def assertInstance(cls):
  def f(v):
    if isinstance(v,cls): return v
    raise AssertionError('Variable "{}" is not an instance of "{}".'.format(v,cls))
  return f

# call as assertType(...)
def assertType(tp,allow={}):
  def f(v):
    try: 
      return tp(v)
    except ValueError:
      if v in allow:
        return allow[v]
      else:
        raise AssertionError('Value "{}" cannot be converted to type "{}".'.format(v,tp))
  return f

def type_jobserver(v):
  if v:
    addr = v.split(':')
    port = int(addr[1]) if len(addr)>1 else 51423
    return (addr[0],port)
  else:
    return v
  
def FANCYDEBUG(*args,**kwargs):
  traceback = kwargs['traceback'] if 'traceback' in kwargs else 2
  frame = inspect.currentframe()
  outerframes = inspect.getouterframes(frame)
  msg = '___\n'
  try:
    msg += ''.join(['{}. {}\n'.format(i,v[4][0].strip()) for i,v in enumerate(outerframes[1:min(len(outerframes),traceback+1)])])
  except:
    pass
  msg += '\n'.join(['{}'.format(v) for v in args])+'\n'
  FancyReport.warning(msg)

def fancyRemoveFiles(files, exclude=set()):
  return set()
  
def fancyRemoveDirs(files, exclude=set()):
  return set()

## Use this class to indicate that the string represents an outputfile.
## In recycling mode, fancypipe will check whether the file can be recycled.
class FancyOutputFile(str):
  pass
#endclass


## Use this class to indicate that the string represents a temporary file.
## It will be removed when no longer needed.
class FancyTempFile(FancyOutputFile):
  pass
#endclass


## Used for output and error reporting
class FancyReport:
  rpc_id = None
  
  def __init__(self,jsonrpc2=False):
    if jsonrpc2:
      # capture output from print statements
      self.stdout0 = sys.stdout
      self.stdout1 = StringIO.StringIO()
      sys.stdout = self.stdout1 # redirect
      self.stderr0 = sys.stderr
      self.stderr1 = StringIO.StringIO()
      sys.stderr = self.stderr1 # redirect
      rand16 = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(16))
      self.rpc_id = rand16

  def uncapture(self):
    sys.stdout = self.stdout0
    sys.stderr = self.stderr0

  @staticmethod
  def warning(s): sys.stderr.write('\033[1;32m{}\033[0m\n'.format(s))
  
  @staticmethod
  def error(s): sys.stderr.write('\033[1;31m{}\033[0m\n'.format(s))

  @staticmethod
  def traceback():
    exc_type, exc_value, exc_traceback = sys.exc_info() 
    return traceback.format_exception(exc_type, exc_value, exc_traceback)

  def success(self,*args,**kwargs):
    if self.rpc_id:
      self.uncapture()
      result = {
        'jsonrpc':'2.0',
        'id':self.rpc_id,
        'result': {
          'stdout': self.stdout1.getvalue(),
          'stderr': self.stderr1.getvalue(),
          'args': FancyLog.stringifyList(args),
          'kwargs': FancyLog.stringifyDict(kwargs)
        }
      }
      indent = 2
    else:
      result = (
        FancyLog.summarizeList(args),
        FancyLog.summarizeDict(kwargs)
      )
      indent = 2
    print(json.dumps(result,indent=indent))

  def fail(self,msg='Error',code=1):
    if self.rpc_id:
      self.uncapture()
      traceback = self.traceback()
      msg = traceback.pop()
      result = {
        'jsonrpc':'2.0',
        'id': self.rpc_id,
        'error':{
          'code': code,
          'message': msg,
          'data': {
            'stdout': self.stdout1.getvalue(),
            'stderr': self.stderr1.getvalue(),
            'traceback': traceback
          }
        }
      }
      print(json.dumps(result,indent=2))
      sys.exit(0)
    else:
      result = '{}\n{}\n\n'.format(msg,json.dumps(FancyReport.traceback(),indent=2))
      FancyReport.error(result)
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
    #if hasattr(v,'tolist'): v = v.tolist()
    if isinstance(v,(tuple, list)): return FancyLog.stringifyList(v)
    if isinstance(v,(dict)): return FancyLog.stringifyDict(v)
    if isinstance(v,(int,bool,float)) or v is None: return v
    return '{}'.format(v)

  @staticmethod
  def summarizeValue(v):
    #if hasattr(v,'tolist'): v = v.tolist()
    if isinstance(v,(tuple, list)): return FancyLog.summarizeList(v)
    if isinstance(v,(dict)): return FancyLog.summarizeDict(v)
    if isinstance(v,(int,bool,float)) or v is None: return v
    return '{}'.format(v)

  @staticmethod
  def stringifyList(args):
    return [FancyLog.stringifyValue(v) for v in args]

  @staticmethod
  def summarizeList(args):
    if len(args)>32:
      args = [ args[i] for i in [0,1,2,3,-2,-1,0] ]
      args[3] = '(...)'
    return [FancyLog.summarizeValue(v) for v in args]

  @staticmethod
  def stringifyDict(kwargs):
    return { str(k):FancyLog.stringifyValue(v) for k,v in kwargs.items() }
    
  @staticmethod
  def summarizeDict(kwargs):
    items = kwargs.items()
    if len(items)>32:
      items = [ items[i] for i in [0,1,2,3,-2,-1,0] ]
      items[3] = ('(...)','(...)')
    return { str(k):FancyLog.summarizeValue(v) for k,v in items }

  def appendLog(self,s):
    lock = multiprocessing.Lock()
    lock.acquire()
    with codecs.open(self.logfile,'a',encoding='utf-8') as log:
      log.write(s)
    lock.release()
    
  # Add an entry to the logfile
  def addEntry(self, logId,cmd,args,kwargs, title=None,commandLine=None):
    loglevel = self.loglevel
    if loglevel>0:
      print('___\nLog({}) {}'.format(logId,cmd+': '+title if title else cmd))
      starttime = datetime.datetime.now()
      params = {
        'id': logId,
        'cmd': cmd,
        'timeStamp': starttime.isoformat(),
      }
    if loglevel>1:
      # prepare args for json-dumping
      if args:
        args = self.summarizeList(args)
        params['args'] = args,
      if kwargs:
        kwargs = self.summarizeDict(kwargs)
        params['kwargs'] = kwargs
      print('Arguments: {},{}'.format(json.dumps(args,indent=2),json.dumps(kwargs,indent=2)))
      if title:
        params['title'] = title
      if commandLine:
        print('Command line: {}'.format(commandLine))
        params['commandLine'] = commandLine
    if loglevel>0 and self.logfile:
      # write to logfile
      self.appendLog('LOG.push('+json.dumps(params)+')\n')
    
  def attachMessage(self,logId,msg, type=None):
    params = {
      'attachTo':logId,
      'message':msg
    }  
    print('Task {} says: {}'.format(logId,msg))
    # write to logfile
    if not self.logfile: return
    self.appendLog('LOG.push('+json.dumps(params,indent=2)+')\n')

  def attachResult(self,logId,name,args=None,kwargs=None, tp=None,worker=''):
    # write to logfile
    if not self.logfile: return
    params = {
      'attachTo':logId,
      'name':name,
      'type':tp,
      'worker':worker
    }
    if tp=='longtext':
      lineCount = len(args)
      if lineCount>12:
        logfilesdir = op.join(self.logdir,'fancylog_files')
        outfile = op.join(logfilesdir,'stdout_{}.txt'.format(logId))
        with codecs.open(outfile,"w",encoding='utf-8') as fp:
          fp.write('\n'.join(args))      
        args = args[0:6] + ['... {} more lines ...'.format(lineCount-12)] + args[-6:0]
        kwargs = {
          'name':outfile,
          'href':'file:///{}'.format(outfile)
        }
    if args: params['args'] = FancyLog.summarizeList(args)
    if kwargs: params['kwargs'] = FancyLog.summarizeDict(kwargs)
    params_json = json.dumps(params,indent=2);
    self.appendLog('LOG.push('+params_json+')\n')

  def reset(self,overwrite=False):
    if self.logfile:
      if op.isfile(self.logfile) and not overwrite:
        raise Exception('Logfile "{}" already exists, use RESET to overwrite.'.format(self.logfile))
      with codecs.open(self.logfile,'w',encoding='utf-8') as log:
        log.write('var LOG = [];\n\n')
      htmlsrc = op.join(op.dirname(__file__),'fancylog.html')
      htmltgt = op.join(self.logdir,'fancylog.html')
      if not op.isfile(htmltgt):
        import shutil
        shutil.copyfile(htmlsrc,htmltgt)
#endclass


## Used to keep track of temporary files.
class FancyClean:
  def __init__(self):
    self.files = set()
    self.dirs = set()

  def update(self,files,dirs=None):
    self.files.update(files)
    if dirs: self.dirs.update(dirs)
  
  def addNewFile(self,f,autoremove):
    if autoremove and not op.isfile(f): self.files.add(f)
        
  def addCreateDir(self,d,autoremove):
    create = []
    while not op.exists(d):
      create.append(d)
      head,tail = op.split(d)
      d = head
    for d in reversed(create):
      try:
        os.mkdir(d)
        if autoremove: self.dirs.add(d)
      except OSError: # thrown if two processes try to create dir at the same time
        pass

  def cleanup(self,exclude,taskKey):
    if (not self.files and not self.dirs): return (set(),set())
    lock = multiprocessing.Lock()
    lock.acquire()
    excludedFiles = set()
    excludedDirs = set()
    E = set()
    for path in exclude:
      E.add(op.realpath(path))
    for f in self.files:
      F = op.realpath(f)
      if F not in E:
        try:
          os.remove(f)
          print('{} deleted file "{}".'.format(taskKey,f))
        except:
          FancyReport.warning('{} tried to delete file "{}", but failed.'.format(taskKey,f))
      else:
        excludedFiles.add(f)
    for d in reversed(sorted(self.dirs)):
      D = op.realpath(d)
      try:
        os.rmdir(d)
        print('{} removed folder "{}".'.format(taskKey,d))
      except:
        excludedDirs.add(d)
    lock.release()
    return (excludedFiles,excludedDirs)
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
    self.task = task # task that will supply the value
    self.outKey = outKey # key of the above task's output
  
  def __getitem__(self,key):
    return FancyLinkItem.fromParent(self.task).setInput(self,key).requestOutput(0)
    
  def __repr__(self):
    return 'FancyLink<{}[{}]>'.format(self.task,self.outKey)
#endclass
  
  
## Base class for task input/output, consisting of list and keyword arguments
class FancyArgs:
  def __init__(self,*args,**kwargs):
    self.args = list(args)
    self.kwargs = kwargs

  def jsondump(self,fp,indent=2):
    return json.dump(FancyLog.stringifyValue((self.args,self.kwargs)),fp,indent=indent)
    
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
    for k,v in d.items():
      if isinstance(v,FancyLink): ans.add(v.task)
      elif isinstance(v,FancyArgs): ans.update( v.sources() )
    return ans

  @staticmethod
  def listTempfiles(d):
    ans = set()
    for i,v in enumerate(d):
      if isinstance(v,FancyTempFile): ans.add(str(v))
      elif isinstance(v,FancyArgs): ans.update( v.tempfiles() )
    return ans

  @staticmethod
  def dictTempfiles(d):
    ans = set()
    for k,v in d.items():
      if isinstance(v,FancyTempFile): ans.add(str(v))
      elif isinstance(v,FancyArgs): ans.update( v.tempfiles() )
    return ans

  def sources(self):
    return self.listSources(self.args) | self.dictSources(self.kwargs)

  def tempfiles(self):
    return self.listTempfiles(self.args) | self.dictTempfiles(self.kwargs)

  def __getitem__(self,key):
    return self.args[key] if isinstance(key,int) else self.kwargs[key]

  def __setitem__(self,key,val):
    if isinstance(key,(int)): self.args[key] = val
    else: self.kwargs[key] = val
    
  def run(self,parent):
    fancyTask = FancyFunc.fromParent(parent).setInput(*self.args,**self.kwargs).run()
    self.args = fancyTask.myOutput.args
    self.kwargs = fancyTask.myOutput.kwargs
#endclass
    

## Special case of FancyArgs, consisting of only a list.
class FancyList(list,FancyArgs):  
  def __init__(self,*args):
    if len(args)>1:
      raise RuntimeError('FancyList takes only one argument (a list or tuple).')
    args = args[0] if len(args)>0 else []
    list.__init__(self,args)
    self.args = self
    self.kwargs = {}

  def ready(self):
    return FancyArgs.listReady(self)

  def sources(self):
    return FancyArgs.listSources(self)

  def files(self):
    return FancyArgs.listFiles(self)
#endclass


## Special case of FancyArgs, consisting of only keyword arguments.
class FancyDict(dict,FancyArgs):
  def __init__(self,*args,**kwargs):
    if not kwargs:
      if len(args)>1:
        raise RuntimeError('FancyDict takes only one argument (a dict).')
      kwargs = args[0] if len(args)>0 else {}
    dict.__init__(self,kwargs)
    self.kwargs = self
    self.args = []

  def ready(self):
    return FancyArgs.dictReady(self)

  def sources(self):
    return FancyArgs.dictSources(self)

  def files(self):
    return FancyArgs.dictFiles(self)
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
## no tempdir, no config file
class Task():
  name = None
  numChildren = 0
  taskId = '0'
  parentKey = None
  taskManager = None
  runParallel = False
  runStatus = Task_Inactive
  ranAt = None
  init = None
  main = None
  done = None
  fancyClean = None
  fancyLog = None
  
  def __init__(self,taskId=None,parentKey=None,taskManager=None):
    if taskId: self.taskId = taskId
    if parentKey: self.parentKey   = parentKey
    self.taskManager = taskManager if taskManager else TaskManager()
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
    return cls(taskId,str(parent))
    
  def newChildId(self):
    self.numChildren += 1
    return self.taskId+'.'+str(self.numChildren)

  # Get an input.
  def getInput(self,key=None):
    return self.myInput[key]

  # Set (extend/update) the input for a subsequent call to run().
  def _prepareInput(self,args,kwargs):
    self.myInput.args.extend(args)
    self.myInput.kwargs.update(kwargs)

  # Set the input for a subsequent call to run().
  def setInput(self,*args,**kwargs):
    self._prepareInput(args,kwargs)
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
  def _unresolvedTasks(self,myArgs,includeSelf):
    def addRequest(tk,sl): # target key, source link
      srcTask,srcKey = (sl.task,sl.outKey)
      if srcTask.runStatus == Task_Completed:
        print('Task {} uses output "{}" from task {}'.format(self,srcKey,srcTask))
        myArgs[tk] = srcTask.myOutput[srcKey]    
      else:
        print('Task {} requests output "{}" from task {}'.format(self,srcKey,srcTask))
        if not srcKey in srcTask.requests: srcTask.requests[srcKey] = set()
        srcTask.requests[srcKey].add(str(self))
        srcId = '{}.{}'.format(srcTask,srcKey)
        if not srcId in self.sourceLinks: self.sourceLinks[srcId] = []
        self.sourceLinks[srcId].append((myArgs,tk))

    unresolvedTasks = { str(self):self } if includeSelf else {}
    mySources = set()
    for i,v in enumerate(myArgs.args):
      if isinstance(v,FancyLink):
        addRequest(i,v)
        mySources.add(v.task)
      elif isinstance(v,FancyArgs):
        unresolvedTasks.update( self._unresolvedTasks(v,False) )
    for k,v in myArgs.kwargs.items():
      if isinstance(v,FancyLink):
        addRequest(k,v)
        mySources.add(v.task)
      elif isinstance(v,FancyArgs):
        unresolvedTasks.update( self._unresolvedTasks(v,False) )

    for src in mySources:
      if src.runStatus == Task_Inactive:
        src.runStatus = Task_ResolvingInput
        unresolvedTasks.update( src._unresolvedTasks(src.myInput,True) )

    return unresolvedTasks

  # Fulfill requests after running (some of) the unresolved tasks.
  # sn: source task, tn: target task
  def _fulfillRequests(self,taskCache):
    affectedTasks = {}
    for sk,targets in self.requests.items():      
      srcId = '{}.{}'.format(self,sk)
      try:
        val = self.myOutput if sk is None else self.myOutput[sk]
      except KeyError: 
        raise KeyError('Task "{}" does not have the requested output "{}"'.format(self,sk))
      for tnId in targets:
        tn = taskCache[tnId]
        affectedTasks[tnId] = tn
        for (targetArgs,tk) in tn.sourceLinks[srcId]:
          targetArgs[tk] = val
    return affectedTasks
        
  def _main(self):
    if self.init:
      self.myOutput = self.init(*self.myOutput.args,**self.myOutput.kwargs)
      if not isinstance(self.myOutput,FancyArgs):
        raise RuntimeError('{}.init() must return a FancyOutput object.'.format(self))
    if self.main:
      self.myOutput = self.main(*self.myOutput.args,**self.myOutput.kwargs)
      if not isinstance(self.myOutput,FancyArgs):
        raise RuntimeError('{}.main() must return a FancyOutput object.'.format(self))
    if self.done:
      self.myOutput = self.done(*self.myOutput.args,**self.myOutput.kwargs)
      if not isinstance(self.myOutput,FancyArgs):
        raise RuntimeError('{}.done() must return a FancyOutput object.'.format(self))
    self.runStatus = Task_ResolvingOutput

  # call when task is completed
  def cleanup(self,taskCache):
    if not self.fancyClean: return
    (excludedFiles,excludedDirs) = self.fancyClean.cleanup(exclude=self.myOutput.tempfiles(),taskKey=str(self))
    if self.parentKey and (excludedFiles or excludedDirs):
      # Inherit tempfiles and tempdirs that could not yet be removed.
      try:
        parent = taskCache[self.parentKey]
        if not parent.fancyClean: parent.fancyClean = FancyClean()
        parent.fancyClean.update(excludedFiles,excludedDirs)
      except KeyError:
        FancyReport.warning('Key "{}" not found in task cache while cleaning up {}.\nThe task cache contains {}.'.format(self.parentKey,self,self.taskManager.taskCache.values()))

  def runningTask(self):
    rKey = self.taskManager.runningTask
    tKey = str(self)
    return tKey if not rKey or rKey == tKey else '{}, called by {}'.format(rKey,tKey)
    
  def logOutput(self):
    pass
    
  # Run this task and return its output
  # If set, "requests" must contains a tuple with names of requested output parameters
  def run(self,requests=None):
    
    if self.parentKey:
      self.taskManager.run(self)
    else:
      manager = self.taskManager
      try: 
        print('Running task {} in {} mode.'.format(self,manager.mode))
        manager.run(self)
        self.logOutput()
        manager.report.success(*self.myOutput.args,**self.myOutput.kwargs)
      except subprocess.CalledProcessError as e:
        manager.report.fail('Fatal error in task {}:\n{}.'.format(self.runningTask(),e.output))
      except:
        manager.report.fail('Fatal error in task {}.'.format(self.runningTask()))    
    return self
#endclass


class ArgParse:
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
    return kwargs

  @classmethod
  def parsedArgs(cls,cmdArgs,fancyConfig=None,customArgs={}):
    configArgs = fancyConfig.classDefaults(cls.__name__) if fancyConfig else {}
    parsedArgs = cls._parseInputs(cmdArgs,configArgs)
    if customArgs:
      for k in parsedArgs: 
        if k in customArgs: parsedArgs[k] = customArgs[k]
    return parsedArgs
#endclass


## Fancy task class, has all features except calling the task from the command line.
class FancyTask(Task,ArgParse):
  title = None
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
      help='Whether to keep intermediate results. Default: False'
    ))
  ])
  outfiles = None # outputs that are returned as files
  
  def __init__(self,tempdir,fancyConfig,fancyLog,taskId,parentKey=None,taskManager=None,autoremove=True,parsedArgs={}):
    Task.__init__(self,taskId,parentKey,taskManager)
    self._tempdir     = tempdir
    self.autoremove   = autoremove
    self.recycle      = not autoremove
    self.fancyLog     = assertInstance(FancyLog)(fancyLog)
    self.fancyConfig  = assertInstance(FancyConfig)(fancyConfig)
    
    # Prepare inputs from defaults and configuration file.
    self._prepareInput([],parsedArgs)
    
  @classmethod
  def fromCommandLine(cls,**customArgs):
    cmdArgs = cls._getParser().parse_args().__dict__;
    parsedArgs = FancyTask.parsedArgs(cmdArgs,customArgs=customArgs)
    taskId = parsedArgs['taskid'] if parsedArgs['taskid'] else '0'
    logReset = False
    if taskId == 'INIT' or taskId == 'RESET': 
      logReset =True
      logOverwrite = (taskId == 'RESET')
      taskId = '0'
    tempdir = parsedArgs['tempdir'] if parsedArgs['tempdir'] else op.join(tempfile.gettempdir(),cls.tempsubdir(taskId))
    logdir = parsedArgs['logdir'] if parsedArgs['logdir'] else tempdir
    logLevel = parsedArgs['loglevel']
    fancyLog = FancyLog(logdir,logLevel)
    if logReset:
      fancyLog.reset(overwrite=logOverwrite)
    noCleanup = parsedArgs['nocleanup']
    configFile = parsedArgs['configfile']
    fancyConfig = FancyConfig.fromFile(configFile)
    # TaskManager setup.
    parsedArgs = TaskManager.parsedArgs(cmdArgs,customArgs=customArgs)
    jobServer = parsedArgs['jobserver']
    jobAuth = parsedArgs['jobauth']
    try: numWorkers = int(parsedArgs['numworkers'])
    except: numWorkers = multiprocessing.cpu_count()
    workerType = parsedArgs['workertype'][0]
    jsonrpc2 = parsedArgs['jsonrpc2']
    taskManager = TaskManager(jobServer,jobAuth,numWorkers,workerType,jsonrpc2)
    parsedArgs = cls.parsedArgs(cmdArgs,fancyConfig=fancyConfig,customArgs=customArgs)
    # Instantiate.
    self = cls(**{
      'tempdir': tempdir,
      'fancyConfig': fancyConfig,
      'fancyLog': fancyLog,
      'taskId': taskId,
      'taskManager': taskManager,
      'autoremove': not noCleanup,
      'parsedArgs': parsedArgs
    })
    return self

  @staticmethod
  def _extendParser(p,inputs):
    for key in inputs:
      inp = inputs[key].copy()
      short = inp.pop('short') if 'short' in inp else key.lower()
      positional = inp.pop('positional') if 'positional' in inp else False
      dest = [key] if positional else ['-{}'.format(short),'--{}'.format(key)]
      if not positional: 
        inp['required'] = False if 'default' in inp else True
      # ignore argument default, defer to parseInputs
      if 'default' in inp: del inp['default']
      # overwrite argument type, defer to parseInputs
      if 'type' in inp: inp['type'] = str
      p.add_argument(*dest,**inp)
    
  @classmethod
  def _getParser(cls):
    p = argparse.ArgumentParser(
      description=cls.description if cls.description else cls.title,
      formatter_class=argparse.ArgumentDefaultsHelpFormatter,
      argument_default=argparse.SUPPRESS,
      conflict_handler='resolve'
    )
    g = p.add_argument_group('FancyTask arguments')
    cls._extendParser(g,FancyTask.inputs)
    g = p.add_argument_group('TaskManager arguments')
    cls._extendParser(g,TaskManager.inputs)
    g = p.add_argument_group('{} module arguments'.format(cls.__name__))
    cls._extendParser(g,cls.inputs)
    return p

  @classmethod
  def fromParent(cls,parent):
    taskId = parent.newChildId()
    tempdir = op.join(parent._tempdir,cls.tempsubdir(parent.numChildren))
    fancyLog = FancyLog.fromParent(parent)
    fancyConfig = FancyConfig.fromParent(parent)
    parsedArgs = cls._parseInputs([],fancyConfig.classDefaults(cls.__name__))
    args = {
      'taskId': taskId,
      'parentKey': str(parent),
      'taskManager': parent.taskManager,
      'tempdir': tempdir,
      'fancyLog': fancyLog,
      'fancyConfig': fancyConfig,
      'parsedArgs': parsedArgs
    }
    if hasattr(parent,'autoremove'): args['autoremove'] = parent.autoremove
    # Instantiate.
    self = cls(**args)
    return self
  
  @classmethod
  def tempsubdir(cls,taskId):
    return '{}_{}'.format(taskId,cls.__name__)

  # tempdir(subdir) returns tempdir/subdir, and registers it for cleanup after running main()
  def tempdir(self,subdir=None,autoremove=None):
    d = self._tempdir
    if subdir: 
      d = op.join(d,subdir)
    autoremove = autoremove if autoremove is not None else self.autoremove
    if not self.fancyClean:
      self.fancyClean = FancyClean()
    self.fancyClean.addCreateDir(d,autoremove)
    return d

  # tempfile(f) returns tempdir/f, and registers it for cleanup after running main()
  def tempfile(self,f=None,ext='',autoremove=None):
    if f is None:
      rand8 = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(8))      
      f = '{}_{}{}'.format(self.__class__.__name__.lower(),rand8,ext)
    f = op.join(self.tempdir(),f)
    autoremove = autoremove if autoremove is not None else self.autoremove
    if not self.fancyClean:
      self.fancyClean = FancyClean()
    self.fancyClean.addNewFile(f,autoremove)
    return FancyTempFile(f)
  
  def getCommandLine(self):
    #return 'python runtask.py {} -args={} -kwargs={}'.format(self.__class__.__name__,json.dumps(FancyLog.stringifyList(self.myInput.args)),json.dumps(FancyLog.stringifyDict(self.myInput.kwargs)))
    return 'python runtask.py {} [...]'.format(self.__class__.__name__)

  # Flag outputs that will be returned as files, so that they can be recycled.
  def outputFiles(self):
    # return self.myInput.files()
    return FancyOutput(
      *[ v for v in self.myInput.args if isinstance(v,FancyOutputFile) ],
      **{ k:v for k,v in self.myInput.kwargs.items() if isinstance(v,FancyOutputFile) }
    )

  # Set the input for a subsequent call to run(); register output files for recycling.
  def setInput(self,*args,**kwargs):
    Task._prepareInput(self,args,kwargs)
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
      if self.recycle:
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
    self.fancyLog.attachResult(self.taskId, 'output',self.myOutput.args,self.myOutput.kwargs, worker=self.ranAt)
#endclass


class FancyLinkItem(FancyTask):
  def init(self,value,key):
    return FancyArgs(value[key])
#endclass
    

class FancyFunc(FancyTask):
  def setFunc(self,func,runLocal=True):
    if runLocal: self.init = func
    else: self.main = func
    return self
#endclass


class FancyExec(FancyTask):
  """ 
  Extended class for tasks that execute jobs outside of Python,
  with a pre-defined main method.
  """
  runParallel = True
  myEnv = None
  myCwd = None
  myProg = None
  stdout = ''
  
  def getCommandLine(self):
    try:
      mainInput = self.init(*self.myInput.args,**self.myInput.kwargs)
      cmd = list(mainInput.args);
      for k,v in mainInput.kwargs.items(): cmd.extend([k,v])
      return ' '.join(['{}'.format(v) for v in cmd])
    except:
      return FancyTask.getCommandLine(self)

  # set environment variables
  def setEnv(self,env):
    self.myEnv = env
    return self
    
  # set current working directory
  def setCwd(self,cwd):
    self.myCwd = cwd
    return self
    
  def setProg(self,prog):
    self.myProg = prog
    self.name = '<{}>'.format(prog)
    return self
  
  def setTitle(self,title):
    self.title = title
    return self

  def main(self,*args,**kwargs):
    cmd = [self.myProg] if self.myProg else []
    cmd.extend(args);
    for k,v in kwargs.items(): cmd.extend([k,v])
    opts = dict(shell=False, stderr=subprocess.STDOUT)
    if self.myCwd: opts['cwd'] = self.myCwd
    if self.myEnv: opts['env'] = self.myEnv
    self.stdout = subprocess.check_output(cmd, **opts).decode('utf-8')
    self.fancyLog.attachResult(self.taskId, 'stdout',self.stdout.split('\n'),{}, tp='longtext', worker=self.ranAt)
    if 'outputFiles' in self.__class__.__dict__: return self.outfiles
    # If no output files have been defined, then echo the inputs, this is useful when an input represents the name of an output file.
    return self.myInput
#endclass


#class FancyFunc(Task):
#  def init(func,*args,**kwargs):
#    return FancyArgs(func(*args,**kwargs))
##endclass

## Worker class used for parallel task execution.
class Worker():
  def __init__(self, jobQueue, resultQueue):
    self.jobQueue = jobQueue
    self.resultQueue = resultQueue

  def getTask(self):
    return self.jobQueue.get()
    
  def putResult(self,result,runId):
    self.resultQueue.put(result)

  def run(self):
    task = None
    runId = None
    while True:
      try:
        (task,runId) = self.getTask()
        if task is None:
          # Poison pill means shutdown
          print('Exiting worker {}'.format(self.name))
          break
        # task.myOutput is initialized to myInput or to init();
        task.taskManager.runningTask = str(task)
        task._main()
        self.putResult((str(task),task.myOutput,task.fancyClean,self.name),runId)
        task = False
      except:
        msg = FancyReport.traceback()
        if task:
          #title = 'Fatal error in worker {} while running task {}.'.format(self.name,task.runningTask())
          #task.fancyLog.attachResult(task.taskId,title,msg,{}, tp='error',worker=self.name)
          result = (None,(task.runningTask(),msg),None,self.name)
        else:
          result = (None,(None,msg),None,self.name)
        self.putResult(result,runId)
        #FancyReport.fail(title)
#endclass


class RemoteWorker(Worker,multiprocessing.Process):
  def __init__(self, managerAddr,managerAuth):
    multiprocessing.Process.__init__(self)
    from multiprocessing.managers import BaseManager
    class ManagerProxy(BaseManager): pass
    ManagerProxy.register('getJobQueue')
    ManagerProxy.register('getResultQueue')
    ManagerProxy.register('getControlQueue')
    print('Connecting to job server {}:{}'.format(managerAddr[0],managerAddr[1]))
    self.manager = ManagerProxy(address=managerAddr,authkey=managerAuth)
    self.manager.connect()
    self.jobQueue = self.manager.getJobQueue()

  def getTask(self):
    return pickle.loads(self.jobQueue.get())
    
  def putResult(self,result,runId):
    resultQueue = self.manager.getResultQueue(runId)
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
    
    
## Maintains a pool of worker threads for workerType T,
## or a pool of worker processes for workerType P.
class LocalPool():
  def __init__(self,numWorkers,workerType):
    self.queueClass = queue.Queue if workerType=='T' else multiprocessing.Queue
    workerClass = WorkerThread if workerType=='T' else WorkerProcess
    self.jobQueue = self.queueClass()
    self.resultQueue = self.queueClass()
    for w in range(numWorkers):
      worker = workerClass(self.jobQueue,self.resultQueue)
      worker.daemon = True
      worker.start()

  def putTask(self,task):
    self.jobQueue.put((task,None))
    
  def getResult(self):
    return self.resultQueue.get()
#endclass


## Connects to a remote pool of workers, started by fancymanager.py,
class RemotePool():
  resultQueue = None
  
  def __init__(self,jobServer,jobAuth):
    self.runId = uuid.uuid4()
    class ManagerProxy(multiprocessing.managers.BaseManager): pass
    ManagerProxy.register('getJobQueue')
    ManagerProxy.register('getResultQueue')
    ManagerProxy.register('popResultQueue')
    ManagerProxy.register('getControlQueue')
    print('Connecting to job server {}:{}.'.format(jobServer[0],jobServer[1]))
    self.manager = ManagerProxy(address=jobServer,authkey=jobAuth)
    self.manager.connect()
    multiprocessing.current_process().authkey = jobAuth
    self.jobQueue = self.manager.getJobQueue()
    self.resultQueue = self.manager.getResultQueue(self.runId)
    
  def putTask(self,task):
    self.jobQueue.put(pickle.dumps((task,self.runId)))
  
  def getResult(self):
    return pickle.loads(self.resultQueue.get())

  def getControlQueue(self):
    return selfmanager.getControlQueue(self.runId)

  def __del__(self):
    if self.resultQueue: self.manager.popResultQueue(self.runId)
#endclass
  

# Connects to a local pool of workers to run tasks in parallel.
class TaskManager(ArgParse):
  # testing, inputs are not yet used.
  inputs = odict([
    ('workertype', dict( type=assertMatch('([pPtT])'), default=('P'),
      help='Either "T" or "P": T uses multi-threading while P uses multi-processing. Use T when the pipeline mainly involves calls to external programs; use P when Python itself is used for number-crunching.'
    )),
    ('numworkers', dict( type=assertType(int), default='auto',
      help='Number of parallel workers. Default: number of CPUs.'
    )),
    ('jobserver', dict( type=type_jobserver, default=False,
      help='Address (ip-address:port) of job server started with "python fancyserver.py -auth=abracadabra"'
    )),
    ('jobauth', dict( default='abracadabra',
      help='Authorization key for submitting jobs to the job manager. Default: abracadabra.'
    )),
    ('jsonrpc2',dict( action='store_true', default=False,
      help='Capture output and return result in jsonrpc2 format.')
    )
  ])
  workerPool = None
  mode = 'serial'
  
  def __init__(self,jobServer=None,jobAuth='',numWorkers=0,workerType='P',jsonrpc2=False):
    self.jobServer = jobServer
    self.jobAuth = jobAuth
    self.numWorkers = numWorkers
    self.workerType = workerType
    if jobServer:
      self.workerPool = RemotePool(jobServer,jobAuth)
      self.mode = 'distributed'
    elif numWorkers>0:
      self.workerPool = LocalPool(numWorkers,workerType)
      self.mode = 'parallel'
    self.report = FancyReport(jsonrpc2)
    
    self.taskCache = {}
    self.runningTask = None
    self.jobCount = 0
    
  # when a node gets pickled, its TaskManager is irrelevant
  def __getstate__(self):
    return None;
    
  def __setstate__(self,state):
    self.__init__()

  def nextReady(self):
    (taskKey,myOutput,fancyClean,workerName) = self.workerPool.getResult()
    if taskKey is None:
      taskKey,msg = myOutput
      raise RuntimeError('Worker {} encountered an error while running task {}.'.format(workerName,taskKey))
    self.jobCount -= 1
    task = self.taskCache[taskKey]
    task.myOutput = myOutput
    if fancyClean:
      task.fancyClean = fancyClean
    task.runStatus = Task_ResolvingOutput    
    task.ranAt = workerName
    return task

  def submit(self,tasks):
    self.taskCache.update(tasks)
    for key,task in tasks.items():
      if task.runStatus == Task_ResolvingInput and task.myInput.ready():
        self.runningTask = str(task)
        self.runStatus = Task_Submitted
        # Initialize task
        if task.fancyLog: task.logEntry()
        task.myOutput = task.myInput
        # Run task
        if self.workerPool and task.runParallel:
          # Queue task, run later
          self.workerPool.putTask(task)
          self.jobCount += 1
        else:
          # Run immediately and submit consecutive tasks
          task._main()
          self.submit( task._unresolvedTasks(task.myOutput,True) )
      elif task.runStatus == Task_ResolvingOutput and task.myOutput.ready():
        task = self.taskCache.pop(key)
        task.runStatus = Task_Completed
        task.cleanup(self.taskCache)
        affectedTasks = task._fulfillRequests(self.taskCache)
        self.submit(affectedTasks)

  def run(self,task):
    if task.runStatus == Task_Inactive:
      task.runStatus = Task_ResolvingInput
      self.submit( task._unresolvedTasks(task.myInput,True) )
    self.runningTask = str(task)
    while task.runStatus is not Task_Completed:
      if self.jobCount == 0:
        tasklist = { k:v.runStatus for k,v in self.taskCache.items() }
        raise RuntimeError('Task {} is waiting for tasks {} to complete, but no jobs are running.'.format(runningTask,tasklist))
      subTask = self.nextReady()
      self.submit( subTask._unresolvedTasks( subTask.myOutput,True ) )
#endclass
