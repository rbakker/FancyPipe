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
# connections between nodes.
#
# FancyPipe requires Python 3 or higher, because of this bug:
# http://bugs.python.org/issue7105
#

import __future__
import os,sys
import os.path as op
import argparse, subprocess
import tempfile, datetime, json
from collections import OrderedDict
import re
import multiprocessing
try:
    import queue
except ImportError:
    import Queue as queue

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
  def __init__(self, logdir=None,loglevel=1):
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
    with open(self.logfile,'a') as log:
      log.write('LOG.push('+json.dumps(params,indent=2)+')\n')
    
  def attachMessage(self,logId,msg, type=None):
    params = {
      'attachTo':logId,
      'message':msg
    }  
    print('Node {} says: {}'.format(logId,msg))
    # write to logfile
    if not self.logfile: return
    with open(self.logfile,'a') as log:
      log.write('LOG.push('+json.dumps(params,indent=2)+')\n')

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
    with open(self.logfile,'a') as log:
      log.write('LOG.push('+params_json+')\n')

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
      od = OrderedDict()
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
      config = OrderedDict()
      config[root.tag] = FancyConfig.etree_to_odict(root)
      return cls(config)
    elif ext.lower() == '.json':
      return cls(json.load(configFile, object_pairs_hook=OrderedDict))
    else:
      raise RuntimeError('Configuration file "{}" has an unrecognized format.'.format(configFile))

  @classmethod
  def fromParent(cls,parentNode):
    config = parentNode.fancyConfig.config.copy()
    parentClass = parentNode.__class__.__name__
    if parentClass in config:
      # overwrite defaults with className-specific defaults
      for k,v in config[parentClass].items(): config[k] = v;
      config.pop(parentClass)
    return cls(config)
    
  def classDefaults(self,nodeClass):
    return self.config[nodeClass] if nodeClass in self.config else {}
#endclass


## Indicates an outgoing link of output <outKey> of node <node>.
class FancyLink:
  def __init__(self,node,outKey):
    self.node = node # list of FancyTasks
    self.outKey = outKey # output key
  
  def __repr__(self):
    return 'FancyLink<{}[{}]>'.format(self.node,self.outKey)
#endclass
  

## Base class for node input/output, consisting of list and keyword arguments
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
      if isinstance(v,FancyLink): ans.add(v.node)
      elif isinstance(v,FancyArgs): ans.update( v.sources() )
    return ans

  @staticmethod
  def dictSources(d):
    ans = set()
    for k,v in self.items():
      if isinstance(v,FancyLink): ans.add(v.node)
      elif isinstance(v,FancyArgs): ans.update( v.sources() )
    return ans

  def sources(self):
    return self.listSources(self.args) | self.dictSources(self.kwargs)

  def __getitem__(self,key):
    return self.args[key] if isinstance(key,int) else self.kwargs[key]

  def __setitem__(self,key,val):
    if isinstance(key,(int)): self.args[key] = val
    else: self.kwargs[key] = val
    

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


## Special case of FancyArgs, consisting of only keyword arguments, with preserved order.
class FancyOrderedDict(OrderedDict,FancyDict):
  def __init__(self,kwargs):
    OrderedDict.__init__(self,kwargs)
    self.kwargs = self
    self.args = []
#endclass


## FancyInput and FancyOutput have no special meaning, but improve
## readability when used to represent the input and output of a node.
FancyInput  = FancyArgs
FancyOutput = FancyArgs

## Constants to indicate the run-status of a node
FancyTask_Inactive = 0
FancyTask_ResolvingInput = 1
FancyTask_Submitted = 2
FancyTask_ResolvingOutput = 3
FancyTask_Completed = 4

## Base class for all pipeline nodes, has all features except 
## calling the node from the command line.
class FancyTask:
  name = None
  title = None
  description = None
  inputs = {
    'tempdir':{'default':None,
      'help':'Temp directory, to store intermediate results. Default: system tempdir.'
    },
    'logdir':{'default':None,
    'help':'Log directory, to store logfile (fancylog.js + fancylog.html) and store attachments. Default: tempdir.'
    },
    'loglevel':{'type':int, 'default':3,
      'help':'Log level, 1 for entries, 2 for standard output, 4 for data, 8 for extra. Default: 3.'
    },
    'taskid':{'default':'0',
      'help':'Hierarchical identifier of the task (e.g. "3.4.7"). Use taskid=INIT to create a new empty log (error if it exists), and taskid=RESET to reset (overwrite) an existing log.'
    },
    'configfile':{'default':None,
      'help':'Configuration file (XML or JSON), to read default parameters from. Default: None.'
    },
    'nocleanup':{'action':'store_true', 'default':False,
      'help':'Whether to cleanup intermediate results. Default: Do cleanup'
    }
  }
  runStatus = FancyTask_Inactive
  
  def __init__(self,tempdir,fancyClean=None,fancyConfig=None,fancyLog=None,taskId='0',cmdArgs={}):
    self._tempdir    = tempdir
    self.fancyClean  = fancyClean if fancyClean else FancyClean()
    self.fancyConfig = fancyConfig if fancyConfig else FancyConfig()
    self.fancyLog    = fancyLog if fancyLog else FancyLog()
    self.taskId      = taskId
    self.numChildren = 0
    self.doRecycle   = self.fancyClean.noCleanup
    self.doParallel  = True
    self.myInput     = FancyInput()
    self.myOutput    = FancyOutput()
    self.outfiles    = None # outputs that are returned as files
    self.requests    = {} # for each sourceKey, a list of target node ids to send the result to
    self.sourceLinks = {} # for each sourceNode.sourceKey a list of (targetArgs,targetKey) pairs that this node receives data from
    self.runStatus   = FancyTask_Inactive
    
    # Prepare inputs from defaults and configuration file.
    self.prepareInput([],self._parseInputs(cmdArgs,self.fancyConfig.classDefaults(self.__class__.__name__)))
  
  def __del__(self):
    print('Cleaning up task {}'.format(self))

  def __repr__(self):
    return "%s[%s]" % (self.getName(), self.taskId)

  @classmethod
  def fromParent(cls,parent,title=None):
    noCleanup = parent.fancyClean.noCleanup
    taskId = parent.newChildId()
    tempdir = op.join(parent._tempdir,cls.tempsubdir(parent.numChildren))
    
    args = {
      'tempdir': tempdir,
      'fancyClean': FancyClean(noCleanup),
      'fancyConfig': FancyConfig.fromParent(parent),
      'fancyLog': parent.fancyLog,
      'taskId': taskId
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
    # positional arguments are ignored
    kwargs = {}
    # first loop: command line arguments
    remaining = cls.inputs.copy()
    for key,inp in cls.inputs.items():
      # typecast
      tp = inp['type'] if 'type' in inp else str
      if key in raw:
        kwargs[key] = tp(raw[key])
        remaining.pop(key)
    # second loop: configuration file arguments
    inputs = remaining.copy()
    for key,inp in inputs.items():
      # typecast
      tp = inp['type'] if 'type' in inp else str
      if key in cfg:
        kwargs[key] = tp(cfg[key])
        remaining.pop(key)
    # final loop: default arguments
    inputs = remaining
    for key,inp in inputs.items():
      if 'default' in inp:
        if hasattr(inp['default'],'__call__'):
          kwargs[key] = inp['default'](kwargs)
        else:
          kwargs[key] = inp['default']
      #else:
      #  raise ValueError('No value supplied for input argument {} {} {}'.format(key,raw,cfg))
    return kwargs
  
  @classmethod
  def tempsubdir(cls,taskId):
    return '{}_{}'.format(taskId,cls.__name__)

  def newChildId(self):
    self.numChildren += 1
    return self.taskId+'.'+str(self.numChildren)

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
  
  def getName(self):
    return self.name if self.name else self.__class__.__name__
    
  def getCommandLine(self):
    return 'python runmodule.py {} [...]'.format(self.__class__.__name__)

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
    self.outfiles = self.outputFiles()
    if not isinstance(self.outfiles,FancyArgs):
      raise TypeError('The method outputFiles() of node {} must return a FancyOutput(...) object.'.format(self))
    return self

  # Flag outputs that will be returned as files, so that they can be recycled.
  def outputFiles(self):
    return FancyOutput(
      *[ v for v in self.myInput.args if isinstance(v,FancyOutputFile) ],
      **{ k:v for k,v in self.myInput.kwargs.items() if isinstance(v,FancyOutputFile) }
    )

  # Generate a link that requests output, use this link only 
  # to return as output of a node, or to set as input to another node.
  def requestOutput(self,key):
    try:
      return self.myOutput[key]
    except:
      if self.doRecycle:
        try:
          f = self.outfiles[key]
          if op.isfile(f): 
            print('Recycling file "{}" in node {}'.format(f,self))
            return f
        except (KeyError,IndexError):
          pass          
      return FancyLink(self,key)

  # Get an output, after calling run().
  def getOutput(self,key=None):
    if not self.myOutput.ready():
      raise RuntimeError('The output of node {} contains unresolved values. Use requestOutput() instead, or run the node first.'.format(self))
    return self.myOutput[key]

  # Initialize requests for output from upstream nodes, called when 
  # myInput or myOutput of a node becomes available.
  def initRequests(self,myArgs):
    def addRequest(tk,sl): # target key, source link
      sn,sk = (sl.node,sl.outKey)
      print('Node {} requests output "{}" from node {}'.format(self,sk,sn))
      if not sk in sn.requests: sn.requests[sk] = set()
      sn.requests[sk].add(str(self))
      srcId = '{}.{}'.format(sn,sk)
      if not srcId in self.sourceLinks: self.sourceLinks[srcId] = []
      self.sourceLinks[srcId].append((myArgs,tk))

    unresolvedNodes = { str(self):self }
    mySources = set()
    for i,v in enumerate(myArgs.args):
      if isinstance(v,FancyLink): 
        addRequest(i,v)
        mySources.add(v.node)
      elif isinstance(v,FancyArgs): 
        unresolvedNodes.update( self.initRequests(v) )
    for k,v in myArgs.kwargs.items():
      if isinstance(v,FancyLink): 
        addRequest(k,v)
        mySources.add(v.node)
      elif isinstance(v,FancyArgs): 
        unresolvedNodes.update( self.initRequests(v) )

    for src in mySources:
      if src.runStatus == FancyTask_Inactive:
        src.runStatus = FancyTask_ResolvingInput
        unresolvedNodes.update( src.initRequests(src.myInput) )
    
    return unresolvedNodes

  # Fulfill requests after running (some of) the unresolved nodes.
  # sn: source node, tn: target node
  def fulfillRequests(self,nodeCache):
    affectedNodes = {}
    for sk,targets in self.requests.items():      
      srcId = '{}.{}'.format(self,sk)
      try:
        val = self.myOutput if sk is None else self.myOutput.args[sk] if isinstance(sk,int) else self.myOutput.kwargs[sk]
      except KeyError: 
        raise KeyError('Node "{}" does not have the requested output "{}"'.format(self,key))
      for tnId in targets:
        tn = nodeCache[tnId]
        affectedNodes[tnId] = tn
        for (targetArgs,tk) in tn.sourceLinks[srcId]:
          targetArgs[tk] = val
    return affectedNodes
        
  def resolveAffected(self,affectedNodes,nodeCache,jobQueue,fancyMP):
    nodeCache.update(affectedNodes)
    for taskName,node in affectedNodes.items():
      if node.runStatus == FancyTask_ResolvingInput and node.myInput.ready():
        node.runStatus = FancyTask_Submitted
        if fancyMP: 
          jobQueue.put(node)
          fancyMP.jobCount += 1
        else:
          jobQueue.put(str(node))
      elif node.runStatus == FancyTask_ResolvingOutput and node.myOutput.ready():
        node.runStatus = FancyTask_Completed
        nodeCache.pop(taskName)
        affectedChildren = node.fulfillRequests(nodeCache)
        node.resolveAffected(affectedChildren,nodeCache,jobQueue,fancyMP)
    
  def logOutput(self):
    self.fancyLog.attachResult(self.taskId, 'output',self.myOutput.args,self.myOutput.kwargs)

  def resolveOutput(self,jobQueue,fancyMP=None):
    # Deal with unresolved links in the output.
    nodeCache = {}
    affectedNodes = self.initRequests(self.myOutput)
    self.resolveAffected(affectedNodes,nodeCache,jobQueue,fancyMP)
    while self.runStatus is not FancyTask_Completed:
      if fancyMP:
        # parallel execution, using MultiProcessing
        if fancyMP.jobCount == 0:
          raise RuntimeError('Waiting for output but no jobs are running.')
        (srcId,myOutput) = fancyMP.resultQueue.get()
        fancyMP.jobCount -= 1
        if srcId is None:
          workerName,srcId = myOutput
          raise RuntimeError('Worker {} encountered an error while running node {}.'.format(workerName,srcId))
        src = nodeCache[srcId]
        src.myOutput = myOutput
      else:
        # serial execution
        srcId = jobQueue.get()
        src = nodeCache[srcId]
        src.myOutput = src._run()
      
      src.runStatus = FancyTask_ResolvingOutput
      affectedNodes = src.initRequests(src.myOutput)
      self.resolveAffected(affectedNodes,nodeCache,jobQueue,fancyMP)

  # Run this node and return its output: outer loop.
  def run(self):
    try:
      self.myOutput = self._run()
      self.runStatus = FancyTask_ResolvingOutput
      if not self.myOutput.ready():
        # resolve output dependencies
        isWorker =  hasattr(multiprocessing.current_process(),'jobQueue')
        if self.doParallel and not isWorker:
          print('Running node {} in parallel mode.'.format(self))
          fancyMP = FancyMP()
          self.resolveOutput(fancyMP.jobQueue,fancyMP)
        else:
          print('Running node {} in serial mode.'.format(self))
          self.resolveOutput(queue.Queue())

      self.logOutput()
      FancyReport.success(*self.myOutput.args,**self.myOutput.kwargs)
      return self
    except:
      msg = FancyReport.traceback()
      title = 'Fatal error in node {}.'.format(self)
      self.fancyLog.attachResult(self.taskId,title,msg,{}, tp='error')
      FancyReport.fail(title)

  # Main processing step, to be implemented by the derived class.
  def main(self,*args,**kwargs):
    raise NotImplementedError

  def _main(self,myInput):
    # Main processing step.
    return self.main(*myInput.args,**myInput.kwargs)

  def _run(self):
    # Add entry to log/console.
    name = self.getName()
    commandLine = self.getCommandLine()
    self.fancyLog.addEntry(self.taskId, name,self.myInput.args,self.myInput.kwargs, title=self.title,commandLine=commandLine)
    myOutput = self._main(self.myInput)
    if not isinstance(myOutput,FancyArgs):
      raise RuntimeError('The function main() of node {} must return a FancyOutput(...) object.'.format(self))
    return myOutput
#endclass


## Extended class for pipeline nodes, adds support for calling the node
## from the command line.
class FancyModule(FancyTask):
  inputs = {}
  
  @classmethod
  def fromCommandLine(cls):
    cmdArgs = cls._getParser().parse_args().__dict__;
    kwargs = FancyTask._parseInputs(cmdArgs,{})
    taskId = kwargs['taskid'] if kwargs['taskid'] else '0'
    if taskId == 'INIT' or taskId == 'RESET': 
      logReset =True
      logOverwrite = (taskId == 'RESET')
      taskId = '0'
    tempdir = kwargs['tempdir'] if kwargs['tempdir'] else op.join(tempfile.gettempdir(),cls.tempsubdir(taskId))
    logdir = kwargs['logdir'] if kwargs['logdir'] else tempdir
    loglevel = kwargs['loglevel']
    fancyLog = FancyLog(logdir,loglevel)
    if logReset:
      fancyLog.reset(overwrite=logOverwrite)
    configfile = kwargs['configfile']
    kwargs = {
      'tempdir': tempdir,
      'fancyClean': FancyClean(kwargs['nocleanup']),
      'fancyConfig': FancyConfig.fromFile(configfile),
      'fancyLog': fancyLog,
      'taskId': taskId,
      'cmdArgs': cmdArgs
    }
    # Instantiate.
    return cls(**kwargs)

  @classmethod
  def _getParser(cls):
    p = argparse.ArgumentParser(
      description=cls.description if cls.description else cls.title,
      formatter_class=argparse.RawTextHelpFormatter,
      argument_default=argparse.SUPPRESS
    )
    cls._extendParser(p,FancyTask.inputs)
    cls._extendParser(p,cls.inputs)
    return p

  @classmethod
  def _extendParser(cls,p,inputs):
    for key,inp in inputs.items():
      inp = inp.copy()
      dest = ['-{}'.format(key),'--{}'.format(key)]
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
  Extended class for pipeline nodes that execute jobs outside of Python,
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


## Worker class used for parallel node execution.
class FancyWorker(multiprocessing.Process):
  def __init__(self, jobQueue, resultQueue):
    multiprocessing.Process.__init__(self)
    self.jobQueue = jobQueue
    self.resultQueue = resultQueue

  def run(self):
    node = False
    while True:
      try:
        node = self.jobQueue.get()
        if node is None:
          # Poison pill means shutdown
          print('Exiting worker {}'.format(self.name))
          break
        myOutput = node._run()
        self.resultQueue.put((str(node),myOutput))
        node = False
      except:
        msg = FancyReport.traceback()
        if node:
          title = 'Fatal error in worker {} while running node {}.'.format(self.name,node)
          node.fancyLog.attachResult(node.taskId,title,msg,{}, tp='error')
          self.resultQueue.put((None,(self.name,str(node))))
        else:
          title = 'Fatal error in worker {}:\n{}'.format(self.name,msg)
        FancyReport.fail(title)
    return
#endclass


## Used when running in multi-process mode, maintains a pool of workers.
class FancyMP:
  def __init__(self,numWorkers='auto'):
    self.workers = []
    from multiprocessing.queues import Queue,SimpleQueue
    self.jobQueue = multiprocessing.Queue()
    self.resultQueue = multiprocessing.Queue()
    self.jobCount = 0
    self.resultCount = 0
    if numWorkers == 'auto': numWorkers = multiprocessing.cpu_count()
    for w in range(numWorkers):
      worker = FancyWorker(self.jobQueue, self.resultQueue)
      self.workers.append(worker)
      #worker.daemon = True
      worker.start()

  def __del__(self):
    for worker in self.workers:
      worker.terminate()
#endclasss
