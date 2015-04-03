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
# as FancyNode or FancyModule classes, and define input-output 
# connections between nodes.

# to do: 
# - json RPC output
# - support positional command line arguments

import os,sys
import os.path as op
import argparse, subprocess
import tempfile, datetime, json
from collections import OrderedDict
import re
import multiprocessing, Queue

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
    print 'Future: Removing file {}'.format(self.f)
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
  def error(s): sys.stderr.write('\033[1;31m{}\033[0m'.format(s))
  @staticmethod
  def success(*out,**kwout):
    if len(out):
      result = FancyLog.stringifyList(out)
      print json.dumps(result)
    if kwout:
      result = FancyLog.stringifyDict(kwout)
      print json.dumps(result,indent=2)
  @staticmethod
  def traceback():
    exc_type, exc_value, exc_traceback = sys.exc_info() 
    return FancyReport.tb.format_exception(exc_type, exc_value, exc_traceback)
  @staticmethod
  def fail(infile,code=1):
    FancyReport.error("Error in {}.\n".format(infile))
    FancyReport.error(json.dumps(FancyReport.traceback(),indent=2))
    print
    sys.exit(code)
#endclass


## Used for logging to console and html-file.
class FancyLog:
  def __init__(self, logdir=None,loglevel=1):
    self.logdir = logdir
    self.logfile = None
    if logdir: 
      if not op.isdir(logdir):
        print 'Creating new log directory "{}".'.format(logdir)
        os.makedirs(logdir)
      logfilesdir = op.join(logdir,'fancylog_files')
      if not op.isdir(logfilesdir):
        print 'Creating new log_files directory "{}".'.format(logfilesdir)
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
    return { k:FancyLog.stringifyValue(v) for k,v in kwargs.iteritems() }
    
  # Add an entry to the logfile
  def addEntry(self, logId,cmd,args,kwargs, title=None,commandLine=None):
    print '___\nLog({}) {}'.format(logId,cmd+': '+title if title else cmd)
    # prepare args for json-dumping
    if args:
      args = self.stringifyList(args)
    if kwargs:
      kwargs = self.stringifyDict(kwargs)
    if self.loglevel > 0:
      if commandLine: 
        print 'Command line: {}'.format(commandLine)
      print 'Arguments: {},{}'.format(json.dumps(args,indent=2),json.dumps(kwargs,indent=2))
    
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
    print 'Node {} says: {}'.format(logId,msg)
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
    self.files.add(ff)

  def addNewFile(self,f):
    f = op.realpath(f)
    if not op.isfile(f): self._addFile(f)
        
  def _addDir(self,dd):
    self.dirs.add(dd)
  
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
          print 'DELETING FILE "{}"'.format(f)
          #os.remove(f)
        except:
          pass
    for d in reversed(sorted(self.dirs)):
      if d not in E:
        try:
          print 'REMOVING DIR "{}"'.format(d)
          #os.rmdir(d)
        except:
          pass
#endclass


## Used to load and keep track of configuration parameters.
class FancyConfig:
  def __init__(self,config):
    self.config = config

  @staticmethod
  def etree_to_odict(t):
    if len(t):
      od = OrderedDict()
      for ch in t.iterchildren():
        od[ch.tag] = FancyConfig.etree_to_odict(ch)
      return od
    else:
      return t.text

  @classmethod
  def fromFile(cls,configFile):
    name,ext = op.splitext(configFile)
    if ext.lower() == '.xml':
      from lxml import etree
      tree = etree.parse(configFile)
      return cls(FancyConfig.etree_to_odict(tree.getroot()))
    elif ext.lower() == '.json':
      return cls(json.load(configFile, object_pairs_hook=OrderedDict))
    else:
      raise RuntimeError('Configuration file "{}" has an unrecognized format.'.format(configFile))

  @classmethod
  def fromParent(cls,parentNode):
    if parentNode.fancyConfig:
      config = parentNode.fancyConfig.config.copy()
      parentClass = parentNode.__class__.__name__
      if parentClass in config:
        # overwrite defaults with className-specific defaults
        for k,v in config[parentClass].iteritems(): config[k] = v;
        config.pop(parentClass)
      return cls(config)
    
  def classDefaults(self,nodeClass):
    return self.config[nodeClass] if nodeClass in self.config else {}
#endclass


## Indicates an outgoing link of output <outKey> of node <node>.
class FancyLink:
  def __init__(self,node,outKey):
    self.node = node # list of FancyNodes
    self.outKey = outKey # output key
  
  def __repr__(self):
    return 'FancyLink<{}[{}]>'.format(self.node,self.outKey)
#endclass
  

## Base class for node input/output, consisting of list and keyword arguments
class FancyArgs:
  def __init__(self,*args,**kwargs):
    self.args = list(args)
    self.kwargs = kwargs

  def ready(self):
    return FancyList(self.args).ready() and FancyDict(self.kwargs).ready()

  def sources(self):
    return FancyList(self.args).sources() | FancyDict(self.kwargs).sources()

  def __getitem__(self,key):
    return self.args[key] if isinstance(key,(int,long)) else self.kwargs[key]

  def __setitem__(self,key,val):
    if isinstance(key,(int,long)): self.args[key] = val
    else: self.kwargs[key] = val
    

## Special case of FancyArgs, consisting of only a list.
class FancyList(list,FancyArgs):  
  def __init__(self,args):
    list.__init__(self,args)
    self.args = self
    self.kwargs = {}

  def ready(self):
    for i,v in enumerate(self):
      if isinstance(v,FancyLink): return False
      elif isinstance(v,FancyArgs) and not v.ready(): return False
    return True

  def sources(self):
    ans = set()
    for i,v in enumerate(self):
      if isinstance(v,FancyLink): ans.add(v.node)
      elif isinstance(v,FancyArgs): ans |= v.sources()
    return ans
#endclass


## Special case of FancyArgs, consisting of only keyword arguments.
class FancyDict(dict,FancyArgs):
  def __init__(self,kwargs):
    dict.__init__(self,kwargs)
    self.kwargs = self
    self.args = []

  def ready(self):
    for k,v in self.iteritems():
      if isinstance(v,FancyLink): return False
      elif isinstance(v,FancyArgs) and not v.ready(): return False
    return True

  def sources(self):
    ans = set()
    for k,v in self.iteritems():
      if isinstance(v,FancyLink): ans.add(v.node)
      elif isinstance(v,FancyArgs): ans |= v.sources()
    return ans
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

## Base class for all pipeline nodes, has all features except 
## calling the node from the command line.
class FancyNode:
  name = None
  title = None
  description = None
  inputs = {
    'tempdir':{'default':None,
      'help':'Temp directory, to store intermediate results.'
    },
    'logdir':{'default':None,
      'help':'Log directory, to store logfile (fancylog.js + fancylog.html) and store attachments.'
    },
    'loglevel':{'type':int, 'default':1,
      'help':'Log level, 1 for entries, 2 for standard output, 4 for data, 8 for extra.'
    },
    'logid':{'default':'1',
      'help':'Hierarchical identifier of the log entry (e.g. "3.4.7"). Use logId=INIT to create a new empty log (error if it exists), and logId=RESET to reset (overwrite) an existing log.'
    },
    'configfile':{'default':None,
      'help':'Configuration file (XML or JSON), to read default parameters from.'
    },
    'nocleanup':{'action':'store_true', 'default':False,
      'help':'Whether to cleanup intermediate results.'
    }
  }

  def __init__(self,tempdir=None,fancyClean=None,fancyConfig=None,fancyLog=None,logId='1'):
    self.tempdir     = tempdir if tempdir else tempfile.gettempdir()
    self.fancyClean  = fancyClean if fancyClean else FancyClean()
    self.fancyClean.addCreateDir(self.tempdir)
    self.fancyConfig = fancyConfig if fancyConfig else None
    self.fancyLog    = fancyLog if fancyLog else FancyLog()
    if logId == 'INIT' or logId == 'RESET': 
      fancyLog.reset(overwrite=(logId == 'RESET'))
      logId = '1';
    self.logId     = logId
    self.numLogs   = 0
    self.doRecycle = self.fancyClean.noCleanup
    self.doParallel = True
    self.myInput   = FancyInput()
    self.myOutput  = FancyOutput()
    self.outfiles  = None # outputs that are returned as files
    self.requests  = {} # (node,inKey) pairs that receive data from this node
    
    # Prepare inputs from defaults and configuration file.
    kwargs = {}
    if self.fancyConfig: kwargs.update(self.fancyConfig.classDefaults(self.__class__.__name__))
    self.prepareInput([],self._parseInputs(kwargs))
  
  def __repr__(self):
    return "%s[%s]" % (self.getName(), self.logId)

  @classmethod
  def fromParent(cls,parent,tempsubdir=None,title=None):
    noCleanup = parent.fancyClean.noCleanup
    args = {
      'tempdir': op.join(parent.tempdir,tempsubdir) if tempsubdir else parent.tempdir,
      'fancyClean': FancyClean(noCleanup),
      'fancyConfig': FancyConfig.fromParent(parent),
      'fancyLog': parent.fancyLog,
      'logId': parent.newChildLogId()
    }
    # Instantiate.
    self = cls(**args)
    if title: 
      self.title = title
    return self

  def validateInputs(self,args):
    pass

  # Parse external inputs, from commandline or configfile.
  @classmethod
  def _parseInputs(cls,raw):
    if 'inputs' not in cls.__dict__: return {}
    # positional arguments are ignored
    kwargs = {}
    for key,inp in cls.inputs.iteritems():
      # typecast
      tp = inp['type'] if 'type' in inp else str
      if key in raw:
        kwargs[key] = tp(raw.pop(key))
      else:
        if 'default' in inp:
          kwargs[key] = inp['default']
    return kwargs
  
  def newChildLogId(self):
    self.numLogs += 1
    return self.logId+'.'+str(self.numLogs)

  # tempfile(F) returns tempdir/F, and registers F for cleanup after running main()
  def tempfile(self,f=None,ext='',autoclean=None):
    if f is None:
      import string
      import random
      rand8 = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(8))      
      f = '{}_{}{}'.format(self.__class__.__name__.lower(),rand8,ext)
    f = op.join(self.tempdir,f)
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

  # Set (update) the run-input.
  def prepareInput(self,args,kwargs):
    self.myInput.args.extend(args)
    self.myInput.kwargs.update(kwargs)

  # Set (update) the run-input.
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
      **{ k:v for k,v in self.myInput.kwargs.iteritems() if isinstance(v,FancyOutputFile) }
    )

  # Generate a link that requests output, use only in calls to setInput or setOutput.
  def requestOutput(self,key):
    try:
      return self.myOutput[key]
    except:
      if self.doRecycle:
        try:
          f = self.outfiles[key]
          if op.isfile(f): 
            print 'Recycling file "{}" in node {}'.format(f,self) 
            return f
        except KeyError,IndexError:
          pass          
      return FancyLink(self,key)

  # Get an output, after calling run().
  def getOutput(self,key=None):
    if not self.myOutput.ready():
      raise RuntimeError('The output of node {} contains unresolved values. Use requestOutput() instead, or run the node first.'.format(self))
    return self.myOutput[key]

  # Initialize requests for output from upstream nodes.
  def initRequests(self,anyArgs,allSources):
    def addRequest(key,source):
      sn,sk = (source.node,source.outKey)
      print 'Node {} requests output "{}" from node {}'.format(self,sk,sn)
      if not sk in sn.requests: sn.requests[sk] = []
      sn.requests[sk].append((anyArgs,key,self))

    for i,v in enumerate(anyArgs.args):
      if isinstance(v,FancyLink): addRequest(i,v)
      elif isinstance(v,FancyArgs): self.initRequests(v,allSources)
    for k,v in anyArgs.kwargs.iteritems():
      if isinstance(v,FancyLink): addRequest(k,v)
      elif isinstance(v,FancyArgs): self.initRequests(v,allSources)

    for src in anyArgs.sources():
      if not src in allSources:
        allSources.add(src)
        src.initRequests(src.myInput,allSources)
    
    return allSources

  # Deal with unresolved links in the output.
  def runSources(self,allSources,jobQueue,fancyMP):
    mySources = self.initRequests(self.myOutput,set())
    for src in mySources: allSources[str(src)] = src
    for src in mySources:
      if src.myInput.ready():
        jobQueue.put(src)
        if fancyMP: fancyMP.jobCount += 1
    
  def logOutput(self):
    self.fancyLog.attachResult(self.logId, 'output',self.myOutput.args,self.myOutput.kwargs)

  def propagateOutput(self,allSources,jobQueue,fancyMP):
    affectedNodes = self.fulfillRequests()
    for node in affectedNodes:
      if str(node) in allSources:
        if node.myInput.ready(): 
          jobQueue.put(node)
          if fancyMP: fancyMP.jobCount += 1
      else:
        if node.myOutput.ready():
          node.propagateOutput(allSources,jobQueue,fancyMP)
  
  def resolveOutput(self,jobQueue,fancyMP=None):
    allSources = {}
    self.runSources(allSources,jobQueue,fancyMP)
    while not self.myOutput.ready():
      if fancyMP:
        # parallel execution, using MultiProcessing
        if fancyMP.jobCount == 0:
          raise RuntimeError('Waiting for output but no jobs are running.')
        (srcId,myOutput) = fancyMP.resultQueue.get(block=True)
        fancyMP.jobCount -= 1
        if srcId is None:
          raise RuntimeError('Worker node {} encountered an error.'.format(myOutput))
        src = allSources.pop(srcId)
      else:
        # serial execution
        src = jobQueue.get(block=True)
        allSources.pop(str(src))
        myOutput = src._run()
        
      src.myOutput = myOutput
      if src.myOutput.ready():
        src.logOutput()
        src.propagateOutput(allSources,jobQueue,fancyMP)
      else:
        src.runSources(allSources,jobQueue,fancyMP)

  # Fulfill requests after running the upstream nodes.
  def fulfillRequests(self):
    affectedNodes = set()
    # Send output to targets
    outputRequests = self.requests.keys()
    for key in outputRequests:
      try:
        val = self.myOutput if key is None else self.myOutput.args[key] if isinstance(key, (int, long)) else self.myOutput.kwargs[key]
      except KeyError: 
        raise KeyError('Node "{}" does not have the requested output "{}"'.format(self,key))
      for targetArgs,targetKey,targetNode in self.requests[key]:
        targetArgs[targetKey] = val
        affectedNodes.add(targetNode)
      self.requests.pop(key)
    return affectedNodes
        
  # Run this node and return its output: outer loop.
  def run(self):
    try:
      self._run()
      if not self.myOutput.ready():
        # resolve output dependencies
        isWorker =  hasattr(multiprocessing.current_process(),'jobQueue')
        if self.doParallel and not isWorker:
          print 'Running node {} in parallel mode.'.format(self)
          fancyMP = FancyMP()
          self.resolveOutput(fancyMP.jobQueue,fancyMP)
        else:
          print 'Running node {} in serial mode.'.format(self)
          self.resolveOutput(Queue.Queue())

      self.logOutput()
      FancyReport.success(*self.myOutput.args,**self.myOutput.kwargs)
      return self
    except:
      msg = FancyReport.traceback()
      self.fancyLog.attachResult(self.logId,'Fatal error in node {}'.format(self),msg,{}, tp='error')
      FancyReport.fail(__file__)

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
    self.fancyLog.addEntry(self.logId, name,self.myInput.args,self.myInput.kwargs, title=self.title,commandLine=commandLine)
    self.myOutput = self._main(self.myInput)
    if not isinstance(self.myOutput,FancyArgs):
      raise RuntimeError('The function main() of node {} must return a FancyOutput(...) object.'.format(self))
#endclass


## Extended class for pipeline nodes, adds support for calling the node
## from the command line.
class FancyModule(FancyNode):
  inputs = {}
  
  @classmethod
  def fromCommandLine(cls):
    cmdArgs = cls._getParser().parse_args().__dict__;
    kwargs = FancyNode._parseInputs(cmdArgs)
    logdir = kwargs['logdir']
    loglevel = kwargs['loglevel']
    configfile = kwargs['configfile']
    kwargs = {
      'tempdir': kwargs['tempdir'],
      'fancyClean': FancyClean(kwargs['nocleanup']),
      'fancyConfig': FancyConfig.fromFile(configfile) if configfile else None,
      'fancyLog': FancyLog(logdir,loglevel),
      'logId': kwargs['logid']
    }
    # Instantiate.
    self = cls(**kwargs)
    # Update with remaining inputs from command line.
    self.setInput(**self._parseInputs(cmdArgs))
    return self

  @classmethod
  def _getParser(cls):
    p = argparse.ArgumentParser(
      description=cls.description if cls.description else cls.title,
      formatter_class=argparse.RawTextHelpFormatter,
      argument_default=argparse.SUPPRESS
    )
    cls._extendParser(p,FancyNode.inputs)
    cls._extendParser(p,cls.inputs)
    return p

  @classmethod
  def _extendParser(cls,p,inputs):
    for key,inp in inputs.iteritems():
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
    mainOutput = FancyNode._main(self,mainInput)
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
    for k,v in mainInput.kwargs.iteritems(): cmd.extend([k,v])
    return ' '.join(['{}'.format(v) for v in cmd])

  def main(self,*args,**kwargs):
    cmd = list(args);
    for k,v in kwargs.iteritems(): cmd.extend([k,v])
    stdout = subprocess.check_output(cmd, shell=False, stderr=subprocess.STDOUT)
    self.fancyLog.attachResult(self.logId, 'stdout',stdout.split('\n'),{}, tp='longtext')
    return FancyOutput(stdout)
    
  def afterMain(self,stdout):
    if 'outputFiles' in self.__class__.__dict__: return self.outfiles
    # If no output files have been defined, then assume that
    # outputs are stored in files with the name given by the corresponding input
    kwargs = {}
    for k,_ in self.requests.iteritems():
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
    while True:
      try:
        node = self.jobQueue.get()
        if node is None:
          # Poison pill means shutdown
          print 'Exiting fancy worker {}'.format(self.name)
          break
        node._run()
        self.resultQueue.put((str(node),node.myOutput))
      except:
        msg = FancyReport.traceback()
        node.fancyLog.attachResult(node.logId,'Fatal error in worker {} during/after execution of node {}'.format(self.name,node),msg,{}, tp='error')
        self.resultQueue.put((None,self.name))
        FancyReport.fail(__file__)
    return
#endclass


## Used when running in multi-process mode, maintains a pool of workers.
class FancyMP:
  def __init__(self,numWorkers='auto'):
    self.jobQueue = multiprocessing.Queue()
    self.resultQueue = multiprocessing.Queue()
    self.jobCount = 0
    self.resultCount = 0
    if numWorkers == 'auto': numWorkers = multiprocessing.cpu_count()
    self.workers = []
    for w in xrange(numWorkers):
      worker = FancyWorker(self.jobQueue, self.resultQueue)
      self.workers.append(worker)
      worker.start()

  def __del__(self):
    for worker in self.workers:
      worker.terminate()
#endclasss
