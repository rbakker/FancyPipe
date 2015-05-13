from fancypipe import FancyModule,FancyOutput
from multiprocessing.managers import BaseManager,DictProxy
import multiprocessing,time
try:
    import queue
except ImportError:
    import Queue as queue

class ManagerProxy(BaseManager):
  pass
        
class FancyManager(FancyModule):
  inputs = {}
  def main(self):
    jobQueue = queue.Queue()
    ManagerProxy.register('getJobQueue',callable=lambda: jobQueue)
    resultQueues = dict()
    def getResultQueue(id):
      if id not in resultQueues: resultQueues[id] = queue.Queue()
      return resultQueues[id] 
    ManagerProxy.register('getResultQueue', callable=getResultQueue)
    def popResultQueue(id):
      try: resultQueues.pop(id)
      except: pass
    ManagerProxy.register('popResultQueue', callable=popResultQueue)
    
    addr = self.jobManager
    if not addr:
      addr = ('localhost',51423)
    m = ManagerProxy(address=addr,authkey=self.jobAuth)
    multiprocessing.current_process().authkey = self.jobAuth
    print('Starting master jobserver at {}:{}'.format(addr[0],addr[1],self.jobAuth))
    s = m.get_server()
    s.serve_forever()
    # the previous statement is blocking, so this will run until killed externally.
    return FancyOutput(status='Done.')
#endclass

if __name__ == '__main__':
  FancyManager.fromCommandLine().run()
