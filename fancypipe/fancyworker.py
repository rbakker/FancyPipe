from fancypipe import FancyModule,FancyOutput,RemoteWorker
from multiprocessing.managers import BaseManager
import multiprocessing,time
try:
    import queue
except ImportError:
    import Queue as queue

class ManagerProxy(BaseManager):
  pass

class FancyHub(FancyModule):
  inputs = {}
  def main(self):
    for w in range(self.numWorkers):
      worker = RemoteWorker(self.jobManager,self.jobAuth)
      worker.authkey = self.jobAuth # to allow TCP/IP from worker to resultManager
      worker.daemon = True
      worker.start()

    # keep running forever (Ctrl-C or kill to stop)
    while True:
      time.sleep(1)

    return FancyOutput(status='Done.')
#endclass

if __name__ == '__main__':
  FancyHub.fromCommandLine().run()
