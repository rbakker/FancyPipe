import sys
sys.path.append('../fancypipe')
from fancypipe import *

class DoSquare(FancyModule):
  def main(self,x):
    y = x*x
    return FancyOutput(y=y)
    
class MainTask(FancyModule):
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
