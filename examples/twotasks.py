import sys
sys.path.append('../fancypipe')
from fancypipe import *

class DoSquare(FancyModule):
  def main(self,x):
    y = x*x
    return FancyOutput(y=y)
    
class DoSum(FancyModule):
  def main(self,x1,x2):
    y = x1+x2
    return FancyOutput(y=y)

class MainTask(FancyModule):
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
