import sys
sys.path.append('../fancypipe')
from fancypipe import *

class ComplexTask(FancyTask):
  def main(self,x,a):
    y = x*a
    return FancyOutput(y=y)
    
class MainModule(FancyModule):
  inputs = {
    'x':{'default':5, 'help':'input x'},
    'a1':{'default':100, 'help':'parameter value 1'},
    'a2':{'default':200, 'help':'parameter value 2'}
  }
  def main(self,x,a1,a2):
    co1 = ComplexTask.fromParent(self).setInput(
      x = x,
      a = a1
    )
    co2 = ComplexTask.fromParent(self).setInput(
      x = x,
      a = a2
    )
    return FancyOutput(
      y1 = co1.requestOutput('y'),
      y2 = co2.requestOutput('y')
    )
    
if __name__ == '__main__':
  MainModule.fromCommandLine().run()
