import sys
sys.path.append('../fancypipe')
from fancypipe import *

class DoSquare(FancyModule):
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
