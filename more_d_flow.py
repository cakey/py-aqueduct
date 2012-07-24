#!/usr/bin/python2.7

import itertools

class Flow():
   def __init__(self, *args, **kwargs):
      def _default_get(arg, parent, outputs):
         parent._put(arg)
      self.child = kwargs.get("child", None)
      self.action = kwargs.get("action", _default_get)
      self.leveloutputs = {}

   def get(self, *args):
      print "creating get"
      def _get(arg, parent, outputs):
         outputs.get(args[0], [])(arg)
         parent._put(arg)
      return Flow(child=self, action=_get)

   def alternate(self, *args):
      self.alternate_no = 0

      def _alternate(arg, parent, outputs):
         newarg = args[self.alternate_no](arg)
         self.alternate_no = (self.alternate_no + 1) % len(args)
         parent._put(newarg)

      return Flow(child=self, action=_alternate)

   def step(self, *args):
      def _step(arg, parent, outputs):
         newarg = args[0](arg)
         parent._put(newarg)
      return Flow(child=self, action=_step)

   def loop(self, *args):
      print "constructing loop"
      flow = args[0]
      
      self.times = None if len(args) < 2 else args[1]
      self._times = self.times
      def _loop(arg, parent, outputs):
         if self._times is None:
            flow.put(arg, parent=self, outputs=outputs)
         elif self._times > 0:
            self._times = self._times - 1
            flow.put(arg, parent=self, outputs=outputs)
         else:
            parent._put(arg)
         self._times = self.times
      return Flow(child=self, action=_loop)

   def on(self, *args):
      newflow = Flow(child=self)
      newflow.leveloutputs[args[0]] = args[1]
      return newflow
      
   def takewhile(self, *args):
      return Flow(child=self, action="takewhile", *args)

   def filter(self, *args):
      return Flow(child=self, action="filter", *args)

   # going down
   def put(self, arg, **kwargs):
      self.parent = kwargs.get("parent", None)
      outputs = kwargs.get("outputs", {})

      self.outputs = dict(outputs, **self.leveloutputs)

      if self.child is None:
         self._put(arg)
         return

      self.child.put(arg, parent=self, outputs=self.outputs)

   # coming up
   def _put(self, arg):
      if self.parent is None:
         return
      # print "applying: ", self.action
      self.action(arg, self.parent, self.outputs)
      self.parent = None # clear state for next run...
      self.outputs = {}

def log(value):
   print value

def times4(value):
   return value * 4

def divide3(value):
   return value / 3.0

lessthan = lambda v : lambda x : x < v
def data_flow():
   inner_loop = (Flow()
      .alternate(times4, divide3)
      .step(int)
      .get("value2"))

   arith = Flow().loop(inner_loop, 30)
   """
   arith = (Flow().startloop()
      .get("value")
      .alternate(times4, divide3)
      .step(int)
      .endloop())
   """ 
   # can add arith.value sugar later...
   bounded_arith = (arith.on("value", log).on("value2", log)
      )#.takewhile(lessthan(10**6)))
   
   bounded_arith.put(500)
      
   ##filtered_arith = bounded_arith.filter(lambda x: x % 3 == 0)
   
   #filtered_arith.put(300)

def imperative():
   def arith(value):
      funcs = [times4, divide3]
      funcno = 0
      while True:
         yield value
         value = int(funcs[funcno](value))
         funcno = (funcno + 1) % len(funcs)

   # imap
   def bounded_arith(value):
      for value in itertools.takewhile(lessthan(10**6), arith(value)):
         yield value
   
   def filtered_arith(value):
      for value in itertools.ifilter(
                     (lambda x: x % 3 == 0),
                     bounded_arith(value)):
         yield value 
   
   map(log, bounded_arith(500))
   map(log, filtered_arith(300))

data_flow()
# imperative()
