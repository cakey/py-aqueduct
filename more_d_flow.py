#!/usr/bin/python2.7

import collections
import itertools
import operator
class NoneParent():
   def _put(self, arg):
      return arg
class Flow():
   def __init__(self, *args, **kwargs):
      def _default_get(arg, parent, outputs):
         return parent._put(arg)
      self.child = kwargs.get("child", None)
      self.action = kwargs.get("action", _default_get)
      self.leveloutputs = {}

   def get(self, *args):
      def _get(arg, parent, outputs):
         for out in outputs[args[0]]:
            if isinstance(out, Flow):
               out.put(arg)
            else:
               out(arg)
         return parent._put(arg)
      return Flow(child=self, action=_get)

   def alternate(self, *args):
      self.alternate_no = 0

      def _alternate(arg, parent, outputs):
         newarg = args[self.alternate_no](arg)
         self.alternate_no = (self.alternate_no + 1) % len(args)
         return parent._put(newarg)

      return Flow(child=self, action=_alternate)

   def step(self, *args):
      def _step(arg, parent, outputs):
         newarg = args[0](arg)
         return parent._put(newarg)
      return Flow(child=self, action=_step)

   def reduce(self, *args):
      self._current = args[0]
      func = args[1]
      def _reduce(arg, parent, outputs):
         self._current = func(self._current, arg)
         return parent._put(self._current)
      return Flow(child=self, action=_reduce)

   def loop(self, *args):
      flow = args[0]
      
      self.times = None if len(args) < 2 else args[1]
      def _loop(arg, parent, outputs):
         if self.times is None:
            while True:
               arg = flow.put(arg, parent=None, outputs=outputs)
         elif hasattr(self.times, '__call__'):
            while self.times(arg):
               arg = flow.put(arg, parent=None, outputs=outputs)
         else:
            for _ in range(self.times):
               arg = flow.put(arg, parent=None, outputs=outputs)
         return parent._put(arg)
      return Flow(child=self, action=_loop)

   def on(self, *args):
      newflow = Flow(child=self)
      newflow.leveloutputs[args[0]] = args[1]
      return newflow
      
   def filter(self, *args):
      def _filter(arg, parent, outputs):
         if args[0](arg):
            return parent._put(arg)
      return Flow(child=self, action=_filter)

   # going down
   def put(self, arg, **kwargs):
      self.parent = kwargs.get("parent", None)
      outputs = kwargs.get("outputs", collections.defaultdict(list))


      self.outputs= collections.defaultdict(list)
      for label, funcs in outputs.iteritems():
         for func in funcs:
            self.outputs[label].append(func)
      for label, func in self.leveloutputs.iteritems():
         self.outputs[label].append(func)

      # reached the bottom (first step)
      if self.child is None:
         return self._put(arg)

      return self.child.put(arg, parent=self, outputs=self.outputs)

   # coming up
   def _put(self, arg):
      # reached the top (last step), so return arg (
      # this will return down the _put's then up the put's
      # so that something like the looping construct can
      # avoid recursion
      parent = self.parent
      if self.parent is None:
         parent = NoneParent()

      # print "applying: ", self.action
      returnee = self.action(arg, parent, self.outputs)
      self.parent = None # clear state for next run...
      self.outputs = {}
      return returnee

def log(value):
   print value
   return value
def log2(value):
   print value, "is running total!"
def times4(value):
   return value * 4

def divide3(value):
   return value / 3 

lessthan = lambda v : lambda x : x < v

def data_flow():
   inner_loop = (Flow()
      .get("value")
      .alternate(times4, divide3)
      .step(int)
      )

   arith = Flow().loop(inner_loop, lessthan(10**4))

   filtered = Flow().filter(lambda x: x % 3 == 0)
  
   filtered_reduced_log = filtered.step(log).reduce(0, operator.add).step(log2)
      
   arith.on("value", filtered_reduced_log).put(300) 
   print

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
      for value in itertools.takewhile(lessthan(10**7), arith(value)):
         yield value
   
   def filtered_arith(value):
      for value in itertools.ifilter(
                     (lambda x: x % 3 == 0),
                     bounded_arith(value)):
         yield value 
   
   #map(log, bounded_arith(500))
   map(log, filtered_arith(300))

data_flow()
#imperative()
