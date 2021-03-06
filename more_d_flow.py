#!/usr/bin/python2.7

import collections
import itertools
import operator
class NoneParent():
   def _put(self, arg):
      return arg
class Flow():
   def __init__(self, *args, **kwargs):
      def _default_get(arg, parent, outputs, limits):
         return parent._put(arg)
      self.child = kwargs.get("child", None)
      self.action = kwargs.get("action", _default_get)
      self.leveloutputs = {}
      self.levellimits = {}

   def get(self, label):
      def _get(arg, parent, outputs, limits):
        
         allowed = all(limit(arg) for limit in limits[label])
         if allowed:
            for out in outputs[label]:
               if isinstance(out, Flow):
                  out.put(arg, outputs=outputs, limits=limits)
               else:
                  out(arg)
            return parent._put(arg)
         else: 
            raise StopIteration()
      return Flow(child=self, action=_get)

   def alternate(self, *args):
      self.alternate_no = 0

      def _alternate(arg, parent, outputs, limits):
         newarg = args[self.alternate_no](arg)
         self.alternate_no = (self.alternate_no + 1) % len(args)
         return parent._put(newarg)

      return Flow(child=self, action=_alternate)

   def then(self, *args):
      return self.step(*args)

   def step(self, *args):
      def _step(arg, parent, outputs, limits):
         out = args[0]
         if isinstance(out, Flow):
            newarg = out.put(arg, outputs=outputs,limits=limits)
         else:
            newarg = out(arg) 
         return parent._put(newarg)
      return Flow(child=self, action=_step)

   def sum(self):
      return self.reduce(0, operator.add)

   def reduce(self, *args):
      self._current = args[0]
      func = args[1]
      def _reduce(arg, parent, outputs, limits):
         self._current = func(self._current, arg)
         return parent._put(self._current)
      return Flow(child=self, action=_reduce)

   def loop(self, *args):
      flow = args[0]
      
      self.times = None if len(args) < 2 else args[1]
      def _loop(arg, parent, outputs, limits):
         try:  
            if self.times is None:
               while True:
                  arg = flow.put(arg, parent=None, outputs=outputs, limits=limits)
            elif hasattr(self.times, '__call__'):
               while (arg is not None) and self.times(arg):
                  arg = flow.put(arg, parent=None, outputs=outputs, limits=limits)
            else:
               for _ in range(self.times):
                  arg = flow.put(arg, parent=None, outputs=outputs, limits=limits)
         except StopIteration:
            pass
         if arg is not None:
            return parent._put(arg)
      return Flow(child=self, action=_loop)

   def each(self, fun_or_flow):
      return self.on("value", fun_or_flow)

   def on(self, *args):
      newflow = Flow(child=self)
      newflow.leveloutputs[args[0]] = args[1]
      return newflow

   def takewhile(self, label, limit_func):
      newflow = Flow(child=self)
      newflow.levellimits[label] = limit_func
      return newflow
      
   def filter(self, *args):
      def _filter(arg, parent, outputs, limits):
         if args[0](arg):
            return parent._put(arg)
      return Flow(child=self, action=_filter)

   def __call__(self, *args, **kwargs):
      return self.put(*args, **kwargs)
   # going down
   def put(self, arg=None, **kwargs):
      self.parent = kwargs.get("parent", None)
      outputs = kwargs.get("outputs", collections.defaultdict(list))
      limits = kwargs.get("limits", collections.defaultdict(list))


      self.outputs= collections.defaultdict(list)
      self.limits = collections.defaultdict(list)

      # copy over outputs
      for label, funcs in outputs.iteritems():
         for func in funcs:
            self.outputs[label].append(func)
      for label, func in self.leveloutputs.iteritems():
         self.outputs[label].append(func)

      # copy over limits
      for label, funcs in limits.iteritems():
         for func in funcs:
            self.limits[label].append(func)

      for label, func in self.levellimits.iteritems():
         self.limits[label].append(func)

      # reached the bottom (first step)
      if self.child is None:
         return self._put(arg)

      return self.child.put(arg, parent=self, outputs=self.outputs, limits=self.limits)

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
      returnee = self.action(arg, parent, self.outputs, self.limits)
      self.parent = None # clear state for next run...
      self.outputs = {}
      self.limits = {}
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
   def basic():
      plus1 = Flow().get("value").step(lambda x: x+1)
      counter = Flow().loop(plus1, lessthan(11))
      cube = Flow().step(lambda x: x**3).step(log)
      cube.put(10)
      cubes = counter.on("value", cube)
      cubes.put(0)
  
   #fibonacci
   fib_step = Flow().get("value").step(lambda (mi,mv, i,x,y): (mi,mv, i+1,y,x+y))
   fib_trips = Flow().loop(fib_step, 
      (lambda (mi,mv, i,x,y): 
         ((mi is None or i < mi) 
         and (mv is None or y < mv))))

   extract = Flow().step(lambda (mi,mv,i,x,y): y)

   def insert(maxes):
      if maxes is None:
         maxes = {}
      iter_max = maxes.get("iter", None)
      value_max = maxes.get("value", None)
      return (iter_max, value_max, 1,1,1)

   fibs = (Flow().step(insert)
                .step(fib_trips)
                .get("last"))
   extract_and_log = extract.step(log)
   def fibonacci():
      fibs.on("value", log2).on("last", extract_and_log).put({"iter": 10})
      fibs.on("value", log2).put({"value": 9010})

      # all do the same thing:
      fibs.on("last", extract_and_log).put({"value": 9010})
      extract_and_log.put(fibs.put({"value":9010}))
      fibs.step(extract_and_log).put({"value":9010})

   # collatz
   def collatz():
      def col(x):
         if x % 2 == 0:
            return x/2
         else:
            return x*3 + 1
      collatz_step = Flow().get("value").step(col)
      collatz = Flow().loop(collatz_step, lambda x: x != 1).get("value")

      # length:
      length = Flow().reduce(0, lambda x,y: x+1).step(log)
      collatz.on("value", log).on("value", length).put(2472)

   # reduce/filter
   def reduce_filter():
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

   # can raise StopIteration?
   # Flow.stop
   # Euler Q1
   counter = Flow().loop(Flow().get("value").step(lambda x: x+1))
   def EQ1():
      """ Add all the natural numbers below one thousand
         that are multiples of 3 or 5 """
      
      # dataflow:
      counter(1000).each( 
         Flow()
            .filter(lambda x: x%3==0 or x%5 ==0)
            .sum()
            .then(log)
         ).put(0)

      # standard:
      log(sum(x for x in range(1000) if x%3==0 or x%5 ==0))
   # EQ1()

   even = lambda x: x%2 == 0
   def EQ2():
      """
         By considering the terms in the Fibonacci sequence whose values do not
         exceed four million, find the sum of the even-valued terms.
      """
      fibs.each(
         Flow()
            .step(extract)
            .filter(even)
            .sum()
            .then(log)
         ).put({"value":4000000})
   #EQ2()
   primes = counter.on("value",
      Flow()
         .step(lambda num: (2,num, (int(num**0.5) + 1)))
         .loop(
            Flow().
            filter(lambda (div,num,_): num % div !=0)
            .step(lambda (div,num,sqrtnum): (div+1, num, sqrtnum)),
            (lambda (div, num, sqrtnum): div < sqrtnum))
         .step(lambda (div, num,_): num)
         .get("prime"))
   def EQ3():
      """
         What is the largest prime factor of the number 600851475143?
      """
      primes(int(600851475143**0.5)).on("prime",
         Flow()
            .filter(lambda x: 600851475143 % x ==0)
            .then(log)
         ).put(1)
   #EQ3()

   def EQ4():
      """
         Find the largest palindrome made from the product of two 3-digit numbers.
      """
      counter = Flow().loop(
            Flow()
               .step(lambda (x,y): (x-1, 999)) 
               .loop(
         Flow()
            .get("value")
            .step(lambda (x, y): (x, y-1))
         , lambda (x, y): y>x
         ), lambda (x, y): x>909)
      log_palindrome_products = (Flow()
         .step(lambda (x,y): x*y)
         .filter(lambda v: str(v) == str(v)[::-1])
         .reduce(0, lambda x,y: max(x,y))
         .then(log))
      counter.each(log_palindrome_products).put((1000,0))
      #combiner = Flow().get("moo")

      #logcombiner = combiner.on("moo", log)
      #counter.takewhile("value", lessthan(10)).each(logcombiner).put(0)
      #counter.takewhile("value", lessthan(20)).each(logcombiner).put(10)
      print "hi"
         
   #EQ4()
   def EQ6():
      """ Find the difference between the sum of the squares of the first one hundred 
         natural numbers and square of the sum """

      # wants a branch/combine function
   def EQ7():
      """ 10001st prime """
      length = (Flow()
         .reduce(0, lambda x,y: x+1)
         .get("length"))
      primes.on("prime",log).on("prime", length.step(log)).takewhile("length", lessthan(10002)).put(2)
      # wants a 'fresh' for length
      # wants a way to take the last value after a stopIteration
   EQ7()
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

#add1 =Flow().step(lambda x: x+1).step(log)
#add1.put(2)
