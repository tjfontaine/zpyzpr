#!/usr/bin/python
# Copyright (c) 2008 Timothy J Fontaine <tjfontaine@gmail.com>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE

import getopt, os, sys, threading, string, subprocess
from datetime import datetime

CHUNK_SIZE_BYTES = 10*1024*1024 # 10 MB
BLOCK_SIZE = 1024

def letter_next(c):
  while(True):
    c = chr((ord(c) + 1) % 255)
    if c in string.ascii_lowercase or c in string.digits: return c

def string_next(s):
  r = []
  for c in s:
    r.append(c)
  r.reverse()

  s = []
  inc = True
  for c in r:
    if inc:
      c = letter_next(c)
      inc = c == '9'
    s.append(c)

  s.reverse()
  return ''.join(s)

class BaseWorker(threading.Thread):
  ext = '.dummy'
  def __init__(self, src, dst, start, size, compression):
    threading.Thread.__init__(self)
    self.status = True
    self.dummy = True
    self.src = src
    self.dst = dst
    self.offset = start
    self.fsize = size
    self.comp = compression

  def run(self):
    return True

class GzipWorker(BaseWorker):
  ext = '.gz'
  def __init__(self, src, dst, start, size, compression):
    BaseWorker.__init__(self, src, dst, start, size, compression)
    self.dummy = False

  def run(self):
    p = subprocess.Popen("dd if=%s skip=%d count=%d bs=1024 2> /dev/null | gzip -%d > %s" % (self.src, self.offset/BLOCK_SIZE, self.fsize/BLOCK_SIZE, self.comp, self.dst+'.gz'), shell=True)
    pid, es = os.waitpid(p.pid, 0)
    self.status = es == 0 # if exit status wasn't 0 this will be false and we can clean up

class Bzip2Worker(BaseWorker):
  ext = '.bz2'
  def __init__(self, src, dst, start, size, compression):
    BaseWorker.__init__(self, src, dst, start, size, compression)
    self.dummy = False

  def run(self):
    p = subprocess.Popen("dd if=%s skip=%d count=%d bs=1024 2> /dev/null | bzip2 -%d > %s" % (self.src, self.offset/BLOCK_SIZE, self.fsize/BLOCK_SIZE, self.comp, self.dst+'.bz2'), shell=True)
    pid, es = os.waitpid(p.pid, 0)
    self.status = es == 0 # if exit status wasn't 0 this will be false and we can clean up

class ZpyZprOpts:
  def __init__(self, argv):
    sopt = 'hkvzjT:bct'
    lopt = ['help', 'keep', 'verbose', 'timing', 'gzip', 'bzip2', 'blocks=', 'compression=', 'threads=']
    self.verbose     = False
    self.timing      = False
    self.blocks      = None # Automaticly determined
    self.threads     = 4 # Should this be determined magically?
    self.keep        = False
    self.compression = 6
    self.pattern     = 'xaa'
    self.worker      = GzipWorker

    try:
      opts, args = getopt.getopt(argv, sopt, lopt)
    except getopt.GetoptError, err:
      sys.stderr.write(str(err) + os.linesep)
      self.usage(True)
      sys.exit(2)

    if len(args) < 1 or len(args) > 2:
      sys.stderr.write('Wrong number of arguments passed.' + os.linesep)
      self.usage(True)
      sys.exit(2)
    
    for o, a in opts:
      if   o in ('-h', '--help'):
        self.usage(False)
        sys.exit()
      elif o in ('-k', '--keep'):
        self.keep = True
      elif o in ('-v', '--verbose'):
        self.verbose = True
        self.timing = True
      elif o in ('-b', '--blocks'):
        self.blocks  = int(a)
      elif o in ('-t', '--threads'):
        self.threads = int(a)
      elif o in ('-c', '--compression'):
        self.compression = int(a)
      elif o in ('-p', '--pattern'):
        self.pattern = a
      elif o in ('-T', '--timing'):
        self.timing = True
      elif o in ('-z', '--gzip'):
        self.worker = GzipWorker
      elif o in ('-j', '--bzip2'):
        self.worker = Bzip2Worker
    
    self.source = args[0]

    if len(args) == 2:
      self.destination = args[1]
    else:
      self.destination = self.source + self.worker.ext

    if not os.path.exists(self.source):
      sys.stderr.write('Source file (%s) does not exist!%s' % (self.source, os.linesep))
      self.usage(True)
      sys.exit(2)

    if os.path.exists(self.destination):
      sys.stderr.write('Destination file (%s) already exists!%s' % (self.destination, os.linesep))
      self.usage(True)
      sys.exit(2)

  def usage(self, err):
    e = os.linesep
    if err:
      p = sys.stderr.write
    else:
      p = sys.stdout.write

    p('zz [opts] <sourcefile> [destinationfile]'+e)
    p(''+e)
    p('-b --blocks=       Specify the logical block size for each compressed block'+e)
    p('                     (Default: 10M or the size of the file divided by the number of threads)'+e)
    p('-c --compression=  Compression Level (Default: 6)'+e)
    p('-h --help          Prints this message'+e)
    p('-k --keep          Keep source files (The original source and intermediate slices)'+e)
    p('-p --pattern       The pattern for intermediate slices (Default: xaa)'+e)
    p('                     The longer this string is the less likely a wrap will ocurr'+e)
    p('                     The succession algo only uses [a-z0-9]'+e)
    p('-t --threads=      Specify the number compression threads (Default: 4)'+e)
    p('-v --verbose       Prints timings and other debug information'+e)

class ZpyZpr:
  def __init__(self, opts):
    self.opts = opts

    self.source_size = os.stat(opts.source).st_size

    self.thread_queue = []
    self.filenames = []
    
    self.create_files()

    b = datetime.now()
    self.log(self.opts.timing, "Beginning Compression (%d Pieces/%d Threads)" % (len(self.filenames), self.opts.threads))
    self.start()

    self.log(self.opts.verbose, "Combing Slices")
    self.combine()
    e = datetime.now()

    self.log(self.opts.timing, 'Total Time: '+str(e - b))

  def cleanup(self):
    for f in self.filenames:
      if os.path.exists(f): os.remove(f)

    if os.path.exists(self.opts.destination):
      os.remove(self.opts.destination)

  def create_files(self):
    bsize = 0
    if not self.opts.blocks:
      # Determine slice size based on number of threads and the default chunk size
      # by default if the file is less than 40MB it'll be size / threads
      if self.source_size < CHUNK_SIZE_BYTES * self.opts.threads:
        bsize = self.source_size / self.opts.threads
      else:
        bsize = CHUNK_SIZE_BYTES
    else:
      bsize = self.opts.blocks

    pattern = self.opts.pattern

    count = 0
    while(count < self.source_size):
      if self.source_size - count < bsize:
        size = self.source_size - count
      else:
        size = bsize

      if os.path.exists(pattern+self.opts.worker.ext):
        self.log(True, '%s Temporary file already exists!' % pattern+self.opts.worker.ext)
        self.cleanup()
        self.exit(1)
      
      self.thread_queue.append(self.opts.worker(self.opts.source, pattern, count, size, self.opts.compression))
      self.filenames.append(pattern+self.opts.worker.ext)
      self.log(self.opts.verbose, 'Added Worker %s (offset:%d, size:%d)' % (pattern, count, size)) 
      count += size
      pattern = string_next(pattern)

  def start(self):
    self.threads = []

    for i in range(self.opts.threads):
      self.threads.append(BaseWorker('', '', 0, 0, 0))

    while(len(self.thread_queue) > 0):
      t = self.inactive_thread()
      if t > -1:
        if not self.threads[t].dummy:
          if not self.threads[t].status:
            self.log(True, 'Thread %s Failed to complete properly' % self.threads[t].dst)
            self.cleanup()
            sys.exit(1)
          self.log(self.opts.verbose, 'Thread %s completed' % self.threads[t].dst)
        self.threads[t] = self.thread_queue.pop()
        self.threads[t].start()
        self.log(self.opts.verbose, 'Thread %s started' % self.threads[t].dst)

    for t in self.threads:
      t.join()

  def log(self, display, message):
    if display: sys.stderr.write('[%s] %s%s' % (datetime.now(), message, os.linesep))

  def inactive_thread(self):
    for i in range(len(self.threads)):
      if not self.threads[i].isAlive(): return i
    return -1
  
  def combine(self):
    if os.path.exists(self.opts.destination):
      self.log(True, "%s Destination File Already Exists" % self.opts.destination)
      sys.exit(1)

    dest = open(self.opts.destination, 'wb')

    for f in self.filenames:
      src = open(f, 'rb')
      dest.writelines(src)
      src.close()
      if not self.opts.keep: os.remove(f)

    if not self.opts.keep: os.remove(self.opts.source)

ZpyZpr(ZpyZprOpts(sys.argv[1:]))
