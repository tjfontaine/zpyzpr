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

from datetime import datetime
import getopt, os, sys, string, signal, struct, time

try:
  from multiprocessing import Process as Thread, Queue, Pipe
  from Queue import Empty
  MULTIPROCESSING = 'multiprocessing'
except Exception, ex:
  try:
    from processing import Process as Thread, Queue, Pipe
    from Queue import Empty
    MULTIPROCESSING = 'processing'
  except Exception, ex:
    from threading import Thread
    from Queue import Queue, Empty
    MULTIPROCESSING = 'threading'
    class FakePipe:
      def __init__(self):
        self.q = Queue()

      def recv(self):
        return self.q.get()

      def send(self, value):
        return self.q.put(value)

      def close(self):
        return True

    def Pipe():
      q = FakePipe()
      return (q, q)

CHUNK_SIZE_BYTES = 10*1024*1024 # 10 MB
BLOCK_SIZE = 1024
#GZIP_HEADER = struct.pack("<BBBBBBBBBB", 31, 139, 8, 0, 0,0,0,0, 2, 3)
GZIP_HEADER = '\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\x03'

class BaseWorker(Thread):
  ext = '.dummy'
  def __init__(self, threadid, compression, queue, pipe):
    Thread.__init__(self)
    self.threadid = threadid
    self.comp = compression
    self.queue = queue
    self.pipe = pipe

  def get_item(self):
    try:
      item = self.pipe.recv()
      if item == 'STOP':
        self.running = False
        return None
      else:
        return item
    except EOFError:
      return None

  def run(self):
    self.running = True
    while self.running:
      item = self.get_item()
      if item:
        compobj = self.get_compobj()
        (self.raw_data, place) = item
        self.fsize = len(self.raw_data)
        data = compobj.compress(self.raw_data)
        data += compobj.flush() 
        self.queue.put((self.threadid, place, self.header(), self.suffix(), data))
        data = None
        self.raw_data = None

class GzipWorker(BaseWorker):
  try:
    import zlib
    enabled = True
  except:
    enabled = False

  ext = '.gz'

  def get_compobj(self):
    return self.zlib.compressobj(self.comp, self.zlib.DEFLATED, -self.zlib.MAX_WBITS, self.zlib.DEF_MEM_LEVEL, 0)

  def header(self):
    return GZIP_HEADER

  def suffix(self):
    return struct.pack('<II', self.zlib.crc32(self.raw_data) & 0xFFFFFFFF, self.fsize)


class Bzip2Worker(BaseWorker):
  try:
    import bz2
    enabled = True
  except:
    enabled = False

  ext = '.bz2'

  def get_compobj(self):
    return self.bz2.BZ2Compressor(self.comp)

  def header(self):
    return ''

  def suffix(self):
    return ''


class ZpyZprOpts:
  def __init__(self, argv):
    sopt = 'b:c:hjkt:vzT'
    lopt = ['help', 'keep', 'verbose', 'timing', 'gzip', 'bzip2', 'blocks=', 'compression=', 'threads=']
    self.verbose     = False
    self.timing      = False
    self.blocks      = None # Automaticly determined
    self.threads     = 4 # Should this be determined magically?
    self.keep        = False
    self.compression = 6

    if GzipWorker.enabled:
      self.worker    = GzipWorker
    elif Bzip2Worker.enabled:
      self.worker    = Bzip2Worker
    else:
      sys.stderr.write('No compression libraries available.' + os.linesep)
      sys.exit(2)

    try:
      opts, args = getopt.getopt(argv, sopt, lopt)
    except getopt.GetoptError, err:
      sys.stderr.write(str(err) + os.linesep)
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
      elif o in ('-T', '--timing'):
        self.timing = True
      elif o in ('-z', '--gzip'):
        if GzipWorker.enabled:
          self.worker = GzipWorker
        else:
          sys.stderr.write('zlib module not available for compression' + os.linesep)
          sys.exit(2)
      elif o in ('-j', '--bzip2'):
        if Bzip2Worker.enabled:
          self.worker = Bzip2Worker
        else:
          sys.stderr.write('bz2 module not available for compression' + os.linesep)
          sys.exit(2)
    
    if len(args) < 1 or len(args) > 2:
      sys.stderr.write('Wrong number of arguments passed.' + os.linesep)
      self.usage(True)
      sys.exit(2)

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

    gzip_enabled = 'gzip compression is '
    if GzipWorker.enabled:
      gzip_enabled += 'enabled'
    else:
      gzip_enabled += 'disabled'


    bzip_enabled = 'bzip2 compression is '
    if Bzip2Worker.enabled:
      bzip_enabled += 'enabled'
    else:
      bzip_enabled += 'disabled'

    p('zz [opts] <sourcefile> [destinationfile]'+e)
    p(''+e)
    p('-b --blocks=       Specify the logical block size for each compressed block'+e)
    p('                     (Default: 10M or the size of the file divided by the number of threads)'+e)
    p('-c --compression=  Compression Level (Default: 6)'+e)
    p('-h --help          Prints this message'+e)
    p('-j --bzip2         Use bzip2 compression'+e)
    p('                     '+bzip_enabled+e)
    p('-k --keep          Keep source files (The original source and intermediate slices)'+e)
    p('-t --threads=      Specify the number compression threads (Default: 4)'+e)
    p('-T --timing        Prints timings only'+e)
    p('-z --gzip          Use gzip compression (Default)'+e)
    p('                     '+gzip_enabled+e)
    p('-v --verbose       Prints timings and other debug information'+e)

class ZpyZpr:
  def __init__(self, opts):
    self.opts = opts
    self.event_queue = Queue()
    self.result_file = None

    self.source_size = os.stat(opts.source).st_size

    self.source = open(opts.source, 'rb')

    self.prepare_threads()

    b = datetime.now()
    self.log(self.opts.timing, "Beginning Compression using %s (%d Pieces/%d Threads)" % (MULTIPROCESSING, len(self.thread_queue), self.opts.threads))
    self.start()

    self.log(self.opts.verbose, "Combing Leftover Slices")
    self.combine()
    e = datetime.now()

    self.result_file.close()
    if not self.opts.keep: os.remove(self.opts.source)

    self.log(self.opts.timing, 'Total Time: '+str(e - b))

    self.source.close()

    self.cleanup()

  def cleanup(self, err=False):
    self.log(self.opts.verbose, 'Joining all threads')
    for t,p in self.threads:
      p.send('STOP')
      p.close()
      t.join()

    if self.result_file:
      self.result_file.close()

    if err and os.path.exists(self.opts.destination):
      os.remove(self.opts.destination)

    self.source.close()

  def prepare_threads(self):
    self.thread_queue = []
    self.completed = []
    self.last_completed = -1
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

    count = 0
    while(count < self.source_size):
      if self.source_size - count < bsize:
        size = self.source_size - count
      else:
        size = bsize

      self.thread_queue.append((len(self.thread_queue), count, size))
      self.completed.append(None)
      self.log(self.opts.verbose, 'Added Worker %d (offset:%d, size:%d)' % (len(self.thread_queue), count, size))
      count += size

  def get_item(self):
    try:
      e = self.event_queue.get(timeout=0.25)
      return e
    except Empty:
      return None

  def run_queue(self):
    item = self.get_item()
    while item:
      (threadid, place, header, suffix, data) = item
      self.log(self.opts.verbose, 'Completed Piece %d' % (place+1))
      self.completed[place] = (header, suffix, data)
      if len(self.thread_queue) > 0:
        self.send_next_block(threadid)
      self.combine()
      item = self.get_item()

  def send_next_block(self, threadid):
    (place, offset, size) = self.thread_queue.pop()
    self.log(self.opts.verbose, 'Started Piece %d' % (place+1))
    self.source.seek(offset)
    self.threads[threadid][1].send((self.source.read(size), place))

  def start(self):
    self.thread_queue.reverse()
    self.last_started = 0
    self.threads = []

    for i in range(self.opts.threads):
      threadid = len(self.threads)
      (parent, client) = Pipe()
      t = self.opts.worker(threadid, self.opts.compression, self.event_queue, client)
      self.threads.append((t, parent))
      t.start()
      self.send_next_block(threadid)

    while self.last_completed < len(self.completed):
      self.run_queue()
      self.log(self.opts.verbose, 'Progress %d/%d' % (self.last_completed, len(self.completed)))

  def log(self, display, message):
    if display: sys.stderr.write('[%s] %s%s' % (datetime.now(), message, os.linesep))

  def combine(self):
    if not self.result_file:
      if os.path.exists(self.opts.destination):
        self.log(True, "%s Destination File Already Exists" % self.opts.destination)
        sys.exit(1)
      self.result_file = open(self.opts.destination, 'wb')

    next_block = self.last_completed + 1

    while(next_block < len(self.completed) and self.completed[next_block]):
      t = self.completed[next_block]
      self.completed[next_block] = None
      (header, suffix, data) = t
      t = None
      self.log(self.opts.verbose, "Combined %s" % (next_block+1))
      src = self.result_file
      src.write(header)
      src.write(data)
      src.write(suffix)
      data = None
      del data

      self.last_completed = next_block
      next_block += 1

    if next_block == len(self.completed): self.last_completed += 1

ZpyZpr(ZpyZprOpts(sys.argv[1:]))
