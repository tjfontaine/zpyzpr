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
import os, sys, signal, struct, time

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
    import subprocess
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

CHUNK_SIZE_BYTES = 1024000 # 1000K
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
        (self.raw_data, place) = item
        self.fsize = len(self.raw_data)

        if not self.popen:
          compobj = self.get_compobj()
          data = compobj.compress(self.raw_data)
          data += compobj.flush()
        else:
          p = subprocess.Popen(self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
          (data, stderr) = p.communicate(self.raw_data)

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

  def __init__(self, threadid, compression, queue, pipe):
    BaseWorker.__init__(self, threadid, compression, queue, pipe)
    if MULTIPROCESSING == 'threading':
      try:
        p = subprocess.Popen(['gzip', '-L'], stdout=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        self.command = ['gzip', '-c', '-%d' % self.comp]
        self.popen = True
      except:
        self.popen = False
    else:
      self.popen = False

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

  def __init__(self, threadid, compression, queue, pipe):
    BaseWorker.__init__(self, threadid, compression, queue, pipe)
    if MULTIPROCESSING == 'threading':
      try:
        p = subprocess.Popen(['bzip2', '-L'], stdout=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        self.command = ['bzip2', '-c', '-%d' % self.comp]
        self.popen = True
      except:
        self.popen = False
    else:
      self.popen = False

  def get_compobj(self):
    return self.bz2.BZ2Compressor(self.comp)

  def header(self):
    return ''

  def suffix(self):
    return ''


class ZpyZpr:
  def __init__(self, sourceFile=None, destinationFile=None,
                     worker=GzipWorker, stdin=False, threads=4,
                     block_size=CHUNK_SIZE_BYTES, compression=6,
                     debug=False):
    self.event_queue = Queue()
    self.completed = {}
    self.last_completed = -1
    self.next_place = 0
    self.total_read = 0
    self.threads = []
    self.eof_reached = False

    self.stdin = stdin
    self.thread_count = threads
    self.debug = debug
    self.destinationFile = destinationFile
    self.block_size = block_size
    self.compression = compression
    self.worker = worker

    if not self.block_size:
      self.block_size = CHUNK_SIZE_BYTES
      self.log(self.debug, 'No Block Size Defined Using Default: %d' % self.block_size)

    if not self.stdin:
      self.source_size = os.stat(sourceFile).st_size
      self.source = open(sourceFile, 'rb')

      if self.source_size < self.block_size * self.thread_count:
        self.block_size = self.source_size / self.thread_count
        self.log(self.debug, 'Source size is smaller than block size using source/thread: %d' % self.block_size)

      if os.path.exists(self.destinationFile):
        raise Exception("%s Destination File Already Exists" % self.destinationFile)

      self.result_file = open(self.destinationFile, 'wb')
    else:
      self.source_size = -1
      self.source = sys.stdin
      self.result_file = sys.stdout

  def cleanup(self, err=False):
    self.log(self.debug, 'Joining all threads')
    for t,p in self.threads:
      p.send('STOP')
      p.close()
      t.join()

    if not self.stdin and self.result_file:
      self.result_file.close()

    if err and not self.stdin and os.path.exists(self.destinationFile):
      os.remove(self.destinationFile)

    if not self.stdin:
      self.source.close() 
      self.result_file.close()

  def __get_item(self):
    try:
      e = self.event_queue.get(timeout=0.25)
      return e
    except Empty:
      return None

  def __run_queue(self):
    item = self.__get_item()
    while item:
      (threadid, place, header, suffix, data) = item
      self.log(self.debug, 'Completed Piece %d' % (place+1))
      self.completed[place] = (header, suffix, data)
      self.__send_next_block(threadid)
      self.combine()
      item = self.__get_item()

  def __send_next_block(self, threadid):
    place = self.next_place
    data = self.__read_next()
    if data:
      self.log(self.debug, 'Started Piece %d' % (place+1))
      self.threads[threadid][1].send((data, place))
      self.next_place += 1

  def __read_next(self):
    if not self.stdin:
      loc = self.source.tell()

    data = self.source.read(self.block_size)
    self.total_read += len(data)

    if data == '':
      self.eof_reached = True
      return None
    else:
      self.log(self.debug, 'Read another %d (%d total read)' % (len(data), self.total_read))
      return data

  def __still_reading(self):
    # Succintly put
    #return not self.eof_reached or self.last_completed < self.next_place-1
    if not self.eof_reached: #If we haven't reached the end of the stream keep processing
      return True
    elif self.last_completed < self.next_place-1: #finished the stream, but haven't completed compression
      return True
    else:
      return False #we're done reading and compressing

  def start(self):
    for i in range(self.thread_count):
      threadid = len(self.threads)
      (parent, client) = Pipe()
      t = self.worker(threadid, self.compression, self.event_queue, client)
      self.threads.append((t, parent))
      t.start()
      self.__send_next_block(threadid)

    while self.__still_reading():
      self.__run_queue()

  def log(self, display, message):
    if display: sys.stderr.write('[%s] %s%s' % (datetime.now(), message, os.linesep))

  def combine(self):
    next_block = self.last_completed + 1

    while(self.completed.has_key(next_block)):
      t = self.completed[next_block]
      del self.completed[next_block]
      (header, suffix, data) = t
      t = None
      self.log(self.debug, "Combined %s" % (next_block+1))
      src = self.result_file
      src.write(header)
      src.write(data)
      src.write(suffix)
      data = None
      del data

      self.last_completed = next_block
      next_block += 1
