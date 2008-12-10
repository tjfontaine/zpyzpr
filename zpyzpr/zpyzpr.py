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
import os, sys, signal

try:
  from multiprocessing import Process as Thread, Queue, Pipe
  from Queue import Empty
except Exception, ex:
  try:
    from processing import Process as Thread, Queue, Pipe
    from Queue import Empty
  except Exception, ex:
    from threading import Thread
    from Queue import Queue, Empty
    import subprocess
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

class BaseWorker(Thread):
  def __init__(self, threadid, compression, queue, pipe):
    Thread.__init__(self)
    self.threadid = threadid
    self.comp = compression
    self.queue = queue
    self.pipe = pipe

  def header(self):
    return ''

  def suffix(self):
    return ''

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

        compobj = self.get_compobj()
        data = compobj.compress(self.raw_data)
        data += compobj.flush()

        self.queue.put((self.threadid, place, self.header(), self.suffix(), data))
        data = None
        self.raw_data = None

class ZpyZpr:
  def __init__(self, worker=None, threads=4,
                     block_size=CHUNK_SIZE_BYTES, compression=6,
                     debug=False, logger=sys.stderr):
    self.event_queue = Queue()
    self.completed = {}
    self.last_completed = -1
    self.next_place = 0
    self.total_read = 0
    self.threads = []
    self.idle_threads = []
    self.eof_reached = False

    self.thread_count = threads
    self.debug = debug
    self.block_size = block_size
    self.compression = compression
    self.worker = worker
    self.logger = logger

    if not self.worker: raise Exception('Cannot initialize compression worker')

    if not self.block_size: self.block_size = CHUNK_SIZE_BYTES

    for i in range(self.thread_count):
      threadid = len(self.threads)
      (parent, client) = Pipe()
      t = self.worker(threadid, self.compression, self.event_queue, client)
      self.threads.append((t, parent))
      t.start()
      self.idle_threads.append(threadid)

  def flush(self, err=False):
    self.log(self.debug, 'Joining all threads')
    for t,p in self.threads:
      p.send('STOP')
      p.close()
      t.join()

    if not err: self.__combine()

  def __get_item(self):
    try:
      e = self.event_queue.get(timeout=0.20)
      return e
    except Empty:
      return None

  def __run_queue(self):
    item = self.__get_item()
    while item:
      (threadid, place, header, suffix, data) = item
      self.log(self.debug, 'Thread %d Completed Piece %d' % (threadid, place+1))
      self.completed[place] = (header, suffix, data)
      self.idle_threads.append(threadid)
      self.__combine()
      self.__send_next_block()
      item = self.__get_item()

  def __send_next_block(self):
    count = len(self.idle_threads)
    while count > 0:
      count -= 1
      threadid = self.idle_threads.pop()
      data = self.__read_next()
      if data:
        place = self.next_place
        self.log(self.debug, 'Thread %d Started Piece %d' % (threadid, place+1))
        self.threads[threadid][1].send((data, place))
        self.next_place += 1
      else:
        self.idle_threads.append(threadid)

  def __read_next(self):
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

  def compressStream(self, source, destination):
    self.source = source
    self.result_file = destination

    self.__send_next_block()

    while self.__still_reading():
      self.__run_queue()
      self.__send_next_block()

  def log(self, display, message):
    if display: self.logger.write('[%s] %s%s' % (datetime.now(), message, os.linesep))

  def __combine(self):
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
