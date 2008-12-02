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

import sys, os
from zz import ZpyZpr, GzipWorker
from datetime import datetime

if __name__ == '__main__':
  block_sizes = [1024, 10*1024, 100*1024, 1000*1024, 10*1024*1024]
  threads = [1, 2, 3, 4, 8, 16]
  compression = 6

  global_start = datetime.now()
  original_size = os.stat(sys.argv[1]).st_size

  for thread in threads:
    for block_size in block_sizes:
      begin = datetime.now()
      zz = ZpyZpr(sourceFile=sys.argv[1], destinationFile=sys.argv[2], worker=GzipWorker, threads=thread, block_size=block_size)
      zz.start()
      zz.combine()
      zz.cleanup()
      end = datetime.now()
      size = os.stat(sys.argv[2]).st_size
      sys.stderr.write('Time: %s | Size: %d\t| Threads: %d\t| Block Size: %d' % (str(end-begin), size-original_size,
                        thread, block_size)+os.linesep)
      os.remove(sys.argv[2])

  end = datetime.now()
  sys.stderr.write('Total Time: %s' % str(end - global_start)+os.linesep)
