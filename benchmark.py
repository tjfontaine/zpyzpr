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

import sys, os, hashlib, StringIO, zlib, struct
from zpyzpr.gzip import Gzip, GZIP_HEADER
from zpyzpr.bzip2 import Bzip2
from datetime import datetime

def decompress(result):
  parts = result.split(GZIP_HEADER)
  data = ''
  idx = 1
  while idx < len(parts):
    part = parts[idx]
    start = 0
    end   = len(part)-struct.calcsize('<II')
    data += zlib.decompress(part[start:end], -zlib.MAX_WBITS)
    idx += 1
  return data

if __name__ == '__main__':
  block_sizes = [10*1024, 100*1024, 1000*1024, 10*1024*1024]
  threads = [2, 3, 4, 8, 16]
  compression = 6

  global_start = datetime.now()

  source_string = open('/dev/urandom', 'rb').read(100*1024*1024)
  source_hash = hashlib.sha1(source_string)
  original_size = len(source_string)

  for thread in threads:
    for block_size in block_sizes:
      begin = datetime.now()
      zz = Gzip(threads=thread, block_size=block_size)
      a = StringIO.StringIO(source_string)
      b = StringIO.StringIO()
      zz.compressStream(a, b)
      zz.flush()
      end = datetime.now()
      data = b.getvalue()
      size = len(data)
      data = decompress(data)
      result = hashlib.sha1(data)
      data = None
      del data

      if source_hash.hexdigest() != result.hexdigest():
        sys.stderr.write('src:%s%s' % (source_hash.hexdigest(), os.linesep))
        sys.stderr.write('dst:%s%s' % (result.hexdigest(), os.linesep))
        sys.stderr.write('Not the same hash as original' + os.linesep)
        sys.exit(2)

      a.close()
      b.close()
      sys.stderr.write('Time: %s | Size: %d\t| Threads: %d\t| Block Size: %d' % (str(end-begin), size-original_size,
                        thread, block_size)+os.linesep)

  end = datetime.now()
  sys.stderr.write('Total Time: %s' % str(end - global_start)+os.linesep)
