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

import zlib, struct
from zpyzpr import BaseWorker, ZpyZpr

#GZIP_HEADER = struct.pack("<BBBBBBBBBB", 31, 139, 8, 0, 0,0,0,0, 2, 3)
GZIP_HEADER = '\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\x03'

class GzipWorker(BaseWorker):
  def get_compobj(self):
    return zlib.compressobj(self.comp, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)

  def header(self):
    return GZIP_HEADER

  def suffix(self):
    return struct.pack('<II', zlib.crc32(self.raw_data) & 0xFFFFFFFF, self.fsize)

class Gzip(ZpyZpr):
  def __init__(self, **kwargs):
    ZpyZpr.__init__(self, worker=GzipWorker, **kwargs)
