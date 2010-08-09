# Copyright (c) 2010 Timothy J Fontaine <tjfontaine@gmail.com>
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

import pylzma, struct, binascii
from cStringIO import StringIO

from zpyzpr import BaseWorker, ZpyZpr

#LZIP_HEADER = struct.pack('<BBBBB', ord('L'), ord('Z'), ord('I'), ord('P'), 0x01, 0x17)
LZIP_HEADER = 'LZIP\x01\x17'

class LzmaCompObj:
  def compress(self, data):
    return pylzma.compress(data)[5:]

  def flush(self):
    return ''

class LzipWorker(BaseWorker):
  def get_compobj(self):
    return LzmaCompObj()

  def header(self):
    return LZIP_HEADER

  def suffix(self):
    crc = binascii.crc32(self.raw_data)
    # member size includes header and trailer
    # header is 6 bytes, trailer is 20
    return struct.pack('<IQQ', crc & 0xffffffff, self.fsize, len(self.data)+6+20)

class Lzip(ZpyZpr):
  def __init__(self, **kwargs):
    ZpyZpr.__init__(self, worker=LzipWorker, **kwargs)
