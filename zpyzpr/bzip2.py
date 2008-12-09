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

import bz2, StringIO
from zpyzpr import BaseWorker, ZpyZpr

class Bzip2Worker(BaseWorker):
  def get_compobj(self):
    return self.bz2.BZ2Compressor(self.comp)

class Bzip2(ZpyZpr):
  def __init__(self, **kwargs):
    ZpyZpr.__init__(self, worker=Bzip2Worker, **kwargs)

def compress(string, level=6, **kwargs):
  zz = Bzip2(compression=level, **kwargs)
  source = StringIO.StringIO(string)
  destin = StringIO.StringIO()
  zz.compressStream(source, destin)
  zz.flush()
  data = destin.getvalue()
  source.close()
  destin.close()
  return data
