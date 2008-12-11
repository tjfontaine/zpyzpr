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

from zpyzpr import MULTIPROCESSING
from datetime import datetime
import getopt, os, sys, traceback

try:
  from zpyzpr.gzip import Gzip
  GZIP_ENABLED = True
except:
  GZIP_ENABLED = False

try:
  from zpyzpr.bzip2 import Bzip2
  BZIP_ENABLED = True
except:
  BZIP_ENABLED = False

class ZpyZprOpts:
  def __init__(self, argv):
    sopt = '123456789cb:hjkt:vzT'
    lopt = ['help', 'keep', 'verbose', 'timing', 'gzip', 'bzip2', 'blocks=', 'compression=', 'threads=', 'stdin']
    self.verbose     = False
    self.timing      = False
    self.blocks      = None # Automaticly determined
    self.keep        = False
    self.compression = 6
    self.stdin       = False
    self.source      = None
    self.destination = None

    if GZIP_ENABLED:
      self.worker    = Gzip
    elif BZIP_ENABLED:
      self.worker    = Bzip2
    else:
      sys.stderr.write('No compression libraries available.' + os.linesep)
      sys.exit(2)

    self.threads     = self.worker.processor_count()+1 # Should this be determined magically?

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
      elif o is '--compression':
        self.compression = int(a)
      elif o in ('1', '2', '3', '4', '5', '6', '7', '8', '9'):
        self.compression = int(o)
      elif o in ('-T', '--timing'):
        self.timing = True
      elif o in ('-z', '--gzip'):
        if GZIP_ENABLED:
          self.worker = Gzip
        else:
          sys.stderr.write('zlib module not available for compression' + os.linesep)
          sys.exit(2)
      elif o in ('-j', '--bzip2'):
        if BZIP_ENABLED:
          self.worker = Bzip2
        else:
          sys.stderr.write('bz2 module not available for compression' + os.linesep)
          sys.exit(2)
      elif o in ('-c', '--stdin'):
        self.stdin = True
    
    if not self.stdin and (len(args) < 1 or len(args) > 2):
      sys.stderr.write('Wrong number of arguments passed.' + os.linesep)
      self.usage(True)
      sys.exit(2)
    elif not self.stdin:
      self.source = args[0]

      if len(args) == 2:
        self.destination = args[1]
      else:
        self.destination = self.source
        if self.worker is Gzip:
          self.destination += '.gz'
        elif self.worker is Bzip2:
          self.destination += '.bz2'
        else:
          sys.stderr.write('Cannot determine destination extension'+os.linesep)
          sys.exit(2)

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
    if GZIP_ENABLED:
      gzip_enabled += 'enabled'
    else:
      gzip_enabled += 'disabled'


    bzip_enabled = 'bzip2 compression is '
    if BZIP_ENABLED:
      bzip_enabled += 'enabled'
    else:
      bzip_enabled += 'disabled'

    p('zz [opts] <sourcefile> [destinationfile]'+e)
    p(''+e)
    p('-b --blocks=       Specify the logical block size for each compressed block'+e)
    p('                     (Default: 10M or the size of the file divided by the number of threads)'+e)
    p('-N --compression=  Compression Level (Default: 6)'+e)
    p('                     -1 -2 .. -9'+e)
    p('-c --stdin         Read from standard input, output to standard out'+e)
    p('-h --help          Prints this message'+e)
    p('-j --bzip2         Use bzip2 compression'+e)
    p('                     '+bzip_enabled+e)
    p('-k --keep          Keep source files (The original source and intermediate slices)'+e)
    p('-t --threads=      Specify the number compression threads (Default: 4)'+e)
    p('-T --timing        Prints timings only'+e)
    p('-z --gzip          Use gzip compression (Default)'+e)
    p('                     '+gzip_enabled+e)
    p('-v --verbose       Prints timings and other debug information'+e)


if __name__ == '__main__':
  opts = ZpyZprOpts(sys.argv[1:])

  zz = opts.worker(threads=opts.threads,
                   block_size=opts.blocks,
                   debug=opts.verbose,
                   logger=sys.stderr)

  try:
    zz.log(opts.timing, 'Beginning Compression using %s (%d Threads)' % (MULTIPROCESSING, opts.threads))
    begin = datetime.now()

    source = open(opts.source, 'rb')
    destin = open(opts.destination, 'wb')

    zz.compressStream(source, destin)
    zz.flush()
    source.close()
    destin.close()

    if not opts.stdin and not opts.keep: os.remove(opts.source)

    end = datetime.now()
    zz.log(opts.timing, 'Total Time: ' + str(end - begin))

  except Exception, ex:
    zz.flush(err=True)
    if not opts.stdin:
      destin.close()
      os.remove(opts.destination)

    if not opts.stdin:
      source.close()

    zz.log(True, repr(ex))
    traceback.print_exc(file=zz.logger)
    sys.exit(1)
