#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
import abc
import fileinput
import sys
import argparse
import subprocess
import shlex
import os
from six.moves import map
from itertools import groupby
import six
import pickle

MAPPER, REDUCER = sys.argv[0] + ' map', sys.argv[0] + ' reduce',


class PyMR(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def mapper(self, line):
        """Method documentation"""
        return

    @abc.abstractmethod
    def reducer(self, key, values):
        count = sum(int(v) for v in values)
        yield key, count

    def _do_mapper(self, files):
#	print(os.listdir('.'), file=sys.stderr)	
        for line in fileinput.input(files):
            for key, value in self.mapper(line):
                yield str(key), str(value)

    # def _do_combiner(self, mapper_out):
    #     mapper_out = sorted(mapper_out, key=lambda x: x[0])
    #     for key, grouped_keyvalues in groupby(mapper_out,
    #                                           key=lambda x: x[0]):
    #         values = (v for k, v in grouped_keyvalues)
    #         for key, value in self.combiner(key, values):
    #             yield key, value

    def _do_reducer(self, files):
        def line_to_keyvalue(line):
            # print(line, file = sys.stderr)
            key, value = line.split('\t', 1)
            return key, value

        keyvalues = map(line_to_keyvalue, fileinput.input(files))
        for key, grouped_keyvalues in groupby(keyvalues,
                                              key=lambda x: x[0]):
            values = (v for k, v in grouped_keyvalues)
            for key, value in self.reducer(key, values):
                yield str(key), str(value)

    @staticmethod
    def _argparser(description):
        parser = argparse.ArgumentParser(description=description)
        subparsers = parser.add_subparsers(help='sub-commands', dest='cmd')
        
        parser_pipe = subparsers.add_parser('pipe', help='pipeline mode')
        parser_localmr = subparsers.add_parser('lmr', help='local mapreduce mode')
        parser_hstreaming = subparsers.add_parser('hadoop', help='Hadoop Streaming mode')
        parser_map = subparsers.add_parser('map', help='mapper mode')
        parser_reducer = subparsers.add_parser('reduce', help='reducer mode')
        
        parser_hstreaming.add_argument('INPUT', help='input folder/file in HDFS')
        parser_hstreaming.add_argument('OUTPUT', help='output path in HDFS')
        parser_hstreaming.add_argument('-j', '--hadoop-streaming-jar', metavar='PATH_TO_JAR',
                            help='hadoop streaming jar path. (default: %(default)s)',
default='/usr/lib/hadoop-mapreduce/hadoop-streaming.jar')
        parser_hstreaming.add_argument('-n',
            '--num-reducer', metavar='N', type=int, help='number of reducer. (default: %(default)s)', default=4)

                                       
        parser_localmr.add_argument('INPUT', help='input folder/file. `-\' for stdin')
        parser_localmr.add_argument('OUTPUT', help='output path')
        parser_localmr.add_argument('-c', '--lmr-cmd', metavar='LMR_CMD', help='lmr command. (default: %(default)s)', default='lmr')
        parser_localmr.add_argument('-s','--split-size', metavar='SIZE', help='size of splits. (default: %(default)s)', default='1m')

        parser_localmr.add_argument('-n',
            '--num-reducer', metavar='N', type=int, help='number of reducer. (default: %(default)s)', default=4)

        parser_pipe.add_argument('FILE', nargs = '*', help='input files')

        parser_map.add_argument('FILE', nargs = '*', help='input files')
        parser_reducer.add_argument('FILE', nargs = '*', help='input files')

        
        # parser.add_argument('--mapper-argument', help='argument for mapper') # TODO
        # parser.add_argument('--reducer-argument', help='argument for reducer') # TODO
        return parser.parse_args()

    def __init__(self, description = '', files=[]):
        self._files = files + [sys.argv[0], __file__]
        self.description = description

    def run(self):
        args = PyMR._argparser(self.description)
        # print(args, file= sys.stderr)
        if args.cmd == 'map':
            mapper_out = self._do_mapper(args.FILE)
            # mapper_out = do_combiner(mapper_out)
            for key, value in mapper_out:
                print('{}\t{}'.format(key, value))
                
        elif args.cmd == 'reduce':
            reducer_out = self._do_reducer(args.FILE)
            for key, value in reducer_out:
                print('{}\t{}'.format(key, value))
                
        elif args.cmd == 'pipe':
            _localpipe(args.FILE)
            
        elif args.cmd == 'lmr':
            _localmr(args.INPUT, args.OUTPUT, args.lmr_cmd, args.split_size, args.num_reducer)
            
        elif args.cmd == 'hadoop' :
            _hstreaming(args.INPUT, args.OUTPUT, args.num_reducer, args.hadoop_streaming_jar, self._files)


def _get_input_process(input):
    if input == '-':
        return sys.stdin
    input = os.path.normpath(input)
    assert os.access(input, os.R_OK)
    if os.path.isdir(input):
        input_files = os.listdir(input)
        # return '{}/*'.format(input)
    else:
        input_files = [input]

    return subprocess.Popen(['cat'] + input_files, stdout=subprocess.PIPE).stdout


def _localmr(input, output, lmr_cmd, split_size, num_reducer):
    
    input_data = _get_input_process(input)
    lmr_process = subprocess.Popen(
        [lmr_cmd, split_size, str(num_reducer), MAPPER, REDUCER, output], stdin=input_data)
    input_data.close()
    lmr_process.wait()


def _localpipe(input_files):
    input_process = subprocess.Popen(['cat'] + input_files, stdout=subprocess.PIPE)
    mapper_process = subprocess.Popen(
        shlex.split(MAPPER), stdin=input_process.stdout, stdout=subprocess.PIPE)
    input_process.stdout.close()

    sort_process = subprocess.Popen(
        shlex.split('sort -k1,1 -t"\t" -s'), stdin=mapper_process.stdout, stdout=subprocess.PIPE)
    mapper_process.stdout.close()

    reducer_process = subprocess.Popen(shlex.split(REDUCER), stdin=sort_process.stdout)
    sort_process.stdout.close()
    reducer_process.wait()


def _hstreaming(input, output, num_reducer, jar_path, files):
    devnull = open('/dev/null', 'w')

    yarn_command = 'yarn'
    try:
        subprocess.call(yarn_command, stdout=devnull, stderr=devnull)
    except OSError:
        yarn_command = 'hadoop'

    # files = ','.join(__file__, sys.argv[0])
    print(files, file=sys.stderr)

    subprocess.Popen([yarn_command, 'jar', jar_path, '-files', ','.join(files), '-mapper',
                      MAPPER, '-reducer', REDUCER, '-input', input, '-output', output])


# if __name__ == '__main__':
    # PyMR().run()
