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

MAPPER, REDUCER = sys.argv[0] + ' -m', sys.argv[0] + ' -r',


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
        mode_group = parser.add_mutually_exclusive_group(required=True)
        mode_group.add_argument(
            '-H', '--hadoop-streaming', nargs=2, metavar=('INPUT', 'OUTPUT'), help='run in Hadoop mode. (INPUT: a folder, file in HDFS)')
        mode_group.add_argument(
            '-L', '--local-mr', nargs=2, metavar=('INPUT', 'OUTPUT'), help='run in local mapreduce mode. (INPUT: a folder, file or `-\'(stdin))')
        mode_group.add_argument(
            '-P', '--local-pipe', nargs='*', metavar='FILE', help='run in pipeline mode')
        mode_group.add_argument(
            '-r', '--reducer', nargs='*', metavar='FILE', help='Reducer mode')
        mode_group.add_argument(
            '-m', '--mapper', nargs='*', metavar='FILE', help='Mapper mode')
        parser.add_argument(
            '--num-reducer', metavar='N', type=int, help='number of reducer', default=4)
        parser.add_argument('--split-size', metavar='SIZE', help='size of splits', default='10m')
        parser.add_argument('--lmr-cmd', metavar='LMR', help='lmr command', default='lmr')
        parser.add_argument('--hadoop-streaming-jar', metavar='PATH',
                            help='hadoop streaming jar path', default='/usr/lib/hadoop-mapreduce/hadoop-streaming.jar')
        # parser.add_argument('--mapper-argument', help='argument for mapper') # TODO
        # parser.add_argument('--reducer-argument', help='argument for reducer') # TODO
        return parser.parse_args()

    def __init__(self, description = '', files=[]):
        self._files = files + [sys.argv[0], __file__]
        self.description = description

    def run(self):
        args = PyMR._argparser(self.description)
        # print(args, file= sys.stderr)
        if args.mapper is not None:
            mapper_out = self._do_mapper(args.mapper)
            # mapper_out = do_combiner(mapper_out)
            for key, value in mapper_out:
                print('{}\t{}'.format(key, value))
        elif args.reducer is not None:
            reducer_out = self._do_reducer(args.reducer)
            for key, value in reducer_out:
                print('{}\t{}'.format(key, value))
        elif args.localpipe is not None:
            input_files = args.localpipe
            _localpipe(input_files)
        elif args.localmr is not None:
            input, output = args.localmr
            _localmr(input, output, args.lmr_cmd, args.split_size, args.num_reducer)
        elif args.hadoop_streaming is not None:
            input, output = args.hadoop_streaming
            _hstreaming(input, output, args.num_reducer, args.hadoop_streaming_jar, self._files)


def _get_input_process(input):
    if input == '-':
        print('-')
        return sys.stdin
    input = os.path.normpath(input)
    assert os.access(input, os.R_OK)
    if os.path.isdir(input):
        input_files = os.listdir(input)
        # return '{}/*'.format(input)
    else:
        input_files = [input]

    return subprocess.Popen(['cat'] + input_files, stdout=subprocess.PIPE).stdout

    # return 'pv {}'.format(input)
    # else:
    # raise Exception('Input is not file nor directory: {}'.format(input))


def _localmr(input, output, lmr_cmd, split_size, num_reducer):
    # input_files = _input_files(input)
    # print(input_cmd)
    input_data = _get_input_process(input)
    lmr_process = subprocess.Popen(
        [lmr_cmd, split_size, str(num_reducer), MAPPER, REDUCER, output], stdin=input_data)
    # lmr_process = subprocess.Popen(['cat'], stdin=input_process.stdout)
    # # input_process.stdout.close()
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
    subprocess.Popen([yarn_command, 'jar', jar_path, '-files', files, '-mapper',
                      MAPPER, '-reducer', 'REDUCER', '-input', input, '-output', output])


# if __name__ == '__main__':
    # PyMR().run()
