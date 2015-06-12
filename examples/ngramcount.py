#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function

from pymr import PyMR


def ngrams(words):
    for length in range(1, 5 + 1):
        for ngram in zip(*(words[i:] for i in range(length))):
            yield ngram


class NgramCount(PyMR):
    
        

    def mapper(self, line):
        # from nltk.tokenize import word_tokenize
        # words = word_tokenize(line.lower())
        import re
        words = re.findall(r'[a-z]+', line.lower())
        for ngram in ngrams(words):
            yield ' '.join(ngram), 1

    def reducer(self, key, values):
        count = sum(int(v) for v in values)
        yield key, count

    # combiner = reducer


if __name__ == '__main__':
    NgramCount('N-gram counter').run()
