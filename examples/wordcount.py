#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymr import PyMR

class WordCount(PyMR):

    def mapper(self, line): # line: str
        import re
        words = re.findall(r'[a-z]+', line.lower())
        for word in words:
            yield word, 1
            
    def reducer(self, key, values): # key: str, values: Iterator[str]
        count = sum(int(v) for v in values)
        yield key, count

    

if __name__ == '__main__':
    WordCount('Word counter').run()
