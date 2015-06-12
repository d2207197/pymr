# pymr

Simple python module for building MapReduce program.

Support following running mode:
- local pipe line
- local mapreduce (depends on [localmapreduce](github.com/d2207197/localmapreduce))
- hadoop streaming

## Usage



## Word Count Example

~~~py
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
    WordCount('Word Counter').run()
~~~

### Sub commands

Five sub commands.

~~~console
$./wordcount.py -h
usage: wordcount.py [-h] {pipe,lmr,hadoop,map,reduce} ...

Word Counter

positional arguments:
  {pipe,lmr,hadoop,map,reduce}
                        sub-commands
    pipe                pipeline mode
    lmr                 local mapreduce mode
    hadoop              Hadoop Streaming mode
    map                 mapper mode
    reduce              reducer mode

optional arguments:
  -h, --help            show this help message and exit
~~~
### Run in pipe mode

pipe mode help message

if no file were provided, read data from stdin.

~~~console
$ ./wordcount.py pipe -h 
usage: wordcount.py pipe [-h] [FILE [FILE ...]]

positional arguments:
  FILE        input files

optional arguments:
  -h, --help  show this help message and exit
~~~

running examples

~~~console
$ ./wordcount.py pipe input_file1 input_file2 > output_file # read from files
$ pv input_file | ./wordcount.py pipe                       # read from stdin
a       573
aagrawal        1
abandonment     1
ability 5
ablation        1
able    13
abms    3
abnormal        1
abound  1
about   10
above   1
absence 1
absent  1
absolute        1
absorption      2
abstract        2
...
~~~


### Run in local mapreduce mode

Download [localmapreduce](github.com/d2207197/localmapreduce) first.
Copy the `lmr` program to any directory in PATH environment variable, like '/usr/local/bin/lmr'. And install all prerequisites.

Read data from folder/file/stdin and store the results in OUTPUT folder. Before running it, make sure OUTPUT folder doesn't exist.

~~~console
$ ./wordcount.py lmr -h 
usage: wordcount.py lmr [-h] [-c LMR_CMD] [-s SIZE] [-n N] INPUT OUTPUT

positional arguments:
  INPUT                 input folder/file. `-' for stdin
  OUTPUT                output path

optional arguments:
  -h, --help            show this help message and exit
  -c LMR_CMD, --lmr-cmd LMR_CMD
                        [LMR] lmr command. (default: lmr)
  -s SIZE, --split-size SIZE
                        [LMR] size of splits. (default: 10m)
  -n N, --num-reducer N
                        [LMR/HSTREAMING]number of reducer. (default: 4)
~~~

running example
~~~console
$ ./wordcount.py lmr input_data out -s 0.5m -n 4
hashing script hashing.py.ZN0c
 >>> Temporary output directory for mapper created: mapper_tmp.l8vd
 >>> Mappers running...
           #11
 >>> Reducer running. Temporary input directory: mapper_tmp.l8vd
 >>> Cleaning...
 >>> Temporary directory deleted: mapper_tmp.l8vd

 * Output directory: out
 * Elasped time: 0:00:11
$ ls out
reducer-00  reducer-01  reducer-02  reducer-03
$ head out/reducer-00
a	22155
aa	2
aaa	12
aaai	2
aaam	1
aam	11
abandonment	2
abbreviation	1
abdominal	1
abduction	4
~~~

