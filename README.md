# pymr

Simple python module for building MapReduce program.

Support following running mode:
- local pipe line
- local mapreduce (depends on [localmapreduce](github.com/d2207197/localmapreduce))
- hadoop streaming

## Install

~~~console
$ pip install https://github.com/d2207197/pymr/archive/master.zip
~~~


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
$ ./wordcount.py -h
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

### Run in Hadoop mode

hadoop mode based on the `hadoop-streaming.jar`. You have to set correct path of the streaming jar file.

~~~console
$ ./ngramcount.py hadoop -h
usage: ngramcount.py hadoop [-h] [-j PATH_TO_JAR] [-n N] INPUT OUTPUT

positional arguments:
  INPUT                 input folder/file in HDFS
  OUTPUT                output path in HDFS

optional arguments:
  -h, --help            show this help message and exit
  -j PATH_TO_JAR, --hadoop-streaming-jar PATH_TO_JAR
                        [HSTREAMING] hadoop streaming jar path. (default:
                        /usr/lib/hadoop-mapreduce/hadoop-streaming.jar)
  -n N, --num-reducer N
                        [LMR/HSTREAMING]number of reducer. (default: 4)
~~~

running example
~~~console
$ ./ngramcount.py hadoop input_data output_data
packageJobJar: [] [/usr/lib/hadoop-mapreduce/hadoop-streaming-2.6.0-cdh5.4.2.jar] /tmp/streamjob5352956424634706112.jar tmpDir=null
15/06/12 23:50:55 INFO client.RMProxy: Connecting to ResourceManager at lost.nlpweb.org/140.114.89.221:8032
15/06/12 23:50:55 INFO client.RMProxy: Connecting to ResourceManager at lost.nlpweb.org/140.114.89.221:8032
15/06/12 23:50:56 INFO lzo.GPLNativeCodeLoader: Loaded native gpl library
15/06/12 23:50:56 INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev 8e266e052e423af592871e2dfe09d54c03f6a0e8]
15/06/12 23:50:56 INFO mapred.FileInputFormat: Total input paths to process : 1
15/06/12 23:50:56 INFO mapreduce.JobSubmitter: number of splits:50
15/06/12 23:50:56 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1433987940295_0060
15/06/12 23:50:56 INFO impl.YarnClientImpl: Submitted application application_1433987940295_0060
15/06/12 23:50:57 INFO mapreduce.Job: The url to track the job: http://lost.nlpweb.org:8088/proxy/application_1433987940295_0060/
15/06/12 23:50:57 INFO mapreduce.Job: Running job: job_1433987940295_0060
15/06/12 23:51:03 INFO mapreduce.Job: Job job_1433987940295_0060 running in uber mode : false
15/06/12 23:51:03 INFO mapreduce.Job:  map 0% reduce 0%
15/06/12 23:51:18 INFO mapreduce.Job:  map 1% reduce 0%
15/06/12 23:51:19 INFO mapreduce.Job:  map 3% reduce 0%
...
~~~
