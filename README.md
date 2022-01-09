# sk-erl
Data parallel and stream parallel skeletons implemented in erlang

Usage:
- start erl shell
- compile utils, sstream, stream, pmar, preduce, gmapred, smapred, test_stream, test_mapred
- to test the stream parallel skeletons using the default configuration, write "test_stream:benchmark()"
- similarly, to run the data parallel skeletons, write "test_mapred:benchmark()"
