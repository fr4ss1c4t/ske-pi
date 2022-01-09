# sk-erl
Data parallel and stream parallel skeletons implemented in erlang

Usage:
* start the erl shell
* compile utils, sstream, stream, pmap, preduce, pmapred, gmapred, smapred, test_stream, test_mapred
* to test the stream parallel skeletons using the default configuration, write "test_stream:benchmark()"
  - (it's possible to configure the length of the list, the length of the chunks onto which the list is split, the number of worker processes and the number of schedulers used (N.B. dependent on the machine used to run the tests))
* similarly, to run the data parallel skeletons, write "test_mapred:benchmark()"
  - (it's also possible to configure the length of the list, the length of chunks and the number of schedulers used)

# acknowledgements
The stream skeletons are taken and then modified from the ParaPhrase/skel software, whilst the data parallel skeletons are modified versions of Joe Armstrong's.
