# sk-erl
Data parallel and stream parallel skeletons implemented in erlang

Usage:
* start the erl shell
* compile the modules utils, stream, pmap, preduce, pmapred, gmapred, test_stream, test_mapred using "c(MODULE_NAME)."
* to test the stream parallel skeletons using the default configuration, use "test_stream:benchmark()."
  - (it's possible to configure the length of the list, the length of the chunks onto which the list is split, the number of worker processes and the number of schedulers used (N.B. dependent on the machine used to run the tests))
* similarly, to run the data parallel skeletons, write "test_mapred:benchmark()."
  - (it's also possible to configure the length of the list, the length of chunks and the number of schedulers used)

# acknowledgements
The stream skeletons ("stream.erl" and "sstream.erl") are taken and then modified from the ParaPhrase/skel library by Sam Elliott, whilst the data parallel skeletons ("gmapred.erl" and "pmap.erl") are modified versions of Joe Armstrong's examples in his Programming Erlang 2nd ed. book.
