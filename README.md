# SkePi
Data parallel and stream parallel skeletons implemented in erlang

## compilation and cleanup
Once you have moved to ske-pi's root directory, you can compile all sources to object code with the following command:
```
> make all
```

All object files can be removed by running the following command from ske-pi's root directory:
```
> make clean
```

## usage
Once you have compiled ske-pi, you may:
* start the erl shell with
```
> cd ebin; erl
```
* test the stream parallel skeletons using the default configuration. Via shell, use:
```
test_stream:benchmark().
```
  - (it's possible to configure the length of the list, the length of the chunks onto which the list is split, the number of worker processes and the number of schedulers used (N.B. dependent on the machine used to run the tests))
* similarly, you may run the data parallel skeletons. Write:
```
test_mapred:benchmark().
```
  - (it's also possible to configure the length of the list, the length of chunks and the number of schedulers used)

## acknowledgements
The stream skeleton ("stream.erl") is taken and then modified from the skel library by the ParaPhrase group, whilst the data parallel skeletons ("gmapred.erl" and "pmap.erl") are modified versions of Joe Armstrong's examples in his Programming Erlang 2nd ed. book.
