# SkePi
Data parallel and stream parallel skeletons implemented in erlang.

## compilation and cleanup
Once you have moved to ske-pi's root directory, you can compile all sources to object code with the following command:
```
> make all
```

All object files can be removed by running the following command from ske-pi's root directory:
```
> make clean
```

## usage examples
Each module has its own usage function printing a brief description and giving a simple usage example.
<<<<<<< HEAD

=======
>>>>>>> 8a1055c473abacbaba8cd8a9f33e19749d372031
Once you have compiled ske-pi, you may:
* start the Erlang shell with
```
> cd ebin; erl
```
* call the usage function for a given module. Via the Erlang shell, use the following command:
```
> <module_name>:usage().
```
* additionally, you may test the stream parallel skeletons using an example with a default configuration. Like so:
```
> test_stream:benchmark().
```

* similarly, you may run the data parallel skeletons example. Write:
```
> test_mapred:benchmark().
```
* for the stream parallel skeletons example, it is possible to configure the length of the list, the length of the chunks onto which the list is split, the number of worker processes and the number of schedulers used (N.B. dependent on the machine used to run the tests)
* you may also configure the length of the list, the length of chunks and the number of schedulers used for the data parallel skeletons example.
* the erlang shell may be exited with the following command:
```
> q().
```

## acknowledgements
The stream skeleton ("stream.erl") is taken and then modified from the skel library by the ParaPhrase group, whilst the data parallel skeletons ("gmapred.erl" and "pmap.erl") are modified versions of Joe Armstrong's examples in his Programming Erlang 2nd ed. book.
