# SkePi
Data parallel and stream parallel skeletons implemented in erlang.

## compilation, debug and cleanup
Once you have moved to ske-pi's root directory, you can compile all sources to object code.


` make all ` or ` make ` may be used to compile with debug mode turned on.

In this mode, some information regarding inter-process communication (and timeouts, if any have occurred), as well as function calls for each skeleton will be saved to a log file (located inside the *ske-pi/logs* directory by default).

` make all DEBUG= ` or ` make DEBUG= ` will compile with debug mode turned off.

` make clean ` may be used to remove the directory containing all the object files and keep the log files.

` make cleanall ` will remove both object files and the log files generated previously.

## usage examples
Each module has its own usage function printing a brief description and giving a simple usage example.


Once you have compiled ske-pi, you may:
* start the Erlang shell with ` cd ebin; erl ` from ske-pi's root directory
* call the usage function for a given skeleton. Via the Erlang shell, use the following command:
```
> <module_name>:usage().
```

* additionally, you may test the stream parallel skeletons' performance using an example with a default configuration. Like so:
```
> test_stream:benchmark().
```

* similarly, you may run the data parallel skeletons example. Write:
```
> test_mapred:benchmark().
```

* the above is also available for the skeletons implementing the google mapreduce skeleton. Try it with:
```
> test_mapred_google:benchmark().
```

* for the stream parallel skeletons example, it is possible to configure the length of the list, the length of the chunks onto which the list is split, the number of worker processes and the number of schedulers used (N.B. dependent on the machine used to run the tests)
* you may also configure the length of the list, the length of chunks and the number of schedulers used for the data parallel skeletons example
* for the google mapreduce example, you may input the name of a directory containing your own test files, an atom to be used to perform an unix grep-like operation on the test files and the number of scherdulers used (please note that the directory with your own test files **must** be located inside ske-pi's top directory and it **must** be in string format)
* the erlang shell may be exited with the following command: ` q(). `

## acknowledgements
The stream skeleton (*stream.erl*) is taken and then modified from the [skel](https://github.com/ParaPhrase/skel) library for the [ParaPhrase Project](http://calvados.di.unipi.it/paragroup/projects/).
