-module(test_mapred_google).
-include("include/defines.hrl").
-export([benchmark/0, benchmark/2, benchmark/3]).

% takes the number of schedulers, the name(optional) of a directory containing some test files and/or an
% some test files and/or an atom(optional) to be searched.
% a parallel grep is performed, searching for that atom inside the test files
% and returning the paths of the files containing the atoms.
% speedup over 10 iterations is calculated
benchmark() ->
   % ?TESTDIR is a default folder containing files, used for test purposes,
   % and '?REGEX' is the atom to be searched
   benchmark(utils:get_schedulers(), ?REGEX, ?TESTDIR).
benchmark(Schedulers_Num, Input) ->
   case is_atom(Input) of
      true -> benchmark(Schedulers_Num, Input, ?TESTDIR);
      false -> benchmark(Schedulers_Num, ?REGEX, Input)
   end.
benchmark(Schedulers_Num, Atom, Testdir)->
   utils:set_schedulers(Schedulers_Num),
   Abspath = filename:join(utils:get_dirpath(),Testdir),
   io:format("> testing with ~w scheduler(s).~n", [Schedulers_Num]),
   io:format("> looking for the atom '~w' in the following path:~n",[Atom]),
   io:format("~s~n",[Abspath]),
   {_,Files} = file:list_dir_all(Abspath),
   Files_Num = length(Files),
   io:format("> ~p files are being processed:~n", [Files_Num]),
   lists:foreach(
         fun(I) ->
            io:format("file '~s'~n", [I])
         end,
         Files),

   io:format("~nrunning tests, please wait...~n~n"),

   Seq = fun() -> utils:seq_grep(Abspath, Atom) end,
   Par = fun() -> utils:par_grep(Abspath,Atom) end,
   Time_Seq = utils:test_loop(?TIMES,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Par = utils:test_loop(?TIMES,Par, []),
   Mean_Par = utils:mean(Time_Par),
   Median_Par = utils:median(Time_Par),
   Speedup_Par = utils:speedup(Mean_Seq,Mean_Par),
   io:format("---SUMMARY OF RESULTS---~n"),
   utils:report(?SEQ, Time_Seq, Mean_Seq, Median_Seq),
   utils:report(?GOOGLE, Time_Par, Mean_Par, Median_Par),
   io:format("speedup of the ~p version is ~p~n", [?GOOGLE, Speedup_Par]).
