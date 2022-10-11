-module(test_mapred_google).
-include("include/defines.hrl").
-include("include/usages.hrl").
-export([benchmark/0, benchmark/2, benchmark/3, benchmark/4, usage/0]).

% takes the number of schedulers, the name(optional) of a directory containing
%some test files and/or an atom(optional) to be searched.
% a parallel grep is performed, searching for that atom inside the test files
% and returning the paths of the files containing the atoms.
% speedup over 10 iterations is calculated

usage() ->
   ?TEST_MR_GOOGLE_H.

benchmark() ->
   % ?TESTDIR is a default folder containing files, used for testing purposes,
   % and '?REGEX' is the atom to be searched in ?TESTDIR
   benchmark(utils:get_schedulers(), ?REGEX, ?TESTDIR).
benchmark(Schedulers_Num, Atom) when is_atom(Atom) ->
   benchmark(Schedulers_Num, Atom, ?TESTDIR);
benchmark(Schedulers_Num, Testdir) ->
   benchmark(Schedulers_Num, ?REGEX, Testdir).
benchmark(Schedulers_Num, Atom, Testdir) ->
   benchmark(Schedulers_Num, Atom, Testdir, Testdir).
benchmark(Schedulers_Num, Atom, Testdir_WC, Testdir_Grep)->
   utils:set_schedulers(Schedulers_Num),
   Abspath_Grep = filename:join(utils:get_dirpath(),Testdir_Grep),
   Abspath_WC = filename:join(utils:get_dirpath(),Testdir_WC),
   io:format("> testing with ~w scheduler(s).~n", [Schedulers_Num]),
   io:format("> [TEST 1] counting all the occurrences of the atoms in ", []),
   io:format("the files located in the following path: ~n", []),
   io:format("~s~n",[Abspath_WC]),
   {_,Files_WC} = file:list_dir_all(Abspath_WC),
   Files_WC_Num = length(Files_WC),
   io:format("> ~w files are being processed:~n", [Files_WC_Num]),
   lists:foreach(
         fun(I) ->
            io:format("file '~s'~n", [I])
         end,
         Files_WC),
   io:format("> [TEST 2] looking for the atom '~w' in the path:~n",[Atom]),
   io:format("~s~n",[Abspath_Grep]),
   {_,Files_Grep} = file:list_dir_all(Abspath_Grep),
   Files_Grep_Num = length(Files_Grep),
   io:format("> ~w files are being processed:~n", [Files_Grep_Num]),
   lists:foreach(
         fun(I) ->
            io:format("file '~s'~n", [I])
         end,
         Files_Grep),

   io:format("~nrunning tests, please wait...~n~n"),

   Seq_WC = fun() -> utils:seq_wc(Abspath_WC) end,
   Seq_Grep = fun() -> utils:seq_grep(Abspath_Grep, Atom) end,
   Par_WC = fun() -> utils:par_wc(Abspath_WC) end,
   Par_Grep = fun() -> utils:par_grep(Abspath_Grep,Atom) end,
   Time_Seq_WC = utils:test_loop(?TIMES,Seq_WC, []),
   Mean_Seq_WC = utils:mean(Time_Seq_WC),
   Median_Seq_WC = utils:median(Time_Seq_WC),
   Time_Seq_Grep = utils:test_loop(?TIMES,Seq_Grep, []),
   Mean_Seq_Grep = utils:mean(Time_Seq_Grep),
   Median_Seq_Grep = utils:median(Time_Seq_Grep),
   Time_WC = utils:test_loop(?TIMES,Par_WC, []),
   Mean_WC = utils:mean(Time_WC),
   Median_WC = utils:median(Time_WC),
   Time_Grep = utils:test_loop(?TIMES,Par_Grep, []),
   Mean_Grep = utils:mean(Time_Grep),
   Median_Grep = utils:median(Time_Grep),
   Speedup_WC = utils:speedup(Mean_Seq_WC,Mean_WC),
   Speedup_Grep = utils:speedup(Mean_Seq_Grep,Mean_Grep),
   io:format("---SUMMARY OF RESULTS---~n"),
   io:format("TEST 1: parallel word count~n"),
   utils:report(?SEQ, Time_Seq_WC, Mean_Seq_WC, Median_Seq_WC),
   utils:report(?GOOGLE, Time_WC, Mean_WC, Median_WC),
   io:format("speedup of the ~s version is ~w~n", [?GOOGLE, Speedup_WC]),
   io:format("TEST 2: parallel word count~n"),
   utils:report(?SEQ, Time_Seq_Grep, Mean_Seq_Grep, Median_Seq_Grep),
   utils:report(?GOOGLE, Time_Grep, Mean_Grep, Median_Grep),
   io:format("speedup of the ~s version is ~w~n", [?GOOGLE, Speedup_Grep]).
