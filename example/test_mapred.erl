-module(test_mapred).
-include("include/defines.hrl").
-export([benchmark/0,benchmark/3]).


% testing the mapreduce skeleton by applying the function
% fn=1+(sin(X))^(10*Exp), where X is a random number from 0 to an upper bound

% default configuration
benchmark() ->
   Schedulers_Num = utils:get_schedulers(),
   Chunks_Exp = ?EXP-round(math:log2(Schedulers_Num)),
   benchmark(Schedulers_Num, ?EXP, Chunks_Exp).
% customise number of schedulers online, length of list and length of chunks
benchmark(Schedulers_Num, Exp, Chunks_Exp) ->
   utils:set_schedulers(Schedulers_Num),
   List = [rand:uniform(?UPPER)|| _ <- utils:create_list(Exp)],

   io:format("> calculating the function fn=1+(sin(X))^(10*Exp), "),
   io:format("where EXP=~p and X is a random number from 0 to ~p.~n",
      [?EXP,?UPPER]),
   io:format("> testing with ~w scheduler(s).~n", [Schedulers_Num]),
   io:format("> the list is 2^~w=~w elements long.~n",[Exp,length(List)]),
   if
      Exp>Chunks_Exp ->
         Chunks_Len = round(math:pow(2,Chunks_Exp)),
         io:format("> split into 2^~w chunks of length 2^~w=~w.~n~n",
         [Exp-Chunks_Exp, Chunks_Exp,Chunks_Len]);
      true ->
         Chunks_Len = round(math:pow(2,Exp)),
         io:format("> split into 2^~w chunks of length 2^~w=~w.~n~n",
         [0, Exp,Chunks_Len])
   end,

   io:format("running tests, please wait...~n~n"),

   % sequential version using a fold and a map
   Seq = fun() ->
      lists:foldl((fun(X,Sum) ->
         X+Sum end),0,lists:map(fun(X)->
            ?COMPUTATION(X,Exp) end,List)) end,

   % naive version
   N_MapRed = fun() ->
      mapred_naive:start(
         fun(Chunk)->lists:sum([?COMPUTATION(X,Exp) || X<-Chunk]) end,
         utils:make_chunks(Chunks_Len,List)) end,

   % smart version
   S_MapRed = fun() ->
      mapred_naive:start(
         fun(Chunk)->lists:sum([?COMPUTATION(X,Exp) || X<-Chunk]) end,
         utils:make_chunks(Chunks_Len,List)) end,

   Time_Seq = utils:test_loop(?TIMES,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Nv = utils:test_loop(?TIMES,N_MapRed, []),
   Mean_Nv = utils:mean(Time_Nv),
   Median_Nv = utils:median(Time_Nv),
   Time_Sm = utils:test_loop(?TIMES,S_MapRed, []),
   Mean_Sm = utils:mean(Time_Sm),
   Median_Sm = utils:median(Time_Sm),
   Speedup_Nv = utils:speedup(Mean_Seq,Mean_Nv),
   Speedup_Sm = utils:speedup(Mean_Seq,Mean_Sm),
   io:format("---SUMMARY OF RESULTS---~n"),
   utils:report(?SEQ, Time_Seq, Mean_Seq, Median_Seq),
   utils:report(?NAIVE, Time_Nv, Mean_Nv, Median_Nv),
   utils:report(?SMART, Time_Sm, Mean_Sm, Median_Sm),
   io:format("speedup of the ~p version is ~p~n", [?NAIVE,Speedup_Nv]),
   io:format("speedup of the ~p version is ~p~n", [?SMART,Speedup_Sm]).
