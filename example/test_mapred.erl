-module(test_mapred).
-include("include/defines.hrl").
-include("include/usages.hrl").
-export([benchmark/0,benchmark/3, usage/0]).

% testing the mapreduce skeleton by applying the function
% fn=1+(sin(X))^(10*Exp), where X is a random number from 0 to an upper bound

usage() ->
   ?TEST_MR_H.
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
   M_Fun = fun(Chunk) -> [?COMPUTATION(X,Exp) || X<-Chunk] end,
   R_Fun = fun(A,B) -> A+B end,
   M_Fun1 = fun(L) -> pmap:start(M_Fun, L) end,
   R_Fun1 = fun(L) -> preduce:start(R_Fun, 0, L) end,
   M_Fun2 = fun(Chunk) -> lists:sum([?COMPUTATION(X,Exp) || X<-Chunk]) end,


   % combination of pmap and preduce skeleton version
   C_MapRed = fun() -> catch(R_Fun1(M_Fun1(List))) end,

   % normal version
   N_MapRed = fun() ->
      mapred:start(M_Fun, R_Fun, 0, List) end,

   % smart version
   S_MapRed = fun() ->
      mapred_smart:start(M_Fun2, R_Fun, 0, List) end,

   Time_Seq = utils:test_loop(?TIMES,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Cm = utils:test_loop(?TIMES,C_MapRed, []),
   Mean_Cm = utils:mean(Time_Cm),
   Median_Cm = utils:median(Time_Cm),
   Time_Nm = utils:test_loop(?TIMES,N_MapRed, []),
   Mean_Nm = utils:mean(Time_Nm),
   Median_Nm = utils:median(Time_Nm),
   Time_Sm = utils:test_loop(?TIMES,S_MapRed, []),
   Mean_Sm = utils:mean(Time_Sm),
   Median_Sm = utils:median(Time_Sm),
   Speedup_Cm = utils:speedup(Mean_Seq,Mean_Cm),
   Speedup_Nm = utils:speedup(Mean_Seq,Mean_Nm),
   Speedup_Sm = utils:speedup(Mean_Seq,Mean_Sm),
   io:format("---SUMMARY OF RESULTS---~n"),
   utils:report(?SEQ, Time_Seq, Mean_Seq, Median_Seq),
   utils:report(?COMB, Time_Cm, Mean_Cm, Median_Cm),
   utils:report(?MAPRED, Time_Nm, Mean_Nm, Median_Nm),
   utils:report(?SMART, Time_Sm, Mean_Sm, Median_Sm),
   io:format("speedup of the ~s version is ~p~n", [?COMB,Speedup_Cm]),
   io:format("speedup of the ~s version is ~p~n", [?MAPRED,Speedup_Nm]),
   io:format("speedup of the ~s version is ~p~n", [?SMART,Speedup_Sm]).
