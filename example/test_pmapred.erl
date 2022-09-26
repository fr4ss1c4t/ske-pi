-module(test_pmapred).
-export([benchmark/0,benchmark/3]).
-import('math',[sin/1,pow/2,log2/1]).

% testing the mapreduce skeleton by applying the function
% fn=1+(sin(X))^Exp, where X is a random number from 0 to 100

% default configuration
benchmark() ->
   Schedulers_Num = utils:get_schedulers(),
   Exp = 20,
   Chunks_Exp = Exp-round(log2(Schedulers_Num)),
   benchmark(Exp,Chunks_Exp,Schedulers_Num).
% customise length of list, length of chunks and number of
% schedulers online
benchmark(Exp,Chunks_Exp,Schedulers_Num) ->
   utils:set_schedulers(Schedulers_Num),
   List = [rand:uniform(100)|| _ <- utils:create_list(Exp)],

   io:format("testing with ~w scheduler(s)~n", [Schedulers_Num]),
   io:format("the list is 2^~w=~w elements long~n",[Exp,length(List)]),
   if
      Exp>Chunks_Exp ->
         Chunks_Len = round(pow(2,Chunks_Exp)),
         io:format("2^~w chunks of length 2^~w=~w~n",
         [Exp-Chunks_Exp, Chunks_Exp,Chunks_Len]);
      true ->
         Chunks_Len = round(pow(2,Exp)),
         io:format("2^~w chunks of length 2^~w=~w~n",
         [0, Exp,Chunks_Len])
   end,

   % sequential version using a fold and a map
   Seq = fun() ->
      lists:foldl((fun(X,Sum) ->
         X+Sum end),0,lists:map(fun(X)->
            1 + pow(sin(X),Exp) end,List)) end,

   % parallel version
   P_MapRed = fun() ->
      pmapred_naive:start(
      fun(Chunk)->lists:sum([1+pow(sin(X),Exp)||X<-Chunk]) end,
      utils:make_chunks(Chunks_Len,List)) end,

   % parallel version with pre-partitioned data
   Chunks = utils:make_chunks(Chunks_Len,List),
   C_MapRed = fun() ->
      pmapred_naive:start(
      fun(Chunk)-> lists:sum([1+pow(sin(X),Exp)||X<-Chunk]) end, Chunks) end,


   Time_Seq = utils:test_loop(12,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Par = utils:test_loop(12,P_MapRed, []),
   Mean_Par = utils:mean(Time_Par),
   Median_Par = utils:median(Time_Par),
   Time_Ch = utils:test_loop(12,C_MapRed, []),
   Mean_Ch = utils:mean(Time_Ch),
   Median_Ch = utils:median(Time_Ch),
   Speedup_Par = utils:speedup(Mean_Seq,Mean_Par),
   Speedup_Ch = utils:speedup(Mean_Seq,Mean_Ch),

   io:format("sequential version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
      [Mean_Seq/1000,Median_Seq/1000]),

   io:format("naive parallel version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
      [Mean_Par/1000,Median_Par/1000]),

   io:format("pre-partitioned version is"),
   io:format(" ~wms, whilst median is ~wms~n",
             [Mean_Ch/1000,Median_Ch/1000]),

   io:format("speedup of naive parallel version is ~w~n", [Speedup_Par]),
   io:format("speedup of pre-partitioned version is ~w~n", [Speedup_Ch]).
