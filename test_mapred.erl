-module(test_mapred).
-export([benchmark/0,benchmark/3]).
-import('math',[sin/1,pow/2,log2/1]).

% testing the mapreduce skeleton by applying the function
% fn=1+(sin(X))^Exp, where X is a random number from 0 to 100
%
% default configuration
benchmark() ->
   Schedulers_num = utils:get_schedulers(),
   Exp = 20,
   Chunks_exp = Exp-round(log2(Schedulers_num)),
   benchmark(Exp,Chunks_exp,Schedulers_num).
% customise length of list, length of chunks and number of
% schedulers online
benchmark(Exp,Chunks_exp,Schedulers_num) ->
   utils:set_schedulers(Schedulers_num),
   List = [rand:uniform(100)|| _ <- utils:create_list(Exp)],

   io:format("testing with ~w scheduler(s)~n", [Schedulers_num]),
   if
      Exp>Chunks_exp ->
         Chunks_len = round(pow(2,Chunks_exp)),
         io:format("2^~w chunks of length 2^~w=~w~n",
         [Exp-Chunks_exp, Chunks_exp,Chunks_len]);
      true ->
         Chunks_len = round(pow(2,Exp)),
         io:format("2^~w chunks of length 2^~w=~w~n",
         [0, Exp,Chunks_len])
   end,

   % google map reduce
   %Hashed_chunks = lists:map(fun(Chunk) ->
   %	{erlang:phash2(Chunk,Chunks_len),Chunk} end,
   %  utils:make_chunks(Chunks_len,List)),

   %G_mapred =
   %   fun() -> lists:sum(clean_up(gmapred:start(fun mapper/3, fun reducer/3,
   %                                         Hashed_chunks))) end,

   % sequential version using a fold and a map
   Seq = fun() ->
      lists:foldl((fun(X,Sum) ->
         X+Sum end),0,lists:map(fun(X)->
            1 + pow(sin(X),Exp) end,List)) end,

   % parallel version
   P_mapred = fun() ->
      lists:sum(
      pmapred:start(
      fun(Chunk)->[1+pow(sin(X),Exp)
         ||X<-Chunk] end,fun lists:sum/1,utils:make_chunks(Chunks_len,List))) end,

   % parallel version with pre-partitioned data
   Chunks = utils:make_chunks(Chunks_len,List),
   C_mapred = fun() ->
      lists:sum(
      pmapred:start(
      fun(Chunk)->[1+pow(sin(X),Exp)
         ||X<-Chunk] end,fun lists:sum/1, Chunks)) end,


   Time_seq = utils:test_loop(12,Seq, []),
   Mean_seq = utils:mean(Time_seq),
   Median_seq = utils:median(Time_seq),
   %Time_g = utils:test_loop(12,G_mapred, []),
   %Mean_g = utils:mean(Time_g),
   %Median_g = utils:median(Time_g),
   Time_p = utils:test_loop(12,P_mapred, []),
   Mean_p = utils:mean(Time_p),
   Median_p = utils:median(Time_p),
   Time_c = utils:test_loop(12,C_mapred, []),
   Mean_c = utils:mean(Time_c),
   Median_c = utils:median(Time_c),
   Speedup1 = utils:speedup(Mean_seq,Mean_p),
   Speedup2 = utils:speedup(Mean_seq,Mean_c),

   io:format("sequential version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
      [Mean_seq/1000,Median_seq/1000]),

   io:format("parallel version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
      [Mean_p/1000,Median_p/1000]),

   io:format("pre-partitioned version is"),
   io:format(" ~wms, whilst median is ~wms~n",
             [Mean_c/1000,Median_c/1000]),

   io:format("speed up of parallel version is ~w~n", [Speedup1]),
   io:format("speed up of pre-partitioned version is ~w~n", [Speedup2]).


mapper(Key,Chunk,Fun) ->
   lists:foreach(fun(X)->Fun(Key,1+sin(X)) end,Chunk).

reducer(Key,Sums,Fun) ->
   Results = lists:foldl(fun(Sum,Acc) -> Sum+Acc end,0,Sums),
   Fun(Key,Results).
