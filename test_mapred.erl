-module(test_mapred).
-export([benchmark/0,benchmark/3]).
% testing the mapreduce skeleton by applying the function
% fn=1+(sin(X))^Exp, where X is a random number from 0 to 100
%
% default configuration
benchmark() -> 
   Schedulers_num = erlang:system_info(schedulers_online),
   Exp = 20,
   Chunks_exp = Exp-round(math:log2(Schedulers_num)),
   benchmark(Exp,Chunks_exp,Schedulers_num).
% customise length of list, length of chunks and number of
% schedulers online
benchmark(Exp,Chunks_exp,Schedulers_num) ->
   erlang:system_flag(schedulers_online,Schedulers_num),
   List = [rand:uniform(100)|| 
           _ <- lists:seq(0,round(math:pow(2,Exp)))],
   if
      Exp>Chunks_exp ->
         Chunks_len = round(math:pow(2,Chunks_exp));
      true ->
         Chunks_len = 1
   end,
   
   io:format("testing with ~w scheduler(s)~n", [Schedulers_num]),
   io:format("2^~w chunks of length 2^~w=~w~n",
	     [Exp-Chunks_exp,Chunks_exp, Chunks_len]),

   M_func = fun(Input) -> 
                  [1 + math:sin(X) || X <- Input] 
            end,
   % google map reduce
   %Hashed_chunks = lists:map(fun(Chunk) -> 
   %	{erlang:phash2(Chunk),Chunk} end, utils:make_chunks(List,Chunks_len)),
   %G_mapred = 
   %fun() -> lists:sum(utils:clean_up(gmapred:start(Hashed_chunks, 
   %                 fun mapper/3, fun reducer/3))) end,

   % sequential version of google map reduce
   Seq = fun() -> lists:sum(lists:map(fun lists:sum/1,lists:map(M_func, utils:make_chunks(List,Chunks_len)))) end,

   % parallel composite version
   P_mapred = fun() -> lists:sum(preduce:start(fun lists:sum/1, pmap:start(M_func,  utils:make_chunks(List,Chunks_len)))) end,

   Time_seq = test_loop(10,Seq, []),
   Mean_seq = mean(Time_seq),
   Median_seq = median(Time_seq),
   %Time_g = test_loop(10,G_mapred, []),
   %Mean_g = mean(Time_g),
   %Median_g = median(Time_g),
   Time_p = test_loop(10,P_mapred, []),
   Mean_p = mean(Time_p),
   Median_p = median(Time_p),
   %Speedup1 = speedup(Mean_seq,Mean_g),
   Speedup2 = speedup(Mean_seq,Mean_p),

   io:format("sequential version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
             [Mean_seq/1000,Median_seq/1000]),
   %io:format("google version is"), 
   %io:format(" ~wms, whilst median is ~wms~n",
   %          [Mean_g/1000,Median_g/1000]),
   io:format("composite version mean is"), 
   io:format(" ~wms, whilst median is ~wms~n",
             [Mean_p/1000,Median_p/1000]),
   %io:format("speed up of google version is ~w~n", [Speedup1]),
   io:format("speed up of composite vers. is ~w~n", [Speedup2]).

test_loop(0,_Fun, Times) ->
   Times;
test_loop(N,Fun,Times) ->
   {Time,_} = timer:tc(Fun),
   test_loop(N-1,Fun,[Time|Times]).

mean(List) ->
   lists:foldl(fun(X,Sum)-> X+Sum end, 0, List) / length(List).

median(List) ->
  lists:nth(round((length(List) / 2)), lists:sort(List)).

speedup(Time_seq,Time_par) ->
   Time_seq/Time_par.

mapper(Key,Chunk,Fun) ->
   lists:foreach(fun(X)->Fun(Key,1+math:sin(X)) end,Chunk).

reducer(Key,Sums,Fun) ->
   Results = lists:foldl(fun(Sum,Acc) -> Sum+Acc end,0,Sums),
   Fun(Key,Results).
