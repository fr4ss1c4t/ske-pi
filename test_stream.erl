-module(test_stream).
-export([benchmark/0,benchmark/4]).
% testing the pipe and farm skeletons by applying the function 
% fn=1+(sin(X))^Exp, where X is a random number from 0 to 100
%
% default configuration
benchmark() -> 
   Schedulers_num = erlang:system_info(schedulers_online),
   Exp = 20,
   Chunks_exp = Exp-round(math:log2(Schedulers_num)),
   benchmark(Exp,Chunks_exp,Schedulers_num,Schedulers_num).
% customise length of list, length of chunks, number of workers
% and schedulers online
benchmark(Exp,Chunks_exp,W,Schedulers_num) ->
   erlang:system_flag(schedulers_online,Schedulers_num),
   List = [rand:uniform(100)|| 
           _ <- lists:seq(0,round(math:pow(2,Exp)))],
   
   io:format("testing with ~w scheduler(s) and ~w worker(s)~n", 
             [Schedulers_num,W]),
   io:format("2^~w chunks of length 2^~w=~w~n",
	     [Exp-Chunks_exp, Chunks_exp,Chunks_len]),


   W_func = fun(Chunks) ->
                  lists:sum(
                     [1 + math:pow(math:sin(X),Exp)
                      || X <- Chunks]) 
            end,

   % sequential version using a fold and a map
   Seq = fun() -> 
         lists:foldl((fun(X,Sum) ->
            X+Sum end),0,lists:map(fun(X)-> 
               1 + math:pow(math:sin(X),Exp) end,List)) end,
   
   % pipeline version with two stages 
   Pipe =
      fun() -> 
         lists:sum(
            stream:start_pipe(
               [W_func, fun lists:sum/1], 
                utils:make_chunks(List,Chunks_len))) end,
   
   % farm version 
   Farm =
      fun() -> 
         lists:sum(
            stream:start_farm(
               W_func, 
               utils:make_chunks(List,Chunks_len),W)) end,

   Time_seq = utils:test_loop(10,Seq, []),
   Mean_seq = utils:mean(Time_seq),
   Median_seq = utils:median(Time_seq),
   Time_pipe = utils:test_loop(10,Pipe, []),
   Mean_pipe = utils:mean(Time_pipe),
   Median_pipe = utils:median(Time_pipe),
   Time_farm = utils:test_loop(10,Farm, []),
   Mean_farm = utils:mean(Time_farm),
   Median_farm = utils:median(Time_farm),
   Speedup = utils:speedup(Mean_seq,Mean_farm),

   io:format("sequential mean is ~wms, whilst median is ~wms~n",
             [Mean_seq/1000,Median_seq/1000]),
   io:format("pipe mean is ~wms, whilst median is ~wms~n",
             [Mean_pipe/1000,Median_pipe/1000]),
   io:format("farm mean is ~wms, whilst median is ~wms~n",
             [Mean_farm/1000,Median_farm/1000]),
   io:format("speedup is ~w~n", [Speedup]).

