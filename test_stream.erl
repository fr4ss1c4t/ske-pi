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
   if
      Exp>Chunks_exp ->
         Chunks_len = round(math:pow(2,Chunks_exp));
      true ->
         Chunks_len = 1
   end,
   
   io:format("testing with ~w scheduler(s) and ~w worker(s)~n", 
             [Schedulers_num,W]),
   io:format("2^~w chunks of length 2^~w=~w~n",
	     [Chunks-Chunks_exp, Chunks_exp,Chunks_len]),

   M_func = fun(Input) -> 
                  [1 + math:pow(math:sin(X),Exp) 
                   || X <- Input] 
            end,
   W_func = fun(Chunks) ->
                  lists:sum(
                     [1 + math:pow(math:sin(X),Exp)
                      || X <- Chunks]) 
            end,
   Seq = 
      fun() -> 
         lists:sum(
            sstream:start_farm(
               W_func,
               utils:make_chunks(List,Chunks_len))) end, 
   Pipe =
      fun() -> 
         lists:sum(
            stream:start_pipe(
               [M_func, fun lists:sum/1], 
                utils:make_chunks(List,Chunks_len))) end,
   Farm =
      fun() -> 
         lists:sum(
            stream:start_farm(
               W_func, 
               utils:make_chunks(List,Chunks_len),W)) end,
   Time_seq = test_loop(10,Seq, []),
   Mean_seq = mean(Time_seq),
   Median_seq = median(Time_seq),
   Time_pipe = test_loop(10,Pipe, []),
   Mean_pipe = mean(Time_pipe),
   Median_pipe = median(Time_pipe),
   Time_farm = test_loop(10,Farm, []),
   Mean_farm = mean(Time_farm),
   Median_farm = median(Time_farm),
   Speedup = speedup(Mean_seq,Mean_farm),

   io:format("sequential mean is ~wms, whilst median is ~wms~n",
             [Mean_seq/1000,Median_seq/1000]),
   io:format("pipe mean is ~wms, whilst median is ~wms~n",
             [Mean_pipe/1000,Median_pipe/1000]),
   io:format("farm mean is ~wms, whilst median is ~wms~n",
             [Mean_farm/1000,Median_farm/1000]),
   io:format("speedup is ~w~n", [Speedup]).

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
