-module(test_stream).
-export([benchmark/0,benchmark/4]).
-import('math',[sin/1,pow/2,log2/1]).

% testing the pipe and farm skeletons by applying the function
% fn=1+(sin(X))^Exp, where X is a random number from 0 to 100

% default configuration
benchmark() ->
Schedulers_num = utils:get_schedulers(),
Exp = 20,
Chunks_exp = Exp-round(log2(Schedulers_num)),
benchmark(Exp,Chunks_exp,Schedulers_num,Schedulers_num).
% customise length of list, length of chunks, number of workers
% and schedulers online
benchmark(Exp,Chunks_exp,W,Schedulers_num) ->
   utils:set_schedulers(Schedulers_num),
   List = [rand:uniform(100)||
   _ <- utils:create_list(Exp)],


   io:format("testing with ~w scheduler(s) and ~w worker(s)~n",
   [Schedulers_num,W]),
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

   M_func = fun(Input) ->
      [1 + pow(sin(X),Exp) || X <- Input]
   end,

   W_func = fun(Chunks) ->
      lists:sum(
      [1 + pow(sin(X),Exp)
      || X <- Chunks])
   end,

   % sequential version using a fold and a map
   Seq = fun() ->
      lists:foldl((fun(X,Sum) ->
         X+Sum end),0,lists:map(fun(X)->
            1 + pow(sin(X),Exp) end,List)) end,

   % pipeline version with two stages
   Pipe =
      fun() ->
         lists:sum(
         stream:start_pipe(
         [M_func, fun lists:sum/1],
            utils:make_chunks(Chunks_len,List))) end,

   % farm version
   Farm =
   fun() ->
      lists:sum(
      stream:start_farm(
      W,W_func,
      utils:make_chunks(Chunks_len,List))) end,

      Time_seq = utils:test_loop(12,Seq, []),
      Mean_seq = utils:mean(Time_seq),
      Median_seq = utils:median(Time_seq),
      Time_pipe = utils:test_loop(12,Pipe, []),
      Mean_pipe = utils:mean(Time_pipe),
      Median_pipe = utils:median(Time_pipe),
      Time_farm = utils:test_loop(12,Farm, []),
      Mean_farm = utils:mean(Time_farm),
      Median_farm = utils:median(Time_farm),
      Speedup1 = utils:speedup(Mean_seq,Mean_pipe),
      Speedup2 = utils:speedup(Mean_seq,Mean_farm),

      io:format("sequential mean is ~wms, whilst median is ~wms~n",
      [Mean_seq/1000,Median_seq/1000]),
      io:format("pipe mean is ~wms, whilst median is ~wms~n",
      [Mean_pipe/1000,Median_pipe/1000]),
      io:format("farm mean is ~wms, whilst median is ~wms~n",
      [Mean_farm/1000,Median_farm/1000]),
      io:format("speedup for pipe is ~w~n",[Speedup1]),
      io:format("speedup for farm is ~w~n", [Speedup2]).
