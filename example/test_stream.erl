-module(test_stream).
-export([benchmark/0,benchmark/4]).
-import('math',[sin/1,pow/2,log2/1]).

% testing the pipe and farm skeletons by applying the function
% fn=1+(sin(X))^Exp, where X is a random number from 0 to 100

% default configuration
benchmark() ->
   Schedulers_Num = utils:get_schedulers(),
   Exp = 20,
   Chunks_Exp = Exp-round(log2(Schedulers_Num)),
   benchmark(Exp,Chunks_Exp,Schedulers_Num,Schedulers_Num).
% customise length of list, length of chunks, number of workers
% and schedulers online
benchmark(Exp,Chunks_Exp,W,Schedulers_Num) ->
   utils:set_schedulers(Schedulers_Num),
   List = [rand:uniform(100)||
   _ <- utils:create_list(Exp)],


   io:format("testing with ~w scheduler(s) and ~w worker(s)~n",
   [Schedulers_Num,W]),
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

   Fun = fun(Input) ->
      [1 + pow(sin(X),Exp) || X <- Input]
   end,

   W_Fun = fun(Chunks) ->
      lists:sum(
      [1 + pow(sin(X),Exp)
      || X <- Chunks])
   end,

   % sequential version is a farm with only one worker
   Seq =
      fun() ->
         stream:start_farm(1,W_Fun, utils:make_chunks(Chunks_Len,List))
      end,

   % pipeline version with two stages
   Pipe =
      fun() ->
         stream:start_pipe(
         [Fun, fun lists:sum/1],
            utils:make_chunks(Chunks_Len,List))
      end,

   % farm version
   Farm =
      fun() ->
         stream:start_farm(W,W_Fun,
         utils:make_chunks(Chunks_Len,List))
      end,

   Time_Seq = utils:test_loop(12,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Pipe = utils:test_loop(12,Pipe, []),
   Mean_Pipe = utils:mean(Time_Pipe),
   Median_Pipe = utils:median(Time_Pipe),
   Time_Farm = utils:test_loop(12,Farm, []),
   Mean_Farm = utils:mean(Time_Farm),
   Median_Farm = utils:median(Time_Farm),
   Speedup_Pipe = utils:speedup(Mean_Seq,Mean_Pipe),
   Speedup_Farm = utils:speedup(Mean_Seq,Mean_Farm),

   io:format("sequential mean is ~wms, whilst median is ~wms~n",
   [Mean_Seq/1000,Median_Seq/1000]),
   io:format("pipe mean is ~wms, whilst median is ~wms~n",
   [Mean_Pipe/1000,Median_Pipe/1000]),
   io:format("farm mean is ~wms, whilst median is ~wms~n",
   [Mean_Farm/1000,Median_Farm/1000]),
   io:format("speedup for pipe is ~w~n",[Speedup_Pipe]),
   io:format("speedup for farm is ~w~n", [Speedup_Farm]).
