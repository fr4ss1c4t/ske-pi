-module(stream).
-include("include/usages.hrl").
-export([usage/0,start_farm/2,start_seq/2,start_pipe/2,start_piped_farm/2,
start_farm/3,start_piped_farm/3]).

usage() -> ?STREAM_H.

% some functions to be used in the testing module for starting farms
% of W workers, pipes with a list of stages and a sequential function
% operating on a stream of inputs, respectively.
start_farm(W_Fun,List) ->
   W = erlang:system_info(schedulers_online),
   start_farm(W,W_Fun,List).
start_farm(W,W_Fun,List) ->
   start([{farm, [{seq,W_Fun}], W}], List).


start_pipe(Stages,List) ->
   start(lists:map(fun(Fun)->
      {seq,Fun}
   end, Stages), List).

start_piped_farm(Stages,List) ->
   W = erlang:system_info(schedulers_online),
   start_piped_farm(W,Stages,List).
start_piped_farm(W,Stages,List) ->
   start(lists:map(fun(Fun)->
      {farm, [{seq,Fun}], W}
   end, Stages), List).

start_seq(W_Fun, List) ->
   start([{seq,W_Fun}],List).

% returns the received results given the input stream and the
% tasks
start(Tasks, List) ->
   run(Tasks,List),
   receive
      {results,Results} -> Results
   end.

% runs the tasks in the workflow given the input stream
run(Tasks,List) when is_pid(Tasks)->
   Bin = utils:spawn_src(List),
   Bin(Tasks);
run(Tasks,List) when is_list(Tasks) ->
   Bin = (utils:spawn_sink())(self()),
   Parsed_Workflow = build(Tasks,Bin),
   run(Parsed_Workflow,List).

run_seq(Seq_Fun,Pid) ->
   Fun = utils:apply(Seq_Fun),
   loop_fun(Fun,Pid).

% parses the tasks
build(Tasks,Bin) ->
   Funcs = [parse(Task) || Task <-Tasks],
   lists:foldr(fun(Func,Pid)-> Func(Pid) end, Bin, Funcs).

parse(Fun) when is_function(Fun,1)->
   parse({seq,Fun});
parse({seq,Fun}) when is_function(Fun,1)->
   build_seq(Fun);
parse({farm,Tasks,W}) ->
   build_farm(W,Tasks);
parse({pipe,Tasks}) ->
   build_pipe(Tasks).

% farm paradigm using a collector and an emitter
build_farm(W,Tasks) ->
   fun(Pid) ->
      Collector = spawn(fun() -> collect(W,Pid) end),
      Workers = spawn_procs(W,Tasks,Collector),
      spawn(fun() -> emit(Workers) end)
   end.

build_pipe(Tasks) ->
   fun(Pid) ->
      build(Tasks,Pid)
   end.

build_seq(Fun) ->
   fun(Pid) ->
      spawn(fun() -> run_seq(Fun,Pid) end)
   end.

loop_fun(Fun,Pid) ->
   receive
      {input,_} = Msg_Input ->
         Msg_Input1 = Fun(Msg_Input),
         Pid ! Msg_Input1,
         loop_fun(Fun,Pid);
      {msg,eos} ->
         Pid ! {msg,eos},
         eos
   end.

emit([Worker|Rest]=Workers) ->
   receive
      {input,_} = Input ->
         Worker ! Input,
         emit(Rest++[Worker]);
      {msg, eos} ->
         stop_procs(Workers)
   end.

collect(W,Pid) ->
   receive
      {input, _} = Input ->
         Pid ! Input,
         collect(W,Pid);
      {msg,eos} when W =< 1 ->
         Pid ! {msg, eos};
      {msg,eos} ->
         collect(W-1,Pid)
   end.

spawn_procs(W,Tasks,Pid) ->
   spawn_procs(W,Tasks,Pid,[]).
spawn_procs(W,_Tasks,_Pid,Workers) when W <1 ->
   Workers;
spawn_procs(W,Tasks,Pid,Workers) ->
   Worker = do_job(Tasks,Pid),
   spawn_procs(W-1,Tasks,Pid,[Worker|Workers]).

stop_procs([]) ->
   eos;
stop_procs([Worker|Rest])->
   Worker ! {msg, eos},
   stop_procs(Rest).

do_job(Tasks,Pid) ->
   build(Tasks,Pid).
