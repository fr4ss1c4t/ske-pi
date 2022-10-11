-module(stream).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([usage/0,
start_farm/3,
start_seq/3,
start_pipe/3,
start_piped_farm/3,
start_farm/4,
start_piped_farm/4]).

usage() -> ?STREAM_H.

% some functions to be used in the testing module for starting farms
% of W workers, pipes with a list of stages and a sequential function
% operating on a stream of inputs, respectively.
start_farm(W_Fun,List,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   W = erlang:system_info(schedulers_online),
   start_farm(W,W_Fun,List,Chunks_Len).
start_farm(W,W_Fun,List,Chunks_Len)->
   ?LOG_CALL(?NOW),
   start(self(),[{farm, [{seq,W_Fun}], W}], List,Chunks_Len).


start_pipe(Stages,List,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   start(self(),lists:map(fun(Fun)->
      {seq,Fun}
   end, Stages), List,Chunks_Len).

start_piped_farm(Stages,List,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   W = utils:get_schedulers(),
   start_piped_farm(W,Stages,List,Chunks_Len).

start_piped_farm(W,Stages,List,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   start(self(),lists:map(fun(Fun)->
      {farm, [{seq,Fun}], W}
   end, Stages), List,Chunks_Len).

start_seq(W_Fun, List, Chunks_Len) ->
   ?LOG_CALL(?NOW),
   start(self(),[{seq,W_Fun}],utils:make_chunks(Chunks_Len,List),Chunks_Len).

% returns the received results given the input stream and the
% tasks
start(Pid,Tasks, List,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   run(Tasks,List,Chunks_Len),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {results,Results} -> Results
   after
      ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid)),
      exit(timed_out)
   end.

% runs the tasks in the workflow given the input stream
run(Tasks,List,Chunks_Len) when is_pid(Tasks)->
   ?LOG_CALL(?NOW),
   Bucket = spawn_drain(List),
   Bucket(Tasks);
run(Tasks,List,Chunks_Len) when is_list(Tasks) ->
   ?LOG_CALL(?NOW),
   Bucket = (spawn_sink())(self()),
   Parsed_Workflow = build(Tasks,Bucket,Chunks_Len),
   run(Parsed_Workflow,List,Chunks_Len).

run_seq(Seq_Fun,Pid) ->
   ?LOG_CALL(?NOW),
   Fun = utils:apply(Seq_Fun),
   loop_fun(Fun,Pid).

% parses the tasks
build(Tasks,Bucket,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   Funs = [parse(Task,Chunks_Len) || Task <-Tasks],
   lists:foldr(fun(Fun,Pid)-> catch(Fun(Pid)) end, Bucket, Funs).

parse(Fun,Chunks_Len) when is_function(Fun,1)->
   ?LOG_CALL(?NOW),
   parse({seq,Fun},Chunks_Len);
parse({seq,Fun},Chunks_Len) when is_function(Fun,1)->
   ?LOG_CALL(?NOW),
   build_seq(Fun);
parse({farm,Tasks,W},Chunks_Len) ->
   ?LOG_CALL(?NOW),
   build_farm(W,Tasks,Chunks_Len);
parse({pipe,Tasks},Chunks_Len) ->
   ?LOG_CALL(?NOW),
   build_pipe(Tasks,Chunks_Len).

% farm paradigm using a collector and an emitter
build_farm(W,Tasks,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   fun(Pid) ->
      Collector = spawn(fun() -> collect(W,Pid) end),
      Workers = spawn_procs(W,Tasks,Collector,Chunks_Len),
      spawn(fun() -> emit(Pid, Workers, [], 0, Chunks_Len) end)
   end.

build_pipe(Tasks,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   fun(Pid) ->
      build(Tasks,Pid,Chunks_Len)
   end.

build_seq(Fun) ->
   ?LOG_CALL(?NOW),
   fun(Pid) ->
      spawn(fun() -> run_seq(Fun,Pid) end)
   end.

loop_fun(Fun,Pid) ->
   ?LOG_CALL(?NOW),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {input,_} = Msg_Input ->
         Msg_Input1 = Fun(Msg_Input),
         Pid ! Msg_Input1,
         ?LOG_SENT(self(),Pid,?NOW),
         loop_fun(Fun,Pid);
      {msg,eos} ->
         utils:stop(Pid),
         ?LOG_SENT(Pid,self(),?NOW),
         eos
   end.

emit(Parent,[Worker|Rest]=Workers, Chunk, Chunks_Len, Chunks_Len_Max) ->
   ?LOG_CALL(?NOW),
   ?LOG_RCVD(self(),Parent,?NOW),
   receive
      {input,Item}  ->
         case Chunks_Len+1 == Chunks_Len_Max of
            true ->
               ?LOG_SENT(Worker,self(),?NOW),
               Worker ! {input,[Item|Chunk]},
               emit(Parent,lists:append(Rest,[Worker]),[],0, Chunks_Len_Max);
            false ->
               emit(Parent,Workers, [Item|Chunk], Chunks_Len+1, Chunks_Len_Max)
         end;
      {msg, eos} ->
         case Chunk == [] of
            true -> stop_procs(Workers);
            false ->
               ?LOG_SENT(Worker,self(),?NOW),
               Worker ! {input,Chunk},
               stop_procs(Workers)
         end
   end.

collect(W,Pid) ->
   ?LOG_CALL(?NOW),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {input, Chunk} = Input ->
         Pid ! Input,
         ?LOG_SENT(self(),Pid,?NOW),
         collect(W,Pid);
      {msg,eos} when W =< 1 ->
         Pid ! {msg, eos},
         ?LOG_SENT(self(),Pid,?NOW);
      {msg,eos} ->
         collect(W-1,Pid)
   end.

% reads from the input stream of chunks of a
% list and sends them to the first function
% in the workflow
spawn_drain(Input) ->
   ?LOG_CALL(?NOW),
   fun(Pid) ->
      spawn(fun() ->start_drain(Input,Pid) end)
   end.
start_drain(Input,Pid) ->
   loop_drain(Input,Pid).
loop_drain([],Pid) ->
   ?LOG_CALL(?NOW),
   utils:stop(Pid);
loop_drain([Input|Inputs],Pid) ->
   utils:send(Input,Pid),
   loop_drain(Inputs,Pid).

% accepts inputs from the output stream and
% accumulates the results in a list
spawn_sink() ->
   ?LOG_CALL(?NOW),
   fun(Pid) ->
      spawn(fun() -> start_sink(Pid) end)
   end.
start_sink(Pid) ->
   ?LOG_CALL(?NOW),
   loop_sink(Pid,[]).
loop_sink(Pid,Results) ->
   receive
      {input,_} = Msg ->
         Input = utils:from_tuple(Msg),
         loop_sink(Pid,Results++[Input]);
      {msg,eos} ->
         utils:send_results(Results,Pid)
   end.

% spawning processes
spawn_procs(W,Tasks,Pid,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   spawn_procs(W,Tasks,Pid,[],Chunks_Len).
spawn_procs(W,_Tasks,_Pid,Workers,Chunks_Len) when W <1 ->
   ?LOG_CALL(?NOW),
   Workers;
spawn_procs(W,Tasks,Pid,Workers,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   Worker = do_job(Tasks,Pid,Chunks_Len),
   spawn_procs(W-1,Tasks,Pid,[Worker|Workers],Chunks_Len).

% stopping process by sending an 'eos' signal
stop_procs([]) ->
   ?LOG_CALL(?NOW),
   eos;
stop_procs([Worker|Rest])->
   utils:stop(Worker),
   ?LOG_SENT(Worker,self(),?NOW),
   stop_procs(Rest).

do_job(Tasks,Pid,Chunks_Len) ->
   ?LOG_CALL(?NOW),
   build(Tasks,Pid,Chunks_Len).
