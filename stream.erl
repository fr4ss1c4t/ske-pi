-module(stream).
-export([start_farm/2,start_farm/3,start_pipe/2,start_seq/2,
loop_fun/2,collect/2,emit/1,stop_procs/1]).
% takes a stream of chunks of a list, the number of workers and
% the task performed by each worker. the emitter distributes each
% chunk between the workers and the
% collector gathers the outputs
%
% usage example:
% List = [[12,3,45231],[1231,231,4],[1],[6,6,6,7,6]].
% W_Fun = fun lists:sort/1.
% stream:start_farm(W_Fun, List).
% expected output:
% [[3,12,45231],[4,231,1231],[1],[6,6,6,6,7]]
start_farm(W_Fun,Input_list) ->
   W = erlang:system_info(schedulers_online),
   start_farm(W,W_Fun,Input_list).
start_farm(W,W_Fun,Input_list) ->
   start([{farm, [{seq,W_Fun}], W}], Input_list).

% takes a list of stages (where each one represents a function)
% and a stream of chunks of a list. the output of one stage is
% the input of the next one
%
% usage example:
% List = [[12,3,45231],[1231,231,4],[1],[6,6,6,7,6]].
% Stage_one = fun(Chunk) -> [X*X || X<-Chunk] end.
% Stage_two = fun lists:sort/1.
% Stages = [Stage_one, Stage_two].
% stream:start_pipe(Stages, List).
% expected output:
% [[9,144,2045843361],[16,53361,1515361],[1],[36,36,36,36,49]]
start_pipe (Stages,Input_list) ->
   start(lists:map(fun(Fun)->
      {seq,Fun}
   end, Stages), Input_list).

% returns the received results given the input stream and the
% workflow
start(Workflow, Input_list) ->
   run(Workflow,Input_list),
   receive
      {results,Results} -> Results
   end.

% runs the functions in the workflow given the input stream
run(Workflow,Input_list) when is_pid(Workflow)->
   Bin = utils:spawn_src(Input_list),
   Bin(Workflow);
run(Workflow,List) when is_list(Workflow) ->
   Bin = (utils:spawn_sink())(self()),
   Parsed_workflow = make(Workflow,Bin),
   run(Parsed_workflow,List).

% parses the workflow
make(Workflow,Bin) ->
   Funcs = [parse(Item) || Item <-Workflow],
   lists:foldr(fun(Func,Pid)-> Func(Pid) end, Bin, Funcs).

parse(Fun) when is_function(Fun,1)->
   parse({seq,Fun});
parse({seq,Fun}) when is_function(Fun,1)->
   make_seq(Fun);
parse({farm,Workflow,W}) ->
   make_farm(W,Workflow);
parse({pipe,Workflow}) ->
   make_pipe(Workflow).

make_pipe(Workflow) ->
   fun(Pid) ->
      make(Workflow,Pid)
   end.

make_seq(Fun) ->
   fun(Pid) ->
      spawn(?MODULE,start_seq,[Fun,Pid])
   end.
start_seq(Seq_fun,Pid) ->
   Fun = utils:apply(Seq_fun),
   loop_fun(Fun,Pid).
loop_fun(Fun,Pid) ->
   receive
      {input,_} = Msg_input ->
         Msg_input1 = Fun(Msg_input),
         Pid ! Msg_input1,
         loop_fun(Fun,Pid);
      {msg,eos} ->
         Pid ! {msg,eos},
         eos
   end.

% farm paradigm using a collector and an emitter
make_farm(W,Workflow) ->
   fun(Pid) ->
      Collector = spawn(?MODULE,collect,[W,Pid]),
      Workers = spawn_procs(W,Workflow,Collector),
      spawn(?MODULE, emit,[Workers])
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

emit([Worker|Rest]=Workers) ->
   receive
      {input,_} = Input ->
         Worker ! Input,
         emit(Rest++[Worker]);
      {msg, eos} ->
         stop_procs(Workers)
   end.

stop_procs([]) ->
   eos;
stop_procs([Worker|Rest])->
   Worker ! {msg, eos},
   stop_procs(Rest).

spawn_procs(W,Workflow,Pid) ->
   spawn_procs(W,Workflow,Pid,[]).
spawn_procs(W,_Workflow,_Pid,Workers) when W <1 ->
   Workers;
spawn_procs(W,Workflow,Pid,Workers) ->
   Worker = do_job(Workflow,Pid),
   spawn_procs(W-1,Workflow,Pid,[Worker|Workers]).

do_job(Workflow,Pid) ->
   make(Workflow,Pid).
