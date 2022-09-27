-module(stream).
-compile(nowarn_export_all).
-compile(export_all).

usage() ->
   io:format("--- stream farm description ---~n",[]),
   io:format("takes the number of worker processes (optional),~n",[]),
   io:format("the task performed by each worker and a stream of chunks~n",[]),
   io:format("of a list. ~n",[]),
   io:format("the emitter distributes each chunk between the workers ~n",[]),
   io:format("and the collector gathers the final output. ~n~n",[]),
   io:format("usage example:~n",[]),
   io:format(">List = [[12,3,45231],[1231,231,4],[1],[6,6,6,7,6]].~n",[]),
   io:format(">W_Fun = fun lists:sort/1.~n",[]),
   io:format(">stream:start_farm(4, W_Fun, List).~n",[]),
   io:format("expected output:~n",[]),
   io:format("[[3,12,45231],[4,231,1231],[1],[6,6,6,6,7]]~n~n",[]),

   io:format("--- stream pipe description ---~n",[]),
   io:format("takes a list of stages(where each one represents a ~n",[]),
   io:format("function) and a list of chunks. ~n",[]),
   io:format("the output of one stage is the input of the next one.~n~n",[]),
   io:format("usage example:~n",[]),
   io:format(">List = [[12,3,45231],[1231,231,4],[1],[6,6,6,7,6]].~n",[]),
   io:format(">Stage_One = fun(Chunk) -> [X*X || X<-Chunk] end.~n",[]),
   io:format(">Stage_Two = fun lists:sort/1.~n",[]),
   io:format(">Stages = [Stage_One, Stage_Two].~n",[]),
   io:format(">stream:start_pipe(Stages, List).~n",[]),
   io:format("expected output:~n",[]),
   io:format("[[9,144,2045843361],[16,53361,1515361],[1],[36,36,36,36,49]]~n",[]).

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
   Parsed_Workflow = make(Tasks,Bin),
   run(Parsed_Workflow,List).

% parses the tasks
make(Tasks,Bin) ->
   Funcs = [parse(Task) || Task <-Tasks],
   lists:foldr(fun(Func,Pid)-> Func(Pid) end, Bin, Funcs).

parse(Fun) when is_function(Fun,1)->
   parse({seq,Fun});
parse({seq,Fun}) when is_function(Fun,1)->
   make_seq(Fun);
parse({farm,Tasks,W}) ->
   make_farm(W,Tasks);
parse({pipe,Tasks}) ->
   make_pipe(Tasks).

% farm paradigm using a collector and an emitter
make_farm(W,Tasks) ->
   fun(Pid) ->
      Collector = spawn(?MODULE,collect,[W,Pid]),
      Workers = spawn_procs(W,Tasks,Collector),
      spawn(?MODULE, emit,[Workers])
   end.

make_pipe(Tasks) ->
   fun(Pid) ->
      make(Tasks,Pid)
   end.

make_seq(Fun) ->
   fun(Pid) ->
      spawn(?MODULE,run_seq,[Fun,Pid])
   end.
run_seq(Seq_Fun,Pid) ->
   Fun = utils:apply(Seq_Fun),
   loop_fun(Fun,Pid).

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

spawn_procs(W,Tasks,Pid) ->
   spawn_procs(W,Tasks,Pid,[]).
spawn_procs(W,_Tasks,_Pid,Workers) when W <1 ->
   Workers;
spawn_procs(W,Tasks,Pid,Workers) ->
   Worker = do_job(Tasks,Pid),
   spawn_procs(W-1,Tasks,Pid,[Worker|Workers]).

do_job(Tasks,Pid) ->
   make(Tasks,Pid).
