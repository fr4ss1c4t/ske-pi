-module(utils).
-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%--------------Data Parallel Utils-----------%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% splits the list into chunks of equal length 
make_chunks([], _) -> [];
make_chunks(List, Len) when Len > length(List) 
                         -> [List];
make_chunks(List, Len) ->
   {Xs, Ys} = lists:split(Len, List),
   [Xs | make_chunks(Ys, Len)].

% applies the input function N times and puts the
% results into a list
apply_n_times(N,Func) ->
   lists:map(Func, lists:seq(0,N-1)).

% creates a list of all the integers from 0 to (2^Exp - 1)
create_list(Exp) ->
   Len = round(math:pow(2,Exp)),
   lists:seq(0, Len-1).

% combines a list of lists into a single list
combine([]) -> [];
combine([X|Xs]) -> lists:append(X, combine(Xs)).

% cleans up the output of the google mapreduce
clean_up(Result) ->
   Tuples = dict:to_list(Result),
   [ X||{_,[X]}<-Tuples].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%---------Stream Parallel Utils------------%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

% reads from the input stream of chunks of a
% list and sends them to the first function
% in the workflow
spawn_src(Input) ->
   fun(Pid) ->
      spawn(?MODULE, start_src,[Input,Pid])
   end.
start_src(Input,Pid) ->
   loop_src(Input,Pid).
loop_src([],Pid) ->
   stop(Pid);
loop_src([Input|Inputs],Pid) ->
   send(Input,Pid),
   loop_src(Inputs,Pid).

% accepts inputs from the output stream and
% accumulates the results in a list
spawn_sink() ->
   fun(Pid) ->
      spawn(?MODULE,start_sink,[Pid])
   end.
start_sink(Pid) ->
   loop_sink(Pid,[]).
loop_sink(Pid,Results) ->
   receive
      {input,_} = Msg ->
         Input = extract(Msg),
         loop_sink(Pid,Results++[Input]);
      {msg,eos} ->
         Pid ! {results,Results}
   end.

% convert to and from tuple form
format(Input) ->
   {input,Input}.
extract({input,Input}) ->
   Input.
apply(Fun) ->
   fun({input,Input}) ->
      {input,Fun(Input)}
   end.

% send messages to a process, eos is an
% atom representing the end of the stream
stop(Pid) ->
   Pid ! {msg,eos}.
send(Input,Pid) ->
   Msg = format(Input),
   Pid ! Msg.
send_results(Results,Pid) ->
   Pid ! {results,Results}.
