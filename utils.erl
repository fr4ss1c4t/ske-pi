-module(utils).
-compile(nowarn_export_all).
-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%--------------Data Parallel Utils-----------%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% splits the list into chunks of equal length
make_chunks(Len,List) ->
   make_chunks(List,[],0,Len).
make_chunks([],Acc,_,_) -> Acc;
make_chunks([Hd|Tl],Acc,Start,Max) when Start==Max ->
   make_chunks(Tl,[[Hd] | Acc],1,Max);
make_chunks([Hd|Tl],[Hd0 | Tl0],Start,Max) ->
   make_chunks(Tl,[[Hd | Hd0] | Tl0],Start+1,Max);
make_chunks([Hd|Tl],[],Start,Max) ->
   make_chunks(Tl,[[Hd]],Start+1,Max).

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

% this function is used as a Map function
make_filter_mapper(MatchWord) ->
  fun (_Index, FileName, Emit) ->
    {ok, [Words]} = file:consult(FileName),
    lists:foreach(fun (Word) ->
      case MatchWord == Word of
        true -> Emit(Word, FileName);
        false -> false
      end
    end, Words)
  end.

% this function is used as a Reduce function
remove_duplicates(Word, FileNames, Emit) ->
  UniqueFiles = sets:to_list(sets:from_list(FileNames)),
  lists:foreach(fun (FileName) -> Emit(Word, FileName) end, UniqueFiles).

%% Auxiliary function to generate {Index, FileName} input
list_numbered_files(DirName) ->
  {ok, Files} = file:list_dir(DirName),
  FullFiles = [ filename:join(DirName, File) || File <- Files ],
  Indices = lists:seq(1, length(Files)),
  lists:zip(Indices, FullFiles). % {Index, FileName} tuples

% gets the path containing the test directory
get_test_dirpath() ->
   {_,Curpath} = file:get_cwd(),
   Abspath = filename:dirname(Curpath)++ "/".

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%--------Stream Parallel Utils-------------%%
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%-------------Testing Utils----------------%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% tests the given function N times and puts the
% results in a list of time measurements
test_loop(0,_Fun, Times) ->
   Times;
test_loop(N,Fun,Times) ->
   {Time,_} = timer:tc(Fun),
   test_loop(N-1,Fun,[Time|Times]).

% takes the mean time of a list of time measurements
% after removing the worst and best ones
mean(List) ->
   Clean_list = tl(lists:reverse(tl(lists:sort(List)))),
   lists:foldl(fun(X,Sum)-> X+Sum end, 0, Clean_list) / length(Clean_list).

% takes the median of a list of time measurements
median(List) ->
lists:nth(round((length(List) / 2)), lists:sort(List)).

% takes the speedup. that is, the improvement in speed
% between the sequential version and the parallel version
speedup(Time_seq,Time_par) ->
   Time_seq/Time_par.

% sets the number of schedulers online
set_schedulers(N) ->
   catch(erlang:system_flag(schedulers_online,N)).

% return the number of schedulers schedulers_online
get_schedulers() ->
   erlang:system_info(schedulers_online).
