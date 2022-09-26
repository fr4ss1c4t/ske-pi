-module(dmapred).
-export([start/3, usage/0]).

usage() ->
   io:format("--- dmapred description ---~n",[]),
   io:format("mapreduce implemented with erlang/otp's dict dictionary ~n",[]),
   io:format("data structure. ~n",[]),
   io:format("takes two user-defined map and reduce functions and a  ~n",[]),
   io:format("list of inputs. it starts as many processes as there~n",[]),
   io:format("elements in the input list, computes intermediate results~n",[]),
   io:format("and sends those to the reduce processes. the final  ~n",[]),
   io:format("results are then collected into a single output.~n ~n",[]),
   io:format("usage example:~n",[]),
   io:format(">IndexedFiles = utils:list_numbered_files(~c<dirpath>~c).~n",[$",$"]),
   io:format(">M_Fun = utils:make_filter_mapper(<regex>).~n",[]),
   io:format(">R_Fun = fun utils:remove_duplicates/3.~n",[]),
   io:format(">dict:find(<regex>, dmapred:start(M_Fun,R_Fun,IndexedFiles)).~n",[]),
   io:format("expected output:~n",[]),
   io:format("{ok, [<filepath(s)-containing-regex>]}~n",[]),
   io:format("NB: <regex> is the erlang atom we are looking for,~n",[]),
   io:format("~c<dirpath>~c is the string of the path of the directory~n",[$",$"]),
   io:format("inside of which we are looking.~n").


% the entry point for the MapReduce framework (the google version)
start(M_Fun, R_Fun,Input) ->
   MRPid = self(),
   Pid = spawn(fun () -> reduce(MRPid, M_Fun, R_Fun, Input) end),
   receive
      {Pid, Result} -> Result
   end.

% the mapper spawns M mapper processes, applies the
% user-defined function to its inputs and then collects
% the results
map(Pid, M_Fun, Input) ->
   spawn_procs(Pid, M_Fun, Input),
   M = length(Input),
   collect(M,dict:new()).

% the reducer spawns R reducer processes, applies the
% user-defined function to the intermediate results generated
% by the mapper and then collects the results
reduce(Parent, M_Fun, R_Fun, Input) ->
   % trap_exit set to true means that signals arriving to a
   % process are converted to {'EXIT', From, Reason} messages
   process_flag(trap_exit, true),
   Pid = self(),

   Intermediate_res = map(Pid,M_Fun,Input),

   spawn_procs(Pid, R_Fun, dict:to_list(Intermediate_res)),
   R = dict:size(Intermediate_res),
   Final_res = collect(R, dict:new()),

   Parent ! {self(), Final_res}.

% the collector receives N key-value pairs and merges them
% together
collect(0,Dict) -> Dict;
collect(N,Dict) ->
   receive
      {Key, Val} ->
      Dict1 = dict:append(Key,Val,Dict),
      collect(N,Dict1);
      {'EXIT', _Who, _Why} ->
         collect(N-1, Dict)
   end.

% spawns a process for each key-value pair
spawn_procs(Parent, Fun, Pairs) ->
   lists:foreach(
   fun({K,V}) ->
      spawn_link(fun() ->
         do_job(Parent, Fun, {K,V}) end)
   end, Pairs).

% sends key-value pairs to Pid and then terminates
do_job(Pid, Fun, {K,V}) ->
   Fun(K,V, fun(K2,V2) -> Pid ! {K2,V2} end).
