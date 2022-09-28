-module(mapred_google).
-include("include/usages.hrl").
-export([start/3, usage/0]).

usage() -> ?MAPRED_GOOGLE_H.


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
