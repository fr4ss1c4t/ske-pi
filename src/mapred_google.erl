-module(mapred_google).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/3, usage/0]).

usage() -> ?MAPRED_GOOGLE_H.

% the entry point for the MapReduce framework (the google version)
start(M_Fun, R_Fun,Input) ->
   ?LOG_CALL(?NOW),
   MRPid = self(),
   Pid = spawn(fun () -> reduce(MRPid, M_Fun, R_Fun, Input) end),
   receive
      {Pid, Result} ->
         ?LOG_RCVD(self(),Pid,?NOW),
         Result
   after
      ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid)),
      exit(timed_out)
   end.

% the mapper spawns M mapper processes, applies the
% user-defined function to its inputs and then collects
% the results
map(Pid, M_Fun, Input) ->
   ?LOG_CALL(?NOW),
   spawn_procs(Pid, M_Fun, Input),
   M = length(Input),
   collect(M,dict:new()).

% the reducer spawns R reducer processes, applies the
% user-defined function to the intermediate results generated
% by the mapper and then collects the results
reduce(Parent, M_Fun, R_Fun, Input) ->
   ?LOG_CALL(?NOW),
   % trap_exit set to true means that signals arriving to a
   % process are converted to {'EXIT', From, Reason} messages
   process_flag(trap_exit, true),
   Pid = self(),

   Intermediate_res = map(Pid,M_Fun,Input),

   spawn_procs(Pid, R_Fun, dict:to_list(Intermediate_res)),
   R = dict:size(Intermediate_res),
   Final_res = collect(R, dict:new()),
   ?LOG_SENT(self(),Parent,?NOW),
   Parent ! {self(), Final_res}.

% the collector receives N key-value pairs and merges them
% together
collect(0,Dict) ->
   ?LOG_CALL(?NOW),
   Dict;
collect(N,Dict) ->
   receive
      {Key, Val} ->
      Dict1 = dict:append(Key,Val,Dict),
      collect(N,Dict1);
      {'EXIT', Who, _Why} ->
         ?LOG_RCVD(self(),Who,?NOW),
         collect(N-1, Dict)
   end.

% spawns a process for each key-value pair
spawn_procs(Parent, Fun, Pairs) ->
   ?LOG_CALL(?NOW),
   lists:foreach(
   fun({K,V}) ->
      spawn_link(fun() ->
         do_job(Parent, Fun, {K,V}) end)
   end, Pairs).

% sends key-value pairs to Pid and then terminates
do_job(Pid, Fun, {K,V}) ->
   ?LOG_CALL(?NOW),
   ?LOG_SENT(self(),Pid,?NOW),
   catch(Fun(K,V, fun(K2,V2) -> Pid ! {K2,V2} end)).
