-module(mapred_naive).
-include("include/usages.hrl").
-include("include/defines.hrl").
-export([start/2, start/4, usage/0]).

usage() -> ?MAPRED_NAIVE_H.

start(M_Fun, List) ->
   R_Fun = fun(X,Y) -> X+Y end,
   Acc = 0,
   start(M_Fun, R_Fun, List, Acc).

start(M_Fun, R_Fun, List, Acc) ->
   ?LOG_CALL(?NOW),
   ?LOG_ERROR(?NOW, ?REASON),
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, M_Fun, R_Fun, List, Acc) end),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {Pid, Result} -> Result
   after ?TIMEOUT -> (?LOG_TIMEOUT(self(),Pid,?NOW,?TIMEOUT))
   end.

reduce(Parent, Tag, M_Fun, R_Fun, List, Acc) ->
   Pid = self(),
   spawn_procs(Pid, Tag, M_Fun, List),
   R = length(List),
   Final_res = collect(R, Tag, R_Fun, Acc),
   Parent ! {self(), Final_res}.

collect(0, _, _, Acc) -> Acc;
collect(N, Tag, R_Fun, Acc) ->
   receive
      {Tag, Result} -> collect(N-1, Tag, R_Fun, R_Fun(Result,Acc))
   end.

spawn_procs(Pid, Tag, M_Fun, List) ->
   lists:foreach(
   fun(I) ->
      spawn_link(fun() ->
         do_job(Pid, Tag, M_Fun, I) end)
   end, List).

do_job(Pid, Tag, M_Fun, X) ->
   Pid ! {Tag, catch(M_Fun(X))},
   ?LOG_SENT(Pid,self(),?NOW).
