-module(mapred_naive).
-include("include/usages.hrl").
-include("include/defines.hrl").
-export([start/2, start/4, usage/0]).

usage() -> ?MAPRED_NAIVE_H.

start(M_Fun, List) ->
   ?LOG_CALL(?NOW),
   R_Fun = fun(X,Y) -> X+Y end,
   Acc = 0,
   start(M_Fun, R_Fun, List, Acc).

start(M_Fun, R_Fun, List, Acc) ->
   ?LOG_CALL(?NOW),
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, M_Fun, R_Fun, List, Acc) end),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {Pid, Result} -> Result
   after ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid))
   end.

reduce(Parent, Tag, M_Fun, R_Fun, List, Acc) ->
   Pid = self(),
   spawn_procs(Pid, Tag, M_Fun, List),
   R = length(List),
   Final_res = collect(Tag, Pid, R, R_Fun, Acc),
   ?LOG_SENT(Parent,self(),?NOW),
   Parent ! {self(), Final_res}.

collect(_, _, 0, _, Acc) ->
   ?LOG_CALL(?NOW),
   Acc;
collect(Tag, Parent, N, R_Fun, Acc) ->
   receive
      {Pid, Tag, Result} -> ?LOG_RCVD(self(),Pid,?NOW),
         collect(Tag,Parent, N-1, R_Fun, R_Fun(Result,Acc))
   end.

spawn_procs(Pid, Tag, M_Fun, List) ->
   ?LOG_CALL(?NOW),
   lists:foreach(
   fun(I) ->
      spawn_link(fun() ->
         do_job(Pid, Tag, M_Fun, I) end)
   end, List).

do_job(Pid, Tag, M_Fun, X) ->
   ?LOG_CALL(?NOW),
   Pid ! {self(), Tag, catch(M_Fun(X))},
   ?LOG_SENT(Pid,self(),?NOW).
