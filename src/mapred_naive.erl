-module(mapred_naive).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/4, start/5, usage/0]).

usage() -> ?MAPRED_NAIVE_H.

start(M_Fun, R_Fun, List, Acc) ->
   ?LOG_CALL(?NOW),
   Split = {processes, utils:get_schedulers()},
   start(M_Fun, R_Fun, List, Acc, Split).
start(M_Fun, R_Fun, {chunked,Chunks}=List, Acc, Split) ->
   ?LOG_CALL(?NOW),
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, M_Fun, R_Fun, Chunks, Acc) end),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {Pid, Result} -> Result
   after
      ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid)),
      exit(timed_out)
   end;
start(M_Fun, R_Fun, List, Acc, Split) when is_integer(Split) ->
   ?LOG_CALL(?NOW),
   Chunks = utils:make_chunks(Split,List),
   start(M_Fun, R_Fun, {chunked,Chunks}, Acc, Split);
start(M_Fun, R_Fun, List, Acc, {processes, X}=Split) ->
   ?LOG_CALL(?NOW),
   L = length(List),
   case L rem X of
      0 ->
         Chunks = utils:make_chunks(L div X, List);
      _ ->
         Chunks = utils:make_chunks(L div X + 1, List)
   end,
   start(M_Fun, R_Fun, {chunked,Chunks}, Acc, Split).

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
