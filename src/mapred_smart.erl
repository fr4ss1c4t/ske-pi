-module(mapred_smart).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/4, start/5, usage/0]).

usage() -> ?MAPRED_SMART_H.

start(M_Fun, R_Fun, Acc, List) ->
   ?LOG_CALL(?NOW),
   Split = {processes, utils:get_schedulers()},
   start(M_Fun, R_Fun, Acc, List, Split).
start(M_Fun, R_Fun, Acc, {chunked,Chunks}=List, Split) ->
   ?LOG_CALL(?NOW),
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, M_Fun, R_Fun, Acc, Chunks) end),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {Pid, Result} -> Result
   after
      ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid)),
      exit(timed_out)
   end;
start(M_Fun, R_Fun, Acc, List, Split) when is_integer(Split) ->
   ?LOG_CALL(?NOW),
   Chunks = utils:make_chunks(Split,List),
   start(M_Fun, R_Fun, Acc, {chunked,Chunks},  Split);
start(M_Fun, R_Fun, Acc, List, {processes, X}=Split) ->
   ?LOG_CALL(?NOW),
   L = length(List),
   case L rem X of
      0 ->
         Chunks = utils:make_chunks(L div X, List);
      _ ->
         Chunks = utils:make_chunks(L div X + 1, List)
   end,
   start(M_Fun, R_Fun, Acc, {chunked,Chunks}, Split).

reduce(Parent, Tag, M_Fun, R_Fun, Acc, List) ->
   Pid = self(),
   spawn_procs(Pid, Tag, M_Fun, List),
   R = length(List),
   Final_res = collect(Tag, Pid, R, R_Fun, Acc),
   ?LOG_SENT(self(),Parent,?NOW),
   Parent ! {self(), Final_res}.

collect(_, _, 0, _, Acc) ->
   ?LOG_CALL(?NOW),
   Acc;
collect(Tag, Parent, N, R_Fun, Acc) ->
   receive
      {Pid, Tag, Result} -> ?LOG_RCVD(self(),Pid,?NOW),
         collect(Tag,Parent, N-1, R_Fun, catch(R_Fun(Result,Acc)))
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
   Pid ! {self(), Tag, catch(catch(M_Fun(X)))},
   ?LOG_SENT(self(),Pid,?NOW).
