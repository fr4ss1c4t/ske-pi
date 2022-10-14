-module(mapred).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/4, start/5, start/6, usage/0]).

usage() -> ?MAPRED_H.

start(M_Fun, R_Fun, Acc, List) ->
   ?LOG_CALL(?NOW),
   start(M_Fun, R_Fun, R_Fun, Acc, List, {processes, utils:get_schedulers()}).
start(M_Fun, R_Fun, Acc, List, Split) ->
   ?LOG_CALL(?NOW),
   start(M_Fun, R_Fun, R_Fun, Acc, List, Split).
start(M_Fun, R_Fun, Combiner, Acc, {chunked,Chunks}=List, Split) ->
   ?LOG_CALL(?NOW),
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () ->
            reduce(S, Tag, M_Fun, R_Fun, Combiner, Acc, Chunks) end),

   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {Pid, Result} -> Result
   after
      ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid)),
      exit(timed_out)
   end;
start(M_Fun, R_Fun,Combiner, Acc, List, Split) when is_integer(Split) ->
   ?LOG_CALL(?NOW),
   Chunks = utils:make_chunks(Split,List),
   start(M_Fun, R_Fun, Combiner, Acc, {chunked,Chunks},  Split);
start(M_Fun, R_Fun, Combiner, Acc, List, {processes, X}=Split) ->
   ?LOG_CALL(?NOW),
   L = length(List),
   case L rem X of
      0 ->
         Chunks = utils:make_chunks(L div X, List);
      _ ->
         Chunks = utils:make_chunks(L div X + 1, List)
   end,
   start(M_Fun, R_Fun, Combiner, Acc, {chunked,Chunks}, Split).

reduce(Parent, Tag, M_Fun, R_Fun, Combiner, Acc, List) ->
   ?LOG_CALL(?NOW),
   Pid = self(),
   spawn_procs(Pid, Tag, M_Fun, List),
   R = length(List),
   Final_Res = collect(Tag, Pid, R, R_Fun, Combiner, Acc),
   ?LOG_SENT(self(),Parent,?NOW),
   Parent ! {self(), Final_Res}.

collect(_, _, 0, _, _, Acc) ->
   ?LOG_CALL(?NOW),
   Acc;
collect(Tag, Parent, N, R_Fun, Combiner, Acc) ->
   receive
      {Pid, Tag, Result} ->
         ?LOG_RCVD(self(),Pid,?NOW),
         Reduced = case is_list(Acc) of
            true ->
               lists:foldl(fun(A,B) -> catch(R_Fun(A,B)) end, [], Result);
            _ ->
               lists:foldl(fun(A,B) -> catch(R_Fun(A,B)) end, 0, Result)
            end,
         collect(Tag,Parent,N-1,R_Fun,Combiner,catch(Combiner(Reduced,Acc)))
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
   ?LOG_SENT(self(),Pid,?NOW).
