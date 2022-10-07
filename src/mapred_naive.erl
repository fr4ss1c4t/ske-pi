-module(mapred_naive).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/4, start/5,start/6, usage/0]).

usage() -> ?MAPRED_NAIVE_H.

start(M_Fun, R_Fun, Acc, List) ->
   ?LOG_CALL(?NOW),
   start(M_Fun, R_Fun, R_Fun, Acc, List, {processes, utils:get_schedulers()}).
start(M_Fun, R_Fun, Acc, List, Split) ->
   ?LOG_CALL(?NOW),
   start(M_Fun, R_Fun, R_Fun, Acc, List, Split).
start(M_Fun, R_Fun, Combiner, Acc, List, Split) ->
   ?LOG_CALL(?NOW),
   R_Fun2 = fun (L) -> lists:foldl(R_Fun, Acc, L) end,
   reduce(M_Fun, R_Fun2, Combiner, List, Split).

map(M_Fun, Chunks) ->
   ?LOG_CALL(?NOW),
   Parent = self (),
   Pids =
      lists:map(fun (C) ->
         F = fun () -> Parent ! {self (), M_Fun(C)},
            ?LOG_SENT(self(),Parent,?NOW)
         end,
         {Pid, _} = erlang:spawn_monitor(F), Pid end,
         Chunks),
   lists:map(fun collect/1, Pids).

reduce(M_Fun, R_Fun, Combiner, List, Split) ->
   ?LOG_CALL(?NOW),
   reduce(M_Fun, R_Fun, Combiner, List, no_split, Split).
reduce(M_Fun, R_Fun, Combiner, List, no_split, SplitTerm) when is_integer(SplitTerm) ->
   ?LOG_CALL(?NOW),
   reduce(M_Fun, R_Fun, Combiner, List, split, SplitTerm);
reduce(M_Fun, R_Fun, Combiner, List, no_split, {processes,X}) ->
   ?LOG_CALL(?NOW),
   L = length(List),
   case L rem X of
      0 ->
         reduce(M_Fun, R_Fun, Combiner, List, split, L div X);
      _ ->
         reduce(M_Fun, R_Fun, Combiner, List, split, L div X + 1)
   end;
reduce(M_Fun, R_Fun, Combiner, List, split, Chunks_Len) ->
   ?LOG_CALL(?NOW),
   Chunks = utils:make_chunks(Chunks_Len,List),
   reduce(M_Fun, R_Fun, Combiner, Chunks).
reduce(M_Fun, R_Fun, Combiner, Chunks) ->
   ?LOG_CALL(?NOW),
   Parent = self (),
   Intermediate_Results = map(M_Fun, Chunks),
   Pids =
      lists:map(fun (L) ->
         F = fun () -> Parent ! {self (), R_Fun(L)},
            ?LOG_SENT(self(),Parent,?NOW)
         end,
         {Pid, _} = erlang:spawn_monitor(F), Pid end,
         Intermediate_Results),
   Results = lists:map(fun collect/1, Pids),
   utils:combine(Combiner, Results).

collect(Pid) ->
   ?LOG_CALL(?NOW),
   ?LOG_RCVD(self(),Pid,?NOW),
   receive
      {Pid, Result} -> Result
   after ?TIMEOUT ->
      (?LOG_TIMEOUT(?NOW,?TIMEOUT,Pid)),
      exit(timed_out)
   end.
