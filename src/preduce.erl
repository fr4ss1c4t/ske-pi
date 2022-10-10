-module(preduce).
-include("include/usages.hrl").
-include("include/defines.hrl").
-export([start/3, start/4,start/5, usage/0]).

usage() -> ?PREDUCE_H.

start(Fun, Acc, List) ->
   ?LOG_CALL(?NOW),
   start(Fun, Fun, Acc, List, {processes, utils:get_schedulers()}).
start(Fun, Acc, List, Split) ->
   ?LOG_CALL(?NOW),
   start(Fun, Fun, Acc, List, Split).
start(Fun, Combiner, Acc, List, Split) ->
   ?LOG_CALL(?NOW),
   Fun2 = fun (L) -> lists:foldl(Fun, Acc, L) end,
   reduce(Fun2, Combiner, List, Split).

reduce(Fun, Combiner, List, Split) ->
   ?LOG_CALL(?NOW),
   reduce(Fun, Combiner, List, no_split, Split).
reduce(Fun, Combiner, List, no_split, Split) when is_integer(Split) ->
   ?LOG_CALL(?NOW),
   reduce(Fun, Combiner, List, split, Split);
reduce(Fun, Combiner, List, no_split, {processes,X}) ->
   ?LOG_CALL(?NOW),
   L = length(List),
   case L rem X of
      0 ->
         reduce(Fun, Combiner, List, split, L div X);
      _ ->
         reduce(Fun, Combiner, List, split, L div X + 1)
   end;
reduce(Fun, Combiner, List, split, Chunks_Len) ->
   ?LOG_CALL(?NOW),
   Chunks = utils:make_chunks(Chunks_Len,List),
   reduce(Fun, Combiner, Chunks).
reduce(Fun, Combiner, List) ->
   ?LOG_CALL(?NOW),
   Parent = self (),
   Pids =
      lists:map(fun (L) ->
         F = fun () -> Parent ! {self (), catch(Fun(L))},
            ?LOG_SENT(self(),Parent,?NOW)
         end,
         {Pid, _} = erlang:spawn_monitor(F), Pid end,
         List),
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
