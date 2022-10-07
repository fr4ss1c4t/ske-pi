-module(pmap).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/2, start/3, usage/0]).

usage() -> ?PMAP_H.

start(M_Fun, List) ->
   ?LOG_CALL(?NOW),
   start(M_Fun, List, {processes, utils:get_schedulers()}).

start(M_Fun, List, Split) when is_integer(Split) ->
   ?LOG_CALL(?NOW),
   Chunks = utils:make_chunks(Split, List),
   S = self(),
   Tag = erlang:make_ref(),
   lists:foreach(
   fun(I) ->
      spawn(fun() ->  do_job(S, Tag, M_Fun, I) end)
   end, Chunks),
   collect(S,length(Chunks), Tag, []);
start(M_Fun, List, {processes,X}=Split) ->
   ?LOG_CALL(?NOW),
   L = length(List),
   case L rem X of
      0 ->
         Chunks = utils:make_chunks(L div X, List);
      _ ->
         Chunks = utils:make_chunks(L div X + 1, List)
   end,
   S = self(),
   Tag = erlang:make_ref(),
   lists:foreach(
   fun(I) ->
      spawn(fun() ->  do_job(S, Tag, M_Fun, I) end)
   end, Chunks),
   collect(S,length(Chunks), Tag, []).

do_job(Parent,Tag, M_Fun, I) ->
   ?LOG_CALL(?NOW),
   Parent ! {Tag, catch(M_Fun(I))},
   ?LOG_SENT(Parent,self(),?NOW).

collect(_,0,_,List) ->
   ?LOG_CALL(?NOW),
   List;
collect(Pid,N,Tag,List) ->
   receive
      {Tag,Result} -> ?LOG_RCVD(self(),Tag,?NOW),
         collect(Pid,N-1,Tag,Result++List)
   after ?TIMEOUT ->
      (?LOG_TIMEOUT(?NOW,?TIMEOUT,Tag)),
      exit(timed_out)
   end.
