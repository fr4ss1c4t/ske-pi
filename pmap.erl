-module(pmap).
-include("include/usages.hrl").
-include("include/defines.hrl").
-export([start/2, usage/0]).

usage() -> ?PMAP_H.

start(M_Fun, Chunks) ->
   ?LOG_CALL(?NOW),
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
         collect(Pid,N-1,Tag,[Result|List])
   after ?TIMEOUT -> (?LOG_TIMEOUT(?NOW,?TIMEOUT,Tag))
   end.
