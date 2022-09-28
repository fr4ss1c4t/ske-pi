-module(pmap).
-include("include/usages.hrl").
-export([start/2, usage/0]).

usage() -> ?PMAP_H.


start(M_Fun, Chunks) ->
   S = self(),
   Tag = erlang:make_ref(),
   lists:foreach(
   fun(I) ->
      spawn(fun() ->  do_job(S, Tag, M_Fun, I) end)
   end, Chunks),
   collect(length(Chunks), Tag, []).

do_job(Parent,Tag, M_Fun, I) ->
   Parent ! {Tag, catch(M_Fun(I))}.

collect(0,_,List) -> List;
collect(N,Tag,List) ->
   receive
      {Tag,Result} -> collect(N-1,Tag,[Result|List])
   end.
