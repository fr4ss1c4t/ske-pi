-module(pmap).
-export([start/2, usage/0]).

usage() ->
   io:format("--- pmap description ---~n",[]),
   io:format("takes a function and a list of chunks. ~n",[]),
   io:format("it spawns a process for each element in the list ~n",[]),
   io:format("of chunks. each mapper process applies the function ~n",[]),
   io:format("to its chunk and collects the results (non-ordered)~n~n",[]),
   io:format("usage example:~n",[]),
   io:format(">List = [[12,3,45231],[1231,231,4],[1],[6,6,6,7,6]].~n",[]),
   io:format(">pmap:start(fun lists:sort/1, List).~n",[]),
   io:format("expected output:~n",[]),
   io:format("[[6,6,6,6,7],[1],[4,231,1231],[3,12,45231]]~n",[]).


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
