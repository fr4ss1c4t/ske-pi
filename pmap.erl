-module(pmap).
-export([start/2]).
% takes a function and a list of chunks and spawns a process
% for each element in the input list. each mapper process
% applies the user-defined function to its input
% and collects the results (non-ordered)
%
% usage example:
% List = [[12,3,45231],[1231,231,4],[1],[6,6,6,7,6]].
% pmap:start(fun lists:sort/1,List).
% exptected output:
% [[6,6,6,6,7],[1],[4,231,1231],[3,12,45231]]

start(M_Fun, List) ->
   S = self(),
   Tag = erlang:make_ref(),
   lists:foreach(
   fun(I) ->
      spawn(fun() ->  do_job(S, Tag, M_Fun, I) end)
   end, List),
   collect(length(List), Tag, []).

do_job(Parent,Tag, M_Fun, I) ->
   Parent ! {Tag, catch(M_Fun(I))}.

collect(0,_,List) -> List;
collect(N,Tag,List) ->
   receive
      {Tag,Result} -> collect(N-1,Tag,[Result|List])
   end.
