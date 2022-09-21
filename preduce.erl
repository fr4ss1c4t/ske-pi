-module (preduce).
-export([start/2]).
% takes a function and a list of chunks and sends each chunk
% to a reducer process. each reducer applies the user-defined
% function to its input and produces a single output, which
% is collected into a list (non-ordered)
%
% usage example:
% preduce:start(fun lists:sum/1, [[1,2,3],[4,5]]).
% expected output:
% [9,6]
start(R_Fun, List) ->
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, R_Fun, List) end),
   receive
      {Pid, Result} -> Result
   end.

reduce(Parent, Tag, R_Fun, List) ->
   Pid = self(),
   spawn_procs(Pid, Tag, R_Fun, List),
   R = length(List),
   Final_res = collect(R, Tag, []),
   Parent ! {self(), Final_res}.

collect(0, _, List) -> List;
collect(N, Tag, List) ->
   receive
      {Tag, Result} -> collect(N-1, Tag, [Result|List])
   end.

spawn_procs(Pid, Tag, R_Fun, List) ->
   lists:foreach(
   fun(I) ->
      spawn_link(fun() ->
         do_job(Pid, Tag, R_Fun, I) end)
   end, List).

do_job(Pid, Tag, R_Fun, X) ->
   Pid ! {Tag, catch(R_Fun(X))}.
