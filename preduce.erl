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
start(R_func, List) ->
   S = self(),
   Pid = spawn(fun () -> reduce(S, R_func, List) end),
   receive
      {Pid, Result} -> Result
   end.

reduce(Parent, R_func, List) ->
   Pid = self(),
   spawn_procs(Pid, R_func, List),
   R = length(List),
   Final_res = collect(R, []),
   Parent ! {self(), Final_res}.

collect(0,List) -> List;
collect(N, List) ->
   receive
      Result -> collect(N-1, [Result|List])
   end.

spawn_procs(Pid, R_func, List) ->
   lists:foreach(
     fun(I) ->
           spawn_link(fun() ->
              do_job(Pid, R_func, I) end)
    end, List).

do_job(Pid, Fun, X) ->
   Pid ! (catch(Fun(X))).
