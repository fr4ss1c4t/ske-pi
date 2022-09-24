-module(pmapred_naive).
-export([start/2, usage/0]).

usage() ->
   io:format("--- pmapred_naive description ---~n",[]),
   io:format("takes a list of chunks and an user-defined function. ~n",[]),
   io:format("it starts as many processes as there are chunks in input, ~n",[]),
   io:format("computes intermediate results and sends those to the  ~n",[]),
   io:format("reduce process. the final results are then collected ~n",[]),
   io:format("into a single output.  ~n~n",[]),
   io:format("usage example:~n",[]),
   io:format(">pmapred_naive:start(fun(Chunk)->lists:sum([X*X||X<-Chunk])",[]),
   io:format("end, [[1,2,3],[4,5]]).~n",[]),
   io:format("expected output:~n",[]),
   io:format("55~n",[]).

start(M_Fun, List) ->
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, M_Fun, List) end),
   receive
      {Pid, Result} -> Result
   end.

reduce(Parent, Tag, M_Fun, List) ->
   Pid = self(),
   spawn_procs(Pid, Tag, M_Fun, List),
   R = length(List),
   Final_res = collect(R, Tag, 0),
   Parent ! {self(), Final_res}.

collect(0, _, Acc) -> Acc;
collect(N, Tag, Acc) ->
   receive
      {Tag, Result} -> collect(N-1, Tag, Result+Acc)
   end.

spawn_procs(Pid, Tag, M_Fun, List) ->
   lists:foreach(
   fun(I) ->
      spawn_link(fun() ->
         do_job(Pid, Tag, M_Fun, I) end)
   end, List).

do_job(Pid, Tag, M_Fun, X) ->
   Pid ! {Tag, catch(M_Fun(X))}.
