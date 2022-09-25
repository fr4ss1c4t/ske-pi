-module(pmapred_naive).
-export([start/2, start/4, usage/0]).

usage() ->
   io:format("--- pmapred_naive description ---~n",[]),
   io:format("takes an user-defined map function and a list of chunks. ~n",[]),
   io:format("it starts as many processes as there are chunks in input, ~n",[]),
   io:format("computes intermediate results and sends those to the  ~n",[]),
   io:format("reduce process. the final results are then collected ~n",[]),
   io:format("into a single output by adding them up.  ~n",[]),
   io:format("additionally, an user-defined reduce function (along~n",[]),
   io:format("with its accumulator) may be specified.~n~n",[]),
   io:format("usage example 1:~n",[]),
   io:format(">M_Fun = fun(Chunk)-> lists:sum([X*X||X<-Chunk]) end.~n",[]),
   io:format(">pmapred_naive:start(M_Fun, [[1,2,3],[4,5]]).~n",[]),
   io:format("expected output:~n",[]),
   io:format("55~n~n"),

   io:format("usage example 2:~n",[]),
   io:format(">M_Fun = fun(Chunk)-> [X*X||X<-Chunk] end.~n",[]),
   io:format(">R_Fun = fun(X,Y) -> X++Y end.~n",[]),
   io:format(">pmapred_naive:start(M_Fun, R_Fun, [[1,2,3],[4,5]], []).~n",[]),
   io:format("expected output:~n",[]),
   io:format("[16,25,1,4,9]~n",[]).

start(M_Fun, List) ->
   R_Fun = fun(X,Y) -> X+Y end,
   Acc = 0,

   start(M_Fun, R_Fun, List, Acc).

start(M_Fun, R_Fun, List, Acc) ->
   S = self(),
   Tag = erlang:make_ref(),
   Pid = spawn(fun () -> reduce(S, Tag, M_Fun, R_Fun, List, Acc) end),
   receive
      {Pid, Result} -> Result
   end.

reduce(Parent, Tag, M_Fun, R_Fun, List, Acc) ->
   Pid = self(),
   spawn_procs(Pid, Tag, M_Fun, List),
   R = length(List),
   Final_res = collect(R, Tag, R_Fun, Acc),
   Parent ! {self(), Final_res}.

collect(0, _, _, Acc) -> Acc;
collect(N, Tag, R_Fun, Acc) ->
   receive
      {Tag, Result} -> collect(N-1, Tag, R_Fun, R_Fun(Result,Acc))
   end.

spawn_procs(Pid, Tag, M_Fun, List) ->
   lists:foreach(
   fun(I) ->
      spawn_link(fun() ->
         do_job(Pid, Tag, M_Fun, I) end)
   end, List).

do_job(Pid, Tag, M_Fun, X) ->
   Pid ! {Tag, catch(M_Fun(X))}.
