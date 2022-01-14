-module(pmapred).
-export([start/3]).

% the entry point for the MapReduce framework
% takes in a list, an user-defined mapper function and an user-
% defined reducer function. it starts as many processes as there
% are elements in the input list, computes intermediate results
% and sends those to the same number of reduce process. the final
% results are then collected into a list.
%
% example usage:
% List = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16].
% Chunks = utils:make_chunks(List,4).
% lists:sum(pmapred:start(Chunks, fun(Chunk)->[X*X||X<-Chunk] end,
%                                             fun lists:sum/1)).
% expected output:
% 1496
start( M_func, R_func,Input) ->
   MRPid = self(),
   Pid = spawn(fun () -> reduce(MRPid, M_func, R_func, Input) end),
   receive
      {Pid, Result} -> Result
   end.

% the mapper spawns M mapper processes, applies the
% user-defined function to its inputs and then collects
% the results
map(Pid, M_func, Input) ->
   spawn_procs(Pid, M_func, Input),
   M = length(Input),
   collect(M,[]).

% the reducer spawns R reducer processes, applies the
% user-defined function to the intermediate results generated
% by the mapper and then collects the results
reduce(Parent, M_func, R_func, Input) ->
   Pid = self(),

   Intermediate_res = map(Pid,M_func,Input),

   spawn_procs(Pid, R_func, Intermediate_res),
   R = length(Intermediate_res),
   Final_res = collect(R, []),

   Parent ! {self(), Final_res}.

% the collector receives N items and merges them
% together
collect(0,List) -> List;
collect(N,List) ->
   receive
      Item ->
         collect(N-1,[Item|List])
   end.

% spawns a process for each Chunk
spawn_procs(Pid, Fun, Chunk) ->
   lists:foreach(
   fun(X) ->
      spawn_link(fun() ->
         do_job(Pid, Fun, X) end)
      end, Chunk).

   % sends Fun(X) to Pid and then terminates
   do_job(Pid, Fun, X) ->
      Pid ! Fun(X).
