-module(sstream).
-export([start_farm/2,start/2]).

% sequential version of the parallel farm on a stream of inputs
start_farm(W_func,Input_list) ->
	start([{farm,[{seq,W_func}]}],Input_list).

start(Workflow,Input_list) ->
   run(Workflow,Input_list),
   receive
      {results,Results} -> Results
   end.

run(Workflow,Input_list) ->
   Fun = make(Workflow),
   self() ! {results, [Fun(X) || X <- Input_list]},
   self().

make(Workflow) ->
   Funcs = [parse(Item) || Item <- Workflow],
   lists:foldr(fun compose/2, fun id/1 ,Funcs).

compose(F,G) -> fun(X) -> F(G(X)) end.

id(X) -> X.

parse(Fun) when is_function(Fun,1) ->
   parse({seq,Fun});
parse({seq,Fun}) when is_function(Fun,1) ->
   Fun;
parse({farm,Workflow}) ->
   make(Workflow).
