-module(preduce).
-include("include/usages.hrl").
-include("include/defines.hrl").
-compile(nowarn_unused_vars).
-export([start/2, start/3, start/4, usage/0]).

usage() -> true.

start(R_Fun, List) ->
   start(R_Fun, R_Fun, List, 0).
start(R_Fun, List, Acc) ->
   start(R_Fun, R_Fun, List, Acc).
start(R_Fun, Combiner, List, Acc) ->
   R_Fun1 = fun (L) -> lists:foldl(R_Fun, Acc, L) end,
   runmany(R_Fun1, Combiner, List).
