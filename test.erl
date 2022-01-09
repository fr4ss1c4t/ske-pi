-module(test).
-export([test/0]).

test() ->
   List = lists:seq(0,round(math:pow(2,10))),
   Chunks = utils:make_chunks(List,round(math:pow(2,8))),
   Hashed_chunks = lists:map(fun(Chunk) -> 
      {erlang:phash2(Chunk),Chunk} end, Chunks),
   Result = gmapred:start(Hashed_chunks,
                          fun mapper/3, fun reducer/3),
   lists:sum(clean_up(Result)).

mapper(Key,Chunk, Fun) ->
   lists:foreach(fun(X) -> Fun(Key,X*X) end,Chunk).

reducer(Key,Counts,Fun) ->
   Result = lists:foldl(fun(Count,Acc) -> Count+Acc end,0,Counts),
   Fun(Key,Result).

clean_up(Result) ->
   Tuples = dict:to_list(Result),
   [ X||{_,[X]}<-Tuples].
