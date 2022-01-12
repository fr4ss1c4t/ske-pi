-module(test).
-export([test/0,chunk/1]).

test() ->
   List = lists:seq(0,round(math:pow(2,10))),
   Chunks = utils:make_chunks(List,round(math:pow(2,5))),
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

split(Max,List) ->
  element(1,lists:foldl(
              fun(E,{[Buff|Acc],C}) when C < Max ->
                    {[[E|Buff]|Acc],C+1};
                 (E,{[Buff|Acc],_})->
                    {[[E],Buff|Acc],1};
                 (E,{[],_})->
                     {[[E]],1} end, {[],0},List)).
chunk(List) ->
   Rev = split(List,11024),
   lists:foldl(fun(E,Acc) ->
[lists:reverse(E)|Acc] end, [], Rev).
