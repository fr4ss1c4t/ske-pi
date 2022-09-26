-module(test_googlemapred).
-export([grep/2,benchmark/0, benchmark/2, benchmark/3]).

% takes the name(optional) of a directory containing some test files and/or an
% atom(optional) to be searched and the numbers of schedulers (optional).
% a parallel grep is performed, searching for that atom inside the test files
% and returning the paths of the files containing the atoms.
% speedup over 10 iterations is calculated
benchmark() ->
   % test is a default folder containing files, used for test purposes,
   % and 'skepi' is the atom to be searched
   benchmark(utils:get_schedulers(),"test", skepi).
benchmark(Schedulers_Num, Input) ->
   case is_atom(Input) of
      true -> benchmark(Schedulers_Num, "test", Input);
      false -> benchmark(Schedulers_Num, Input, skepi)
   end.
benchmark(Schedulers_Num, Testdir, Atom)->
   utils:set_schedulers(Schedulers_Num),
   Abspath = utils:get_test_dirpath()++Testdir,
   io:format("testing with ~w scheduler(s)~n", [Schedulers_Num]),
   io:format("looking for the atom '~w' in the following path:~n",[Atom]),
   io:format("~s~n",[Abspath]),
   {_,Files} = file:list_dir_all(Abspath),
   Files_Num = length(Files),
   io:format("~p files are being processed:~n", [Files_Num]),
   lists:foreach(
         fun(I) ->
            io:format("file '~s'.~n", [I])
         end,
         Files),

   %TODO make it work
   Seq =
      fun() ->
      %   lists:foldl(fun utils:remove_duplicates/3,[],
      %      lists:map(utils:make_filter_mapper(Atom), Indexed))
      %end,
         %seq_grep(Abspath,Atom)
         io:format("SEQUENTIAL~n",[])
      end,
   Par = fun() -> grep(Abspath,Atom) end,
   Time_Seq = utils:test_loop(12,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Par = utils:test_loop(12,Par, []),
   Mean_Par = utils:mean(Time_Par),
   Median_Par = utils:median(Time_Par),
   Speedup_Par = utils:speedup(Mean_Seq,Mean_Par),

   io:format("sequential version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
      [Mean_Seq/1000,Median_Seq/1000]),

   io:format("parallel version mean is"),
   io:format(" ~wms, whilst median is ~wms~n",
      [Mean_Par/1000,Median_Par/1000]),

   io:format("speedup of the parallel version is ~w~n", [Speedup_Par]).


% similiar to the unix command "grep <word> <dirpath>".
% it should print a list of files that contain the Regex searched
grep(Dirpath, Regex) ->
  Indexed = utils:list_numbered_files(Dirpath),
  Index = dmapred:start(utils:make_filter_mapper(Regex),
                        fun utils:remove_duplicates/3,
                        Indexed),
  dict:find(Regex, Index).

% same as above but operating sequentially
seq_grep(Dirpath,Regex)->
   Indexed = utils:list_numbered_files(Dirpath),
   Index = seq_mapred(utils:make_filter_mapper(Regex),
                         fun utils:remove_duplicates/3,
                         Indexed),
   dict:find(Regex, Index).

seq_mapred(M_Fun, R_Fun,Input) ->
  %% Map Phase
  IntermediateLists = lists:map(fun ({K1,V1}) -> M_Fun(K1,V1) end, Input),
  IntermediatePairs = lists:flatten(IntermediateLists),
  IntermediateMap = lists:foldl(
    fun({K2,V2}, Dict) ->
      dict:append(K2,V2,Dict)
    end, dict:new(), IntermediatePairs),

  %% Reduce Phase
  dict:map(fun(K2, ListOfV2) ->
             R_Fun(K2, ListOfV2)
           end, IntermediateMap).
