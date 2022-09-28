-module(test_mapred_google).
-export([grep/2,benchmark/0, benchmark/2, benchmark/3]).

% takes the name(optional) of a directory containing some test files and/or an
% atom(optional) to be searched and the numbers of schedulers (optional).
% a parallel grep is performed, searching for that atom inside the test files
% and returning the paths of the files containing the atoms.
% speedup over 10 iterations is calculated
benchmark() ->
   % "test" is a default folder containing files, used for test purposes,
   % and 'skepi' is the atom to be searched
   benchmark("test", skepi, utils:get_schedulers()).
benchmark(Input, Schedulers_Num) ->
   case is_atom(Input) of
      true -> benchmark("test", Input, Schedulers_Num);
      false -> benchmark(Input, skepi, Schedulers_Num)
   end.
benchmark(Testdir, Atom, Schedulers_Num)->
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
            io:format("- file '~s'~n", [I])
         end,
         Files),

   Seq = fun() -> seq_grep(Abspath,Atom, Files) end,
   Par = fun() -> grep(Abspath,Atom) end,
   Time_Seq = utils:test_loop(12,Seq, []),
   io:format("SEQ MAPREDUCE times: ~p~n",[Time_Seq]),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Par = utils:test_loop(12,Par, []),
   io:format("GOOGLE MAPREDUCE times: ~p~n",[Time_Par]),
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
  Indexed = utils:index_file_list(Dirpath),
  Index = googlemapred:start(utils:match_to_file(Regex),
                        fun utils:get_unique/3,
                        Indexed),
  dict:find(Regex, Index).

% same as above but operating sequentially
seq_grep(Dirpath, Regex, Files)->
   %lists:foreach(fun (File) ->
   %   {ok, [Words]} = file:consult(File),
   %   lists:foreach(fun (Word) ->
   %     case Regex == Word of
   %       true -> Word;
   %       false -> false
   %     end
   %  end, Words)
  %end, Files).
  timer:sleep(100),
  io:format("SEQ~n", []).

seq_mapred(Map, Reduce,Input) ->
  %% Map Phase
  IntermediateLists = lists:map(fun ({K1,V1}) -> Map(K1,V1) end, Input),
  IntermediatePairs = lists:flatten(IntermediateLists),
  IntermediateMap = lists:foldl(
    fun({K2,V2}, Dict) ->
      dict:append(K2,V2,Dict)
    end, dict:new(), IntermediatePairs),

  %% Reduce Phase
  dict:map(fun(K2, ListOfV2) ->
             Reduce(K2, ListOfV2)
           end, IntermediateMap).
