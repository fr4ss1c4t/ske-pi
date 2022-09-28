-module(test_mapred_google).
-include("include/defines.hrl").
-export([grep/2,benchmark/0, benchmark/2, benchmark/3]).

% takes the number of schedulers, the name(optional) of a directory containing some test files and/or an
% some test files and/or an atom(optional) to be searched.
% a parallel grep is performed, searching for that atom inside the test files
% and returning the paths of the files containing the atoms.
% speedup over 10 iterations is calculated
benchmark() ->
   % ?TESTDIR is a default folder containing files, used for test purposes,
   % and '?REGEX' is the atom to be searched
   benchmark(utils:get_schedulers(), ?REGEX, ?TESTDIR).
benchmark(Schedulers_Num, Input) ->
   case is_atom(Input) of
      true -> benchmark(?TESTDIR, Input, Schedulers_Num);
      false -> benchmark(Input, ?REGEX, Schedulers_Num)
   end.
benchmark(Schedulers_Num, Atom, Testdir)->
   utils:set_schedulers(Schedulers_Num),
   Abspath = utils:get_test_dirpath()++Testdir,
   io:format("> testing with ~w scheduler(s).~n", [Schedulers_Num]),
   io:format("> looking for the atom '~w' in the following path:~n",[Atom]),
   io:format("~s~n",[Abspath]),
   {_,Files} = file:list_dir_all(Abspath),
   Files_Num = length(Files),
   io:format("> ~p files are being processed:~n", [Files_Num]),
   lists:foreach(
         fun(I) ->
            io:format("file '~s'~n", [I])
         end,
         Files),

   io:format("~nrunning tests, please wait...~n~n"),

   Seq = fun() -> seq_grep(Abspath,Atom, Files) end,
   Par = fun() -> grep(Abspath,Atom) end,
   Time_Seq = utils:test_loop(?TIMES,Seq, []),
   Mean_Seq = utils:mean(Time_Seq),
   Median_Seq = utils:median(Time_Seq),
   Time_Par = utils:test_loop(?TIMES,Par, []),
   Mean_Par = utils:mean(Time_Par),
   Median_Par = utils:median(Time_Par),
   Speedup_Par = utils:speedup(Mean_Seq,Mean_Par),
   io:format("---SUMMARY OF RESULTS---~n"),
   utils:report(?SEQ, Time_Seq, Mean_Seq, Median_Seq),
   utils:report(?GOOGLE, Time_Par, Mean_Par, Median_Par),
   io:format("speedup of the ~p version is ~p~n", [?GOOGLE, Speedup_Par]).


% similiar to the unix command "grep <word> <dirpath>".
% it should print a list of files that contain the Regex searched
grep(Dirpath, Regex) ->
  Indexed = utils:index_file_list(Dirpath),
  Index = mapred_google:start(utils:match_to_file(Regex),
                        fun utils:get_unique/3,
                        Indexed),
  dict:find(Regex, Index).

%TODO fix this asap
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
  timer:sleep(100).
  %io:format("SEQ~n", []).

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
