% function f(X,Exp)=1+(sin(X))^(10*Exp), to be used in tests
-define(COMPUTATION(X,Exp), (1 + math:pow(math:sin(X),Exp*10))).

% this is to define an exponent, usually used list lengths (eg. the list
% will be 2^EXP elements long)
-define(EXP, 20).

% this is an upperbound, to be used when generating random numbers
-define(UPPER, 100).

% this is the number of times test are run, to
-define(TIMES, 12).

% to be used whe converting to milliseconds
-define(MSEC, 1000).

% the atom to be search in the google mapreduce tests
-define(REGEX, (skepi)).

% default directory, to be used in the google mapreduce tests
-define(TESTDIR, ("test")).

% some strings identifying the type of skeleton
-define(SEQ, ("SEQ")).
-define(NAIVE, ("NAIVE MAPREDUCE")).
-define(SMART, ("SMART MAPREDUCE")).
-define(GOOGLE, ("GOOGLE MAPREDUCE")).
-define(PIPE, ("STREAM PIPE")).
-define(FARM, ("STREAM FARM")).
-define(PIPED_FARM, ("STREAM PIPES OF FARMS")).


% for debugging purposes
-ifdef(debug).
-define(LOG(MSG), io:format("{~p,~p}: ~p~n", [?MODULE,?LINE,MSG])).
-else.
-define(LOG(MSG), true).
-endif.
