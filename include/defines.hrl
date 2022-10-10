% function f(X,Exp)=1+(sin(X))^(10*Exp), to be used in tests
-define(COMPUTATION(X,Exp), (1 + math:pow(math:sin(X),Exp*10))).

% this is to define an exponent, usually used list lengths (eg. the list
% will be 2^EXP elements long)
-define(EXP, 20).

% this is an upperbound, to be used when generating random numbers
-define(UPPER, 100).

% this is the number of times test are run, to be used in test_loop()
-define(TIMES, 12).

% default timeout in milliseconds
-define(TIMEOUT, (20000)).

% to be used whe converting to milliseconds
-define(MSEC, 1000).

% the default atom, to be searched in the google mapreduce tests
-define(REGEX, (skepi)).

% the default directories, to be used in the google mapreduce tests
-define(TESTDIR_GREP, ("test_grep")).
-define(TESTDIR_WC, ("test_wc")).

% time and date, to be used in debug mode showing inter-process communication
-define(NOW, (utils:print_time())).

% some atoms identifying the type of skeleton
-define(SEQ, ("Sequential")).
-define(COMB, ("Combined MapReduce")).
-define(MAPRED, ("MapReduce")).
-define(SMART, ("Smart MapReduce")).
-define(GOOGLE, ("Google MapReduce")).
-define(PIPE, ("Stream Pipe")).
-define(FARM, ("Stream Farm")).
-define(PIPED_FARM, ("Stream Pipes of Farms")).


% for debugging purposes (on by default), to turn it off, compile with
% the command 'make DEBUG='
-ifdef(debug).
-define(REASON, ('unknown')).
-define(LOG_DEFAULT_PATH,
   (io_lib:format("logs/~s.log",[?MODULE_STRING]))).
-define(LOG_PATH, (filename:join(utils:get_dirpath(), ?LOG_DEFAULT_PATH))).
-define(CALL_MSG(At),
   (io_lib:format("[~s] {FILE:~s.erl,LINE:~p}: ~p/~p was called~n",
      [At,?MODULE_STRING,(?LINE)-1,?FUNCTION_NAME,?FUNCTION_ARITY]))
).
-define(LOG_CALL(At), (file:write_file(?LOG_PATH,?CALL_MSG(At),[append]))).
-define(SENT_MSG(From,To,At),
   (io_lib:format("[~s] MSG OUT: PID ~p -> PID ~p~n",[At,From,To]))
).
-define(RCVD_MSG(By,From,At),
   (io_lib:format("[~s] MSG IN: PID ~p <- PID ~p~n",[At,By,From]))
).
-define(TIMEOUT_MSG(At,Timeout,From),
   (io_lib:format("[~s] TIMEOUT: ~p/~p timed out after ~pms waiting for PID ~p~n",
      [At,?FUNCTION_NAME,?FUNCTION_ARITY,Timeout,From]))
).
-define(ERROR_MSG(At, Reason),
   (io_lib:format("[~s] {FILE:~s.erl,LINE:~p} ERROR: ~p/~p failed! reason: ~p~n",
   [At,?MODULE_STRING,(?LINE)-1,?FUNCTION_NAME,?FUNCTION_ARITY,Reason]))
).
-define(LOG_SENT(From,To,At),
   (file:write_file(?LOG_PATH,?SENT_MSG(From,To,At),[append]))
).
-define(LOG_RCVD(To,From,At),
   (file:write_file(?LOG_PATH,?RCVD_MSG(To,From,At),[append]))
).
-define(LOG_TIMEOUT(At,Timeout,From),
   (file:write_file(?LOG_PATH,?TIMEOUT_MSG(At,Timeout,From),[append]))
).
-define(LOG_ERROR(At,Reason),
   (file:write_file(?LOG_PATH,?ERROR_MSG(At,Reason),[append]))
).
-else.
-define(REASON, true).
-define(LOG_DEFAULT_PATH, true).
-define(LOG_PATH, true).
-define(CALL_MSG(At),true).
-define(LOG_CALL(At), true).
-define(SENT_MSG(From,To,At), true).
-define(RCVD_MSG(To,From,At), true).
-define(TIMEOUT_MSG(At,Timeout,From), true).
-define(ERROR_MSG(At, Reason), true).
-define(LOG_SENT(From,To,At), true).
-define(LOG_RCVD(To,From,At), true).
-define(LOG_TIMEOUT(At,Timeout,From), true).
-define(LOG_ERROR(At,Reason), true).
-endif.
