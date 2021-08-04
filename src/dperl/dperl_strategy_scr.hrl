-ifndef(_dperl_STRATEGY_SCR_HRL_).
-define(_dperl_STRATEGY_SCR_HRL_, true).

% transparently handled types in this behaviour module (defined in callback module)
-type scrState() :: any().              % processing state details defined in callback module
-type scrAnyKey() :: any().             % mostly scalars or lists of scalars
-type scrAnyKeys() :: [scrAnyKey()].    % list of changed/dirty/all keys
-type scrAnyVal() :: term().            % type decided by callback, usually map()
-type scrAnyKeyVal() :: {scrAnyKey(),scrAnyVal()}.  % used for cleanup/refresh combined operations
-type scrAnyKeyVals() :: [scrAnyKeyVal()].          % used for cleanup/refresh combined operations

% scr behaviour types
-type scrChannel() :: binary().
-type scrDirection() :: push | pull.
-type scrCycle() :: cleanup | refresh.  % stateful phases (sync is stateless)
-type scrCycleState() :: map().         % behaviour overall state per scrCycle()
        % initially for both: #{lastAttempt => ?EPOCH, lastSuccess => ?EPOCH}
        % initially for cleanup: #{minKey=>?SCR_MIN_KEY, maxKey=>?SCR_MAX_KEY, lastKey=>?SCR_INIT_KEY};
        % initially for refresh: #{}
        % ToDo: Are keys limited to non_neg_integer() / other Erlang types ???? 
-type scrPhase() :: sync | scrCycle().  
-type scrBatchSize() :: integer().     
-type scrMessage()   :: binary().
-type scrErrorInfo() :: no_op | {error,term()} | scrMessage().
-type scrDynStatus() :: map().           % what should be stored in dperlNodeJobDyn table for this job.
-type scrOperation() :: finalize_src_events | no_log | string().  %  "Protected"|"Deleted"|"Inserted"|"Updated".
-type scrSoftError() :: true|false|idle. % idle only used in special case where the dst is not ready and we have to do an idle wait.
                                         % true signifies the one or more events in the sync cycle resulted in an error
                                         % false means all the events were processed successfully by the sync cycle

-type scrMsecInterval() :: integer().   % delays in milli-seconds
-type scrBatchInterval() :: scrMsecInterval().  % delay from end of c or r batch to start of next
-type scrCycleInterval() :: scrMsecInterval().  % delay from end of c or r cycle to start of next
-type scrHoursOfDay() :: [integer()].   % execute only on these hours, [] = any hour

-define(SCR_MIN_KEY, -1).               % initial MinKey value. Real keys must be bigger (in Erlang sort order)
-define(SCR_MAX_KEY, <<255>>).          % initial MaxKey value. Real keys must be smaller (in Erlang sort order)
-define(SCR_INIT_KEY, 0).               % initial LastKey value. Real keys must be bigger (in Erlang sort order)

-endif.