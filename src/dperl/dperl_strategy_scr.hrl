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
-type scrCycleState() :: map().         % initially: #{lastAttempt => ?EPOCH, lastSuccess => ?EPOCH}
-type scrPhase() :: sync | scrCycle().  
-type scrBatchSize() :: integer().     
-type scrMessage() :: binary().
-type scrStatus() :: no_op | {error,term()} | scrMessage().
-type scrOperation() :: finalize_src_events | no_log | string().  %  "Protected"|"Deleted"|"Inserted"|"Updated".
-type scrSoftError() :: true|false.

-type scrMsecInterval() :: integer().   % delays in milli-seconds
-type scrHoursOfDay() :: [integer()].   % execute only on these hours, [] = any hour

-endif.