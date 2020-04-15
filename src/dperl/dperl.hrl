-ifndef(_dperl_HRL_).
-define(_dperl_HRL_, true).

-define(LOG_TAG, "_dperl_").
-include_lib("dderl/src/dderl.hrl").

-type plan() :: at_most_once|at_least_once|on_all_nodes.

-define(TABLESPEC(__T,__O),
        {__T, record_info(fields, __T), ?__T, #__T{}, __O}).
-define(TABLESPEC(__TA,__T,__O),
        {__TA, record_info(fields, __T), ?__T, #__T{}, __O}).

-define(NOT_FOUND, '$notfound').

-record(dperlJob, {
          name      :: binary(),
          module    :: atom(),
          args      :: any(),
          srcArgs   :: any(),
          dstArgs   :: any(),
          enabled   :: true|false,
          running   :: true|false,
          plan      :: plan(),
          nodes     :: [atom()],
          opts = [] :: list()
         }).
-define(dperlJob, [binstr,atom,term,term,term,boolean,atom,atom,list,list]).

-define(JOBDYN_TABLE, 'dperlNodeJobDyn@').
-record(dperlNodeJobDyn, {
          name          :: binary(), % same as dperlJob.name
          state         :: map(),
          status        :: atom(),
          statusTime    :: ddTimestamp()
         }).
-define(dperlNodeJobDyn, [binstr,map,atom,timestamp]).

-record(dperlService, {
          name      :: binary(),
          module    :: atom(),
          args      :: any(),
          resource  :: any(),
          interface :: any(),
          enabled   :: true|false,
          running   :: true|false,
          plan      :: plan(),
          nodes     :: [atom()],
          opts = [] :: list()
         }).
-define(dperlService, [binstr,atom,term,term,term,boolean,atom,atom,list,list]).

-define(SERVICEDYN_TABLE, 'dperlServiceDyn@').
-record(dperlServiceDyn, {
          name          :: binary(), % same as dperlService.name
          state         :: map(),
          status        :: atom(),
          statusTime    :: ddTimestamp()
         }).
-define(dperlServiceDyn, [binstr,map,atom,timestamp]).

-define(G(__JS,__F),
        if is_record(__JS, dperlJob) -> (__JS)#dperlJob.__F;
           is_record(__JS, dperlService) -> (__JS)#dperlService.__F;
           true -> error({badarg, __JS})
        end).
-define(S(__JS,__F,__V),
        if is_record(__JS, dperlJob) -> (__JS)#dperlJob{__F = __V};
           is_record(__JS, dperlService) -> (__JS)#dperlService{__F = __V};
           true -> error({badarg, __JS})
        end).
-define(RC(__JS),
        if is_record(__JS, dperlJob) -> job;
           is_record(__JS, dperlService) -> service;
           true -> error({badarg, __JS})
        end).
-define(SUP(__JS),
        if is_record(__JS, dperlJob) -> dperl_job_sup;
           is_record(__JS, dperlService) -> dperl_service_sup;
           true -> error({badarg, __JS})
        end).

-define(EPOCH, {0,0}).

-define(TOAC_KEY_INDEX_ID, 1).

-define(GET_RPC_TIMEOUT,
          ?GET_CONFIG(rpcTimeout, [], 2000,
                      "Max timeout in millisecond for a rpc call")
        ).

-define(GET_WORKER_CHECK_INTERVAL(__Type),
          case __Type of
              job ->
                  ?GET_CONFIG(jobCheckInterval, [], 1000,
                              "Interval in millisecond between job configuration"
                              " checks");
              service ->
                  ?GET_CONFIG(serviceCheckInterval, [], 5000,
                              "Interval in millisecond between service configuration"
                              " checks")
          end
        ).

-define(GET_CLUSTER_CHECK_INTERVAL(__Type),
           ?GET_CONFIG(clusterCheckInterval, [__Type], 200,
                      "Interval in milliseconds between cluster checks")
        ).

-define(GET_LINK_RETRY_INTERVAL(__MODULE, __JOB_NAME),
          ?GET_CONFIG(linkRetryInterval, [__MODULE, __JOB_NAME], 2000,
                      "Retry interval in millisecond of a broken link")
       ).

-define(GET_FOCUS(__KEY),
          ?GET_CONFIG({focus, __KEY}, [], #{},
                      "Channel to key transformation function mapping")
       ).

-define(GET_IGNORED_JOBS_LIST,
          ?GET_CONFIG(ignoredJobs, [], [],
                      "List of job names that will be ignored by metrics" 
                      " and not presented in dashboards")
        ).

-define(CONNECT_CHECK_IMEM_LINK(__State),
        (fun(#state{active_link = _ActiveLink, links = _Links,
                    cred = #{user := _User, password := _Password},
                    imem_sess = _OldSession} = _State) ->
                 #{schema := _Schema} = lists:nth(_ActiveLink, _Links),
                 case catch _OldSession:run_cmd(schema, []) of
                     _Schema -> {ok, _State};
                     _ ->
                         catch _OldSession:close(),
                         case dperl_dal:connect_imem_link(
                                _ActiveLink, _Links, _User, _Password) of
                             {ok, _Session, _Pid} ->
                                 {ok, _State#state{imem_sess = _Session,
                                                   imem_connected = true}};
                             {_Error, _NewActiveLink} ->
                                 {error, _Error,
                                  _State#state{active_link = _NewActiveLink,
                                               imem_connected = false}}
                         end
                 end
         end)(__State)).

-define(CYCLE_ALWAYS_WAIT(__MODULE, __JOB_NAME),
          ?GET_CONFIG(cycleAlwaysWait,[__MODULE, __JOB_NAME], 1000,
                      "Delay in millisecond before restarting the cycle,"
                      " if current cycle is not idle and cycle is not failed")
       ).

-define(CYCLE_IDLE_WAIT(__MODULE, __JOB_NAME),
          ?GET_CONFIG(cycleIdleWait, [__MODULE, __JOB_NAME], 3000,
                      "Delay in millisecond before restarting the cycle,"
                      " if current cycle was idle")
       ).

-define(CYCLE_ERROR_WAIT(__MODULE, __JOB_NAME),
          ?GET_CONFIG(cycleErrorWait, [__MODULE, __JOB_NAME], 5000,
                      "Delay in millisecond before restarting the cycle,"
                      " if current cycle has failed")
       ).

-define(CLEANUP_INTERVAL(__MODULE, __JOB_NAME),
          ?GET_CONFIG(cleanupInterval, [__MODULE, __JOB_NAME], 100000,
                     "Delay in millisecond between any cleanup success"
                     " end time and the next cleanup attempt start time")
       ).

-define(REFRESH_INTERVAL(__MODULE, __JOB_NAME),
          ?GET_CONFIG(refreshInterval, [__MODULE, __JOB_NAME], 10000000,
                     "Delay in millisecond between any refresh success"
                     " end time and the next cleanup attempt start time")
       ).

-define(CLEANUP_BATCH_INTERVAL(__MODULE, __JOB_NAME),
          ?GET_CONFIG(cleanupBatchInterval, [__MODULE, __JOB_NAME], 2000,
                     "Delay in millisecond between cleanups whith in a"
                     " complete cleanup cycle")
       ).

-define(REFRESH_BATCH_INTERVAL(__MODULE, __JOB_NAME),
          ?GET_CONFIG(refreshBatchInterval, [__MODULE, __JOB_NAME], 10000,
                     "Delay in millisecond between refreshes whith in a"
                     " refresh cleanup cycle")
       ).

-define(REFRESH_HOURS(__MODULE, __JOB_NAME),
          ?GET_CONFIG(refreshHours, [__MODULE, __JOB_NAME], [],
                     "List of hour(s) of the day when refresh should happen."
                     " [] for every time possible")
       ).

-define(MAX_BULK_COUNT(__MODULE, __JOB_NAME),
          ?GET_CONFIG(maxBulkCount, [__MODULE, __JOB_NAME], 100,
                      "Max count for each bulk")
       ).

-define(MAX_CLEANUP_BULK_COUNT(__MODULE, __JOB_NAME),
          ?GET_CONFIG(maxCleanupBulkCount, [__MODULE, __JOB_NAME], 100,
                      "Max count for each cleanup bulk")
       ).

-define(MAX_REFRESH_BULK_COUNT(__MODULE, __JOB_NAME),
          ?GET_CONFIG(maxRefreshBulkCount, [__MODULE, __JOB_NAME], 100,
                      "Max count for each refresh bulk")
       ).

-define(KPI_CLEANUP_TIME(__JOB_NAME),
          ?GET_CONFIG(kpiCleanupTime, [__JOB_NAME], 30,
                      "Number of minutes after which a kpi record can be "
                      "cleaned out") * 60
       ).

-define(JOB_DOWN_MOD_EXCLUSIONS,
          ?GET_CONFIG(jobDownModExclusions, [], [],
                      "List of job modules to be exculded from job down count")
       ).

-define(JOB_DOWN_NAME_EXCLUSIONS,
          ?GET_CONFIG(jobDownNameExclusions, [], [],
                      "List of job names to be exculded from job down count")
       ).

-define(JOB_ERROR_MOD_EXCLUSIONS,
          ?GET_CONFIG(jobErrorModExclusions, [], [],
                      "List of job modules to be exculded from job error count")
       ).

-define(JOB_ERROR_NAME_EXCLUSIONS,
          ?GET_CONFIG(jobErrorNameExclusions, [], [],
                      "List of job names to be exculded from job error count")
       ).

-define(JOB_DYN_STATUS_TIMEOUT,
          ?GET_CONFIG(jobDynStatusTimeout, [], 60000,
                      "Time in milliseconds after which status is considered"
                      " stale") * 1000
       ).

-define(JOB_DESCRIPTIONS, 
          ?GET_CONFIG(jobDescriptions, [], #{},
                      "Map for specifying the description of platforms for the"
                      " dperl dashboard")
       ).

-define(JOB_ERROR, <<"dperlJobError">>).

-define(SESSION_CLOSE_ERROR_CODES(__MODULE),
          ?GET_CONFIG(oraSessionCloseErrorCodes, [__MODULE, get(name)], [28,3113,3114,6508],
                      "List of oracle error codes on which session is recreated")
       ).

-define(STMT_CLOSE_ERROR_CODES(__MODULE),
          ?GET_CONFIG(oraStmtmCloseErrorCodes, [__MODULE, get(name)], [4069,4068],
                      "List of oracle error codes on which stmts are recreated")
       ).

-ifndef(TEST).

-define(STATUS_INTERVAL,
          ?GET_CONFIG(activityStatusInterval, [get(name)], 60000,
                      "Delay in millisecond before writing activity status in "
                      "status dir")
       ).

-define(STATUS_FILE,
          ?GET_CONFIG(activityStatusFile, [get(name)], "ServiceActivityLog.sal",
                      "File name of activity status log")
       ).

-else.

-define(STATUS_INTERVAL, 1000).

-define(STATUS_FILE, "ServiceActivityLog.sal").

-endif.

-define(ST, erlang:get_stacktrace()).

% Job logger interfaces
-define(JInfo(__F,__A),         ?Info ([{enum,get(jname)}],"[~p] "__F,[get(jstate)|__A])).
-define(JWarn(__F,__A),         ?Warn ([{enum,get(jname)}],"[~p] "__F,[get(jstate)|__A])).
-define(JDebug(__F,__A),        ?Debug([{enum,get(jname)}],"[~p] "__F,[get(jstate)|__A])).
-define(JError(__F,__A),        ?Error([{enum,get(jname)}],"[~p] "__F,[get(jstate)|__A], [])).
-define(JError(__F,__A,__S),    ?Error([{enum,get(jname)}],"[~p] "__F,[get(jstate)|__A],__S)).
-define(JTrace(__F,__A),
    case get(debug) of
        true -> ?JInfo(__F,__A);
        _ -> no_op
    end).

-define(JInfo(__F),     ?JInfo (__F,[])).
-define(JWarn(__F),     ?JWarn (__F,[])).
-define(JError(__F),    ?JError(__F,[])).
-define(JDebug(__F),    ?JDebug(__F,[])).
-define(JTrace(__F),    ?JTrace(__F,[])).

-define(NODE, atom_to_list(node())).

% Service logger interfaces
-define(SInfo(__F,__A),         ?Info ([{enum,get(jname)}],__F,__A)).
-define(SWarn(__F,__A),         ?Warn ([{enum,get(jname)}],__F,__A)).
-define(SDebug(__F,__A),        ?Debug([{enum,get(jname)}],__F,__A)).
-define(SError(__F,__A),        ?Error([{enum,get(jname)}],__F,__A, [])).
-define(SError(__F,__A,__S),    ?Error([{enum,get(jname)}],__F,__A,__S)).

-define(SInfo(__F),     ?SInfo (__F,[])).
-define(SWarn(__F),     ?SWarn (__F,[])).
-define(SError(__F),    ?SError(__F,[])).
-define(SDebug(__F),    ?SDebug(__F,[])).

-endif. %_dperl_HRL_
