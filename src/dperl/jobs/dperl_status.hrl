-ifndef(_dperl_STATUS_HRL_).
-define(_dperl_STATUS_HRL_, true).

-include_lib("dperl/dperl.hrl").

-record(context, {
            name                           :: list(),
            channel                        :: binary(),
            aggr_channel                   :: binary(),
            metrics                        :: map(),
            focus                          :: map(),
            heart_interval = 10000         :: integer(),
            aggregators = #{}              :: map(),
            node_collector                 :: list(),
            stale_time                     :: integer(),
            cpu_overload_cnt = 0           :: integer(),
            mem_overload_cnt = 0           :: integer(),
            eval_suspend_cnt = 0           :: integer(),
            errors = []                    :: list(),
            received_cnt = 0               :: integer(),
            requested_cnt = 0              :: integer(),
            success_cnt = 0                :: integer(),
            sync_cnt = 0                   :: integer(),
            active_link = 1                :: integer(),
            imem_sess = undefined          :: undefined | tuple(),
            connection = #{}               :: map(),
            target_node                    :: atom(),
            requested_metrics = #{}        :: map(),    %% list of metrics that have been requested
            debug = none                   :: atom(),   %% none or detail or simple
            is_loaded = false              :: boolean(),
            sync_only = false              :: boolean() %% set to true when refresh and cleanup are false
        }).

-define(STALE_TIME(__MODULE, __JOB_NAME),
          ?GET_CONFIG(cleanupStaleTimeout, [__MODULE, __JOB_NAME], 60000, "Cleanup Threshold time to delete rows after checking heartbeat")
       ).

-define(MAX_MEMORY_THRESHOLD(__NODE),
          ?GET_CONFIG(maxMemoryThreshold, [__NODE], 70, "Memory Threshold percentage above which should cause high memory error, configurable per node")
       ).

-define(METRIC_WAIT_THRESHOLD(__JOB_NAME),
          ?GET_CONFIG(metricWaitTimeout, [__JOB_NAME], 300, "Max wait time for the agent to respond to a metric request is  in seconds")
       ).

-define(METRICS_TAB(__NAME), list_to_atom("metrics_tab_" ++ __NAME)).

-define(AGGR, <<"_AGGR">>).  %% Aggregator skvh table ending. user with dashboard table ex: dperlS_AGGR

-record(dperl_metrics, {key           :: tuple(),
                       agg_round     :: list(),
                       agg_precise   :: list(),
                       state         :: term(),
                       time          :: ddTimestamp()
                      }).

-endif.
