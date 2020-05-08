-module(dperl_sup).
-behaviour(supervisor).

-include("dperl.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    ?Info("~p starting...~n", [?MODULE]),
    lists:map(
      fun({Table, ColumnNames, ColumnTypes, DefaultRecord, Opts}) ->
        ok = dperl_dal:check_table(
          Table, ColumnNames, ColumnTypes, DefaultRecord, Opts
        )
      end,
      [
        ?TABLESPEC(dperlJob,[]),
        ?TABLESPEC(
          ?JOBDYN_TABLE, dperlNodeJobDyn,
          [
            {scope,local},
            {local_content,true},
            {record_name,dperlNodeJobDyn}
          ]
        ),
        ?TABLESPEC(dperlService, []),
        ?TABLESPEC(
          ?SERVICEDYN_TABLE, dperlServiceDyn,
          [
            {scope,local},
            {local_content,true},
            {record_name,dperlServiceDyn}
          ]
        )
      ]
    ),
    ok = dderl:add_d3_templates_path(
      dderl, filename:join(priv_dir(), "dashboard_scripts")
    ),
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok,_} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok,
     {#{strategy => one_for_one, intensity => 5, period => 10},
      [#{id => dperl_metrics,
         start => {dperl_metrics, start_link, []},
         restart => permanent, shutdown => 5000, type => worker,
         modules => [dperl_metrics]},
       #{id => dperl_job_sup,
         start => {dperl_worker_sup, start_link, [job]},
         restart => permanent, shutdown => 60000, type => supervisor,
         modules => [dperl_worker_sup]},
       #{id => dperl_service_sup,
         start => {dperl_worker_sup, start_link, [service]},
         restart => permanent, shutdown => 60000, type => supervisor,
         modules => [dperl_worker_sup]},
       #{id => dperl_job_cp,
         start => {dperl_cp, start_link, [job]},
         restart => permanent, shutdown => 5000, type => worker,
         modules => [dperl_cp]},
       #{id => dperl_service_cp,
         start => {dperl_cp, start_link, [service]},
         restart => permanent, shutdown => 5000, type => worker,
         modules => [dperl_cp]},
       #{id => dperl_auth_cache,
         start => {dperl_auth_cache, start_link, []},
         restart => permanent, shutdown => 5000, type => worker,
         modules => [dperl_auth_cache]}
      ]}}.

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} -> "priv";
        PDir -> PDir
    end.
