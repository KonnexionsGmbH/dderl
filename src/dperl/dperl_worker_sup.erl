-module(dperl_worker_sup).
-behaviour(supervisor).

-include("dperl.hrl").

%% Supervisor callbacks
-export([start_link/1, init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(service) -> start_link(dperl_job_sup);
start_link(job) ->  start_link(dperl_service_sup);
start_link(Ext) when Ext == dperl_job_sup; Ext == dperl_service_sup ->
    ?Info("~p starting...~n", [Ext]),
    case supervisor:start_link({local, Ext}, ?MODULE, [Ext]) of
        {ok,_} = Success ->
            ?Info("~p started!~n", [Ext]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [Ext, Error]),
            Error
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([_Ext]) ->
    {ok, { #{strategy => one_for_one, intensity => 5, period => 1}, []} }.
