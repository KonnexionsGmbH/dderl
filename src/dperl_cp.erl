-module(dperl_cp).
-behaviour(gen_server).

-include("dperl.hrl").

% gen_server exports
-export([start_link/1, init/1, terminate/2, handle_call/3, handle_cast/2,
         handle_info/2, code_change/3]).

-record(state, {type, last_seen_nodes = []}).

start_link(Type) when Type == service; Type == job ->
    Name = list_to_atom(lists:concat(["dperl_", Type, "_cp"])),
    case gen_server:start_link({local, Name}, ?MODULE, [Type], []) of
        {ok, Pid} ->
            ?Info("~p started!", [Name]),
            {ok, Pid};
        {error, Error} ->
            ?Error("starting ~p: ~p", [Name, Error]),
            {error, Error};
        Other ->
            ?Error("starting ~p: ~p", [Name, Other]),
            Other
    end.

init([Type]) ->
    process_flag(trap_exit, true),
    case Type of
        job ->
            ok = dperl_dal:subscribe({table, dperlJob, detailed});
        service ->
            ok = dperl_dal:subscribe({table, dperlService, detailed})
    end,
    imem_dal_skvh:create_check_channel(?JOB_ERROR, [audit]),
    erlang:send_after(?GET_WORKER_CHECK_INTERVAL(Type), self(), check_workers),
    {ok, #state{type = Type}}.

handle_call(Request, _From, State) ->
    ?Error("unsupported call ~p", [Request]),
    {reply,badarg,State}.

handle_cast(Request, State) ->
    ?Error("unsupported cast ~p", [Request]),
    {noreply,State}.

-define(MTE(__Rest), {mnesia_table_event,__Rest}).
%% schema deletion event
handle_info(?MTE({delete,schema,{schema,RecType},_,_}), State)
  when RecType == dperlJob; RecType == dperlService ->
    ?Info("stop all jobs/services"),
    [catch dperl_worker:stop(Mod, Name) || {Mod, Name} <- dperl_worker:list(State#state.type)],
    {noreply, State};
%% handles deleteion of a record and if the job was supposed to be running on this node then
%% job is stopped if not ignored
handle_info(?MTE({delete,JSM,{JSM,_},[JobOrService],_}), State)
  when (is_record(JobOrService, dperlJob) andalso JSM == dperlJob) orelse
       (is_record(JobOrService, dperlService) andalso JSM == dperlService) ->
    case check_plan(JobOrService) of
        true ->
            ?Debug("stop ~p_~s", [?G(JobOrService,module), ?G(JobOrService,name)]),
            stop(JobOrService);
        false ->
            no_op
    end,
    {noreply, State};
%% handles record update or new record insert. Checks if the record changes 
%% require a restart, cold start or stopping the job/service
handle_info(?MTE({write,JSM,JobOrService, OldJobOrServices, _}), State)
  when (is_record(JobOrService, dperlJob) andalso JSM == dperlJob) orelse
       (is_record(JobOrService, dperlService) andalso JSM == dperlService) ->
    NewEnabled = ?G(JobOrService, enabled),
    {OldEnabled, IsEqual} =
    case OldJobOrServices of
        [] -> {false, false};  %% new record insert
        [OldJobOrService | _] -> 
            %% comparing old and new job or service configuration excluding running column value
            IsSame = JobOrService == ?S(OldJobOrService, running, ?G(JobOrService, running)),
            {?G(OldJobOrService, enabled), IsSame}
    end,
    case {NewEnabled, OldEnabled, IsEqual} of
        {true, true, false} -> stop(JobOrService);
        {A, A, _} -> no_op;
        {false, true, _} -> stop(JobOrService);
        {true, false, _} ->
            case check_plan(JobOrService) of
                true ->
                    stop(JobOrService),
                    start(JobOrService);
                false -> no_op
            end
    end,
    {noreply, State};
handle_info(check_workers, #state{type = Type, last_seen_nodes = LNodes} = State) ->
    case dperl_dal:data_nodes() of
        LNodes -> 
            check_workers(Type, LNodes),
            erlang:send_after(?GET_WORKER_CHECK_INTERVAL(Type), self(), check_workers),
            {noreply, State};
        SeenNodes ->
            log_nodes_status(SeenNodes -- LNodes, Type, "Node added"),
            log_nodes_status(LNodes -- SeenNodes, Type, "Node disappeared"),
            erlang:send_after(?GET_CLUSTER_CHECK_INTERVAL(Type), self(), check_workers),
            {noreply, State#state{last_seen_nodes = SeenNodes}}
    end;
handle_info(Info, State) ->
    ?Error("unsupported info ~p", [Info]),
    {noreply,State}.

terminate(Reason, _State) when Reason == normal; Reason == shutdown ->
    ?Info("shutdown"),
    {ok, _} = dperl_dal:unsubscribe({table, dperlJob, detailed}),
    {ok, _} = dperl_dal:unsubscribe({table, dperlService, detailed});
terminate(Reason, State) -> ?Error("crash ~p : ~p", [Reason, State]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

check_workers(Type, DNodes) when is_atom(Type), is_list(DNodes) ->
    JobsORServices = dperl_dal:get_enabled(Type),
    RunningJobsOrServices = dperl_worker:list(Type),
    EnabledJobsOrServices = 
    lists:map(
        fun(#dperlJob{module = Mod, name = Name}) -> {Mod, Name};
           (#dperlService{module = Mod, name = Name}) -> {Mod, Name}
        end, JobsORServices),
    JobsOrServicesToBeStopped = RunningJobsOrServices -- EnabledJobsOrServices,
    [catch dperl_worker:stop(Mod, Name) || {Mod, Name} <- JobsOrServicesToBeStopped],
    check_workers(JobsORServices, DNodes);
check_workers([], _DNodes) -> ok;
check_workers([JobOrService | JobsOrServices], DNodes) ->
    IsAlive =
    case whereis(dperl_worker:child_id(?G(JobOrService,module),
                                      ?G(JobOrService,name))) of
        undefined -> false;
        Pid when is_pid(Pid) -> is_process_alive(Pid)
    end,
    try imem_config:reference_resolve(JobOrService) of
        JobOrServiceRefResolved ->
            case {check_plan(JobOrServiceRefResolved, DNodes), IsAlive} of
                {false, true}  -> stop(JobOrService);
                {true,  false} -> start(JobOrService);
                {_, _}         ->
                    case check_config(JobOrService) of
                        ok -> ok;
                        _ -> stop(JobOrService)
                    end
            end
    catch
        _:Error ->
            ?Error("invalid new config ~p. job/service skipped", [Error], ?ST),
            dperl_dal:disable(JobOrService),
            ok
    end,
    check_workers(JobsOrServices, DNodes).

-spec check_plan(#dperlJob{} | #dperlService{}) -> true | false.
check_plan(JobOrService) -> check_plan(JobOrService, dperl_dal:data_nodes()).

-spec check_plan(#dperlJob{} | #dperlService{}, list()) -> true | false.
check_plan(#dperlJob{plan = Plan, nodes = Nodes} = Job, DataNodes) ->
    check_plan(Job, Plan, Nodes, DataNodes);
check_plan(#dperlService{plan = Plan, nodes = Nodes} = Service, DataNodes) ->
    check_plan(Service, Plan, Nodes, DataNodes).

check_plan(JobOrService, Plan, Nodes, DataNodes) ->
    case catch check_plan(Plan, Nodes, DataNodes) of
        Result when is_boolean(Result) -> Result;
        Error ->
            ?Error("checking plan Plan : ~p Error : ~p", [Plan, Error]),
            ?Error("Disabling the job/service due to errors"),
            dperl_dal:disable(JobOrService),
            false
    end.

check_config(JobOrService) ->
    Module = ?G(JobOrService,module),
    Name = ?G(JobOrService,name),
    case ?G(JobOrService, running) of
        true -> no_op;
        _ -> 
            %set running to true
            dperl_dal:set_running(JobOrService, true)
    end,
    case dperl_worker:child_spec(?RC(JobOrService), {Module, Name}) of
        {ok, #{id := {Module,Name}, modules := [Module],
               start := {dperl_worker, start_link,
                         [ActiveJobOrService | _]}}} ->
            ActiveRunning = ?G(ActiveJobOrService,running),
            case catch imem_config:reference_resolve(
                   ?S(JobOrService,running,ActiveRunning)) of
                {'ClientError', Error} ->
                    ?Error("invalid new config ~p. job/service not restarted",
                           [Error]),
                    ok;
                ActiveJobOrService -> ok;
                _ -> stop
            end;
        Error -> Error
    end.

-spec check_plan(atom(), list(), list()) -> true|false.
check_plan(_, [], _DataNodes) -> true;
%% Running on all nodes
check_plan(on_all_nodes, Nodes, _DataNodes) ->
    lists:member(node(), Nodes);
%% Running on atleast one node in the cluster
check_plan(at_least_once, Nodes, DataNodes) ->
    case nodes_in_db(Nodes, DataNodes) of
        [] -> true;
        DBNodes -> hd(DBNodes) == node()
    end;
%% Makes sure only one instance runs
check_plan(at_most_once, Nodes, DataNodes) ->
    NodesInCluster = nodes_in_db(Nodes, DataNodes),
    NodesCountInCluster = length(NodesInCluster),
    if
         NodesCountInCluster > length(Nodes)/2 ->
            hd(NodesInCluster) == node();
        true ->
            case get_max_db_island() of
                '$none' -> 
                    length(Nodes) == 2 andalso hd(NodesInCluster) == node();
                MaxIslandSize ->
                    case MaxIslandSize > NodesCountInCluster of
                        true -> false;
                        false -> hd(NodesInCluster) == node()
                    end
            end
    end;
check_plan(Plan, _, _DataNodes) -> 
    ?Error("Invalid plan ~p", [Plan]),
    error(badarg).

get_max_db_island() -> get_max_db_island(imem_meta:nodes(), []).
get_max_db_island([], []) -> '$none';
get_max_db_island([], Acc) -> lists:max(Acc);
get_max_db_island([Node|Nodes], Acc) ->
    case rpc:call(Node, imem_meta, data_nodes, [], ?GET_RPC_TIMEOUT) of
        {badrpc, _} ->
            get_max_db_island(Nodes, Acc);
        DBNodes ->
            get_max_db_island(Nodes, [length(DBNodes) | Acc])
    end.

-spec nodes_in_db(list(), list()) -> list().
nodes_in_db(Nodes, DataNodes) -> nodes_in_db(Nodes, DataNodes, []).
nodes_in_db([], _, NodesInCluster) -> lists:reverse(NodesInCluster);
nodes_in_db([Node | Nodes], DataNodes, NodesInCluster) ->
    case lists:member(Node, DataNodes) of
        false ->
            nodes_in_db(Nodes, DataNodes, NodesInCluster);
        _ ->
            nodes_in_db(Nodes, DataNodes, [Node|NodesInCluster])
    end.

start(JobOrService) ->
    start(JobOrService, ?G(JobOrService,module), ?G(JobOrService,name)).
start(JobOrService, Mod, Name) ->
    ?Debug("start ~p_~s", [Mod, Name]),
    try dperl_worker:start(imem_config:reference_resolve(JobOrService))
    catch
        _:Error ->
            ?Error([{enum, dperl_dal:to_atom(Name)}],
                   "~p disabled, on start ~p", [Mod, Error], ?ST),
            dperl_dal:disable(JobOrService)
    end.

stop(JobOrService) ->
    stop(JobOrService, ?G(JobOrService,module), ?G(JobOrService,name)).
stop(JobOrService, Mod, Name) ->
    case dperl_worker:is_alive(?RC(JobOrService), Name) of
        true ->
            ?Debug("stop ~p_~s", [Mod, Name]),
            try dperl_worker:stop(Mod, Name)
            catch
                _:Error ->
                    ?Error([{enum, dperl_dal:to_atom(Name)}],
                           "~p disabled, on stop ~p at ~p", [Mod, Error, ?ST]),
                    dperl_dal:disable(JobOrService)
            end;
        false -> no_op
    end.

-spec log_nodes_status(list(), atom(), string()) -> ok.
log_nodes_status([], _Type, _Msg) -> ok;
log_nodes_status([Node | Nodes], Type, Msg) ->
    ?Warn([{enum, Node}], "~p : ~s", [Type, Msg]),
    log_nodes_status(Nodes,Type,  Msg).
