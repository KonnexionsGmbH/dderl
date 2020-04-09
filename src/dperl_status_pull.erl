-module(dperl_status_pull).

-include("dperl_status.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

% dperl_worker exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, format_status/2, get_status/1, init_state/1]).

% dperl_strategy_scr export
-export([connect_check_src/1, get_source_events/2, connect_check_dst/1,
         do_cleanup/2, do_refresh/2, fetch_src/2, fetch_dst/2, delete_dst/2,
         insert_dst/3, update_dst/3, report_status/3]).

% Converting pid from string to local pid, required for rpc registration checks.
-export([is_process_alive/1]).

connect_check_src(#context{connection = Conn} = Ctx) when map_size(Conn) == 0 -> {ok, Ctx};
connect_check_src(#context{connection = #{credential := #{user := User, password := Password}, links := Links},
                           active_link = ActiveLink, imem_sess = OldSession} = Ctx) ->
    #{schema := Schema} = lists:nth(ActiveLink, Links),
    case catch OldSession:run_cmd(schema, []) of
        Schema -> {ok, Ctx};
        _ ->
            catch OldSession:close(),
            case dperl_dal:connect_imem_link(ActiveLink, Links, User, Password) of
                {ok, Session, _Pid} -> {ok, Ctx#context{imem_sess = Session}};
                {Error, NewActiveLink} ->
                    ?JError("Error connecting to remote : ~p", [Error]),
                    {error, Error, Ctx#context{active_link = NewActiveLink, imem_sess = undefined, target_node = undefined}}
            end
    end.

connect_check_dst(Ctx) -> {ok, Ctx}.

do_cleanup(#context{channel = Channel, aggr_channel = AggrChannel,
                    name = JobName, stale_time = StaleTime} = State, _BulkSize) ->
    FocusReg = clean_stale_registrations(Channel, JobName),
    clean_stale_rows(Channel, FocusReg, StaleTime, State#context.debug),
    clean_stale_rows(AggrChannel, FocusReg, StaleTime, State#context.debug),    
    {ok, finish, State}.

do_refresh(_, _) -> error(sync_only_job).

get_source_events(#context{is_loaded = true} = Ctx, _BulkSize) ->
    {ok, sync_complete, Ctx#context{ is_loaded = false }};
get_source_events(#context{metrics = Metrics, focus = Focus, channel = Channel, requested_metrics = NotResponded,
                            sync_cnt = SyncCnt, imem_sess = Session} = Ctx, _BulkSize) ->
    if Ctx#context.debug == none -> no_op;
       true ->
        ?JInfo("Metrics requested : ~p Metrics received : ~p", [Ctx#context.requested_cnt, Ctx#context.received_cnt])
    end,
    {RequestedMetrics, RquestedCnt} = request_metrics(Metrics, Ctx, Session, SyncCnt, NotResponded),
    {NewReqMetrics, FocusRequestCnt} =
    case maps:size(Focus) of
        0 -> {RequestedMetrics, 0};
        _ ->
            Registrations = get_focus_registrations(Channel),
            request_focus_metrics(Registrations, Focus, Session, SyncCnt, RequestedMetrics)
    end,
    % on error in jobDyn table for jobs with only sync enabled, error status is not 
    % cleared as the state is empty issue #476
    if 
        Ctx#context.sync_only -> 
            dperl_dal:update_job_dyn(list_to_binary(Ctx#context.name), idle);
        true -> no_op
    end,
    {ok, sync_complete, Ctx#context{requested_cnt = RquestedCnt + FocusRequestCnt, received_cnt = 0,
        sync_cnt = SyncCnt + 1, requested_metrics = NewReqMetrics, is_loaded = true}}.

fetch_src(_Key, _State) -> ?NOT_FOUND.

fetch_dst(_Key, _State) -> ?NOT_FOUND.

delete_dst(_Key, State) -> {false, State}.

insert_dst(_Key, _Val, State) -> {false, State}.

update_dst(_Key, _Val, State) -> {false, State}.

report_status(_Key, _, _State) -> no_op.

get_status(_Ctx) -> #{}.

init_state(_Ignored) -> #context{}.

init({#dperlJob{name=NameBin, dstArgs=#{channel := Channel}, args=Args, srcArgs=SrcArgs}, Ctx}) ->
    ChannelBin = list_to_binary(Channel),
    AggrChannel = <<ChannelBin/binary, ?AGGR/binary>>,
    Metrics = add_channel_to_agr_metrics(maps:get(metrics, SrcArgs, []), ChannelBin, []),
    Focus = maps:get(focus, SrcArgs, #{}),
    HeartBeatInterval = maps:get(heartBeatInterval, Args, 10000),
    Aggregators = fetch_aggregators(Metrics, #{focus => fetch_aggregators(Focus)}),
    TableOpts = maps:get(tableOpts, Args, []),
    NodeCollector = maps:get(node_collector, SrcArgs, undefined),
    Connection = sort_links(maps:get(connection, SrcArgs, #{})),
    Name = binary_to_list(NameBin),
    Debug = maps:get(debug, Args, none),
    SyncOnly = case Args of 
        #{cleanup := false, refresh := false} -> true;
        _ -> false
    end,
    case is_safe_funs(lists:flatten(maps:values(Aggregators))) of
        true ->
            StaleTime = ?STALE_TIME(?MODULE, NameBin),
            ets:new(?METRICS_TAB(Name), [public, named_table, {keypos,2}]),
            imem_dal_skvh:create_check_channel(ChannelBin, TableOpts),
            imem_dal_skvh:create_check_channel(AggrChannel, [{type, map}]),
            imem_snap:exclude_table_pattern(Channel),
            ?JInfo("Cleaning up before start"),
            NodeList = atom_to_list(node()),
            dperl_dal:remove_deep(ChannelBin, [Name, NodeList]),
            dperl_dal:remove_deep(AggrChannel, [Name, NodeList]),
            ?JInfo("Starting"),
            self() ! heartbeat,
            {ok, Ctx#context{name=Name, channel = ChannelBin, metrics = Metrics, focus = Focus,
                             heart_interval = HeartBeatInterval, aggregators = Aggregators,
                             node_collector = NodeCollector, aggr_channel = AggrChannel,
                             stale_time = StaleTime, debug = Debug, connection = Connection,
                             sync_only = SyncOnly}};
        false ->
            ?JError("One or more of the aggregator funs are not safe to be executed"),
            {stop, not_safe_aggr_funs}
    end;
init({Args, _}) ->
    ?JError("bad start parameters ~p", [Args]),
    {stop, badarg}.

handle_call(Request, _From, Ctx) ->
    ?JWarn("Unsupported handle_call ~p", [Request]),
    {reply, ok, Ctx}.

handle_cast(Request, Ctx) ->
    ?JWarn("Unsupported handle_cast ~p", [Request]),
    {noreply, Ctx}.

handle_info({metric, ReqRef, _, _, _} = Response, #context{requested_metrics = RMs} = Ctx) ->
    NewCtx =
    case maps:get(ReqRef, RMs, none) of
        none -> process_metric(Response, Ctx);
        _ -> process_metric(Response, Ctx#context{requested_metrics = maps:remove(ReqRef, RMs)})
    end,
    {noreply, NewCtx};
handle_info(heartbeat, #context{target_node = undefined, heart_interval = HInt} = Ctx) ->
    erlang:send_after(HInt, self(), heartbeat),
    {noreply, Ctx};
handle_info(heartbeat, #context{channel = Channel, name = Name, heart_interval = HInt,
                                aggr_channel = AggrChannel, target_node = TNode,
                                requested_metrics = RequestedMetrics} = Ctx) ->
    #context{
        success_cnt = SuccessCnt,
        mem_overload_cnt = MemOverloadCnt,
        cpu_overload_cnt = CpuOverloadCnt,
        eval_suspend_cnt = EvalCrashCnt,
        errors = Errors
    } = Ctx,
    Value = #{
        time => erlang:system_time(milli_seconds),
        success_count => SuccessCnt,
        memory_overload_count => MemOverloadCnt,
        cpu_overload_count => CpuOverloadCnt,
        eval_crash_count => EvalCrashCnt,
        errors => Errors
    },
    {HeartbeatInfo, NewCtx} = dperl_status_pre:preprocess(heartbeat, Value, imem_meta:time(), TNode, Ctx),
    provision(Name, AggrChannel, HeartbeatInfo, false),            %% writing to aggr table
    provision(Name, Channel, HeartbeatInfo, true),
    RMetrics = filter_not_responded_metrics(Name, RequestedMetrics),
    erlang:send_after(HInt, self(), heartbeat),
    {noreply, NewCtx#context{success_cnt = 0, mem_overload_cnt = 0, errors = [],
                             cpu_overload_cnt = 0, eval_suspend_cnt = 0,
                             requested_metrics = RMetrics}};
handle_info(Request, Ctx) ->
    ?JWarn("handle_info ~p", [Request]),
    {noreply, Ctx}.

terminate(Reason, #context{channel=Channel, name=Name, aggr_channel = AggrChannel}) ->
    NodeList = atom_to_list(node()),
    dperl_dal:remove_deep(Channel, [Name, NodeList]),
    dperl_dal:remove_deep(AggrChannel, [Name, NodeList]),
    ?JInfo("terminate ~p", [Reason]).

code_change(OldVsn, Ctx, Extra) ->
    ?JInfo("code_change ~p:~n~p", [OldVsn, Extra]),
    {ok, Ctx}.

format_status(Opt, [PDict, Ctx]) ->
    ?JInfo("format_status ~p:~n~p", [Opt, PDict]),
    Ctx.

process_metric({metric, ReqRef, _Timestamp, _Node, {error, user_input}}, #context{} = Ctx) ->
    ?JError("User Input error when requesting metric ~p", [ReqRef]),
    Ctx;
process_metric({metric, ReqRef, _Timestamp, Node, {error, Error}}, #context{errors = Errors, target_node = TNode} = Ctx) ->
    ?JError("Error ~p when requesting metric ~p", [Error, ReqRef]),
    MetricBin = imem_datatype:term_to_io(ReqRef),
    ErrorBin = imem_datatype:term_to_io(Error),
    Ctx#context{errors = [#{metricKey => MetricBin, error => ErrorBin} | Errors],
                          target_node = get_target_node(Node, TNode)};
process_metric({metric, ReqRef, _Timestamp, Node, cpu_overload}, #context{cpu_overload_cnt = CpuOverloadCnt, target_node = TNode} = Ctx) ->
    ?JWarn("Unable to get metric ~p as server is on cpu overload", [ReqRef]),
    Ctx#context{cpu_overload_cnt = CpuOverloadCnt + 1, target_node = get_target_node(Node, TNode)};
process_metric({metric, ReqRef, _Timestamp, Node, memory_overload}, #context{mem_overload_cnt = MemOverloadCnt, target_node = TNode} = Ctx) ->
    ?JWarn("Unable to get metric ~p as server is on memory overload", [ReqRef]),
    Ctx#context{mem_overload_cnt = MemOverloadCnt + 1, target_node = get_target_node(Node, TNode)};
process_metric({metric, ReqRef, _Timestamp, Node, eval_crash_suspend}, #context{eval_suspend_cnt = EvalCrashCnt,
                                                                              target_node = TNode} = Ctx) ->
    ?JWarn("Unable to get metric ~p as server is on eval crash suspend state", [ReqRef]),
    Ctx#context{eval_suspend_cnt = EvalCrashCnt + 1, target_node = get_target_node(Node, TNode)};
process_metric({metric, MetricReqKey, Timestamp, Node, OrigMetrics}, #context{channel = Channel, name = Name, 
            received_cnt = ReceivedCnt, success_cnt = SuccessCnt, aggregators = Aggrs,
            aggr_channel = AggrChannel, target_node = TNode} = Ctx) ->
    %% Append the channel to the key...
    {Metrics, PreCtx} = dperl_status_pre:preprocess(MetricReqKey, OrigMetrics, Timestamp, Node, Ctx),
    {AggrMetrics, AgrPrecise} = process_aggregators(Aggrs, PreCtx, MetricReqKey, Metrics, Timestamp),
    Writes = provision(Name, Channel, AggrMetrics, true),
    provision(Name, AggrChannel, AgrPrecise, false),             %% writing to aggr table
    if Ctx#context.debug == detail ->
        ?JInfo("Metric : ~p received : ~p written : ~p", [MetricReqKey, length(Metrics), Writes]);
       true -> no_op
    end,
    PreCtx#context{received_cnt = ReceivedCnt + 1, success_cnt = SuccessCnt + 1, 
                             target_node = get_target_node(Node, TNode)}.

process_aggregators(Aggregators, Ctx, {_, {focus, _, _}} = FocusKey, Metrics, Time) ->
    FocusAgggregators = maps:get(focus, Aggregators, []),
    process_aggregators(FocusAgggregators, Ctx, FocusKey, Metrics, Time, {[], []});
process_aggregators(Aggregators, Ctx, MetricKey, Metrics, Time) ->
    MetricAggregators = maps:get(MetricKey, Aggregators, []),
    process_aggregators(MetricAggregators, Ctx, MetricKey, Metrics, Time, {[], []}).

process_aggregators([], _Ctx, _MetricKey, _Metrics, _Time, Acc) -> Acc;
process_aggregators([{AgrMod, AgrFun, Opts} | RestAggregators], #context{name = Name, aggr_channel = AggrChannel, channel = Channel} = Ctx, 
                    MetricKey, Metrics, Time, {AggRAcc, AggPAcc} = Acc) ->
    EtsKey = {MetricKey, AgrMod, AgrFun},
    %{AggrRound, AggrPrecise, AggrState, LastTime} = LastAgrInfo
    {LastAggRound, LastAggPrecise, _, _} = LastAgrInfo = 
    case ets:lookup(?METRICS_TAB(Name), EtsKey) of
        [#dperl_metrics{agg_round = AR, agg_precise = AP, state = AS, time = LTime}] ->
            {AR, AP, AS, LTime};
        [] -> {[], [], #{}, undefined}
    end,
    NewAcc =
    case catch erlang:apply(AgrMod, AgrFun, [MetricKey, {Metrics, Time}, Ctx, LastAgrInfo, Opts]) of
        {AggMetrics, AggPrecise, AggState} when is_list(AggMetrics) ->
            remove_old_keys(Name, Channel, AggMetrics, LastAggRound),          %% removing from the main table
            remove_old_keys(Name, AggrChannel, AggPrecise, LastAggPrecise),      %% removing from the aggr table
            ets:insert(?METRICS_TAB(Name), #dperl_metrics{key = EtsKey, 
                                                         agg_round = AggMetrics,
                                                         agg_precise = AggPrecise,
                                                         state = AggState,
                                                         time = Time}),
            {AggRAcc ++ AggMetrics, AggPAcc ++ AggPrecise};
        Error ->
            ?JError("Aggregation error for ~p:~p Error : ~p", [AgrMod, AgrFun, Error]),
            Acc
    end,
    process_aggregators(RestAggregators, Ctx, MetricKey, Metrics, Time, NewAcc).

provision(Name, Channel, Infos, ShouldEncode) ->
    provision(Name, Channel, Infos, ShouldEncode, 0).

provision(_Name, _Channel, [], _ShouldEncode, Writes) -> Writes;
provision(Name, Channel, [#{ckey := OrigKey, cvalue := Value} | Infos], ShouldEncode, Writes) ->
    Key = [Name, atom_to_list(node()) | OrigKey],
    case dperl_dal:write_if_different(Channel, Key, ensure_json_encoded(Value, ShouldEncode)) of
        no_op -> provision(Name, Channel, Infos, ShouldEncode, Writes);
        _ -> provision(Name, Channel, Infos, ShouldEncode, Writes + 1)
    end.

ensure_json_encoded(Value, false) -> Value;
ensure_json_encoded(Value, true) when is_binary(Value) -> Value;
ensure_json_encoded(Value, true) -> imem_json:encode(Value).

%%TODO: This should take into account the already requested metrics with no response yet.
request_metrics(Metrics, Context, Session, SyncCnt, NotResponded) when is_map(NotResponded)->
    request_metrics(Metrics, Context, Session, SyncCnt, {NotResponded, 0});
request_metrics([], _Context, _Session, _SyncCnt, Acc) -> Acc;
request_metrics([#{key := {agr, AgrMetric, []}} = Metric | Rest], Context, Session, SyncCnt, Acc) ->
    MetricKey = {agr, AgrMetric, Context#context.channel},
    request_metrics([Metric#{key => MetricKey} | Rest], Context, Session, SyncCnt, Acc);
request_metrics([Metric | Rest], Context, Session, SyncCnt, {RMetrics, _} = Acc) ->
    ReqResult = request_metric(Metric, Session, SyncCnt, RMetrics),
    request_metrics(Rest, Context, Session, SyncCnt, acc_if_ok(ReqResult, Acc)).

-spec request_metric(map(), term(), integer(), map()) -> ok | error | posponed.
request_metric(#{key := MetricKey, metric_src := Mod} = Metric, Session, SyncCnt, RMetrics) ->
    Frequency = maps:get(frequency, Metric, 1),
    case is_time_to_run(Frequency, {Mod, MetricKey}, SyncCnt, RMetrics) of
        true -> request_metric(Metric, Session);
        false -> posponed
    end;
request_metric(Metric, _Session, _SyncCnt, _RMetrics) ->
    ?JError("Metrics not in the correct format : ~p", [Metric]),
    error.

-spec request_metric(map(), term()) -> ok | error.
request_metric(#{location := local, metric_src := Mod, key := MetricKey}, _Session) ->
    ok = imem_gen_metrics:request_metric(Mod, MetricKey, {Mod, MetricKey}, self()),
    {ok, {Mod, MetricKey}};
request_metric(#{location := remote, metric_src := Mod, key := MetricKey}, Session) ->
    ok = Session:run_cmd(request_metric, [Mod, MetricKey, {Mod, MetricKey}]),
    {ok, {Mod, MetricKey}};
request_metric(Metric, _Session) ->
    ?JError("Metrics not in the correct format : ~p", [Metric]),
    error.

-spec acc_if_ok(term(), tuple()) -> tuple().
acc_if_ok({ok, Ref}, {RMetrics, Count}) ->
    {RMetrics#{Ref => imem_meta:time()}, Count + 1};
acc_if_ok(_, Acc) -> Acc.

get_focus_registrations(Channel) ->
    BaseKey = ["register", "focus"],
    imem_dal_skvh:read_shallow(system, Channel, [BaseKey]).

request_focus_metrics(Registrations, Focus, Session, SyncCnt, RequestedMetrics) when is_map(RequestedMetrics) ->
    request_focus_metrics(Registrations, Focus, Session, SyncCnt, {RequestedMetrics, 0});
request_focus_metrics([], _Focus, _Session, _SyncCnt, Acc) -> Acc;
request_focus_metrics([#{cvalue := CValue} | Registrations], Focus, Session, SyncCnt, Acc) ->
    KeyTopicsList = imem_json:decode(CValue),
    NewAcc = request_focus_metrics_internal(KeyTopicsList, Focus, Session, SyncCnt, Acc),
    request_focus_metrics(Registrations, Focus, Session, SyncCnt, NewAcc).

-spec request_focus_metrics_internal(list(), map(), term(), integer(), tuple()) -> tuple().
request_focus_metrics_internal([], _, _, _, Acc) -> Acc;
request_focus_metrics_internal([[Topic, Key] | Rest], Focus, Session, SyncCnt, {RMetrics, _} = Acc) ->
    case maps:is_key(Topic, Focus) of
        true ->
            Metric = maps:get(Topic, Focus),
            MetricKey = {focus, binary_to_list(Topic), binary_to_list(Key)},
            ReqResult = request_metric(Metric#{key => MetricKey}, Session, SyncCnt, RMetrics),
            request_focus_metrics_internal(Rest, Focus, Session, SyncCnt, acc_if_ok(ReqResult, Acc));
        false ->
            request_focus_metrics_internal(Rest, Focus, Session, SyncCnt, Acc)
    end.

clean_stale_rows(Channel, FocusReg, StaleTime, Debug) ->
    TableName = imem_dal_skvh:atom_table_name(Channel),
    CleanFun = fun() ->
        FirstKey = imem_meta:first(TableName),
        clean_stale_rows(TableName, FirstKey, StaleTime, #{}, FocusReg, Debug, 0)
    end,
    case imem_meta:transaction(CleanFun) of
        {atomic, ok} -> ok;
        ErrorResult ->
            ?JError("Error cleaning stale rows, result: ~p", [ErrorResult])
    end.

clean_stale_rows(_TableName, '$end_of_table', _StaleTime, _Heartbeats, _FocusReg, _Debug, 0) -> ok;
clean_stale_rows(_TableName, '$end_of_table', _StaleTime, _Heartbeats, _FocusReg, none, _Count) -> ok;
clean_stale_rows(_TableName, '$end_of_table', _StaleTime, _Heartbeats, _FocusReg, _Debug, Count) ->
    ?JInfo("clean up deleted ~p rows", [Count]);
clean_stale_rows(TableName, NextKey, StaleTime, Heartbeats, FocusReg, Debug, Count) ->
    [RawRow] = imem_meta:read(TableName, NextKey),
    #{ckey := CKey} = imem_dal_skvh:skvh_rec_to_map(RawRow),
    {NHeartMap, NCount} = case CKey of
        [Cid, Node, Platform, "focus", _TNode, FocusId, _FKey] ->
            case lists:member(FocusId, FocusReg) of
                true ->
                    HBKey = [Cid, Node, Platform, "system_info"],
                    clean_stale_row(maps:get(HBKey, Heartbeats, undefined), Heartbeats,
                        Count, TableName, RawRow, HBKey, StaleTime);
                false ->
                    imem_meta:remove(TableName, RawRow),
                    {Heartbeats, Count + 1}
            end;
        [Cid, Node, Platform, _Group | _Rest] ->
            HBKey = [Cid, Node, Platform, "system_info"],
            clean_stale_row(maps:get(HBKey, Heartbeats, undefined), Heartbeats,
                Count, TableName, RawRow, HBKey, StaleTime);
        _ -> {Heartbeats, Count}
    end,
    NKey = imem_meta:next(TableName, NextKey),
    clean_stale_rows(TableName, NKey, StaleTime, NHeartMap, FocusReg, Debug, NCount).

clean_stale_row(current, Heartbeats, Count, _TableName, _RawRow, _HBKey, _StaleTime) -> {Heartbeats, Count};
clean_stale_row(stale, Heartbeats, Count, TableName, RawRow, _HBKey, _StaleTime) ->
    imem_meta:remove(TableName, RawRow),
    {Heartbeats, Count + 1};
clean_stale_row(undefined, Heartbeats, Count, TableName, RawRow, HBKey, StaleTime) ->
    case fetch_heartbeat(TableName, HBKey) of
        not_found ->
            imem_meta:remove(TableName, RawRow),
            {Heartbeats#{HBKey => stale}, Count + 1};
        CValue ->
            T = extract_time(CValue),
            ElapsedTime = erlang:system_time(milli_seconds) - T,
            case ElapsedTime > StaleTime of
                true ->
                    imem_meta:remove(TableName, RawRow),
                    {Heartbeats#{HBKey => stale}, Count + 1};
                false ->
                    {Heartbeats#{HBKey => current}, Count}
            end
    end.

clean_stale_registrations(Channel, JobName) ->
    BaseKey = ["register", "focus"],
    lists:usort(clean_stale_registrations(Channel, JobName, imem_dal_skvh:read_shallow(system, Channel, [BaseKey]), [])).

%% TODO: Find better name as this cleans the registrations and returns the list
%% of topics for later data cleanup, maybe name should be more descriptive.
clean_stale_registrations(_Channel, _JobName, [], Acc) -> Acc;
clean_stale_registrations(Channel, JobName, [#{ckey := ["register", "focus", [NodeString, PidString]], cvalue := CValue} = Reg | Rest], Acc) ->
    Node = list_to_existing_atom(NodeString),
    case rpc:call(Node, dperl_status_pull, is_process_alive, [PidString]) of
        true ->
            KeyTopicsList = [[binary_to_list(Topic), binary_to_list(Key)] || [Topic, Key] <- imem_json:decode(CValue)],
            clean_stale_registrations(Channel, JobName, Rest, KeyTopicsList ++ Acc);
        false ->
            remove_registration(Channel, Reg, JobName),
            clean_stale_registrations(Channel, JobName, Rest, Acc);
        {badrpc, _} ->
            %% Unable to check the registration, removing invalid data.
            remove_registration(Channel, Reg, JobName),
            clean_stale_registrations(Channel, JobName, Rest, Acc)
    end.

remove_registration(Channel, Reg, JobName) ->
    imem_dal_skvh:remove(system, Channel, Reg),
    %removing focus ets entry
    #{cvalue := FocusVal} = Reg,
    case imem_json:decode(FocusVal) of
        [FocusId | _] ->
            MetricKey = {dperl_metrics, list_to_tuple([focus | dperl_dal:key_from_json(FocusId)])},
            EtsFocusKey = {MetricKey, dperl_status_agr, write_to_channel},
            ets:delete(?METRICS_TAB(JobName), EtsFocusKey);
        _ -> no_op
    end.

is_process_alive(PidString) ->
    Pid = list_to_pid(PidString),
    erlang:is_process_alive(Pid).

is_time_to_run(Frequency, Key, SyncCnt, RMetrics) ->
    case RMetrics of
        #{Key := _} -> false;
        _ -> 
            Frequency == 1 orelse (SyncCnt rem Frequency == erlang:phash2(Key, 1023) rem Frequency)
    end.

sort_links(Connection) when map_size(Connection) == 0 -> #{};
sort_links(#{links := Links} = Connection) ->
    Connection#{links => dperl_dal:sort_links(Links)}.

-spec is_safe_funs(list()) -> boolean().
is_safe_funs([]) -> true;
is_safe_funs([{Mod, Fun, _Opts} | ModFuns]) ->
    lists:member({Mod, Fun, 5}, imem_compiler:safe(Mod)) andalso is_safe_funs(ModFuns).

remove_old_keys(Name, Channel, NewKeyVals, OldKeyVals) ->
    NewKeys = [Key || #{ckey := Key} <- NewKeyVals],
    OldKeys = [Key || #{ckey := Key} <- OldKeyVals],
    %% Removing old keys that are not valid any more
    [dperl_dal:remove_from_channel(Channel, [Name, atom_to_list(node()) | Key]) || Key <- OldKeys -- NewKeys].

-spec get_target_node(atom(), atom()) -> atom().
get_target_node(MetricNode, undefined) -> MetricNode;
get_target_node(MetricNode, TargetNode) when node() =:= MetricNode -> TargetNode;
get_target_node(MetricNode, _TargetNode) -> MetricNode.

-spec extract_time(map() | binary()) -> map().
extract_time(Value) when is_binary(Value) ->
    #{<<"time">> := T} = imem_json:decode(Value, [return_maps]),
    T;
extract_time(#{time := T}) -> T.

-spec fetch_aggregators(list(), map()) -> map().
fetch_aggregators([], Acc) -> Acc;
fetch_aggregators([#{metric_src := Mod, key := MetricKey} = Metric | Metrics], Acc) ->
    NewAcc = Acc#{{Mod, MetricKey} => fetch_aggregators(Metric)},
    fetch_aggregators(Metrics, NewAcc).

-spec fetch_aggregators(map()) -> list().
fetch_aggregators(Metric) ->
    case maps:get(aggregators, Metric, none) of
        none -> [{dperl_status_agr,write_to_all, #{}}];
        Aggrs -> 
            lists:foldr(
                fun({AMod, AFun}, AgrAcc) -> [{AMod, AFun, #{}} | AgrAcc];
                   ({AMod, AFun, Opts}, AgrAcc) -> [{AMod, AFun, Opts} | AgrAcc]
                end, [], Aggrs)
    end.

-spec add_channel_to_agr_metrics(list(), binary(), list()) -> list().
add_channel_to_agr_metrics([], _Channel, Acc) -> lists:reverse(Acc);
add_channel_to_agr_metrics([#{key := {agr, Agr, []}} = Metric | Metrics], Channel, Acc) ->
    add_channel_to_agr_metrics(Metrics, Channel, [Metric#{key => {agr, Agr, Channel}} | Acc]);
add_channel_to_agr_metrics([Metric | Metrics], Channel, Acc) ->
    add_channel_to_agr_metrics(Metrics, Channel, [Metric | Acc]).

-spec fetch_heartbeat(atom(), list()) -> not_found | map().
fetch_heartbeat(Table, Key) ->
    fetch_heartbeat(imem_dal_skvh:read_deep(system, atom_to_binary(Table,utf8), [Key])).

-spec fetch_heartbeat(list()) -> not_found | map() | binary().
fetch_heartbeat([]) -> not_found;
fetch_heartbeat([#{ckey := [_, _, _, "system_info", _TNode, "system_info", "heartbeat"],
                   cvalue := Value} | _]) -> Value;
fetch_heartbeat([_ | Rest]) -> fetch_heartbeat(Rest).

-spec filter_not_responded_metrics(binary(), map()) -> {list(), map()}.
filter_not_responded_metrics(Name, Metrics) ->
    Now = imem_meta:time(),
    DiffLimit = ?METRIC_WAIT_THRESHOLD(Name),
    maps:fold(
        fun(M, Time, WaitingMetrics) ->
                case imem_datatype:sec_diff(Time, Now) of
                    Diff when Diff < DiffLimit -> WaitingMetrics;
                    _ ->
                        ?Error("Metric : ~p has not got any response till ~p seconds", [M, DiffLimit]),
                        maps:remove(M, WaitingMetrics)
                end
            end, Metrics, Metrics).
