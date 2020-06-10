-module(dpjob_status_agr).

-include("../dperl_status.hrl").

-export([sync_puller_error/5, check_nodes/5, merge_node_errors/5,
         check_memory_on_nodes/5, check_heartbeats/5, check_job_error/5,
         check_job_down/5, write_to_all/5, write_to_aggr/5, write_to_channel/5,
         format_system_memory/5, sessions_rate/5, restricted_sessions_rate/5,
         upstream_requests_rate/5, downstream_requests_rate/5,
         focus_requests_rate/5, check_memory_on_node/5]).

-safe([sync_puller_error/5, check_nodes/5, merge_node_errors/5,
       check_memory_on_nodes/5, check_heartbeats/5, check_job_down/5,
       check_job_error/5, write_to_all/5, write_to_aggr/5, write_to_channel/5,
       format_system_memory/5, sessions_rate/5, restricted_sessions_rate/5,
       upstream_requests_rate/5, downstream_requests_rate/5,
       focus_requests_rate/5, check_memory_on_node/5]).

-define(PUSHER_MAP_CACHE_TIMEOUT, 300).  %% 5 minutes after which the job map in the state is rebuild

%% Node level aggregator
sync_puller_error({_, {job_status, _}}, {Infos, _T}, #context{name = Name, channel = Channel}, {_, _, State, _}, _Opts) ->
    NewState =
    case State of
        #{time := Time} ->
            case imem_datatype:sec_diff(Time, imem_meta:time()) < ?PUSHER_MAP_CACHE_TIMEOUT of
                true -> State;
                false -> #{pushers => build_job_map(Channel), time => imem_meta:time()}
            end;
        _ -> #{pushers => build_job_map(Channel), time => imem_meta:time()}
    end,
    Pushers = maps:get(pushers, NewState, #{}),
    NewInfos =
    lists:map(
        fun(#{cvalue := #{status := stopped}} = Info) -> Info;
           (#{ckey := [_, _, _, "job_status", Job], cvalue := Status} = Info) ->
                case Pushers of
                    #{Job := puller} -> Info;
                    #{Job := Pullers} ->
                        Info#{cvalue => update_push_status(Pullers, Name, Channel, Status)};
                    _ -> Info
                end;
           (Info) -> Info
        end, Infos),
    {NewInfos, [], NewState}.

%% Node level aggregator
check_nodes({_, MKey}, {Infos, _T}, #context{channel = Channel}, {_, _, State, _}, Opts) 
  when MKey == erlang_nodes; MKey == data_nodes->
    AggrMetrics =
    lists:foldl(
        fun(#{ckey := [_Pla, "system_info", Node, "system_info", "erlang_nodes"] = Key, 
              cvalue := #{nodes := Nodes, required_nodes := RNodes}}, Acc) ->
                check_nodes_internal(Channel, Key, Node, Nodes, RNodes) ++ Acc;
           (#{ckey := [_Pla, "system_info", Node, "system_info", "data_nodes"] = Key, 
              cvalue := #{data_nodes := DNodes, required_nodes := RNodes}}, Acc) ->
                Nodes = [N || #{node  := N} <- DNodes],
                check_nodes_internal(Channel, Key, Node, Nodes, RNodes) ++ Acc;
           (_, Acc) -> Acc
        end, [], Infos),
    case Opts of
        #{write_to_channel := true} -> {AggrMetrics, AggrMetrics, State};
        _ -> {[], AggrMetrics, State}
    end.

%% Master aggregator
merge_node_errors({_, {agr, node_error, _}}, {[#{cvalue := NodeErrors}], _T}, #context{channel = Channel}, {_, _, State, _}, _Opts) ->
    ErrorKey = [binary_to_list(Channel), "system_info", "aggregation", "error", "nodes"],
    NErrors =
    case lists:usort(maps:values(NodeErrors)) of
        []      -> [];
        [Error] -> [#{ckey => ErrorKey, cvalue => #{error => Error}}];
        Errors  -> [#{ckey => ErrorKey, cvalue => #{error => Errors}}]
    end,
    {NErrors, [], State}.

%% Master aggregator
check_memory_on_nodes({_, {agr, node_memory, _}}, {[#{cvalue := NodeStatuses}], _T}, #context{channel = Channel}, {_, _, State, _}, _Opts) ->
    NodeMemErrors =
    maps:fold(
        fun(Node, Value, Acc) ->
            case check_memory(Node, Value) of
                false -> Acc;
                Mem -> Acc#{Node => Mem}
            end
        end, #{}, NodeStatuses),
    if map_size(NodeMemErrors) == 0 -> {[], [], State};
       true -> 
            ErrorKey = [binary_to_list(Channel), "system_info", "aggregation", "error", "memory"],
            {[#{ckey => ErrorKey, cvalue => #{error => <<"Memory too high">>, info => NodeMemErrors}}], [], State}
    end.

%% Node level aggregator
check_memory_on_node({_, system_information}, {[#{ckey:= Key, cvalue := Value}], _T}, _Ctx, {_, _, State, _}, _Opts) ->
    [Pla, "system_info", Node, "system_info", "node_status"] = Key,
    case check_memory(Node, Value) of
        false -> {[], [], State};
        Mem -> 
            {[#{ckey => [Pla, "system_info", Node, "error", "memory"],
                cvalue => #{error => <<"Memory too high">>, info => Mem}}], [], State}
    end.

%% Master aggregator
check_heartbeats({_, {agr, heartbeat, _}}, {[#{cvalue := Heatbeats}], _T}, #context{channel = Channel,
        node_collector= NodeCollecotor, stale_time = StaleTime}, {_, _, State, _}, _Opts) ->
    {[Nodes], true} = imem_meta:select(dperlJob, [{#dperlJob{name = list_to_binary(NodeCollecotor), nodes = '$1', _ = '_'}, [], ['$1']}]),
    MissingNodes = maps:fold(
        fun(Node, Time, Acc) ->
            case erlang:system_time(milli_seconds) - Time of
                T when T > StaleTime -> Acc;
                _ -> lists:delete(Node, Acc)
            end
        end, Nodes, Heatbeats),
    case MissingNodes of
        [] -> {[], [], State};
        MissingNodes ->
            ErrorKey = [binary_to_list(Channel), "system_info", "aggregation", "error", "heartbeat"],
            {[#{ckey => ErrorKey, cvalue => #{error => <<"Missing heartbeat">>, info => MissingNodes}}], [], State}
    end.

%% Master aggregator
check_job_error({_, {agr, error_count, _}}, {[#{cvalue := NodeErrors}], _T}, #context{channel = Channel}, {_, _, State, _}, _Opts) ->
    ErrorMap = maps:fold(
        fun(Node, Count, Acc) when Count > 0 -> Acc#{Node => Count};
           (_, _, Acc) -> Acc
        end, #{}, NodeErrors),
    if map_size(ErrorMap) > 0 ->
            ErrorKey = [binary_to_list(Channel), "system_info", "aggregation", "error", "job_error_count"],
            {[#{ckey => ErrorKey, cvalue => #{error => <<"Jobs with errors">>, info => ErrorMap}}], [], State};
        true -> {[], [], State}
    end.

%% Master aggregator
check_job_down({_, {job_down_count, _}}, {Infos, _T}, #context{channel = ChannelBin}, {_, _, State, _}, _Opts) ->
    Channel = binary_to_list(ChannelBin),
    DownMap = lists:foldl(
        fun(#{ckey := [_Pla, "system_info", Node, "system_info", "job_down_count"],
              cvalue := #{job_down_count := Count}}, _) when Count > 0 -> #{list_to_binary(Node) => Count};
           (_, Acc) -> Acc
        end, #{}, Infos),
    if map_size(DownMap) > 0 ->
            ErrorKey = [Channel, "system_info", "aggregation", "error", "job_down_count"],
            {[#{ckey => ErrorKey, cvalue => #{error => <<"Down jobs">>, info => DownMap}}], [], State};
        true -> {[], [], State}
    end.

format_system_memory({_, system_information}, {[#{cvalue := Value} = Info], _T}, _Ctx, {_, _, State, _}, _Opts) ->
    #{free_memory := FreeMemory,
      total_memory := TotalMemory,
      erlang_memory := ErlMemory} = Value,
    ValueRounded = Value#{free_memory => bytes_to_mb(FreeMemory),
                          total_memory => bytes_to_mb(TotalMemory),
                          erlang_memory => bytes_to_mb(ErlMemory)},
    {[Info#{cvalue => ValueRounded}], [Info], State}.

write_to_all({_, _}, {Infos, _T}, _Ctx, {_, _, State, _}, _Opts) -> {Infos, Infos, State}.  % system_information

write_to_aggr({_, _}, {Infos, _T}, _Ctx, {_, _, State, _}, _Opts) -> {[], Infos, State}. % job_error_count

write_to_channel({_, _}, {Infos, _T}, _Ctx, {_, _, State, _}, _Opts) -> {Infos, [], State}. % focus, jobs, errors

%% mpro aggregators
sessions_rate({mpro_metrics, {sessions, _}}, {[#{ckey := Key, cvalue := #{sessions := Sessions}}], Time}, 
  _Ctx, {_, _, State, LastTime}, Opts) ->
    calculate_rate(sessions_rate, Sessions, LastTime, Time, Key, check_opts(Opts), State).

restricted_sessions_rate({mpro_metrics, {restricted_sessions, _}}, {[#{ckey := Key, cvalue := #{restricted_sessions := RSessions}}], Time}, 
  _Ctx, {_, _, State, LastTime}, Opts) ->
    calculate_rate(restricted_sessions_rate, RSessions, LastTime, Time, Key, check_opts(Opts), State).

upstream_requests_rate({mpro_metrics, {upstream_requests, _}}, {[#{ckey := Key, cvalue := #{upstream_requests := Requests}}], Time}, 
  _Ctx, {_, _, State, LastTime}, Opts) ->
    calculate_rate(upstream_requests_rate, Requests, LastTime, Time, Key, check_opts(Opts), State).

downstream_requests_rate({mpro_metrics, {downstream_requests, _}}, {[#{ckey := Key, cvalue := #{downstream_requests := Requests}}], Time}, 
  _Ctx, {_, _, State, LastTime}, Opts) ->
    calculate_rate(downstream_requests_rate, Requests, LastTime, Time, Key, check_opts(Opts), State).

-spec focus_requests_rate({atom, {atom, list(), list()}}, {list(), term()}, #context{}, tuple(), map()) -> {list(), list(), term()}.
focus_requests_rate({mpro_metrics, {focus, _Topic, _FocusKey}}, {[], _Time}, _Ctx, {_, _, State, _}, _Opts) ->
    {[], [], State};
focus_requests_rate({mpro_metrics, {focus, _Topic, _FocusKey}}, {Rows, Time}, _Ctx, {_, _, State, LastTime}, Opts) ->
    {Rates, NewState} = lists:foldl(
        fun(#{ckey := K, cvalue := V} = I, {AccRates, AccState}) ->
            {RateValue, RateState} = calculate_focus_rate(lists:last(K), V, AccState, LastTime, Time, check_opts(Opts)),
            {[I#{cvalue => RateValue} | AccRates], RateState}
        end,
        {[], State}, Rows),
    {Rates, [], NewState}.

%% Internal helper functions
find_pullers(Channel) ->
    case imem_meta:select(dperlJob, [{#dperlJob{name = '$1', dstArgs = '$2', _ = '_'}, 
                            [{'==', '$2', #{channel => binary_to_list(Channel)}}], ['$1']}]) of
        {[], true} -> puller;
        {Pullers, true} -> Pullers
    end.

-spec update_push_status([binary()], list(), binary(), map()) -> map().
update_push_status(Pullers, CollectorName, Channel, StatusValue) ->
    Rows = imem_dal_skvh:read_deep(system, Channel, [[CollectorName]]),
    update_push_status(get_error_jobs(Rows), Pullers, StatusValue).

-spec update_push_status([binary()], [binary()], map()) -> map().
update_push_status([], _Pullers, StatusValue) -> StatusValue;
update_push_status([ErrorJob | Rest], Pullers, StatusValue) ->
    case lists:member(ErrorJob, Pullers) of
        true -> StatusValue#{status => error};
        false -> update_push_status(Rest, Pullers, StatusValue)
    end.

-spec get_error_jobs([map()]) -> [binary()].
get_error_jobs([]) -> [];
get_error_jobs([#{ckey := [_, _, _, _, _, "job_status", Name], cvalue := ValueBin} | Rest]) ->
    case imem_json:decode(ValueBin, [return_maps]) of
        #{<<"status">> := <<"error">>} -> [list_to_binary(Name) | get_error_jobs(Rest)];
        _ -> get_error_jobs(Rest)
    end;
get_error_jobs([_Row | Rest]) -> get_error_jobs(Rest).

check_nodes_internal(_Channel, _Key, _Node, Nodes, Nodes) -> [];
check_nodes_internal(Channel, Key, Node, Nodes, RequiredNodes) ->
    case RequiredNodes -- Nodes of
        [] -> [];
        MissingNodes -> 
            ErrorKey = [binary_to_list(Channel), "system_info", Node, "error", lists:last(Key)],
            Error = #{error => list_to_binary(io_lib:format("Missing nodes ~p", [MissingNodes]))},
            [#{ckey => ErrorKey, cvalue => Error}]
    end.

bytes_to_mb(Bytes) -> list_to_binary(lists:concat([Bytes div 1000000, "MB"])).

calculate_rate(_RateName, V2, undefined, _T2, _Key, _Opts, State) ->
    {[], [], State#{lastValue => V2, lastRate => undefined}};
calculate_rate(_RateName, V2, T1, T2, _Key, _Opts, #{lastRate := undefined} = State) ->
    V1 = maps:get(lastValue, State, 0),
    Rate = calculate_rate(V1, V2, T1, T2),
    {[], [], State#{lastValue => V2, lastRate => Rate}};
calculate_rate(RateName, V2, T1, T2, Key, #{exp_factor := Factor}, #{lastRate := LRate} = State) when is_number(LRate) ->
    V1 = maps:get(lastValue, State, 0),
    RateKey =  lists:sublist(Key, 3) ++ [atom_to_list(RateName), []],
    Rate = apply_factor(calculate_rate(V1, V2, T1, T2), LRate, Factor),
    RoundVal = [#{ckey => RateKey, cvalue => #{RateName => round(Rate)}}],
    NewState = State#{lastValue => V2, lastRate => Rate},
    {RoundVal, [#{ckey => RateKey, cvalue => #{RateName => Rate}}], NewState};
calculate_rate(_RateName, V2, _T1, _T2, _Key, _Opts, State) ->
    {[], [], State#{lastValue => V2, lastRate => undefined}}.

calculate_rate(Val, Val, _T1, _T2) -> 0;
calculate_rate(V1, V2, T1, T2) ->
    (V2 - V1) * (1000000 / imem_datatype:musec_diff(T1, T2)).

-spec check_opts(map()) -> map().
check_opts(#{exp_factor := 1} = Opts) -> Opts;
check_opts(#{exp_factor := Factor} = Opts) when is_float(Factor),
    Factor >= 0.5, Factor < 1 -> Opts;
check_opts(Opts) -> Opts#{exp_factor => 1}.

-spec apply_factor(number(), number(), number()) -> number().
apply_factor(Rate, _OldRate, 1) -> Rate;
apply_factor(Rate, OldRate, Factor) ->
    Rate * Factor + OldRate * (1-Factor).

-spec build_job_map(binary()) -> map().
build_job_map(Channel) ->
    TableName = imem_dal_skvh:atom_table_name(Channel),
    JobMapFun = fun() ->
        FirstKey = imem_meta:first(TableName),
        build_job_map(TableName, FirstKey, #{})
    end,
    case imem_meta:transaction(JobMapFun) of
        {atomic, NewAcc} -> NewAcc;
        ErrorResult ->
            ?JError("Error building job map: ~p", [ErrorResult]),
            #{}
    end.

-spec build_job_map(atom(), term() | atom(), map()) -> map().
build_job_map(_TableName, '$end_of_table', Acc) -> Acc;
build_job_map(TableName, CurKey, Acc) ->
    [RawRow] = imem_meta:read(TableName, CurKey),
    NewAcc =
    case imem_dal_skvh:skvh_rec_to_map(RawRow) of
        #{ckey := [_, _, _, _, "cluster", "jobs", Job], cvalue := Val} ->
            case imem_json:decode(Val, [return_maps]) of
                #{<<"direction">> := <<"push">>, 
                  <<"srcArgs">> := #{<<"channel">> := SChannel}} ->
                        Pullers = find_pullers(SChannel),
                        Acc#{Job => Pullers};
                _ -> Acc#{Job => puller}
            end;
        _ -> Acc
    end,
    NextKey = imem_meta:next(TableName, CurKey),
    build_job_map(TableName, NextKey, NewAcc).

-spec path_to_values(map()) -> [[term()]].
path_to_values(Map) ->
    path_to_values(maps:keys(Map), Map, []).

-spec path_to_values(list(), map(), list()) -> [[term()]].
path_to_values([], _, _Acc) -> [];
path_to_values([Key | Keys], Map, Acc) ->
    NewAcc = [Key | Acc],
    S = case Map of
        #{Key := Value} when is_map(Value) ->
            path_to_values(maps:keys(Value), Value, NewAcc);
        _ -> [lists:reverse(NewAcc)]
    end,
    S ++ path_to_values(Keys, Map, Acc).

-spec calculate_focus_rate(list(), map(), map(), term(), term(), map()) -> {map(), map()}.
calculate_focus_rate(Protocol, Value, State, undefined, Time, Opts) when map_size(State) =:= 0 ->
    calculate_focus_rate(Protocol, Value, #{lastValue => #{}, lastRate => #{}}, undefined, Time, Opts);
calculate_focus_rate(Protocol, Value, #{lastValue := LValue} = State, undefined, _Time, _Opts) ->
    {Value, State#{lastValue => LValue#{Protocol => Value}}};
calculate_focus_rate(Protocol, Value, #{lastValue := LValue, lastRate := LRate} = State, LastTime, Time, Opts) ->
    ProtLValue = maps:get(Protocol, LValue, #{}),
    ProtLRate = maps:get(Protocol, LRate, #{}),
    Paths = path_to_values(Value),
    {RateValue, NewProtRate} = calculate_protocol_rate(Paths, Value, ProtLValue, ProtLRate, LastTime, Time, Opts),
    NewState = State#{lastRate => LRate#{Protocol => NewProtRate}, lastValue => LValue#{Protocol => Value}},
    {add_total_focus_rate(Paths, RateValue), NewState}.

-spec calculate_protocol_rate(list(), map(), map(), map(), term(), term(), map()) -> {map(), map()}.
calculate_protocol_rate(Paths, Value, ProtLValue, ProtLRate, T1, T2, _Opts) when map_size(ProtLRate) == 0 ->
    NewRate = 
        lists:foldl(fun(Path, AccRate) ->
            V1 = map_get(Path, ProtLValue, 0),
            V2 = map_get(Path, Value, 0),
            Rate = calculate_rate(V1, V2, T1, T2),
            map_put(Path, Rate, AccRate)
        end, ProtLRate, Paths),
    {Value, NewRate};
calculate_protocol_rate(Paths, Value, ProtLValue, ProtLRate, T1, T2, #{exp_factor := Factor}) ->
    lists:foldl(fun(Path, {V, AccRate}) ->
        V1 = map_get(Path, ProtLValue, 0),
        V2 = map_get(Path, V, 0),
        LRate = map_get(Path, AccRate, 0),
        Rate = apply_factor(calculate_rate(V1, V2, T1, T2), LRate, Factor),
        RatePath = focus_rate_path(Path),
        {map_put(RatePath, round(Rate), V), map_put(Path, Rate, AccRate)}
    end, {Value, ProtLRate}, Paths).

-spec focus_rate_path(list()) -> list().
focus_rate_path(Path) ->
    [R | PathRev] = lists:reverse(Path),
    RateKey = <<(to_binary(R))/binary, <<"_rate">>/binary >>,
    lists:reverse([RateKey | PathRev]).

-spec map_get(list(), term(), term()) -> term().
map_get([], Value, _Default) -> Value;
map_get([K | Rest], M, Default) when is_map(M) -> map_get(Rest, maps:get(K, M, Default), Default);
map_get(_, _, Default) -> Default.

-spec map_put(list(), term(), map()) -> map().
map_put([], Value, _Map) -> Value;
map_put([K | Rest], Value, Map) ->
    case Map of
        #{K := KMap} when is_map(KMap) -> Map#{K => map_put(Rest, Value, KMap)};
        _ -> Map#{K => map_put(Rest, Value, #{})}
    end.

-spec add_total_focus_rate(list(), map()) -> map().
add_total_focus_rate(Paths, #{esme := EsmeMap} = Value) ->
    TotalRate =
    lists:foldl(
        fun([esme | Path], Total) ->
            LastKey = lists:last(Path),
            case binary:match(LastKey, <<"resp">>) of
                nomatch ->
                    RatePath = focus_rate_path(Path),
                    Total + map_get(RatePath, EsmeMap, 0);
                _ -> Total
            end;
           (_, Total) -> Total
        end, 0, Paths),
    Value#{total_rate => TotalRate};
add_total_focus_rate(_Paths, Value) -> %% for focus of rest
    TotalRate =
    case Value of
        #{del := #{del := #{<<"collect_rate">> := C}},
          sub := #{sub := #{<<"submit_sm_rate">> := S}}} -> S + C;
        #{del := #{del := #{<<"collect_rate">> := C}}} -> C;
        #{sub := #{sub := #{<<"submit_sm_rate">> := S}}} -> S;
        _ -> 0
    end,
    Value#{total_rate => TotalRate}.

-spec to_binary(term()) -> binary().
to_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_binary(List) when is_list(List) -> list_to_binary(List);
to_binary(Bin) when is_binary(Bin) -> Bin.

-spec check_memory(list(), map()) -> list().
check_memory(Node, #{free_memory := FreeMemory, total_memory := TotalMemory}) ->
    MaxMemory = ?MAX_MEMORY_THRESHOLD(Node),
    UsedMemory = (100 - FreeMemory / TotalMemory * 100),
    if UsedMemory >= MaxMemory -> erlang:round(UsedMemory);
       true -> false
    end;
check_memory(_, _) -> false.

%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

apply_factor_test_() -> [
    ?_assertEqual(10, apply_factor(10, 0, 1)),
    ?_assertEqual(9.1, apply_factor(10, 1, 0.9)),
    ?_assertEqual(5.5, apply_factor(6, 5, 0.5))
].

sync_puller_error_test_() ->
    Ctx = #context{channel = <<"TEST">>, name = <<"TESTCOLLECTOR">>},
    Invalid = {metrics, {invalid_key, undefined}},
    Key = {dperl_metrics, {job_status, []}},
    EmptyInfos = {[], undefined},
    EmptyPrev = {undefined, undefined, #{}, undefined},
    StartSec = 10,
    StartMicros = 123456,
    StartTime = {StartSec, StartMicros},
    State =  #{pushers => #{}, time => StartTime},
    EmptyResult = {[], [], State},
    BasePrev = {undefined, undefined, State, undefined},
    RefreshTime = {StartSec + ?PUSHER_MAP_CACHE_TIMEOUT + 1, StartMicros},
    PushersMap = #{
        "dperlSPullMaster" => puller,
        "E2SM1" => puller,
        "MBSN1" => puller,
        "MBSN1Push" => [<<"MBSN1">>],
        "MBSN2Push" => [<<"MBSN1">>],
        "UAMPu" => [<<"UAMP">>]},
    NewState = #{pushers => PushersMap, time => RefreshTime},
    RefreshResult = {[], [], NewState},
    Prev = {undefined, undefined, NewState, undefined},
    RawPullerInfos = [
        {"E2SM1", #{state => #{lastAuditTime => [0,0],lastRefreshKey => -1},status => error}},
        {"MBSN1", #{state => #{lastAuditTime => [0,0],lastRefreshKey => -1},status => error}},
        {"dperlSPullMaster", #{state =>
            #{cleanup => #{
                count => 5,
                lastAttempt => [1494415670,385774],
                lastSuccess => [1494415671,47635]
            }},
            status => idle}
        }
    ],
    {PullerInfos, _} = dperl_status_pre:preprocess(Key, RawPullerInfos , undefined, node(), Ctx),
    IdentityResult = {PullerInfos, [], NewState},
    RawInfos = [
        {"MBSN1Push", #{state => #{lastAuditTime => [0,0], lastRefreshKey => -1}, status => idle}},
        {"UAMPu", #{state => #{lastAuditTime => [0,0], lastRefreshKey => -1}, status => idle}}
    ],
    {Infos, _} = dperl_status_pre:preprocess(Key, RawInfos, undefined, node(), Ctx),
    Data = [
        #{ckey => ["t","t@127","n","MBSN1","n","job_status","MBSN1"],
            cvalue => <<"{\"status\":\"error\"}">>, chash => <<"h">>},
        #{ckey => ["t","t@127","n","UAMP","n","job_status","UAMP"],
            cvalue => <<"{\"status\":\"idle\"}">>, chash => <<"h">>}
    ],
    ErrorInfos = [I#{cvalue => V#{status => error}} || #{cvalue := V} = I <- Infos],
    Result = {[hd(Infos) | tl(ErrorInfos)], [], NewState},
    {foreach,
        fun() ->
            meck:new(imem_meta),
            meck:new(imem_dal_skvh, [passthrough])
        end,
        fun(_) ->
            meck:unload(imem_meta),
            meck:unload(imem_dal_skvh)
        end,
        [
            ?_assertException(error, function_clause, sync_puller_error(Invalid, {}, Ctx, {}, #{})),
            {"Handles empty data",
            fun() ->
                meck:expect(imem_meta, transaction, 1, {atomic, #{}}),
                meck:expect(imem_meta, time, 0, StartTime),
                ?assertEqual(EmptyResult, sync_puller_error(Key, EmptyInfos, Ctx, EmptyPrev, #{})),
                ?assert(meck:validate(imem_meta))
            end},
            {"Do not refresh before expiration",
            fun() ->
                meck:expect(imem_meta, time, 0, StartTime),
                ?assertEqual(EmptyResult, sync_puller_error(Key, EmptyInfos, Ctx, BasePrev, #{})),
                ?assert(meck:validate(imem_meta))
            end},
            {"Refresh list of pushers after expiration",
            fun() ->
                meck:expect(imem_meta, time, 0, RefreshTime),
                meck:expect(imem_meta, transaction, 1, {atomic, PushersMap}),
                ?assertEqual(RefreshResult, sync_puller_error(Key, EmptyInfos, Ctx, BasePrev, #{})),
                ?assert(meck:validate(imem_meta))
            end},
            {"Should return same input if there are no pushers in the data",
            fun() ->
                meck:expect(imem_meta, time, 0, RefreshTime),
                ?assertEqual(IdentityResult, sync_puller_error(Key, {PullerInfos, undefined}, Ctx, Prev, #{})),
                ?assert(meck:validate(imem_meta))
            end},
            {"Should update the state of the pusher on error",
            fun() ->
                meck:expect(imem_meta, time, 0, RefreshTime),
                meck:expect(imem_dal_skvh, read_deep, 3, Data),
                ?assertEqual(Result, sync_puller_error(Key, {Infos, undefined}, Ctx, Prev, #{})),
                ?assert(meck:validate(imem_meta)),
                ?assert(meck:validate(imem_dal_skvh))
            end}
        ]
    }.

check_nodes_test_() ->
    Ctx = #context{channel = <<"TEST">>, name = <<"TESTCOLLECTOR">>},
    ErlangKey = {imem_metrics,erlang_nodes},
    DataKey = {imem_metrics, data_nodes},
    EmptyInfos = {[], undefined},
    EmptyPrev = {undefined, undefined, undefined, undefined},
    EmptyResult = {[], [], undefined},
    RawErlangInfos = #{nodes => ['dperl1@127.0.0.1'], required_nodes => ['dperl1@127.0.0.1',dperl_missing@127]},
    {ErlangInfos, _} = dperl_status_pre:preprocess(ErlangKey, RawErlangInfos, undefined, 'dperl1@127.0.0.1', Ctx),
    ErlangResultRows = [
        #{ckey => ["TEST","system_info","dperl1@127.0.0.1","error","erlang_nodes"],
          cvalue => #{error => <<"Missing nodes [dperl_missing@127]">>}}
    ],
    ErlangResult = {[], ErlangResultRows, undefined},
    RawDataInfos = #{data_nodes => [#{node => 'dperl1@127.0.0.1',schema => dperl}],
        required_nodes => ['dperl1@127.0.0.1',dperl_missing@127]},
    {DataInfos, _} = dperl_status_pre:preprocess(DataKey, RawDataInfos, undefined, 'dperl1@127.0.0.1', Ctx),
    DataResultRows = [
        #{ckey => ["TEST","system_info","dperl1@127.0.0.1","error","data_nodes"],
          cvalue => #{error => <<"Missing nodes [dperl_missing@127]">>}}
    ],
    DataResult = {[], DataResultRows, undefined},
    {foreach,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [
            {"Should handle empty erlang nodes",
            fun() ->
                ?assertEqual(EmptyResult, check_nodes(ErlangKey, EmptyInfos, Ctx, EmptyPrev, #{}))
            end},
            {"Should handle empty data nodes",
            fun() ->
                ?assertEqual(EmptyResult, check_nodes(DataKey, EmptyInfos, Ctx, EmptyPrev, #{}))
            end},
            {"Should report erlang node errors",
            fun() ->
                ?assertEqual(ErlangResult, check_nodes(ErlangKey, {ErlangInfos, undefined}, Ctx, EmptyPrev, #{}))
            end},
            {"Should report data node errors",
            fun() ->
                ?assertEqual(DataResult, check_nodes(DataKey, {DataInfos, undefined}, Ctx, EmptyPrev, #{}))
            end}
        ]
    }.

merge_node_errors_test_() ->
    Ctx = #context{channel = <<"TEST">>, name = <<"TESTCOLLECTOR">>},
    Key = {dperl_metrics, {agr, node_error, []}},
    EmptyInfos = {[#{cvalue => #{}}], undefined},
    EmptyPrev = {undefined, undefined, undefined, undefined},
    EmptyResult = {[], [], undefined},
    {foreach,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [
            {"Handles empty data",
            fun() ->
                ?assertEqual(EmptyResult, merge_node_errors(Key, EmptyInfos, Ctx, EmptyPrev, #{}))
            end}
        ]
    }.

check_memory_on_nodes_test_() ->
    Ctx = #context{channel = <<"TEST">>, name = <<"TESTCOLLECTOR">>},
    Key = {dperl_metrics, {agr, node_memory, []}},
    EmptyInfos = {[#{cvalue => #{}}], undefined},
    EmptyPrev = {undefined, undefined, undefined, undefined},
    EmptyResult = {[], [], undefined},
    {foreach,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [
            {"Handles empty data",
            fun() ->
                ?assertEqual(EmptyResult, check_memory_on_nodes(Key, EmptyInfos, Ctx, EmptyPrev, #{}))
            end}
        ]
    }.

% check_heartbeats_test_() ->
%     Ctx = #context{channel = <<"TEST">>, name = <<"TESTCOLLECTOR">>},
%     Key = {dperl_metrics, {agr, heartbeat, []}},
%     EmptyInfos = {[#{cvalue => #{}}], undefined},
%     EmptyPrev = {undefined, undefined, undefined, undefined},
%     EmptyResult = {[], [], undefined},
%     {foreach,
%         fun() ->
%             ok
%         end,
%         fun(_) ->
%             ok
%         end,
%         [
%             {"Handles empty data",
%             fun() ->
%                 ?assertEqual(EmptyResult, check_heartbeats(Key, EmptyInfos, Ctx, EmptyPrev, #{}))
%             end}
%         ]
%     }.

%{AggrRound, AggrPrecise, AggrState, LastTime} = LastAgrInfo
focus_requests_rate_test_() ->
    Ctx = #context{channel = <<"TEST">>, name = <<"TESTCOLLECTOR">>},
    Key = {mpro_metrics, {focus, "shortid", "1234"}},
    EmptyInfos = {[], undefined},
    EmptyPrev = {undefined, undefined, undefined, undefined},
    EmptyResult = {[], [], undefined},
    Focus = #{smpp => #{esme => #{downstream => #{<<"call_input_01">> => 8333}}}},
    FirstResultData = [#{
        ckey => ["TEST","focus","dperl1@127.0.0.1",["shortid","1234"],"smpp"],
        cvalue => #{esme => #{downstream => #{<<"call_input_01">> => 8333}}}
    }],
    FirstNewState = #{
        lastValue => #{"smpp" => #{esme => #{downstream => #{<<"call_input_01">> => 8333}}}},
        lastRate => #{}
    },
    FirstPrev = {undefined, undefined, #{}, undefined},
    FirstResult = {FirstResultData, [], FirstNewState},
    {FocusInfos, _} = dperl_status_pre:preprocess(Key, Focus, {}, 'dperl1@127.0.0.1', Ctx),
    FocusResultData = [#{
        ckey => ["TEST","focus","dperl1@127.0.0.1",["shortid","1234"],"smpp"],
        cvalue => #{esme => #{downstream => #{<<"call_input_01">> => 8333,
            <<"call_input_01_rate">> => 20}}, total_rate => 20}
    }],
    State = #{
        lastValue => #{"smpp" => #{esme => #{downstream => #{<<"call_input_01">> => 8293}}}},
        lastRate => #{"smpp" => #{esme => #{downstream => #{<<"call_input_01">> => 0}}}}
    },
    StartTime = {0, 0},
    UpdatedTime1 = {2, 0}, % Test after 2 seconds.
    Prev = {undefined, undefined, State, StartTime},
    NewState = #{
        lastValue => #{"smpp" => #{esme => #{downstream => #{<<"call_input_01">> => 8333}}}},
        lastRate => #{"smpp" => #{esme => #{downstream => #{<<"call_input_01">> => 20.0}}}}
    },
    FocusResult = {FocusResultData, [], NewState},
    {foreach,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [
            {"Handles empty data",
            fun() ->
                ?assertEqual(EmptyResult, focus_requests_rate(Key, EmptyInfos, Ctx, EmptyPrev, #{}))
            end},
            {"Sets the state on first call",
            fun() ->
                ?assertEqual(FirstResult, focus_requests_rate(Key, {FocusInfos, StartTime}, Ctx, FirstPrev, #{}))
            end},
            {"Correctly calculate rates",
            fun() ->
                ?assertEqual(FocusResult, focus_requests_rate(Key, {FocusInfos, UpdatedTime1}, Ctx, Prev, #{}))
            end}
        ]
    }.

path_to_values_test_() ->
    Input = #{
        a => #{b => 5, d => #{ e => 6, f => 7 }},
        c => 6
    },
    Result = [[a, b], [a, d, e], [a, d, f], [c]],
    ?_assertEqual(Result, path_to_values(Input)).

map_put_test_() ->
    {foreach,
        fun() ->
            ok
        end,
        fun(_) ->
            ok
        end,
        [
            {"Set value on empty map",
            fun() ->
                Input = #{},
                Path = [a, b, c],
                Value = 3,
                Result = #{a => #{b => #{c => 3}}},
                ?assertEqual(Result, map_put(Path, Value, Input))
            end},
            {"Replace existing value",
            fun() ->
                Input = #{a => #{c => 3, d => 4}, b => 4},
                Path1 = [a, c],
                Path2 = [a, d],
                Value = 5,
                Result1 = #{a => #{c => 5, d => 4}, b => 4},
                Result2 = #{a => #{c => 3, d => 5}, b => 4},
                ?assertEqual(Result1, map_put(Path1, Value, Input)),
                ?assertEqual(Result2, map_put(Path2, Value, Input))
            end},
            {"Replace existing branch by value",
            fun() ->
                Input = #{a => #{c => #{e => 1}, d => 4}, b => 3},
                Path = [a, c],
                Value = 5,
                Result = #{a => #{c => 5, d => 4}, b => 3},
                ?assertEqual(Result, map_put(Path, Value, Input))
            end},
            {"Replace existing value by new branch",
            fun() ->
                Input = #{a => #{c => 5, d => 4}, b => 3},
                Path = [a, c, e],
                Value = 1,
                Result = #{a => #{c => #{e => 1}, d => 4}, b => 3},
                ?assertEqual(Result, map_put(Path, Value, Input))
            end}
        ]
    }.


-endif.
