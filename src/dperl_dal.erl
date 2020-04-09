-module(dperl_dal).

-include("dperl.hrl").

-export([select/2, subscribe/1, unsubscribe/1, write/2, check_table/5,
         sql_jp_bind/1, sql_bind_jp_values/2, read_channel/2,
         io_to_oci_datetime/1, create_check_channel/1, write_channel/3,
         read_check_write/4, read_audit_keys/3, read_audit/3, get_enabled/1,
         update_job_dyn/2, update_job_dyn/3, job_error_close/1, to_binary/1,
         count_sibling_jobs/2, create_check_channel/2, write_protected/6,
         get_last_state/1, get_last_audit_time/1, connect_imem_link/4,
         remove_from_channel/2, remove_deep/2, data_nodes/0, job_state/1,
         job_error/3, job_error/4, job_error_close/0, update_service_dyn/4,
         update_service_dyn/3, all_keys/1, read_gt/3, time_str/1, ts_str/1,
         safe_json_map/1, read_gt_lt/4, normalize_map/1, read_keys_gt/3,
         write_if_different/3, maps_diff/2, read_keys_gt_lt/4, oci_opts/2,
         oci_fetch_rows/2, key_from_json/1, disable/1, set_running/2,
         read_siblings/2, read_channel_raw/2, worker_error/4, sort_links/1,
         get_pool_name/1, remote_dal/3, get_pool_name/2, run_oci_stmt/3,
         activity_logger/3, create_check_index/2, to_atom/1, report_status/7,
         key_to_json/1, key_to_json_enc/1]).

check_table(Table, ColumnNames, ColumnTypes, DefaultRecord, Opts) ->
    case catch imem_meta:create_check_table(
                 Table, {ColumnNames, ColumnTypes, DefaultRecord},
                 Opts, system) of
        {'EXIT', {'ClientError', _} = Reason} ->
            ?Error("create_check table ~p, ~p", [Table, Reason]);
        Else ->
            ?Info("create_check table ~p... ~p", [Table, Else])
    end.

-spec create_check_channel(binary() | list()) ->  ok | no_return().
create_check_channel(Channel) when is_list(Channel) ->
    create_check_channel(list_to_binary(Channel));
create_check_channel(Channel) when is_binary(Channel) ->
    imem_dal_skvh:create_check_channel(Channel).

-spec create_check_channel(binary() | list(), list()) -> ok | no_return().
create_check_channel(Channel, Opts) when is_list(Channel) ->
    create_check_channel(list_to_binary(Channel), Opts);
create_check_channel(Channel, Opts) when is_binary(Channel) ->
    imem_dal_skvh:create_check_channel(Channel, Opts).

-spec create_check_index(binary(), list()) ->
    ok | {'ClientError', term()} | {'SystemException', term()}.
create_check_index(Channel, IndexDefinition) ->
    TableName = to_atom(imem_dal_skvh:table_name(Channel)),
    case imem_meta:init_create_index(TableName, IndexDefinition) of
        ok ->                                           ok;
        {'ClientError',{"Index already exists", _}} ->  ok;
        Other ->                                        Other
    end.

-spec write_channel(binary(), any(), any()) -> ok | {error, any()}.
write_channel(Channel, Key, Val) when is_map(Val); byte_size(Val) > 0 ->
   case catch imem_dal_skvh:write(system, Channel, Key, Val) of
       Res when is_map(Res) -> ok;
       {'EXIT', Error} -> {error, Error};
       {error, Error} -> {error, Error};
       Other -> {error, Other}
   end.

write_if_different(Channel, Key, Val) ->
    case imem_dal_skvh:read(system, Channel, [Key]) of
        [#{cvalue := Val}] -> no_op;
        _ -> write_channel(Channel, Key, Val)
    end.

read_channel(Channel, Key) when is_list(Channel) ->
    read_channel(list_to_binary(Channel), Key);
read_channel(Channel, Key) when is_binary(Channel) ->
   case imem_dal_skvh:read(system, Channel, [Key]) of
       [#{cvalue := Val}] when is_binary(Val) -> safe_json_map(Val);
       [#{cvalue := Val}] -> Val;
       _ -> ?NOT_FOUND
   end.

read_channel_raw(Channel, Key) when is_list(Channel) ->
    read_channel_raw(list_to_binary(Channel), Key);
read_channel_raw(Channel, Key) when is_binary(Channel) ->
   imem_dal_skvh:read(system, Channel, Key).

read_siblings(Channel, Key) when is_list(Channel) ->
    read_siblings(list_to_binary(Channel), Key);
read_siblings(Channel, Key) when is_binary(Channel) ->
   imem_dal_skvh:read_siblings(system, Channel, Key).

read_check_write(Channel, Key, Val, Type) ->
    case read_channel(Channel, Key) of
        ?NOT_FOUND -> 
            if
                (Type == map andalso is_map(Val)) orelse (Type == bin andalso is_binary(Val)) ->
                    write_channel(Channel, Key, Val);
                Type == bin andalso is_map(Val) ->
                    write_channel(Channel, Key, imem_json:encode(Val));
                Type == map andalso is_binary(Val) ->
                    write_channel(Channel, Key, imem_json:decode(Val, [return_maps]))
            end;
        _ -> no_op
    end.

read_audit_keys(Channel, TimeStamp, Limit) when is_list(Channel) ->
    read_audit_keys(list_to_binary(Channel), TimeStamp, Limit);
read_audit_keys(Channel, TimeStamp, Limit) when is_binary(Channel) ->
    case imem_dal_skvh:audit_readGT(system, Channel, TimeStamp, Limit) of
        Audits when length(Audits) > 0 ->
            [#{time := StartTime} | _] = Audits,
            #{time := EndTime} = lists:last(Audits),
            {StartTime, EndTime, 
              [K || #{ckey := K} <- Audits, K /= undefined]};
        _ -> {TimeStamp, TimeStamp, []}
    end.

read_audit(Channel, TimeStamp, Limit) when is_list(Channel) ->
    read_audit(list_to_binary(Channel), TimeStamp, Limit);
read_audit(Channel, TimeStamp, Limit) when is_binary(Channel) ->
    case imem_dal_skvh:audit_readGT(system, Channel, TimeStamp, Limit) of
        Audits when length(Audits) > 0 ->
            [#{time := StartTime} | _] = Audits,
            #{time := EndTime} = lists:last(Audits),
            {StartTime, EndTime,
             [#{key => K,
                oval => if is_binary(Ov) -> safe_json_map(Ov);
                           true -> Ov end,
                nval => if is_binary(Nv) -> safe_json_map(Nv);
                           true -> Nv end}
              || #{ckey := K, ovalue := Ov, nvalue := Nv} <- Audits, K /= undefined]
            };
        _ -> {TimeStamp, TimeStamp, []}
    end.

remove_from_channel(Channel, Key) when is_list(Channel) ->
    remove_from_channel(list_to_binary(Channel), Key);
remove_from_channel(Channel, Key) ->
    case imem_dal_skvh:read(system, Channel, [Key]) of
        [] -> no_op;
        Row -> imem_dal_skvh:remove(system, Channel, Row)
    end.

remove_deep(Channel, BaseKey) ->
    Rows = imem_dal_skvh:read_deep(system, Channel, [BaseKey]),
    imem_dal_skvh:remove(system, Channel, Rows).

read_gt(Channel, StartKey, BulkSize) when is_binary(Channel) ->
    lists:map(
      fun(#{ckey := Key, cvalue := Val}) -> {Key, safe_json_map(Val)} end,
      imem_dal_skvh:readGT(system, Channel, StartKey, BulkSize)).

read_gt_lt(Channel, StartKey, EndKey, BulkSize) ->
    KeyVals = case imem_dal_skvh:readGELTMap(system, Channel, 
                          StartKey, EndKey, BulkSize + 1) of
        [#{ckey := StartKey} | Rest] -> Rest;
        List -> lists:sublist(List, BulkSize)
    end,
    lists:map(fun(#{ckey := Key, cvalue := Val}) -> {Key, safe_json_map(Val)} end,
      KeyVals).

read_keys_gt(Channel, StartKey, BulkSize) ->
    read_keys_gt_lt(Channel, StartKey, <<255>>, BulkSize).

read_keys_gt_lt(Channel, StartKey, EndKey, BulkSize) ->
    case imem_dal_skvh:readGELTKeys(system, Channel, StartKey, EndKey, BulkSize + 1) of
        [StartKey | T] -> T;
        Keys -> lists:sublist(Keys, BulkSize)
    end.

subscribe(Event)            -> imem_meta:subscribe(Event).
unsubscribe(Event)          -> imem_meta:unsubscribe(Event).
write(Table, Record)        -> imem_meta:write(Table, Record).
select(Table, MatchSpec)    -> imem_meta:select(Table, MatchSpec).
sql_jp_bind(Sql)            -> imem_meta:sql_jp_bind(Sql).

sql_bind_jp_values(BindParamsMeta, JpPathBinds) ->
    imem_meta:sql_bind_jp_values(BindParamsMeta, JpPathBinds).

io_to_oci_datetime(Dt) ->
    oci_util:to_dts(imem_datatype:io_to_datetime(Dt)).

get_enabled(job) ->
    {Jobs, _} = imem_meta:select(dperlJob,
                                 [{#dperlJob{enabled=true, _='_'},[],['$_']}]),
    Jobs;
get_enabled(service) ->
    {Services, _} = imem_meta:select(dperlService,
                                     [{#dperlService{enabled=true, _='_'},[],
                                       ['$_']}]),
    Services.

disable(#dperlJob{name = Name}) ->
    update_job_dyn(Name, error),
    imem_meta:transaction(fun() ->
        case imem_meta:read(dperlJob, Name) of
            [Job] ->
                ok = imem_meta:write(dperlJob, Job#dperlJob{enabled = false, running = false});
            _ -> no_op
        end
    end);
disable(#dperlService{name = Name}) ->
    imem_meta:transaction(fun() ->
        case imem_meta:read(dperlService, Name) of
            [Service] ->
                ok = imem_meta:write(dperlService,
                                     Service#dperlService{enabled = false, running = false});
            _ -> no_op
        end
    end).

set_running(#dperlJob{name = Name}, Running) ->
    imem_meta:transaction(fun() ->
        case imem_meta:read(dperlJob, Name) of
            [#dperlJob{running = _} = Job] ->
                ok = imem_meta:write(dperlJob, Job#dperlJob{running = Running});
            _ -> no_op
        end
    end);
set_running(#dperlService{name = Name}, Running) ->
    imem_meta:transaction(fun() ->
        case imem_meta:read(dperlService, Name) of
            [#dperlService{running = _} = Service] ->
                ok = imem_meta:write(dperlService, Service#dperlService{running = Running});
            _ -> no_op
        end
    end).


update_service_dyn(ServiceName, State, ActiveThreshold, OverloadThreshold)
  when is_binary(ServiceName), is_map(State), is_integer(ActiveThreshold),
       is_integer(OverloadThreshold) ->
    Status =
    case maps:get(req, State, 0) of
        Req when Req < ActiveThreshold -> idle;
        Req when Req >= ActiveThreshold andalso
                Req < OverloadThreshold -> active;
        Req when Req >= OverloadThreshold -> overload
    end,
    imem_meta:transaction(fun() ->
        case imem_meta:read(?SERVICEDYN_TABLE, ServiceName) of
            [] ->
                update_service_dyn(ServiceName, State, Status);
            [#dperlServiceDyn{state = OldState}] ->
                NewState = maps:merge(OldState, State),
                if OldState /= NewState ->
                    update_service_dyn(ServiceName, NewState, Status);
                true -> ok
                end
        end
    end).

update_service_dyn(ServiceName, State, Status) when is_binary(ServiceName) andalso
  is_map(State) andalso (Status == stopped orelse Status == idle orelse
  Status == active orelse Status == overload) ->
        imem_meta:write(
            ?SERVICEDYN_TABLE,
            #dperlServiceDyn{name = ServiceName, state = State,
                            status = Status, statusTime = imem_meta:time_uid()}).

update_job_dyn(JobName, State) when is_binary(JobName) andalso is_map(State) ->
    imem_meta:transaction(fun() ->
    case imem_meta:read(?JOBDYN_TABLE, JobName) of
        [] ->
            ok = imem_meta:write(
                   ?JOBDYN_TABLE,
                   #dperlNodeJobDyn{name = JobName, state = State,
                                   status = synced,
                                   statusTime = imem_meta:time_uid()});
        [#dperlNodeJobDyn{state=OldState} = J] ->
            NewState = maps:merge(OldState, State),
            if OldState /= NewState ->
                   ok = imem_meta:write(
                          ?JOBDYN_TABLE,
                          J#dperlNodeJobDyn{state = NewState,
                                           statusTime = imem_meta:time_uid()});
               true -> ok
            end
    end end);
update_job_dyn(JobName, Status)
  when is_binary(JobName) andalso 
       (Status == synced orelse
        Status == cleaning orelse Status == cleaned orelse
        Status == refreshing orelse Status == refreshed orelse
        Status == idle orelse Status == error orelse Status == stopped) ->
    case imem_meta:read(?JOBDYN_TABLE, JobName) of
        [] ->
            ok = imem_meta:write(
                   ?JOBDYN_TABLE,
                   #dperlNodeJobDyn{name = JobName, state = #{},
                                   status = Status,
                                   statusTime = imem_meta:time_uid()});
        [#dperlNodeJobDyn{status = OldStatus} = J] ->
            if OldStatus /= Status orelse
               (OldStatus == Status andalso Status == error) ->
                   ok = imem_meta:write(
                          ?JOBDYN_TABLE,
                          J#dperlNodeJobDyn{status = Status,
                                           statusTime = imem_meta:time_uid()});
               true -> ok
            end
    end.

update_job_dyn(JobName, State, Status)
  when is_binary(JobName) andalso is_map(State) andalso
       (Status == synced orelse Status == undefined orelse
        Status == cleaning orelse Status == cleaned orelse
        Status == refreshing orelse Status == refreshed orelse
        Status == idle orelse Status == error orelse Status == stopped) ->
    case imem_meta:read(?JOBDYN_TABLE, JobName) of
        [] ->
            ok = imem_meta:write(
                   ?JOBDYN_TABLE,
                   #dperlNodeJobDyn{name = JobName, state = State,
                                   status = Status,
                                   statusTime = imem_meta:time_uid()});
        [#dperlNodeJobDyn{state=OldState, status=OldStatus, statusTime = OTime} = J] ->
            NewState = maps:merge(OldState,State),
            TimeDiff = imem_datatype:sec_diff(OTime),
            if NewState /= OldState orelse (OldStatus == error andalso Status /= idle)
               orelse (OldStatus /= error andalso Status /= OldStatus) 
               orelse (OldStatus == Status andalso Status == idle)
               orelse TimeDiff > 1  ->
                   ok = imem_meta:write(
                          ?JOBDYN_TABLE,
                          J#dperlNodeJobDyn{state = NewState, status = Status,
                                           statusTime = imem_meta:time_uid()});
               true -> ok
            end
    end.

get_last_state(JobName) when is_binary(JobName) ->
    case imem_meta:read(?JOBDYN_TABLE, {JobName, node()}) of
        [#dperlNodeJobDyn{state = State}] ->
            State;
        _ -> #{}
    end.

get_last_audit_time(JobName) ->
    case get_last_state(JobName) of
        #{lastAuditTime := LastAuditTime} ->
            LastAuditTime;
        _ -> {0,0,0}
    end.

count_sibling_jobs(Module, Channel) ->
    {Args, true} = imem_meta:select(dperlJob, [{#dperlJob{module=Module, 
        srcArgs='$1', dstArgs = '$2', _= '_'}, [], [{{'$1', '$2'}}]}]),
    length(lists:filter(fun({#{channel := Chn}, _}) when Chn == Channel -> true;
                           ({_, #{channel := Chn}}) when Chn == Channel -> true;
                           (_) -> false
                        end, Args)).

connect_imem_link(ActiveLink, Links, User, Password) when is_list(Password) ->
    connect_imem_link(ActiveLink, Links, User, list_to_binary(Password));
connect_imem_link(ActiveLink, Links, User, Password) when is_binary(Password) ->
    Link = lists:nth(ActiveLink, Links),
    case connect_imem(User, erlang:md5(Password), Link) of
        {ok, Session, Pid} ->
            {ok, Session, Pid};
        {error, Error} ->
            NewActiveLink = if
                length(Links) > ActiveLink ->
                    ActiveLink + 1;
                true  ->
                    1
            end,
            {Error, NewActiveLink}
    end.

connect_imem(User, Password, #{ip := Ip, port := Port, secure := Secure,
                               schema := Schema}) ->
    case erlimem:open({tcp, Ip, Port, if Secure -> [ssl]; true -> [] end},
                      Schema) of
        {ok, {erlimem_session, Pid} = Session} ->
            case catch Session:auth(?MODULE,<<"TODO">>,{pwdmd5,{User,Password}}) of
                OK when OK == ok; element(1, OK) == ok ->
                    case catch Session:run_cmd(login,[]) of
                        {error, _} ->
                            {error, eaccess};
                        _ ->
                            {ok, Session, Pid}
                    end;
                {'EXIT', _} ->
                    {error, eaccess};
                _ ->
                    {error, eaccess}
            end;
        Error -> 
            {error, Error}
    end.

data_nodes() -> data_nodes(imem_meta:data_nodes(), []).
data_nodes([], Acc) -> Acc;
data_nodes([{_,Node}|DataNodes], Acc) -> data_nodes(DataNodes, [Node|Acc]).

job_state(Name) -> imem_meta:read(?JOBDYN_TABLE, Name).

report_status(Module, StatusTable, {Channel, ShortId}, JobName, Status) when is_binary(Channel) ->
    report_status(Module, StatusTable, {binary_to_list(Channel), ShortId}, JobName, Status);
report_status(Module, StatusTable, {Channel, ShortId}, JobName, Status) when is_binary(ShortId) ->
    report_status(Module, StatusTable, {Channel, binary_to_list(ShortId)}, JobName, Status);
report_status(Module, StatusTable, {Channel, ShortId}, JobName, Status) when is_binary(JobName) ->
    report_status(Module, StatusTable, {Channel, ShortId}, binary_to_list(JobName), Status);
report_status(Module, StatusTable, {Channel, ShortId}, JobName, Status) when is_integer(ShortId) ->
    report_status(Module, StatusTable, {Channel, integer_to_list(ShortId)}, JobName, Status);
report_status(Module, StatusTable, {Channel, ShortId}, JobName, Status) when is_list(StatusTable) ->
    report_status(Module, list_to_binary(StatusTable), {Channel, ShortId}, JobName, Status);
report_status(Module, StatusTable, {Channel, ShortId}, JobName, Status) ->
    try
        write_channel(StatusTable,
          [atom_to_list(Module), Channel, ShortId, JobName], Status)
    catch
        C:E ->
            ?Error("~p,~p to ~p : ~p",
                   [Channel, ShortId, StatusTable, {C,E}], ?ST)
    end.

%% pusher report_status
report_status(StatusKey, AuditTimeOrKey, no_op, StatusTable, Channel, JobName, Module) ->
    report_status(StatusKey, AuditTimeOrKey, #{}, StatusTable, Channel, JobName, Module);
report_status(StatusKey, AuditTimeOrKey, {error, Error}, StatusTable, Channel, JobName, Module) ->
    report_status(StatusKey, AuditTimeOrKey, error_obj(Error), StatusTable, Channel, JobName, Module);
report_status(StatusKey, AuditTimeOrKey, Error, StatusTable, Channel, JobName, Module) when not is_map(Error) ->
    report_status(StatusKey, AuditTimeOrKey, error_obj(Error), StatusTable, Channel, JobName, Module);
report_status(StatusKey, AuditTimeOrKey, Status, StatusTable, Channel, JobName, Module) when is_map(Status) ->
    AuditTime =
    case AuditTimeOrKey of
        {at, ATime} ->
            ATime;
        Key ->
            get_key_audit_time(Channel, Key)
    end,
    report_status(Module, StatusTable, {Channel, StatusKey}, JobName, status_obj(AuditTime, Status)).

worker_error(service, _Type, _Operation, _Msg) -> no_op;
worker_error(job, Type, Operation, Msg) ->
  job_error(Type, Operation, Msg).

job_error(s, Operation, Msg) -> job_error(<<"sync">>, Operation, Msg);
job_error(c, Operation, Msg) -> job_error(<<"cleanup">>, Operation, Msg);
job_error(r, Operation, Msg) -> job_error(<<"refresh">>, Operation, Msg);
job_error(i, Operation, Msg) -> job_error(<<"idle">>, Operation, Msg);
job_error(f, Operation, Msg) -> job_error(<<"finish">>, Operation, Msg);
job_error(undefined, Operation, Msg) -> job_error(<<"undefined">>, Operation, Msg);
job_error(Type, Operation, Msg) ->
    job_error(undefined, Type, Operation, Msg).

job_error(Key, Type, Operation, Msg) when is_binary(Type), is_binary(Operation)->
    JobName = binary_to_list(get(name)),
    ErKey = if Key == undefined -> [JobName, ?NODE];
               true -> [JobName, ?NODE, Key]
    end,
    case read_channel(?JOB_ERROR, ErKey) of
        ?NOT_FOUND ->
            Value = #{<<"TYPE">> => Type, 
                      <<"OPERATION">> => Operation,
                      <<"TIMESTAMP">> => imem_datatype:timestamp_to_io(imem_meta:time()),
                      <<"MESSAGE">> => 
                        if 
                            is_binary(Msg) -> 
                                case io_lib:printable_list(binary_to_list(Msg)) of
                                    true -> Msg;
                                    false -> list_to_binary(io_lib:format("~p", [Msg]))
                                end;
                            is_list(Msg) -> 
                                case io_lib:printable_list(Msg) of
                                    true -> list_to_binary(Msg);
                                    false -> list_to_binary(lists:flatten(io_lib:format("~p", [Msg])))
                                end;
                            true ->
                                list_to_binary(lists:flatten(io_lib:format("~p", [Msg])))
                        end},
            write_channel(?JOB_ERROR, ErKey, imem_json:encode(Value));
        _ -> no_op
    end.

job_error_close() -> job_error_close(undefined).

job_error_close(undefined) ->
    remove_from_channel(?JOB_ERROR, [binary_to_list(get(name)), ?NODE]);
job_error_close(Key) ->
    remove_from_channel(?JOB_ERROR, [binary_to_list(get(name)), ?NODE, Key]).

all_keys(Channel) when is_list(Channel) ->
    all_keys(list_to_existing_atom(Channel));
all_keys(Channel) when is_binary(Channel) ->
    all_keys(binary_to_existing_atom(Channel, utf8));
all_keys(Channel) when is_atom(Channel) ->
    lists:map(
      fun(K) -> sext:decode(K) end,
      imem_meta:return_atomic(
        imem_meta:transaction(fun mnesia:all_keys/1, [Channel]))
     ).

time_str(Time) ->
    case Time div 1000 of
        TimeMs when TimeMs > 1000 ->
            integer_to_list(TimeMs div 1000)++"s";
        TimeMs ->
            integer_to_list(TimeMs)++"ms"
    end.

-spec ts_str(ddTimestamp() | ddTimeUID()) -> binary().
ts_str(TimeStamp) -> imem_datatype:timestamp_to_io(TimeStamp).

-spec safe_json_map(binary()) -> map().
safe_json_map(Value) when is_binary(Value) ->
    case catch imem_json:decode(Value, [return_maps]) of
        {'EXIT', _} ->
            imem_json:decode(
              unicode:characters_to_binary(Value, latin1, utf8),
              [return_maps]);
        null -> null;
        DecodedValue when is_map(DecodedValue) -> DecodedValue;
        DecodedValue when is_list(DecodedValue) -> DecodedValue
    end.

-spec normalize_map(map()) -> map().
normalize_map(Map) when is_map(Map)->
    maps:map(
        fun(_, V) when is_list(V) -> lists:sort(V);
           (_, V) when is_map(V) -> normalize_map(V);
           (_, V) -> V
        end, Map);
normalize_map(M) -> M.

write_protected(Channel, Key, Prov, IsSamePlatform, Config, Type) ->
    case re:run(Config, <<"(?i)dperl">>) of
        nomatch ->
            %% protectedConfig is target
            case read_channel(Channel, Key) of
                ?NOT_FOUND -> write_channel(Channel, Key, <<"null">>);
                #{<<"AuditTime">> := _} -> write_channel(Channel, Key, <<"null">>);
                _ -> no_op
            end;
        _ -> if IsSamePlatform == true -> read_check_write(Channel, Key, Prov, Type);
                true -> no_op
             end
    end.

maps_diff(Src, Dst) when is_map(Src), is_map(Dst) ->
    DstKeys = maps:keys(Dst),
    SrcKeys = maps:keys(Src),
    if SrcKeys == DstKeys ->
            lists:foldl(
                fun(K, M) ->
                    L = maps:get(K, Src, '$missing'),
                    R = maps:get(K, Dst, '$missing'),
                    if L /= R ->
                           M#{K => #{local => L, remote => R}};
                       true -> M
                    end
                end, #{}, DstKeys);
       true -> 
            #{localMissinKeys => SrcKeys -- DstKeys, remoteMissingKeys => DstKeys -- SrcKeys}
    end.

key_from_json([]) -> [];
key_from_json([B | Key]) when is_binary(B) ->
    [unicode:characters_to_list(B) | key_from_json(Key)];
key_from_json([T | Key]) -> [T | key_from_json(Key)];
key_from_json(KeyJson) when is_binary(KeyJson) ->
    key_from_json(imem_json:decode(KeyJson)).

-spec key_to_json(integer() | [list() | binary()]) -> [binary()] | binary().
key_to_json(Key) when is_integer(Key) -> integer_to_binary(Key);
key_to_json([]) -> [];
key_to_json([L | Key]) when is_list(L) ->
    [unicode:characters_to_binary(L) | key_to_json(Key)];
key_to_json([K | Key]) ->
    [K | key_to_json(Key)].

-spec key_to_json_enc(integer() | [list() | binary()]) -> binary().
key_to_json_enc(Key) when is_integer(Key) -> integer_to_binary(Key);
key_to_json_enc(Key) when is_list(Key) ->
    imem_json:encode(key_to_json(Key)).

oci_opts(LogFun, Opts) ->
    PState = {pstate, #{jname => get(jname)}},
    [{logfun, LogFun} |
     case proplists:get_value(ociOpts, Opts, '$none') of
         '$none' ->
             [{ociOpts, [{logging, true}, PState]} | Opts];
         OciOpts ->
             case proplists:get_value(logging, OciOpts, '$none') of
                 '$none' ->
                     [{ociOpts, [{logging, true}, PState | OciOpts]} | Opts];
                 _ ->
                     [{ociOpts, [PState | OciOpts]} | Opts]
             end
     end].

oci_fetch_rows(Stmt, Limit) ->
    case Stmt:fetch_rows(Limit) of
        {{rows, []}, _} -> [];
        {{rows, Rows}, true} -> Rows;
        {{rows, Rows}, false} when length(Rows) >= Limit -> Rows;
        {{rows, Rows}, false} when length(Rows) < Limit -> 
            Rows ++ oci_fetch_rows(Stmt, Limit - length(Rows))
    end.

sort_links(Links) ->
    lists:sort(fun(#{prio := A}, #{prio := B}) -> A < B end,
               [L || #{use := true} = L <- Links]).

-spec get_pool_name(tuple()) -> atom().
get_pool_name(#dperlJob{name = Name, dstArgs = DstArgs}) ->
    to_atom(maps:get(poolName, DstArgs, Name)).

-spec get_pool_name(map(), term()) -> atom().
get_pool_name(#{poolName := PoolName}, _Default) -> to_atom(PoolName);
get_pool_name(_, Default) -> to_atom(Default).

-spec to_atom(binary() | list() | atom()) -> atom().
to_atom(Bin) when is_binary(Bin) -> binary_to_atom(Bin, utf8);
to_atom(List) when is_list(List) -> list_to_atom(List);
to_atom(Atom) when is_atom(Atom) -> Atom.

-spec to_binary(binary() | list() | atom()) -> binary().
to_binary(List) when is_list(List) ->
    list_to_binary(List);
to_binary(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8);
to_binary(Bin) when is_binary(Bin) ->
    Bin.

-spec remote_dal(tuple(), atom(), list()) -> term(). 
remote_dal(Session, Fun, Args) ->
    case catch Session:run_cmd(dal_exec, [imem_dal_skvh, Fun, Args]) of
        {ok, Result} -> {ok, Result};
        {'EXIT', Error} ->
            ?JError("imem_dal_skvh:~p(~p). Error : ~p", [Fun, Args, Error]),
            throw(Error);
        {error, {{_, Error}, _}} ->
            ?JError("imem_dal_skvh:~p(~p). Error : ~p", [Fun, Args, Error]),
            throw(element(1, Error));
        Error when is_tuple(Error) -> 
            ?JError("imem_dal_skvh:~p(~p). Error : ~p", [Fun, Args, Error]),
            throw(Error);
        Result -> Result
    end.

-spec run_oci_stmt(tuple(), tuple(), integer()) -> list() | tuple().
run_oci_stmt(Stmt, Params, Limit) ->
    case catch Stmt:exec_stmt([Params]) of
        {cols, _} ->
            dperl_dal:oci_fetch_rows(Stmt, Limit);
        Error ->
            ?JError("Error processing result: ~p", [Error]),
            {error, Error}
    end.

-spec activity_logger(map(), list(), list()) -> ok.
activity_logger(StatusCtx, Name, Extra) ->
    case global:whereis_name(Name) of
        undefined ->
            SInterval = ?STATUS_INTERVAL,
            SFile = ?STATUS_FILE,
            {ok, HostName} = inet:gethostname(),
            Pid = spawn_link(
                fun() ->
                    log_activity(StatusCtx, HostName, SFile, Extra, SInterval)
                end),
            global:register_name(Name, Pid),
            ?JInfo("activity logger started for ~p ~p", [Name, Pid]);
        Pid ->
            ?JDebug("activity logger already running ~p", [Pid])
    end.

%%------------------------------------------------------------------------------
%% private
%%------------------------------------------------------------------------------

-spec error_obj(term()) -> #{error => binary()}.
error_obj(Error) when is_binary(Error) ->
    #{error => Error};
error_obj(Error) when is_list(Error) ->
    error_obj(list_to_binary(Error));
error_obj(Error) ->
    error_obj(imem_datatype:term_to_io(Error)).

-spec status_obj(list() | tuple() | undefined, map()) -> map().
status_obj(AuditTime, Status) when is_tuple(AuditTime) ->
    status_obj(tuple_to_list(AuditTime), Status);
status_obj(AuditTime, Status) when is_map(Status) ->
    Status#{auditTime => AuditTime}.

-spec get_key_audit_time(binary(), term()) -> ddTimestamp() | undefined.
get_key_audit_time(Channel, Key) ->
    case catch read_channel(Channel, Key) of
        #{<<"AuditTime">> := AuditTime} ->
            AuditTime;
        _ ->
            undefined
    end.

log_activity(Ctx, HostName, SFile, Extra, SInterval) ->
    erlang:send_after(SInterval, self(), write_status),
    receive
        write_status ->
            <<D:2/binary,".", M:2/binary, ".", Y:4/binary, " ", H:2/binary,
              ":", Mi:2/binary, ":",  S:2/binary, _/binary>> = 
                imem_datatype:timestamp_to_io(imem_meta:time()),
            % timestamp format 2019-01-10 17:43:01
            Time = [Y, "-", M, "-", D, " ", H, ":", Mi, ":", S],
            Data = [HostName, "\n", Time, "\n", Extra, "\n"],
            imem_file:write_file(Ctx, SFile, Data, 3000),
            log_activity(Ctx, HostName, SFile, Extra, SInterval)
    end.

%-------------------------------------------------------------------------------
% TESTS
%-------------------------------------------------------------------------------

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {inparallel,
        [ {"key from json", ?_assertEqual(["test", 1234], key_from_json(<<"[\"test\", 1234]">>))}
        , {"key to json", ?_assertEqual([<<"test">>, 1234], key_to_json(["test", 1234]))}
        , {"key to json int", ?_assertEqual(<<"0">>, key_to_json(0))}
        , {"key to json enc", ?_assertEqual(<<"[\"test\",1234]">>, key_to_json_enc(["test", 1234]))}
        , {"key to json enc int", ?_assertEqual(<<"0">>, key_to_json_enc(0))}
        ]
    }.

-endif.
