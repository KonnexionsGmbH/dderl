-module(dperl_metrics).

-include("dperl.hrl").
-include_lib("jobs/dperl_status.hrl").

-behaviour(imem_gen_metrics).

-export([start_link/0,get_metric/1,get_metric/2,request_metric/3,get_metric_direct/1]).

-export([init/0,handle_metric_req/3,request_metric/1, terminate/2]).

-export([mbsKey/2,ramsKey/2,mproKey/2,intKey/2]).

-safe([get_metric/1, get_metric_direct/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    imem_gen_metrics:start_link(?MODULE).

-spec get_metric(term()) -> term().
get_metric(MetricKey) ->
    imem_gen_metrics:get_metric(?MODULE, MetricKey).

-spec get_metric(term(), integer()) -> term().
get_metric(MetricKey, Timeout) ->
    imem_gen_metrics:get_metric(?MODULE, MetricKey, Timeout).

-spec request_metric(term(), term(), pid()) -> term().
request_metric(MetricKey, ReqRef, ReplyTo) ->
    imem_gen_metrics:request_metric(?MODULE, MetricKey, ReqRef, ReplyTo).

-spec get_metric_direct(term()) -> term().
get_metric_direct(job_down_count) ->
    Exclusions = lists:usort(?JOB_DOWN_NAME_EXCLUSIONS ++ modules_to_jobs(?JOB_DOWN_MOD_EXCLUSIONS)),
    job_down_count(Exclusions);
get_metric_direct(job_error_count) ->
    Exclusions = lists:usort(?JOB_ERROR_NAME_EXCLUSIONS ++ modules_to_jobs(?JOB_ERROR_MOD_EXCLUSIONS)),
    job_error_count(Exclusions);
get_metric_direct(UnknownMetric) ->
    ?Error("Unknown metric requested direct ~p", [UnknownMetric]),
    undefined.

-spec request_metric(term()) -> noreply | {ok, term()}.
request_metric({focus, "shortid", ShortIdStr}) ->
    case catch list_to_integer(ShortIdStr) of
        {'EXIT', _Error} -> {ok, {error, user_input}};
        _ -> noreply
    end;
request_metric(_) -> noreply.

%% imem_gen_metrics callback
init() -> {ok, undefined}.

handle_metric_req({jobs, ExcludedJobs}, ReplyFun, State) ->
    ReplyFun(process_jobs(imem_meta:dirty_read(dperlJob), ExcludedJobs)),
    State;
handle_metric_req({job_status, ExcludedJobs}, ReplyFun, State) ->
    ReplyFun(process_status(imem_meta:dirty_read(?JOBDYN_TABLE), ExcludedJobs)),
    State;
handle_metric_req({errors, ExcludedJobs}, ReplyFun, State) ->
    ReplyFun(process_errors(dperl_dal:read_gt(?JOB_ERROR, {}, 1000), ExcludedJobs)),
    State;
handle_metric_req({job_down_count, ExcludedJobs}, ReplyFun, State) ->
    JobDownCount = job_down_count([list_to_bin(J) || J <- get_ignored_jobs(ExcludedJobs)]),
    ReplyFun(JobDownCount),
    State;
handle_metric_req({job_error_count, ExcludedJobs}, ReplyFun, State) ->
    JobErrorCount = job_error_count([list_to_bin(J) || J <- get_ignored_jobs(ExcludedJobs)]),
    ReplyFun(JobErrorCount),
    State;
handle_metric_req({focus, "shortid", ShortId}, ReplyFun, State) when is_list(ShortId) ->
    Result = maps:fold(
        fun(Chn, Fun, Acc) when is_list(Chn), is_atom(Fun) ->
            case not erlang:function_exported(?MODULE, Fun, 2) orelse
                 ?MODULE:Fun("shortid", ShortId) of
                true -> Acc;
                {API, Keys} ->
                    case catch dperl_dal:API(Chn, Keys) of
                        Values when is_list(Values) ->
                            [#{ckey =>
                              ["focus", "cluster", ["shortid", ShortId],
                               [Chn,
                                if is_list(CKey) ->
                                       [if is_atom(CK) -> atom_to_list(CK);
                                           true -> CK
                                        end || CK <- CKey];
                                   true -> CKey
                                end]],
                              cvalue => CVal}
                            || #{ckey := CKey, cvalue := CVal} <- Values] ++ Acc;
                        _ -> Acc
                    end;
                nomatch -> Acc;
                _Error -> Acc
            end;
           (_, _, Acc) -> Acc
        end, [], ?GET_FOCUS("shortid")),
    ReplyFun(Result),
    State;
handle_metric_req({focus, "shortid", _ShortId}, ReplyFun, State) ->
    ?Warn("ShortId has to be a list"),
    ReplyFun([]),
    State;
%% aggregator metrics
handle_metric_req({agr, AgrMetric, Channel}, ReplyFun, State) when Channel == [] ->
    ?JError("Channel cannot be empty for aggregator metric : ~p", [AgrMetric]),
    ReplyFun(undefined),
    State;
handle_metric_req({agr, AgrMetric, Channel}, ReplyFun, State) when is_list(Channel) ->
    handle_metric_req({agr, AgrMetric, list_to_bin(Channel)}, ReplyFun, State);
handle_metric_req({agr, AgrMetric, Channel}, ReplyFun, State) when is_binary(Channel) ->
    ReplyFun(fetch_metric(AgrMetric, Channel)),
    State;
handle_metric_req(UnknownMetric, ReplyFun, State) ->
    ?Error("Unknown metric requested ~p when state ~p", [UnknownMetric, State]),
    ReplyFun({error, unknown_metric}),
    State.

terminate(_Reason, _State) -> ok.

%% Helper functions
mbsKey("shortid", [I|_] = ShortId) when I >= $0, I =< $9 ->
    {read_siblings, [[ShortId,[]]]};
mbsKey("shortid", SubKey) ->
    case re:split(SubKey, "-", [{return, list}]) of
        [_,_,[I|_] = ShortId|_] when I >= $0, I =< $9 ->
            {read_channel_raw, [[ShortId,SubKey]]};
        _ -> nomatch
    end.

ramsKey("shortid", [I|_] = ShortId) when I >= $0, I =< $9 ->
    {read_siblings, [[ShortId,[]]]}.

intKey("shortid", ShortIdStr) when is_list(ShortIdStr) ->
    case catch list_to_integer(ShortIdStr) of
        ShortId when is_integer(ShortId) ->
            {read_channel_raw, [ShortId]};
        _ -> nomatch
    end.

mproKey("shortid", ShortIdStr) when is_list(ShortIdStr) ->
    case catch list_to_integer(ShortIdStr) of
        ShortId when is_integer(ShortId) ->
            {read_channel_raw, [[P,ShortId] || P <- [smpp, tpi]]};
        _ -> nomatch
    end.

-spec process_jobs([#dperlJob{}], list()) -> [map()].
process_jobs(Jobs, ExcludedJobs) ->
    process_jobs(Jobs, get_ignored_jobs(ExcludedJobs), ?JOB_DESCRIPTIONS).

-spec process_jobs([#dperlJob{}], list(), map()) -> [map()].
process_jobs([], _, _) -> [];
process_jobs([#dperlJob{name = Name, module = Module, args = Args,
                       srcArgs = SrcArgs, dstArgs = DstArgs, running = Running,
                       enabled = Enabled, plan = Plan, nodes = Nodes,
                       opts = Opts} | Rest], IgnoredJobs, JobDescriptions) ->
    {Channel, Direction} = case DstArgs of
        #{channel := DstChannel} -> {DstChannel, pull};
        _ -> case SrcArgs of
            #{channel := SrcChannel} -> {SrcChannel, push};
            _ -> {"none", push}
        end
    end,
    case is_ignored_job(Name, IgnoredJobs) of
        true -> process_jobs(Rest, IgnoredJobs, JobDescriptions);
        false ->
                JobName = bin_to_list(Name),
                Label = case Args of
                    #{label := L} when is_list(L) -> list_to_bin(L);
                    #{label := L} when is_binary(L) -> L;
                    _ -> <<>>
                end,
                Desc = maps:get(Label, JobDescriptions, <<>>),
                Value = #{module => Module, args => to_json(Args), srcArgs => to_json(SrcArgs),
                          dstArgs => to_json(DstArgs), enabled => Enabled, plan => Plan,
                          nodes => Nodes, opts => Opts, channel => list_to_bin(Channel),
                          direction => Direction, running => Running, platform => Label,
                          desc => Desc},
                [{JobName, Value} | process_jobs(Rest, IgnoredJobs, JobDescriptions)]
    end.

-spec process_status([#dperlNodeJobDyn{}], list()) -> [map()].
process_status(Statuses, ExcludedJobs) ->
    process_status(Statuses, get_ignored_jobs(ExcludedJobs), []).

-spec process_status([#dperlNodeJobDyn{}], list(), list()) -> [map()].
process_status([], _, Acc) -> Acc;
process_status([#dperlNodeJobDyn{name = Name, state = State, statusTime = StatusTime,
                                status = Status} | Rest], IgnoredJobs, Acc) ->
    case is_ignored_job(Name, IgnoredJobs) of
        false ->
            case is_status_timed_out(StatusTime) of
                false ->
                    JobName = bin_to_list(Name),
                    Value = #{status => Status,
                              state => to_json(State)},
                    process_status(Rest, IgnoredJobs, [{JobName, Value} | Acc]);
                true -> process_status(Rest, IgnoredJobs, Acc)
            end;
        true -> process_status(Rest, IgnoredJobs, Acc)
    end.

-spec process_errors([{term(), map()}], list()) -> [map()].
process_errors(Errors, ExcludedJobs) -> process_errors(Errors, get_ignored_jobs(ExcludedJobs), []).

-spec process_errors([{term(), map()}], list(), list()) -> [map()].
process_errors([], _, Acc) -> Acc;
process_errors([{ErrorKey, Value} | Rest], IgnoredJobs, Acc) ->
    %% TODO: What to do when ErrorKey doesn't match ?...
    NewAcc =
    case ErrorKey of
        [Job, Node] ->
            case is_ignored_job(Job, IgnoredJobs) of
                false ->
                    [{[Job, Node], Value} | Acc];
                true -> Acc
            end;
        [Job, Node, ErrorId] ->
            case is_ignored_job(Job, IgnoredJobs) of
                false ->
                    Key = [Job, Node, ensure_json(ErrorId)],
                    [{Key, Value} | Acc];
                true -> Acc
            end;
        InvalidErrorKey ->
            ?Error("Invalid error key processing errors: ~p", [InvalidErrorKey]),
            Acc
    end,
    process_errors(Rest, IgnoredJobs, NewAcc).

-spec job_error_count(list()) -> integer().
job_error_count(NameExclusions) ->
    case imem_meta:select(?JOBDYN_TABLE, [{#dperlNodeJobDyn{status = error,
                                    statusTime = '$1', name ='$2', _ = '_'}, [], [{{'$1', '$2'}}]}]) of
        {[], true} -> 0;
        {Infos, true} ->
           lists:foldl(fun({StatusTime, Name}, Acc) ->
                case is_status_timed_out(StatusTime) of
                    true  -> Acc;
                    false -> 
                        case lists:member(Name, NameExclusions) of
                            false -> Acc + 1;
                            true -> Acc
                        end
                end
            end, 0, Infos)
    end.

-spec job_down_count(list()) -> integer().
job_down_count(NameExclusions) ->
    case imem_meta:select(dperlJob, [{#dperlJob{name ='$1', plan = '$2', nodes = '$3', _ = '_'}, [], [{{'$1', '$2', '$3'}}]}]) of
        {[], true} -> 0;
        {JobPlans, true} ->
            lists:foldl(
                fun({JobName, Plan, Nodes}, Acc) ->
                        case not lists:member(JobName, NameExclusions) of
                            true -> case job_down_count_i(JobName, Plan, Nodes) of
                                        true -> Acc + 1;
                                        false -> Acc
                                    end;
                            false -> Acc
                        end
                end, 0, JobPlans)
    end.

-spec job_down_condition(binary()) -> boolean().
job_down_condition(JobName) when is_binary(JobName) ->
    case imem_meta:select(dperlJob, [{#dperlJob{enabled = '$1', name = JobName, _ = '_'}, [], ['$1']}]) of
        {[], true} -> false;
        {[IsEnabled], true} -> IsEnabled
    end.

job_down_count_i(JobName, _Plan, []) -> not job_down_condition(JobName);
job_down_count_i(JobName, on_all_nodes, Nodes) ->
    (not job_down_condition(JobName)) andalso lists:member(node(), Nodes);
job_down_count_i(JobName, _Plan, Nodes) ->
    (not job_down_condition(JobName)) andalso node() == hd(Nodes).

to_json(Map) when is_map(Map) ->
    maps:map(fun(password, _V) -> <<"*****">>;
                (_K, V) when is_map(V) -> to_json(V);
                (_K, V) when is_tuple(V) -> 
                    case catch to_epoch(V) of
                        {'EXIT', _} -> tuple_to_list(V);
                        Epoch -> Epoch
                    end;
                (_K, V) when is_list(V) -> 
                    case catch format_list(V) of
                        {'EXIT', _} -> iolist_to_binary(io_lib:format("~p", [V]));
                        L -> L
                    end;
                (_K, V) -> V
             end, Map);
to_json(Term) -> Term.

format_list(List) ->
    case io_lib:printable_list(List) of
        true -> list_to_bin(List);
        false ->
            lists:map(fun(L) when is_map(L) -> to_json(L);
                         (L) when is_tuple(L) -> format_list(tuple_to_list(L));
                         (L) when is_list(L) -> format_list(L);
                         (<<>>) -> null;
                         (L) -> L
                      end, List)
    end.

to_epoch({_, T}) -> to_epoch(T);
to_epoch({Mega, Sec, _}) -> (Mega * 1000000) + Sec.

ensure_json(Value) when is_binary(Value) -> bin_to_list(Value);
ensure_json(Value) when is_atom(Value) -> atom_to_list(Value);
ensure_json(Value) -> Value.

is_ignored_job(JobName, IgnoredJobs) when is_binary(JobName) ->
    is_ignored_job(bin_to_list(JobName), IgnoredJobs);
is_ignored_job(JobName, IgnoredJobs) when is_list(JobName) ->
    lists:member(JobName, IgnoredJobs).

get_ignored_jobs(ExcludedJobs) ->
    {NeverRunJobs, true} = imem_meta:select(dperlJob, [{#dperlJob{running = undefined,
                                    name = '$1', _ = '_'}, [], ['$1']}]),
    lists:usort(?GET_IGNORED_JOBS_LIST ++ [bin_to_list(J) || J <- NeverRunJobs] ++ ExcludedJobs).

is_status_timed_out(StatusTime) ->
    imem_datatype:musec_diff(StatusTime) > ?JOB_DYN_STATUS_TIMEOUT.

modules_to_jobs([]) -> [];
modules_to_jobs(Modules) ->
    lists:foldl(
        fun(Mod, Acc) ->
            {Jobs, true} = imem_meta:select(dperlJob, [{#dperlJob{module = Mod, name = '$1', _ = '_'}, [], ['$1']}]),
            Jobs ++ Acc
        end, [], Modules).

fetch_metric(node_memory, Channel) ->
    FoldFun =
        fun(#{ckey := [_, _, _, "system_info", Node, "system_info", "node_status"],
              cvalue := #{free_memory := FreeMemory, total_memory := TotalMemory}}, Acc) ->
                Acc#{list_to_atom(Node) => #{free_memory => FreeMemory, total_memory => TotalMemory}};
           (_, Acc) -> Acc
        end,
    fold_table(Channel, FoldFun);
fetch_metric(error_count, Channel) ->
    FoldFun =
        fun(#{ckey := [_, _, _, "system_info", Node, "system_info", "job_error_count"],
              cvalue := #{job_error_count := Count}}, Acc) ->
                Acc#{list_to_atom(Node) => Count};
           (_, Acc) -> Acc
        end,
        fold_table(Channel, FoldFun);
fetch_metric(heartbeat, Channel) ->
    FoldFun =
        fun(#{ckey := [_, _, _, "system_info", Node, "system_info", "heartbeat"],
              cvalue := #{time := Time}}, Acc) ->
                Acc#{list_to_atom(Node) => Time};
           (_, Acc) -> Acc
        end,
        fold_table(Channel, FoldFun);
fetch_metric(node_error, Channel) ->
    FoldFun =
        fun(#{ckey := [_, _, _, "system_info", Node, "error", _Type], cvalue := #{error := Error}}, Acc) ->
                Acc#{list_to_atom(Node) => Error};
           (_, Acc) -> Acc
        end,
    fold_table(Channel, FoldFun).

fold_table(Channel, FoldFun) ->
    TableName = imem_dal_skvh:atom_table_name(<<Channel/binary, ?AGGR/binary>>),
    TransactionFun = 
        fun() ->
            FirstKey = imem_meta:first(TableName),
            fold_table(TableName, FirstKey, FoldFun, #{})
        end,
    case imem_meta:transaction(TransactionFun) of
        {atomic, Result} -> Result;
        ErrorResult -> 
            ?JError("Error fetching rows, result: ~p", [ErrorResult])
    end.

fold_table(_Table, '$end_of_table', _FoldFun, Acc) -> Acc;
fold_table(Table, CurKey, FoldFun, Acc) ->
    [RawRow] = imem_meta:read(Table, CurKey),
    RowMap = imem_dal_skvh:skvh_rec_to_map(RawRow),
    NewAcc = FoldFun(RowMap, Acc),
    NextKey = imem_meta:next(Table, CurKey),
    fold_table(Table, NextKey, FoldFun, NewAcc).

-spec bin_to_list(binary() | list()) -> list().
bin_to_list(L) when is_list(L) -> L;
bin_to_list(B) -> binary_to_list(B).

-spec list_to_bin(binary() | list()) -> binary().
list_to_bin(B) when is_binary(B) -> B;
list_to_bin(L) -> list_to_binary(L).