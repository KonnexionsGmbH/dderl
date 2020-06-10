-module(dpjob_status_pre).

-include("../dperl_status.hrl").

-export([preprocess/5]).

preprocess(MetricKey, Value, Timestamp, Node, Ctx) ->
    try preprocess_internal(MetricKey, Value, Timestamp, Node, Ctx)
    catch
        Error:Exception:Stacktrace ->
            ?Error("Unable to format the Metric ~p : ~p, error ~p:~p ~p", [MetricKey, Value, Error, Exception, Stacktrace]),
            {[], Ctx}
    end.

preprocess_internal(heartbeat, Value, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), "system_info", atom_to_list(Node), "system_info", "heartbeat"],
               cvalue => Value},
    {[Metric], Ctx};
preprocess_internal({imem_metrics, erlang_nodes}, NodeInfo,  _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), "system_info", atom_to_list(Node), "system_info", "erlang_nodes"],
               cvalue => NodeInfo},
    {[Metric], Ctx};
preprocess_internal({imem_metrics, data_nodes}, DNodeInfo,  _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), "system_info", atom_to_list(Node), "system_info", "data_nodes"],
               cvalue => DNodeInfo},
    {[Metric], Ctx};
preprocess_internal({imem_metrics, system_information}, NodeStatus, _Timestamp, Node, #context{channel = Channel} = Ctx) -> 
    Metric = #{ckey => [binary_to_list(Channel), "system_info", atom_to_list(Node), "system_info", "node_status"],
               cvalue => NodeStatus},
    {[Metric], Ctx};
preprocess_internal({dperl_metrics, {job_down_count, _}}, JobDownCount, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), "system_info", atom_to_list(Node), "system_info", "job_down_count"],
               cvalue => #{job_down_count => JobDownCount}},
    {[Metric], Ctx};
preprocess_internal({dperl_metrics, {job_error_count, _}}, JobErrorCount, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), "system_info", atom_to_list(Node), "system_info", "job_error_count"],
               cvalue => #{job_error_count => JobErrorCount}},
    {[Metric], Ctx};
preprocess_internal({dperl_metrics, {jobs, _}}, JobInfos, _Timestamp, _Node, #context{channel = Channel} = Ctx) ->
    Metrics = lists:foldl(
        fun({Job, Value}, Acc) ->
            Key = [binary_to_list(Channel), Job, "cluster", "jobs", Job],
            [#{ckey => Key, cvalue => Value} | Acc]
        end, [], JobInfos),
    {Metrics, Ctx};
preprocess_internal({dperl_metrics, {job_status, _}}, JobStatuses, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metrics = lists:foldl(
        fun({Job, Value}, Acc) ->
            Key = [binary_to_list(Channel), Job, atom_to_list(Node), "job_status", Job],
            [#{ckey => Key, cvalue => Value} | Acc]
        end, [], JobStatuses),
    {Metrics, Ctx};
preprocess_internal({dperl_metrics, {errors, _}}, JobStatuses, _Timestamp, _Node, #context{channel = Channel} = Ctx) ->
    Metrics = lists:foldl(
        fun({[Job, Node], Value}, Acc) ->
            Key = [binary_to_list(Channel), Job, Node, "error", Job],
            [#{ckey => Key, cvalue => Value} | Acc];
           ({[Job, Node, ErrorId], Value}, Acc) ->
            ErrorId1 = case io_lib:printable_list(ErrorId) of
                true -> ErrorId;
                false -> format(ErrorId)
            end,
            Key = [binary_to_list(Channel), Job, Node, "error", ErrorId1],
            [#{ckey => Key, cvalue => Value} | Acc]
        end, [], JobStatuses),
    {Metrics, Ctx};
preprocess_internal({dperl_metrics, {focus,_,_}}, OrigValues, _Timestamp, _Node, #context{channel = Channel} = Ctx) ->
    FocusMetrics =
        [F#{ckey => [binary_to_list(Channel) | CKey]} ||
             #{ckey := CKey} = F <- OrigValues],
    {FocusMetrics, Ctx};
preprocess_internal({dperl_metrics, {agr, AgrMetric, _, _}}, AgrData, _Timestamp, _Node, #context{} = Ctx) -> 
    Metric = #{ckey => [AgrMetric], cvalue => AgrData},
    {[Metric], Ctx};
%% mpro Metrics
preprocess_internal({mpro_metrics, run_levels}, RunLevels, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    {maps:fold(
        fun(Protocol, RunLevel, Acc) -> 
            Metric = #{ckey => [binary_to_list(Channel), atom_to_list(Protocol), atom_to_list(Node), "run_level", ""],
                       cvalue => #{run_level => RunLevel}},
            [Metric | Acc]
        end, [], RunLevels), Ctx};
preprocess_internal({mpro_metrics, {focus, Topic, FocusKey}}, FocusMetrics, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    {maps:fold(
        fun(Protocol, FocusMetric, Acc) ->
            Metric = #{ckey => [binary_to_list(Channel), "focus", atom_to_list(Node), [Topic, FocusKey], atom_to_list(Protocol)],
                       cvalue => FocusMetric},
            [Metric | Acc]
        end, [], FocusMetrics), Ctx};
preprocess_internal({mpro_metrics, {MetricKey, Protocol}}, Value, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), atom_to_list(Protocol), atom_to_list(Node), atom_to_list(MetricKey), ""],
               cvalue => #{MetricKey => Value}},
    {[Metric], Ctx};
preprocess_internal({MetricMod, MetricKey}, Value, _Timestamp, Node, #context{channel = Channel} = Ctx) ->
    Metric = #{ckey => [binary_to_list(Channel), atom_to_list(MetricMod), atom_to_list(Node), format(MetricKey), ""],
               cvalue => Value},
    {[Metric], Ctx}.

format(Key) ->
    lists:flatten(io_lib:format("~p", [Key])).
