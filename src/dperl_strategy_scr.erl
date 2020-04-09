-module(dperl_strategy_scr).

-include("dperl.hrl").

-export([execute/4]).

-ifdef(TEST).
-export([load_src_after_key/3, load_dst_after_key/3]).
-endif.

-type state() :: any().

-callback connect_check_src(state()) ->
    {ok, state()} | {error, any()} | {error, any(), state()}.
-callback get_source_events(state(), integer()) ->
    {error, any()} | {ok, list(), state()} | {ok, sync_complete, state()}.
-callback connect_check_dst(state()) ->
    {ok, state()} | {error, any()} | {error, any(), state()}.
-callback fetch_src(any(), state()) -> ?NOT_FOUND | term().
-callback fetch_dst(any(), state()) -> ?NOT_FOUND | term().
-callback delete_dst(any(), state()) -> {true, state()} | {false, state()}.
-callback insert_dst(any(), any(), state()) -> {true, state()} | {false, state()}.
-callback update_dst(any(), any(), state()) -> {true, state()} | {false, state()}.
-callback report_status(any(), any(), state()) -> ok | {error, any()}.
-callback do_refresh(state(), integer()) ->
    {error, any()} | {ok, state()} | {ok, finish, state()}.

% optional callbacks
-callback should_cleanup(ddTimestamp()|undefined,
                         ddTimestamp()|undefined,
                         integer(), integer(), state()) -> true | false.
-callback should_refresh(ddTimestamp()|undefined,
                         ddTimestamp()|undefined,
                         integer(), integer(), [integer()], state()) -> true | false.
-callback is_equal(any(), any(), any(), state()) -> true | false.
-callback update_channel(any(), boolean(), any(), any(), state()) -> {true, state()} | {false, state()}.
-callback finalize_src_events(state()) -> {true, state()} | {false, state()}.
-callback should_sync_log(state()) -> true | false.

-optional_callbacks([should_cleanup/5, should_refresh/6, is_equal/4,
                     update_channel/5, finalize_src_events/1,
                     should_sync_log/1]).

-callback do_cleanup(state(), integer()) ->
    {error, any()} | {ok, state()} | {ok, finish, state()}.
-callback do_cleanup(list(), list(), boolean(), state()) -> 
    {error, any()} | {ok, state()} | {ok, finish, state()}.
-callback do_cleanup(list(), list(), list(), boolean(), state()) -> 
    {error, any()} | {ok, state()} | {ok, finish, state()}.
-optional_callbacks([do_cleanup/2, do_cleanup/4,do_cleanup/5]).

% chunked cleanup context
-record(cleanup_ctx,
        {srcKeys          :: list(),
         srcCount         :: integer(),
         dstKeys          :: list(),
         dstCount         :: integer(),
         bulkCount        :: integer(),
         minKey           :: any(),
         maxKey           :: any(),
         lastKey          :: any(),
         deletes = []     :: list(),
         differences = [] :: list(),
         inserts = []     :: list()}).


-define(DL(__S,__F,__A),
        ?Debug([{state,__S},{mod, Mod},{job, Job}],__F,__A)).
-define(DL(__S,__F), ?DL(__S,__F,[])).

-define(S(__F),     ?DL(sync,__F)).
-define(S(__F,__A), ?DL(sync,__F,__A)).
-define(C(__F),     ?DL(cleanup,__F)).
-define(C(__F,__A), ?DL(cleanup,__F,__A)).
-define(R(__F),     ?DL(refresh,__F)).
-define(R(__F,__A), ?DL(refresh,__F,__A)).

-define(RESTART_AFTER(__Timeout, __Args),
        erlang:send_after(__Timeout, self(),
                          {internal, {behavior, ?MODULE, __Args}})).

-spec execute(atom(), string(), state(), map()) -> state().
execute(Mod, Job, State, #{sync := _, cleanup := _, refresh := _}
        = Args)
  when is_map(Args) ->
    try execute(sync, Mod, Job, State, Args) catch
        Class:{step_failed, NewArgs} when is_map(NewArgs) ->
            ?JError("~p ~p step_failed~n~p", [Mod, Class, erlang:get_stacktrace()]),
            dperl_dal:update_job_dyn(Job, error),
            ?RESTART_AFTER(?CYCLE_ERROR_WAIT(Mod, Job), NewArgs),
            dperl_dal:job_error(get(jstate), atom_to_binary(Class, utf8), Class),
            State;
        Class:{step_failed, NewState} ->
            ?JError("~p ~p step_failed~n~p", [Mod, Class, erlang:get_stacktrace()]),
            dperl_dal:update_job_dyn(Job, Mod:get_status(NewState), error),
            ?RESTART_AFTER(?CYCLE_ERROR_WAIT(Mod, Job), Args),
            dperl_dal:job_error(get(jstate), <<"step failed">>, Class),
            NewState;
        Class:Error ->
            ?JError("~p ~p ~p~n~p", [Mod, Class, Error, erlang:get_stacktrace()]),
            dperl_dal:update_job_dyn(Job, error),
            ?RESTART_AFTER(?CYCLE_ERROR_WAIT(Mod, Job), Args),
            dperl_dal:job_error(get(jstate), atom_to_binary(Class, utf8), Class),
            State
    end;
execute(Mod, Job, State, Args) when is_map(Args) ->
    execute(Mod, Job, State,
            maps:merge(#{sync => true, cleanup => true, refresh => true},
                       Args)).

-spec execute(sync|cleanup|refresh, atom(), string(), state(), map()) ->
    state() | no_return().
% [sync]
execute(sync, Mod, Job, State, #{sync := Sync} = Args) ->
    put(jstate,s),
    ?S("Connect/check source if not already connected (trivial for push)"),
    State1 =
    case Mod:connect_check_src(State) of
        {error, Error, S1} ->
            ?JError("sync(~p) failed at connect_check_src : ~p", [Mod, Error]),
            dperl_dal:job_error(<<"sync">>, <<"connect_check_src">>, Error),
            error({step_failed, S1});
        {error, Error} ->
            ?JError("sync(~p) failed at connect_check_src : ~p", [Mod, Error]),
            dperl_dal:job_error(<<"sync">>, <<"connect_check_src">>, Error),
            error(step_failed);
        {ok, S1} -> 
            dperl_dal:job_error_close(),
            S1
    end,
    if Sync == true ->
            ?S("Get pending list of events (max n) to process from source"),
            case Mod:get_source_events(State1, ?MAX_BULK_COUNT(Mod, Job)) of
                {error, Error1, S2} ->
                    ?JError("sync(~p) failed at get_source_events : ~p", [Mod, Error1]),
                    dperl_dal:job_error(<<"sync">>, <<"get_source_events">>, Error1),
                    error({step_failed, S2});
                {error, Error1} ->
                    ?JError("sync(~p) failed at get_source_events : ~p", [Mod, Error1]),
                    dperl_dal:job_error(<<"sync">>, <<"get_source_events">>, Error1),
                    error({step_failed, State1});
                {ok, sync_complete, S2} ->
                    ?S("If lists of pending events is empty: goto [cleanup]"),
                    dperl_dal:job_error_close(),
                    execute(cleanup, Mod, Job, S2, Args);
                {ok, [], S2} ->
                    ?S("no pending events, re-enter [sync] after cycleAlwaysWait"),
                    execute(finish, Mod, Job, S2, Args);
                {ok, Events, S2} ->
                    ?S("Connect to destination if not already connected (trivial for pull)"),
                    dperl_dal:job_error_close(),
                    State3 =
                    case Mod:connect_check_dst(S2) of
                        {error, Error1, S3} ->
                            ?JError("sync(~p) failed at connect_check_dst : ~p", [Mod, Error1]),
                            dperl_dal:job_error(<<"sync">>, <<"connect_check_dst">>, Error1),
                            error({step_failed, S3});
                        {error, Error1} ->
                            ?JError("sync(~p) failed at connect_check_dst : ~p", [Mod, Error1]),
                            dperl_dal:job_error(<<"sync">>, <<"connect_check_dst">>, Error1),
                            error(step_failed);
                        {ok, S3} -> 
                            dperl_dal:job_error_close(),
                            S3
                    end,
                    ?S("Process the events; goto [finish]"),
                    case process_events(Events, Mod, State3) of
                        {true , State4} ->
                            ?JError("sync(~p) failed at process_events", [Mod]),
                            dperl_dal:job_error(<<"sync">>, <<"process_src_events">>, <<"error">>),
                            error({step_failed, State4});
                        {false, State4} ->
                            dperl_dal:update_job_dyn(Job, Mod:get_status(State4), synced),
                            dperl_dal:job_error_close(),
                            execute(finish, Mod, Job, State4, Args);
                        %% idle used for dperl_mec_ic to have idle timeout on
                        %% Try later error from oracle
                        %% would be removed in the future when new
                        %% behavior is used for mec_ic
                        {idle, State4} ->
                            execute(idle, Mod, Job, State4, Args)
                    end
           end;
       true ->
           ?S("disabled! trying cleanup"),
           execute(cleanup, Mod, Job, State1, Args)
    end;

% [cleanup]
execute(cleanup, Mod, Job, State, #{cleanup := true} = Args) ->
    put(jstate,c),
    #{lastAttempt := LastAttempt,
      lastSuccess := LastSuccess} = CleanupState = get_state(cleanup, Job),
    ShouldCleanupFun =
    case erlang:function_exported(Mod, should_cleanup, 5) of
        true -> fun(LA, LS, BI, CI) -> Mod:should_cleanup(LA, LS, BI, CI, State) end;
        false -> fun(LA, LS, BI, CI) -> should_cleanup(LA, LS, BI, CI) end
    end,
    case apply(ShouldCleanupFun,
               [LastAttempt, LastSuccess, ?CLEANUP_BATCH_INTERVAL(Mod, Job),
                ?CLEANUP_INTERVAL(Mod, Job)]) of
        false ->
            ?C("(sync phase was nop) if last cleanup + cleanupInterval < now goto [refresh]"),
            execute(refresh, Mod, Job, State, Args);
        true ->
            set_state(cleanup, Job, start),
            Args1 =
            if LastAttempt =< LastSuccess ->
                   ?JInfo("Starting cleanup cycle"),
                   case Args of
                       #{stats := #{cleanup_count := CC} = Stats} ->
                           Args#{stats => Stats#{cleanup_count => CC + 1}};
                       Args ->
                           Stats = maps:get(stats, Args, #{}),
                           Args#{stats => Stats#{cleanup_count => 1}}
                   end;
               true ->
                   case Args of
                       #{stats := #{cleanup_count := CC} = Stats} ->
                           Args#{stats => Stats#{cleanup_count => CC + 1}};
                       Args ->
                           ?JInfo("Resuming cleanup cycle"),
                           Stats = maps:get(stats, Args, #{}),
                           Args#{stats => Stats#{cleanup_count => 1}}
                   end
            end,
            ?C("Connect to destination if not already connected (trivial for pull)"),
            State1 =
            case Mod:connect_check_dst(State) of
                {error, Error, S1} ->
                    ?JError("cleanup(~p) failed at connect_check_dst : ~p", [Mod, Error]),
                    dperl_dal:job_error(<<"cleanup">>, <<"connect_check_dst">>, Error),
                    error({step_failed, S1});
                {error, Error} ->
                    ?JError("cleanup(~p) failed at connect_check_dst : ~p", [Mod, Error]),
                    dperl_dal:job_error(<<"cleanup">>, <<"connect_check_dst">>, Error),
                    error(step_failed);
                {ok, S1} -> 
                    dperl_dal:job_error_close(),
                    S1
            end,
            ?C("Read and compare list of active keys between source and destination"),
            ?C("Build a list of provisioning actions to be taken (aggregated audit list)"),
            ?C("If list provisioning action is non empty: perform the actions; goto [finish]"),
            CleanupBulkCount = ?MAX_CLEANUP_BULK_COUNT(Mod, Job),
            DoCleanupArgs =
            case (erlang:function_exported(Mod, load_src_after_key, 3) andalso
                  erlang:function_exported(Mod, load_dst_after_key, 3)) of
                false -> [State1, CleanupBulkCount];
                true ->
                    #{minKey := MinKey, maxKey := MaxKey,
                      lastKey := LastKey} = CleanupState,
                    #{deletes := Deletes, inserts := Inserts, 
                      differences := Diffs, lastKey := NextLastKey} =
                    cleanup_refresh_collect(
                      Mod,
                      #cleanup_ctx{minKey = MinKey, maxKey = MaxKey,
                                   lastKey = LastKey, bulkCount = CleanupBulkCount},
                      State1),
                    % update last key
                    case dperl_dal:select(
                           ?JOBDYN_TABLE,
                           [{#dperlNodeJobDyn{name=Job,_='_'},[],['$_']}]) of
                        {[#dperlNodeJobDyn{state = #{cleanup := OldCleanupState}
                                          = NodeJobDynState}], true}
                          when is_map(OldCleanupState) ->
                            dperl_dal:update_job_dyn(
                              Job,
                              NodeJobDynState#{
                                cleanup =>
                                (case Args1 of
                                    #{stats := #{cleanup_count := CC2}} ->
                                        OldCleanupState#{count => CC2};
                                    Args1 -> OldCleanupState
                                 end)#{lastKey => NextLastKey}});
                        _ -> ok
                    end,
                    cleanup_log("Orphan", Deletes),
                    cleanup_log("Missing", Inserts),
                    cleanup_log("Difference", Diffs),
                    case erlang:function_exported(Mod, do_cleanup, 5) of
                        true ->  [Deletes, Inserts, Diffs, NextLastKey == MinKey, State1];
                        false -> [Deletes, Inserts, NextLastKey == MinKey, State1]
                    end
            end,
            case apply(Mod, do_cleanup, DoCleanupArgs) of
                {error, Error1} ->
                    ?JError("cleanup(~p) failed at do_cleanup : ~p", [Mod, Error1]),
                    dperl_dal:job_error(<<"cleanup">>, <<"do_cleanup">>, Error1),
                    error({step_failed, Args1});
                {error, Error1, S2} ->
                    ?JError("cleanup(~p) failed at do_cleanup : ~p", [Mod, Error1]),
                    dperl_dal:job_error(<<"cleanup">>, <<"do_cleanup">>, Error1),
                    error({step_failed, S2});
                {ok, S2} ->
                    dperl_dal:job_error_close(),
                    if length(DoCleanupArgs) == 2 ->
                        set_state(
                          cleanup, Job, start,
                          case Args1 of
                              #{stats := #{cleanup_count := CC0}} ->
                                  (Mod:get_status(S2))#{count => CC0};
                              Args1 -> Mod:get_status(S2)
                          end);
                       true -> no_op
                    end,
                    execute(finish, Mod, Job, S2, Args1);
                {ok, finish, S2} ->
                    set_state(
                      cleanup, Job, stop,
                      case Args1 of
                          #{stats := #{cleanup_count := CC1}} ->
                              (Mod:get_status(S2))#{count => CC1};
                          Args1 -> Mod:get_status(S2)
                      end),
                    dperl_dal:job_error_close(),
                    ?JInfo("Cleanup cycle is complete"),
                    execute(finish, Mod, Job, S2, Args1)
            end
    end;
execute(cleanup, Mod, Job, State, Args) ->
    put(jstate,c),
    ?C("disabled! trying refresh"),
    execute(refresh, Mod, Job, State, Args);

% [refresh]
execute(refresh, Mod, Job, State, #{refresh := true} = Args) ->
    put(jstate,r),
    #{lastAttempt := LastAttempt,
      lastSuccess := LastSuccess} = get_state(refresh, Job),
    ShouldRefreshFun =
    case erlang:function_exported(Mod, should_refresh, 6) of
        true -> fun(LA, LS, BI, RI, RH) -> Mod:should_refresh(LA, LS, BI, RI, RH, State) end;
        false -> fun(LA, LS, BI, RI, RH) -> should_refresh(LA, LS, BI, RI, RH) end
    end,
    case apply(ShouldRefreshFun,
               [LastAttempt, LastSuccess, ?REFRESH_BATCH_INTERVAL(Mod, Job),
            ?REFRESH_INTERVAL(Mod, Job), ?REFRESH_HOURS(Mod, Job)]) of
        false ->
            ?R("If last refresh + refreshInterval < now(): goto [idle]"),
            ?R("If current hour is not in refreshHours): goto [idle]"),
            execute(idle, Mod, Job, State, Args);
        true ->
            set_state(refresh, Job, start),
            Args1 =
            if LastAttempt =< LastSuccess ->
                   ?JInfo("Starting refresh cycle"),
                   case Args of
                       #{stats := #{refresh_count := RC} = Stats} ->
                           Args#{stats => Stats#{refresh_count => RC + 1}};
                       Args ->
                           Stats = maps:get(stats, Args, #{}),
                           Args#{stats => Stats#{refresh_count => 1}}
                   end;
               true ->
                   case Args of
                       #{stats := #{refresh_count := RC} = Stats} ->
                           Args#{stats => Stats#{refresh_count => RC + 1}};
                       Args ->
                           ?JInfo("Resuming refresh cycle"),
                           Stats = maps:get(stats, Args, #{}),
                           Args#{stats => Stats#{refresh_count => 1}}
                   end
            end,
            ?R("Connect to destination if not already connected (trivial for pull)"),
            State1 =
            case Mod:connect_check_dst(State) of
                {error, Error, S1} ->
                    ?JError("refresh(~p) failed at connect_check_dst : ~p", [Mod, Error]),
                    dperl_dal:job_error(<<"refresh">>, <<"connect_check_dst">>, Error),
                    error({step_failed, S1});
                {error, Error} ->
                    ?JError("refresh(~p) failed at connect_check_dst : ~p", [Mod, Error]),
                    dperl_dal:job_error(<<"refresh">>, <<"connect_check_dst">>, Error),
                    error(step_failed);
                {ok, S1} -> 
                    dperl_dal:job_error_close(),
                    S1
            end,
            ?R("Read and compare values between source and existing destination keys"),
            ?R("Build a list provisioning actions to be taken"),
            ?R("If list of provisioning actions is empty: goto [finish]"),
            ?R("Perform the actions: goto [finish]"),
            case Mod:do_refresh(State1, ?MAX_REFRESH_BULK_COUNT(Mod, Job)) of
                {error, Error1} ->
                    ?JError("refresh(~p) failed at do_refresh : ~p", [Mod, Error1]),
                    dperl_dal:job_error(<<"refresh">>, <<"do_refresh">>, Error1),
                    error({step_failed, Args1});
                {ok, S2} ->
                    set_state(
                      refresh, Job, start,
                      case Args1 of
                          #{stats := #{refresh_count := RC0}} ->
                              (Mod:get_status(S2))#{count => RC0};
                          Args1 -> Mod:get_status(S2)
                      end),
                    dperl_dal:job_error_close(),
                    execute(finish, Mod, Job, S2, Args1);
                {ok, finish, S2} ->
                    set_state(
                      refresh, Job, stop,
                      case Args1 of
                          #{stats := #{refresh_count := RC1}} ->
                              (Mod:get_status(S2))#{count => RC1};
                          Args1 -> Mod:get_status(S2)
                      end),
                    dperl_dal:job_error_close(),
                    ?JInfo("Refresh cycle is complete"),
                    execute(finish, Mod, Job, S2, Args1)
                
            end
    end;
execute(refresh, Mod, Job, State, Args) ->
    put(jstate,r),
    ?R("disabled! going idle"),
    execute(idle, Mod, Job, State, Args);
execute(idle, Mod, Job, State, Args) ->
    put(jstate,i),
    ?RESTART_AFTER(?CYCLE_IDLE_WAIT(Mod, Job), Args),
    dperl_dal:update_job_dyn(Job, Mod:get_status(State), idle),
    State;
execute(finish, Mod, Job, State, Args) ->
    put(jstate,f),
    ?RESTART_AFTER(?CYCLE_ALWAYS_WAIT(Mod, Job), Args),
    State.

-spec get_state(cleanup|refresh, binary()) ->
    {ddTimestamp() | undefined, ddTimestamp() | undefined}.
get_state(Type, Job) when (Type == cleanup orelse Type == refresh)
                          andalso is_binary(Job) ->
    maps:merge(
      if Type == cleanup ->
             #{minKey => -1, maxKey => <<255>>, lastKey => 0};
         true -> #{}
      end,
      case dperl_dal:select(
             ?JOBDYN_TABLE,
             [{#dperlNodeJobDyn{name=Job,state='$1',_='_'},[],['$1']}]) of
          {[#{Type:=State}], true} when is_map(State) -> State;
          {_, true} -> #{lastAttempt => ?EPOCH, lastSuccess => ?EPOCH}
      end).

-spec set_state(cleanup|refresh, binary(), start | stop) -> any().
set_state(Type, Job, Status) -> set_state(Type, Job, Status, #{}).

-spec set_state(cleanup|refresh, binary(), start | stop, map()) -> any().
set_state(Type, Job, Status, State0)
  when (Type == cleanup orelse Type == refresh) andalso
       (Status == start orelse Status == stop) andalso is_binary(Job) ->
    {NodeJobDyn, NewStatus0} =
    case dperl_dal:select(
           ?JOBDYN_TABLE,
           [{#dperlNodeJobDyn{name=Job,_='_'},[],['$_']}]) of
        {[#dperlNodeJobDyn{state=#{Type:=OldState}} = NJD], true}
          when is_map(OldState) ->            
            {NJD, OldState};
        {[#dperlNodeJobDyn{} = NJD], true} ->
            {NJD, #{lastAttempt => os:timestamp(),
                    lastSuccess => ?EPOCH}}
    end,
    {NewStatus, State} =
    case maps:get(count, State0, '$not_found') of
        '$not_found' -> {NewStatus0, State0};
        Count ->
            {NewStatus0#{count => Count}, maps:remove(count, State0)}
    end,
    TypeState = case {Type, Status} of
                    {cleanup, start} -> cleaning;
                    {cleanup, stop} -> cleaned;
                    {refresh, start} -> refreshing;
                    {refresh, stop} -> refreshed
                end,
    % create 'Type' state if doesn't exists
    % if exists update 'LastSuccess' timestamp to current time
    NodeJobDynState = NodeJobDyn#dperlNodeJobDyn.state,
    dperl_dal:update_job_dyn(
      Job,
      maps:merge(
      NodeJobDynState#{
        Type =>
        if Status == start ->
               NewStatus#{lastAttempt => imem_meta:time()};
           true ->
               NewStatus#{lastSuccess => imem_meta:time()}
        end}, State), TypeState).

%
% default callbacks
% 
should_cleanup(LastAttempt, LastSuccess, BatchInterval, Interval) ->
    if LastAttempt > LastSuccess -> 
           imem_datatype:msec_diff(LastAttempt) > BatchInterval;
       true ->
           imem_datatype:msec_diff(LastSuccess) > Interval
    end.

should_refresh(LastAttempt, LastSuccess, BatchInterval, Interval, Hours) ->
    if LastAttempt > LastSuccess ->
           imem_datatype:msec_diff(LastAttempt) > BatchInterval;
       true ->
           case imem_datatype:msec_diff(LastSuccess) > Interval of
               false -> false;
               true ->
                   if length(Hours) > 0 ->
                          {Hour,_,_} = erlang:time(),
                          case lists:member(Hour, Hours) of
                              true -> true;
                              _ -> false
                          end;
                      true -> true
                   end
           end
    end.

is_equal(_Key, S, S, _State) -> true;
is_equal(_Key, S, D, _State) when is_map(S), is_map(D) ->
    dperl_dal:normalize_map(S) == dperl_dal:normalize_map(D);
is_equal(_Key, S, D, _State) when is_list(S), is_list(D) ->
    lists:sort(S) == lists:sort(D);
is_equal(_Key, _, _, _State) -> false.

cleanup_log(_Msg, []) -> no_op;
cleanup_log(Msg, [K | _] = Keys) when is_integer(K) ->
    ?JWarn("~s (~p) ~w", [Msg, length(Keys), Keys]);
cleanup_log(Msg, Keys) ->
    ?JWarn("~s (~p) ~p", [Msg, length(Keys), Keys]).

sync_log(_, _, false) -> no_op;
sync_log(Msg, {Key, _}, ShouldLog) -> sync_log(Msg, Key, ShouldLog);
sync_log(Msg, Key, _) when is_binary(Key) -> ?JInfo("~s : ~s", [Msg, Key]);
sync_log(Msg, Key, _) -> ?JInfo("~s : ~p", [Msg, Key]).

%%----------------
%% chunked cleanup
%%

process_events(Keys, Mod, State) ->
    ShouldLog =
    case erlang:function_exported(Mod, should_sync_log, 1) of
        true -> Mod:should_sync_log(State);
        false -> true
    end,
    process_events(Keys, Mod, State, ShouldLog, false).

process_events([], Mod, State, _ShouldLog, IsError) ->
    case erlang:function_exported(Mod, finalize_src_events, 1) of
        true -> execute_prov_fun(no_log, Mod, finalize_src_events, [State], false, IsError);
        false -> {IsError, State}
    end;
process_events([Key | Keys], Mod, State, ShouldLog, IsError) ->
    {NewIsError, NewState} =
    case {Mod:fetch_src(Key, State), Mod:fetch_dst(Key, State)} of
        {S, S} ->
            Mod:report_status(Key, no_op, State),
            {IsError, State}; %% nothing to do
        {{protected, _}, ?NOT_FOUND} -> % pusher protection
            ?JError("Protected ~p is not found on target", [Key]),
            Error = <<"Protected key is not found on target">>,
            Mod:report_status(Key, Error, State),
            dperl_dal:job_error(Key, <<"sync">>, <<"process_events">>, Error),
            {true, State};
        {{protected, S}, D} -> % pusher protection
            execute_prov_fun("Protected", Mod, update_channel, [Key, true, S, D, State], ShouldLog, IsError, check);
        {{protected, IsSamePlatform, S}, D} -> % puller protection
            execute_prov_fun("Protected", Mod, update_channel, [Key, IsSamePlatform, S, D, State], ShouldLog, IsError, check);
        {?NOT_FOUND, _D} -> execute_prov_fun("Deleted", Mod, delete_dst, [Key, State], ShouldLog, IsError);
        {S, ?NOT_FOUND} -> execute_prov_fun("Inserted", Mod, insert_dst, [Key, S, State], ShouldLog, IsError);
        {error, _} -> {true, State};
        {_, error} -> {true, State};
        {{error, _} = Error, _} ->
            ?JError("Fetch src ~p : ~p", [Key, Error]),
            Mod:report_status(Key, Error, State),
            {true, State};
        {_, {error, _} = Error} ->
            ?JError("Fetch dst ~p : ~p", [Key, Error]),
            Mod:report_status(Key, Error, State),
            {true, State};
        {{error, Error, State1}, _} ->
            ?JError("Fetch src ~p : ~p", [Key, Error]),
            Mod:report_status(Key, {error, Error}, State1),
            {true, State1};
        {_, {error, Error, State1}} ->
            ?JError("Fetch dst ~p : ~p", [Key, Error]),
            Mod:report_status(Key, {error, Error}, State1),
            {true, State1};
        {S, D} ->
            DiffFun =
            case erlang:function_exported(Mod, is_equal, 4) of
                true -> fun Mod:is_equal/4;
                false -> fun is_equal/4
            end,
            case DiffFun(Key, S, D, State) of
                false -> execute_prov_fun("Updated", Mod, update_dst, [Key, S, State], ShouldLog, IsError);
                true ->
                    Mod:report_status(Key, no_op, State),
                    {IsError, State}
            end
    end,
    process_events(Keys, Mod, NewState, ShouldLog, NewIsError).

execute_prov_fun(Op, Mod, Fun, Args, ShouldLog, IsError) ->
    case catch apply(Mod, Fun, Args) of
        {false, NewState} ->
            sync_log(Op, hd(Args), ShouldLog),
            {IsError, NewState};
        {true, NewState} -> {true, NewState};
        {idle, NewState} -> {idle, NewState};
        Error ->
            case Op of
                finalize_src_events ->
                    ?JError("Executing : ~p Error : ~p", [Fun, Error]);
                _ ->
                    ?JError("Executing : ~p for key : ~p Error : ~p",
                            [Fun, hd(Args), Error])
            end,
            {true, lists:last(Args)}
    end.

execute_prov_fun(Op, Mod, Fun, Args, ShouldLog, IsError, check) ->
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true -> execute_prov_fun(Op, Mod, Fun, Args, ShouldLog, IsError);
        false ->
            ?Error("Function : ~p not exported in mod : ~p", [Fun, Mod]),
            {true, lists:last(Args)}
    end.

-spec cleanup_refresh_collect(atom(), #cleanup_ctx{}, state()) ->
    #{deletes => list(), inserts => list(), lastKey => any()}.
cleanup_refresh_collect(Mod,
                #cleanup_ctx{minKey = MinKey, maxKey = MaxKey,
                             lastKey = LastKey, bulkCount = BulkCnt} = CleanupCtx,
                State) ->
    CurKey = if
                 LastKey =< MinKey -> MinKey;  % throw to cycle start if getting
                 LastKey >= MaxKey -> MinKey;  % out of key bounds by re-config
                 true -> LastKey
             end,
    SrcKeys = Mod:load_src_after_key(CurKey, BulkCnt, State),
    DstKeys = Mod:load_dst_after_key(CurKey, BulkCnt, State),
    cleanup_refresh_compare(CleanupCtx#cleanup_ctx{
        srcKeys = SrcKeys, srcCount = length(SrcKeys),
        dstKeys = DstKeys, dstCount = length(DstKeys), lastKey = CurKey}).

-spec cleanup_refresh_compare(#cleanup_ctx{}) -> #{deletes => list(), differences => list(), inserts => list(), lastKey => any()}.
cleanup_refresh_compare(#cleanup_ctx{
                   srcKeys = SrcKeys, dstKeys = [], deletes = Deletes, 
                   inserts = Inserts, minKey = MinKey, differences = Diffs,
                   dstCount = DstCount, bulkCount = BulkCnt, srcCount = SrcCount})
  when DstCount < BulkCnt, SrcCount < BulkCnt ->
    Remaining = fetch_keys(SrcKeys),
    #{deletes => Deletes, differences => Diffs, inserts => Inserts++Remaining, lastKey => MinKey};
cleanup_refresh_compare(#cleanup_ctx{srcKeys = SrcKeys, dstKeys = [], deletes = Deletes, dstCount = DstCount,
                                     inserts = Inserts, differences = Diffs})
  when DstCount == 0 ->
    Remaining = fetch_keys(SrcKeys),
    #{deletes => Deletes, differences => Diffs, inserts => Inserts++Remaining, lastKey => last_key(SrcKeys)};
cleanup_refresh_compare(#cleanup_ctx{dstKeys = [], deletes = Deletes, differences = Diffs,
                                     inserts = Inserts, lastKey = LK}) ->
    #{deletes => Deletes, differences => Diffs, inserts => Inserts, lastKey => LK};
cleanup_refresh_compare(#cleanup_ctx{
                   srcCount = SrcCount, dstKeys = DstKeys, bulkCount = BulkCnt,
                   minKey = MinKey, srcKeys = [], deletes = Deletes, 
                   inserts = Inserts, dstCount = DstCount, differences = Diffs})
  when SrcCount < BulkCnt, DstCount < BulkCnt ->
    Remaining = fetch_keys(DstKeys),
    #{deletes => Deletes++Remaining, differences => Diffs, inserts => Inserts, lastKey => MinKey};
cleanup_refresh_compare(#cleanup_ctx{srcKeys = [], deletes = Deletes,
                                     inserts = Inserts, dstKeys = DstKeys,
                                     differences = Diffs, srcCount = SrcCount}) 
  when SrcCount == 0 ->
    Remaining = fetch_keys(DstKeys),
    #{deletes => Deletes ++ Remaining, differences => Diffs, inserts => Inserts, lastKey => last_key(DstKeys)};
cleanup_refresh_compare(#cleanup_ctx{srcKeys = [], deletes = Deletes, differences = Diffs,
                                     inserts = Inserts, lastKey = LK}) ->
    #{deletes => Deletes, differences => Diffs, inserts => Inserts, lastKey => LK};
cleanup_refresh_compare(#cleanup_ctx{srcKeys = [K|SrcKeys], dstKeys = [K|DstKeys]}
                = CleanupCtx) ->
    cleanup_refresh_compare(CleanupCtx#cleanup_ctx{srcKeys = SrcKeys, dstKeys = DstKeys,
                                                   lastKey = last_key([K])});
cleanup_refresh_compare(#cleanup_ctx{srcKeys = [{K, _} | SrcKeys], dstKeys = [{K, _} | DstKeys],
                                     differences = Diffs} = CleanupCtx) ->
    cleanup_refresh_compare(CleanupCtx#cleanup_ctx{srcKeys = SrcKeys, dstKeys = DstKeys,
                                                   lastKey = K, differences = [K | Diffs]});
cleanup_refresh_compare(#cleanup_ctx{srcKeys = [SK|SrcKeys], dstKeys = [DK | DstKeys],
                                     inserts = Inserts, deletes = Deletes} = CleanupCtx) ->
    case {last_key([SK]), last_key([DK])} of
        {K1, K2} when K1 < K2 -> cleanup_refresh_compare(
                CleanupCtx#cleanup_ctx{srcKeys = SrcKeys,
                                       inserts = [K1|Inserts], lastKey = K1});
        {K1, K2} when K2 < K1 -> cleanup_refresh_compare(
                CleanupCtx#cleanup_ctx{dstKeys = DstKeys,
                                       deletes = [K2|Deletes], lastKey = K2})
    end.

fetch_keys([]) -> [];
fetch_keys([{_, _} | _] = KVs) -> [K || {K, _} <- KVs];
fetch_keys(Keys) -> Keys.

last_key([{_, _} | _] = KVs) -> element(1, lists:last(KVs));
last_key(Keys) -> lists:last(Keys).

%% ----------------------


%% ----------------------
%% Eunit Tests
%% ----------------------

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

load_batch(CurKey, BulkCnt, Keys) ->
    lists:sort(lists:foldl(
        fun({K, _} = E , Acc) ->
            if length(Acc) < BulkCnt andalso K > CurKey -> [E | Acc];
               true -> Acc
            end;
           (E, Acc) ->
            if length(Acc) < BulkCnt andalso E > CurKey -> [E | Acc];
               true -> Acc
            end
        end, [], Keys)).

load_src_after_key(CurKey, BulkCnt, {SrcKeys, _}) -> 
    load_batch(CurKey, BulkCnt, SrcKeys).

load_dst_after_key(CurKey, BulkCnt, {_, DstKeys}) -> 
    load_batch(CurKey, BulkCnt, DstKeys).

cleanup_refresh_compare_test() ->
    BulkCnt = 1000,
    SrcCount = rand:uniform(1000),
    SrcKeys = lists:usort([rand:uniform(3000) || _ <- lists:seq(1, SrcCount)]),
    DstCount = rand:uniform(1000),
    DstKeys = lists:usort([rand:uniform(3000) || _ <- lists:seq(1, DstCount)]),
    #{deletes := Dels, inserts := Ins} =
    cleanup_refresh_collect(?MODULE,
                #cleanup_ctx{minKey = -1, maxKey = <<255>>,
                             lastKey = 0, bulkCount = BulkCnt},
                {SrcKeys, DstKeys}),
    Cleaned = lists:sort(lists:foldr(fun(K, Acc) ->
                case lists:member(K, Dels) of
                    true -> 
                        lists:delete(K, Acc);
                    false -> Acc
                end
            end, DstKeys, Dels) ++ Ins),
    ?assertEqual(Cleaned, SrcKeys).

complete_cleanup_refresh(AllSrcKeys, AllDstKeys) ->
    BulkCnt = 100,
    MaxKey = <<255>>,
    Ctx = #cleanup_ctx{minKey = -1, maxKey = MaxKey, lastKey = 0, 
                       bulkCount = BulkCnt},
    #{deletes := Dels, differences := Diffs, inserts := Ins} = cleanup_refresh_loop(Ctx, 0, {AllSrcKeys, AllDstKeys}, #{}),
    Cleaned = lists:usort(lists:foldr(fun(K, Acc) ->
                case lists:member(K, Dels) of
                    true -> 
                        lists:delete(K, Acc);
                    false -> Acc
                end
            end, fetch_keys(AllDstKeys), Dels) ++ Ins),
    ?assertEqual(Cleaned, lists:usort(fetch_keys(AllSrcKeys))),
    Diffs1 = lists:usort(lists:foldl(
                fun({K, V}, Acc) -> 
                    case lists:keyfind(K, 1, AllDstKeys) of
                        {K, V} -> Acc;
                        {K, _} -> [K | Acc];
                        false -> Acc
                    end;
                   (_, Acc) -> Acc
                end, [], AllSrcKeys)),
    ?assertEqual(Diffs1, lists:usort(Diffs)).

complete_cleanup_refresh(AllSrcKeys, AllDstKeys, BulkCnt) ->
    MaxKey = <<255>>,
    Ctx = #cleanup_ctx{minKey = -1, maxKey = MaxKey, lastKey = 0, 
                       bulkCount = BulkCnt},
    cleanup_refresh_collect(?MODULE, Ctx, {AllSrcKeys, AllDstKeys}).

cleanup_refresh_loop(_, -1, _, Acc) -> Acc;
cleanup_refresh_loop(Ctx, CurKey, AllKeys, Acc) ->
    #{deletes := Dels, differences := Diffs, inserts := Ins, lastKey := LastKey} = 
    cleanup_refresh_collect(?MODULE, Ctx#cleanup_ctx{lastKey = CurKey}, AllKeys),
    NewAcc = Acc#{deletes => Dels ++ maps:get(deletes, Acc, []), 
                  differences => Diffs ++ maps:get(differences, Acc, []),
                  inserts => Ins ++ maps:get(inserts, Acc, [])},
    cleanup_refresh_loop(Ctx, LastKey, AllKeys, NewAcc).

cleanup_only_test() ->
    [ok] = lists:usort([begin 
        AllSrcKeys = lists:usort([rand:uniform(5000) || _ <- lists:seq(1, 2000)]),
        AllDstKeys = lists:usort([rand:uniform(5000) || _ <- lists:seq(1, 2000)]),
        complete_cleanup_refresh(AllSrcKeys, AllDstKeys) 
    end || _ <- lists:seq(1, 10)]),
    AllSrcKeys1 = lists:usort([rand:uniform(5000) || _ <- lists:seq(1, 2000)]),
    AllDstKeys1 = lists:usort([rand:uniform(5000) || _ <- lists:seq(1, 2000)]),
    ok = complete_cleanup_refresh(AllSrcKeys1, AllDstKeys1),
    %cleanup with SrcKeys missing 500 to 1000
    ok = complete_cleanup_refresh([K || K <- AllSrcKeys1, K < 500 orelse K > 1000], AllDstKeys1),
    %cleanup with DstKeys missing 500 to 1000
    ok = complete_cleanup_refresh(AllSrcKeys1, [K || K <- AllDstKeys1, K < 500 orelse K > 1000]),
    %cleanup with DstKeys as []
    ok = complete_cleanup_refresh(AllSrcKeys1, []),
    %cleanup with SrcKeys as []
    ok = complete_cleanup_refresh([], AllDstKeys1).

cleanup_refresh_test() ->
    %% cleanup with refresh tests
    %refresh test with differences
    [ok] = lists:usort([begin
        AllSrcKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), S} || S <- lists:seq(1, 2000)]))),
        AllDstKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), S} || S <- lists:seq(1, 2000)]))),
        complete_cleanup_refresh(AllSrcKeys, AllDstKeys)
    end || _ <- lists:seq(1,10)]),
    [ok] = lists:usort([begin
        AllSrcKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
        AllDstKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
        complete_cleanup_refresh(AllSrcKeys, AllDstKeys)
    end || _ <- lists:seq(1,10)]),
    AllSrcKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    AllDstKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    ok = complete_cleanup_refresh(AllSrcKeys, AllDstKeys),
    %cleanup refresh with SrcKeys missing 500 to 1000
    ok = complete_cleanup_refresh([{K, V} || {K, V} <- AllSrcKeys, K < 500 orelse K > 1000], AllDstKeys),
    %cleanup refresh with DstKeys missing 500 to 1000
    ok = complete_cleanup_refresh(AllSrcKeys, [{K, V} || {K, V} <- AllDstKeys, K < 500 orelse K > 1000]).

cleanup_refresh_boundary_test() ->
    AllSrcKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    AllDstKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    %cleanup refresh with DstKeys as []
    ok = complete_cleanup_refresh(AllSrcKeys, []),
    %cleanup refresh with SrcKeys as []
    ok = complete_cleanup_refresh([], AllDstKeys),
    %cleanup refresh with DstKeys as [{1, 10}]
    ok = complete_cleanup_refresh(AllSrcKeys, [{1, 10}]),
    %cleanup refresh with SrcKeys as [{1, 10}]
    ok = complete_cleanup_refresh([{1, 10}], AllDstKeys),
    %cleanup refresh with less DstKeys
    ok = complete_cleanup_refresh(AllSrcKeys, lists:sublist(AllDstKeys, 100)),
    %cleanup refresh with less SrcKeys
    ok = complete_cleanup_refresh(lists:sublist(AllSrcKeys, 100), AllDstKeys).

cleanup_refresh_only_dels_test() ->
    AllKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    DeleteKeys = [{6000, 6}, {7000, 7}, {8000, 8}],
    #{inserts := Ins, deletes := Dels, differences := Diffs} = complete_cleanup_refresh(AllKeys, AllKeys ++ DeleteKeys, 2000),
    ?assertEqual([], Diffs),
    ?assertEqual([], Ins),
    ?assertEqual([6000, 7000, 8000], Dels).    

cleanup_refresh_only_ins_test() ->
    AllKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    InsertKeys = [{6000, 6}, {7000, 7}, {8000, 8}],
    #{inserts := Ins, deletes := Dels, differences := Diffs} = complete_cleanup_refresh(AllKeys ++ InsertKeys, AllKeys, 2000),
    ?assertEqual([], Diffs),
    ?assertEqual([6000, 7000, 8000], Ins),
    ?assertEqual([], Dels). 

cleanup_refresh_no_op_test() ->
    AllKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    #{inserts := Ins, deletes := Dels, differences := Diffs} = complete_cleanup_refresh(AllKeys, AllKeys, 2000),
    ?assertEqual([], Diffs),
    ?assertEqual([], Ins),
    ?assertEqual([], Dels).

cleanup_refresh_no_diff_test() ->
    AllSrcKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), 1} || _ <- lists:seq(1, 2000)]))),
    AllDstKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), 1} || _ <- lists:seq(1, 2000)]))),
    #{differences := Diffs} = complete_cleanup_refresh(AllSrcKeys, AllDstKeys, 2000),
    ?assertEqual([], Diffs).

-endif.
