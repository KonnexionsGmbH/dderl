-module(dperl_strategy_scr).

%% implements the scr behaviour for sync / cleanup / refresh / cleanupCumRefresh (c/r) jobs

-include("dperl.hrl").
-include("dperl_strategy_scr.hrl").

-export([execute/4]).

-ifdef(TEST).
-export([ load_src_after_key/3  
        , load_dst_after_key/3
        ]).
-endif.

% check if source data is currently acessible
-callback connect_check_src(scrState()) ->
            {ok, scrState()} | {error, any()} | {error, any(), scrState()}.

% get a key list (limited in length) of recent changes on source side
% often achieved by scanning an audit log after last checked timestamp (from state).
% Basis for sync cycle (differential change provisioning)
% For normal cleanup, a list of (possibly) dirty keys is returned
% For c/r jobs, a list of (possibly) dirty KV pairs is returned where V can 
% stand for (choice of the callback module and more precisely typed there):
% - a scalar value
% - a value map (usually representing a JSON part)
% - some hash of the value or KV pair (e.g. used in copy-jobs)
% - a structured value, e.g. {Content,Meta}
% Structured values can contain properties which should be ignored in
% an overridden compare function is_equal()
-callback get_source_events(scrState(), scrBatchSize()) ->
            {ok, scrAnyKeys(), scrState()} | 
            {ok, scrAnyKeyVals(), scrState()} | 
            {ok, sync_complete, scrState()} | 
            {error, scrAnyKey()}.

% check if destination data is currently acessible
-callback connect_check_dst(scrState()) ->
            {ok, scrState()} | {error, any()} | {error, any(), scrState()}.

% fetch one item from source (if it exists)
% return scrAnyVal() for cleanup
% return scrAnyKeyVal() for c/r
% the error pattern decides on the logging details
-callback fetch_src(scrAnyKey() | scrAnyKeyVal() , scrState()) -> 
    ?NOT_FOUND | scrAnyVal() | scrAnyKeyVal() | 
    {error, term(), scrState()} | 
    {error, term()} | 
    error.

% fetch one item from destination (if it exists)
% return scrAnyVal() for cleanup
% return scrAnyKeyVal() for c/r
% the error pattern decides on the logging details
-callback fetch_dst(scrAnyKey() | scrAnyKeyVal(), scrState()) -> 
    ?NOT_FOUND | scrAnyVal() | scrAnyKeyVal() | 
    {error, term(), scrState()} | 
    {error, term()} | 
    error.

% delete one item from destination (if it exists)
% scrSoftError() = true signals that a soft error happened which is skipped without throwing
% leaving a chance to fix it in one of the next cycles 
-callback delete_dst(scrAnyKey() | scrAnyKeyVal(), scrState()) -> 
            {scrSoftError(), scrState()}.

% insert one item to destination (which is assumed to not exist)
% scrAnyVal() is used to insert the value, not the value part of scrAnyKeyVal()
% scrSoftError() = true signals that a soft error happened which is skipped without throwing 
% leaving a chance to fix it in one of the next cycles 
-callback insert_dst(scrAnyKey() | scrAnyKeyVal(), scrAnyVal(), scrState()) -> 
            {scrSoftError(), scrState()}.

% update one item in destination (which is assumed to exist)
% scrAnyVal() is used to change the value, not the value part of scrAnyKeyVal()
% scrSoftError() = true signals that a soft error happened which is skipped without throwing 
% leaving a chance to fix it in one of the next cycles 
-callback update_dst(scrAnyKey() | scrAnyKeyVal(), scrAnyVal(), scrState()) -> 
            {scrSoftError(), scrState()}.

% allow the callback implementation act upon an error or warning message
% result is ignored and used for debugging only
-callback report_status(scrAnyKey() | scrAnyKeyVal(), scrErrorInfo(), scrState()) -> 
            ok | no_op | {error, term()}.

% execute one more refresh cycle with limited block size
-callback do_refresh(scrState(), scrBatchSize()) -> 
            {{ok, scrState()} | {ok, finish, scrState() | error, any()}}.

% optional callbacks

% override callback for cleanup execution permission
-callback should_cleanup( LastAttempt::ddTimestamp(),
                          LastSuccess::ddTimestamp(),
                          BatchInterval::scrMsecInterval(),     % delay between cleanup batches
                          CycleInterval::scrMsecInterval(),     % delay between cleanup cycles
                          scrState()) -> 
            true | false.

% override callback for refresh execution permission
-callback should_refresh( LastAttempt::ddTimestamp(),
                          LastSuccess::ddTimestamp(),
                          BatchInterval::scrMsecInterval(),     % delay between refresh batches
                          CycleInterval::scrMsecInterval(),     % delay between refresh cycles
                          scrHoursOfDay(), 
                          scrState()) -> 
            true | false.

% Compare function, used to loosen compare semantics, ignoring some differences if needed.
% Comparisons will depend on the (FROZEN) callback state just before processing doing batch
% comparison (sync / cleanup / refresh). This must be taken into account in the optional
% override is_equal/4 in the callback module.
% is_equal() is not called and assumed to be true if Erlang compares values/KVPs equal
-callback is_equal(scrAnyKey() | scrAnyKeyVal(), scrAnyVal(), scrAnyVal(), scrState()) -> 
            true | false.

% override for destination channel insert/update/delete (final data change)
% only used for protected configurations (reversing the direction of data flow)
-callback update_channel( scrAnyKey() | scrAnyKeyVal(), 
                          IsSamePlatform::boolean(), 
                          SourceVal::scrAnyVal(), 
                          DestVal::scrAnyVal(), 
                          scrState()) -> {true, scrState()} | {false, scrState()}.

% called at the end of all the sync cycles, for example used for smsc push job
% to push the addresslists. 
% optional callback when there is a risk of target inconsistency after individual sync updates.
-callback finalize_src_events(scrState()) -> {true, scrState()} | {false, scrState()}.

% can be used in callback to suppress the logging of individual sync results
-callback should_sync_log(scrState()) -> true | false.

-optional_callbacks([should_cleanup/5, should_refresh/6, is_equal/4,
                     update_channel/5, finalize_src_events/1,
                     should_sync_log/1]).

% execute simple cleanup for accumulated list of dirty keys (or KVPs in case of c/r).
% Must decide if cleanup is finished.
-callback do_cleanup( scrState(), scrBatchSize()) -> 
    {ok, scrState()} | {ok, finish, scrState()} | {error, term(), scrState()} | {error, term()}.

% prepare cleanup sync for found differences (Deletes and Inserts)
% by adding these keys to the sync event list in the state (dirty Keys).
% These (potentially) 'dirty' keys will be dealt with in the following sync cycle
-callback do_cleanup( Deletes::scrAnyKeys(), 
                      Inserts::scrAnyKeys(), 
                      IsCycleComplete::boolean(),    % NextLastKey == MinKey
                      scrState()) -> 
            {ok, scrState()} | 
            {ok, finish, scrState()} | 
            {error, term(), scrState()} | 
            {error, term()}.

% prepare cleanup/refresh sync for found differences (Deletes, Diffs, Inserts)
% by adding these Keys to the sync event list in the state (dirty Keys)
% These (potentially) 'dirty' keys will be dealt with in the following sync cycle
-callback do_cleanup( Deletes::scrAnyKeys(), 
                      Inserts::scrAnyKeys(),
                      Diffs::scrAnyKeys(), 
                      IsCycleComplete::boolean(),    % NextLastKey == MinKey
                      scrState()) -> 
            {ok, scrState()} | 
            {ok, finish, scrState()} | 
            {error, term(), scrState()} | 
            {error, term()}.

% bulk load one batch of keys (for cleanup) or kv-pairs (cleanup/refresh) from source.
% up to scrBatchSize() existing keys MUST be returned in key order.
% Postponing trailing keys to next round is not permitted.
% A callback module implementing this and also the load_dst_after_key callback signals that it wants
% to do override source loading for cleanup or c/r combined processing.
% Returning less than scrBatchSize() items does not prevent further calls of this function.
% In that case, if called again in same cycle, {ok, [], scrState()} must be returned.
-callback load_src_after_key(LastSeen::scrAnyKey()|scrAnyKeyVal(), scrBatchSize(), scrState()) ->
            {ok, scrAnyKeys(), scrState()} | 
            {ok, scrAnyKeyVals(), scrState()} | 
            {error, term(), scrState()}.

% bulk load one batch of kv-pairs for combined cleanup/refresh from destination.
% up to scrBatchSize() existing keys MUST be returned in key order.
% Postponing trailing keys to next round is not permitted.
% A callback module implementing this and load_src_after_key signals that it wants
% to do a cleanup/refresh combined processing.
% Returning less than scrBatchSize() items does not prevent further calls of this function.
% If called again in same cycle, {ok, [], scrState()} must be returned.
-callback load_dst_after_key(LastSeen::scrAnyKey()|scrAnyKeyVal(), scrBatchSize(), scrState()) ->
            {ok, scrAnyKeys(), scrState()} | 
            {ok, scrAnyKeyVals(), scrState()} | 
            {error, term(), scrState()}.

-optional_callbacks([ do_cleanup/2  % simple cleanup (or simple cleanup/refresh)
                    , do_cleanup/4  % cleanup with inserts and deletes only (no updates)
                    , do_cleanup/5  % cleanup/refresh with updates on KV-Pairs
                    , load_src_after_key/3 % overrides cleanup or c/r source loading
                    , load_dst_after_key/3 % overrides cleanup or c/r destination loading
                    ]).

% chunked cleanup context where scrAnyKeyVal() types are used for c/r combination
-record(cleanup_ctx,{ srcKeys          :: scrAnyKeys()| scrAnyKeyVal()
                    , srcCount         :: integer()   % length(srcKeys)
                    , dstKeys          :: scrAnyKeys()| scrAnyKeyVal()
                    , dstCount         :: integer()   % length(dstKeys)
                    , bulkCount        :: scrBatchSize()
                    , minKey           :: scrAnyKey() | scrAnyKeyVal()
                    , maxKey           :: scrAnyKey() | scrAnyKeyVal()
                    , lastKey          :: scrAnyKey() | scrAnyKeyVal()
                    , deletes = []     :: scrAnyKeys()  % Keys to be deleted
                    , inserts = []     :: scrAnyKeys()  % Keys to be inserted
                    , differences = [] :: scrAnyKeys()  % Keys to modify values
                    }).

% Debug macros
-define(DL(__S,__F,__A),
        ?Debug([{state,__S},{mod, Mod},{job, Job}],__F,__A)).

-define(DL(__S,__F), ?DL(__S,__F,[])).

-define(S(__F),     ?DL(sync,__F)).
-define(S(__F,__A), ?DL(sync,__F,__A)).

-define(C(__F),     ?DL(cleanup,__F)).
-define(C(__F,__A), ?DL(cleanup,__F,__A)).

-define(R(__F),     ?DL(refresh,__F)).
-define(R(__F,__A), ?DL(refresh,__F,__A)).

% trigger macro for next action
-define(RESTART_AFTER(__Timeout, __Args),
        erlang:send_after(__Timeout, self(),
                          {internal, {behavior, ?MODULE, __Args}})).

% execute (start or restart) the synchronisation job
-spec execute(jobModule(), jobName(), scrState(), jobArgs()) -> scrState().
execute(Mod, Job, State, #{sync:=_, cleanup:=_, refresh:=_} = Args) when is_map(Args) ->
    try 
        execute(sync, Mod, Job, State, Args) 
    catch
        Class:{step_failed, NewArgs}:Stacktrace when is_map(NewArgs) ->
            ?JError("~p ~p step_failed~n~p", [Mod, Class, Stacktrace]),
            dperl_dal:update_job_dyn(Job, error),
            ?RESTART_AFTER(?CYCLE_ERROR_WAIT(Mod, Job), NewArgs),
            dperl_dal:job_error(get(jstate), atom_to_binary(Class, utf8), Class),
            State;
        Class:{step_failed, NewState}:Stacktrace ->
            ?JError("~p ~p step_failed~n~p", [Mod, Class, Stacktrace]),
            dperl_dal:update_job_dyn(Job, Mod:get_status(NewState), error),
            ?RESTART_AFTER(?CYCLE_ERROR_WAIT(Mod, Job), Args),
            dperl_dal:job_error(get(jstate), <<"step failed">>, Class),
            NewState;
        Class:Error:Stacktrace ->
            ?JError("~p ~p ~p~n~p", [Mod, Class, Error, Stacktrace]),
            dperl_dal:update_job_dyn(Job, error),
            ?RESTART_AFTER(?CYCLE_ERROR_WAIT(Mod, Job), Args),
            dperl_dal:job_error(get(jstate), atom_to_binary(Class, utf8), Class),
            State
    end;
execute(Mod, Job, State, Args) when is_map(Args) ->
    execute(Mod, Job, State, maps:merge(#{sync=>true, cleanup=>true, refresh=>true}, Args)).

%% execute one provisioning cycle (round) which can mean one sync, cleanup, refresh or a c/r cycle
%% with priority in this order
-spec execute(scrPhase(), jobModule(), jobName(), scrState(), jobArgs()) -> scrState() | no_return().
execute(sync, Mod, Job, State, #{sync:=Sync} = Args) ->
    % perform a sync cycle
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
    if 
        Sync == true ->
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
                        %% try later error from oracle
                        %% results in calling idle wait instead of always wait
                        {idle, State4} ->
                            execute(idle, Mod, Job, State4, Args)
                    end
            end;
        true ->
           ?S("disabled! trying cleanup"),
           execute(cleanup, Mod, Job, State1, Args)
    end;
execute(cleanup, Mod, Job, State, #{cleanup:=true} = Args) ->
    % perform a cleanup cycle if due
    put(jstate,c),
    #{lastAttempt:=LastAttempt, lastSuccess:=LastSuccess} = CleanupState = get_cycle_state(cleanup, Job),
    ShouldCleanupFun = case erlang:function_exported(Mod, should_cleanup, 5) of
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
            set_cycle_state(cleanup, Job, start),
            Args1 = if 
                LastAttempt =< LastSuccess ->
                    ?JInfo("Starting cleanup cycle"),
                    case Args of
                        #{stats := #{cleanup_count:=CC} = Stats} ->
                            Args#{stats => Stats#{cleanup_count => CC + 1}};
                        Args ->
                            Stats = maps:get(stats, Args, #{}),
                            Args#{stats => Stats#{cleanup_count => 1}}
                    end;
                true ->
                    case Args of
                        #{stats := #{cleanup_count:=CC} = Stats} ->
                            Args#{stats => Stats#{cleanup_count => CC + 1}};
                        Args ->
                            ?JInfo("Resuming cleanup cycle"),
                            Stats = maps:get(stats, Args, #{}),
                            Args#{stats => Stats#{cleanup_count => 1}}
                    end
            end,
            ?C("Connect to destination if not already connected (trivial for pull)"),
            State1 = case Mod:connect_check_dst(State) of
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
            DoCleanupArgs = case (erlang:function_exported(Mod, load_src_after_key, 3) andalso
                                  erlang:function_exported(Mod, load_dst_after_key, 3)) of
                false ->
                    % launch simple cleanup cycle using do_cleanup/2
                    [State1, CleanupBulkCount];
                true ->
                    % launch cleanup processing using do_cleanup/4 on scalar keys
                    % launch cleanup/refresh combined processing using do_cleanup/5 on kv-pairs
                    % note: Key also used here in the sense of kv-pairs (KVP) where the value can itself
                    % be structured (e.g. {Content::map(), Meta::map()} in Office365 contact sync)  
                    #{minKey:=MinKey, maxKey:=MaxKey, lastKey:=LastKey} = CleanupState,
                    Ctx = #cleanup_ctx{ minKey=MinKey, maxKey=MaxKey, lastKey=LastKey
                                      , bulkCount=CleanupBulkCount},
                    {RefreshCollectResult, State2} = cleanup_refresh_collect(Mod, Ctx, State1),
                    Deletes = maps:get(deletes,RefreshCollectResult),
                    Inserts = maps:get(inserts,RefreshCollectResult),
                    Diffs = maps:get(differences,RefreshCollectResult),
                    NextLastKey = maps:get(lastKey,RefreshCollectResult),
                    % update last key ToDo: This is UGLY. To be cast into functions !!!!
                    MatchSpec = [{#dperlNodeJobDyn{name=Job,_='_'},[],['$_']}],
                    case dperl_dal:select(?JOBDYN_TABLE, MatchSpec) of
                        {[#dperlNodeJobDyn{state = #{cleanup:=OldState} = NodeJobDyn}], true} 
                          when is_map(OldState) ->
                            dperl_dal:update_job_dyn(
                              Job,
                              NodeJobDyn#{
                                cleanup =>
                                (case Args1 of
                                    #{stats := #{cleanup_count:=CC2}} ->    OldState#{count=>CC2};
                                    Args1 ->                                OldState
                                 end)#{lastKey=>NextLastKey}});
                        _ -> ok
                    end,
                    cleanup_log("Orphan", Deletes),
                    cleanup_log("Missing", Inserts),
                    cleanup_log("Difference", Diffs),
                    case erlang:function_exported(Mod, do_cleanup, 5) of
                        true ->  [Deletes, Inserts, Diffs, NextLastKey == MinKey, State2];
                        false -> [Deletes, Inserts, NextLastKey == MinKey, State2]
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
                    if 
                        length(DoCleanupArgs) == 2 ->
                            set_cycle_state(cleanup, Job, start,
                                case Args1 of
                                    #{stats := #{cleanup_count := CC0}} ->
                                        (Mod:get_status(S2))#{count => CC0};
                                    Args1 -> Mod:get_status(S2)
                                end);
                       true -> 
                           no_op
                    end,
                    execute(finish, Mod, Job, S2, Args1);
                {ok, finish, S2} ->
                    set_cycle_state(cleanup, Job, stop,
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
execute(refresh, Mod, Job, State, #{refresh := true} = Args) ->
    % execute a refresh cycle if due
    put(jstate,r),
    #{lastAttempt:=LastAttempt, lastSuccess:=LastSuccess} = get_cycle_state(refresh, Job),
    ShouldRefreshFun = case erlang:function_exported(Mod, should_refresh, 6) of
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
            set_cycle_state(refresh, Job, start),
            Args1 = if 
                LastAttempt =< LastSuccess ->
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
            State1 = case Mod:connect_check_dst(State) of
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
                    set_cycle_state(refresh, Job, start,
                        case Args1 of
                            #{stats := #{refresh_count := RC0}} ->
                                (Mod:get_status(S2))#{count => RC0};
                            Args1 -> Mod:get_status(S2)
                        end),
                    dperl_dal:job_error_close(),
                    execute(finish, Mod, Job, S2, Args1);
                {ok, finish, S2} ->
                    set_cycle_state(refresh, Job, stop,
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


%% evaluate cycle status by 
-spec get_cycle_state(scrCycle(), jobName()) -> scrCycleState().
get_cycle_state(Cycle, Name) when (Cycle==cleanup orelse Cycle==refresh) andalso is_binary(Name) ->
    maps:merge(
        if 
            Cycle==cleanup ->   
                #{minKey=>?SCR_MIN_KEY, maxKey=>?SCR_MAX_KEY, lastKey=>?SCR_INIT_KEY};
            true ->             
                #{}
        end,
        case dperl_dal:select(
                ?JOBDYN_TABLE, 
                [{#dperlNodeJobDyn{name=Name, state='$1', _='_'}, [], ['$1']}]) of
            {[#{Cycle:=CycleState}], true} when is_map(CycleState) -> 
                CycleState;
            {_, true} -> 
                #{lastAttempt => ?EPOCH, lastSuccess => ?EPOCH}
        end).

% update dperlNodeJobDyn table according to planned action, reset statistics 
-spec set_cycle_state(scrCycle(), jobName(), start | stop) -> ok.
set_cycle_state(Cycle, Job, Action) -> set_cycle_state(Cycle, Job, Action, #{}).

% update dperlNodeJobDyn table according to planned action, update statistics in  
-spec set_cycle_state(scrCycle(), jobName(), start | stop, jobArgs()) -> ok.
set_cycle_state(Cycle, Job, Action, Stats0)
        when (Cycle==cleanup orelse Cycle==refresh) andalso
             (Action==start orelse Action==stop) andalso is_binary(Job) ->
    {NodeJobDyn, CycleState1} = case dperl_dal:select(?JOBDYN_TABLE,
                [{#dperlNodeJobDyn{name=Job, _='_'}, [], ['$_']}]) of
        {[#dperlNodeJobDyn{state=#{Cycle:=CycleState0}} = NJD], true} when is_map(CycleState0) ->            
            {NJD, CycleState0};
        {[#dperlNodeJobDyn{} = NJD], true} ->
            {NJD, #{lastAttempt=>imem_meta:time(), lastSuccess=>?EPOCH}}
        end,
    {CycleState2, Stats1} = case maps:get(count, Stats0, '$not_found') of
        '$not_found' -> {CycleState1, Stats0};
        Count ->        {CycleState1#{count => Count}, maps:remove(count, Stats0)}
    end,
    CycleState3 = if 
        Action == start ->  CycleState2#{lastAttempt => imem_meta:time()};
        true ->             CycleState2#{lastSuccess => imem_meta:time()}
    end,
    Status = case {Cycle, Action} of
        {cleanup, start} -> cleaning;
        {cleanup, stop} -> cleaned;
        {refresh, start} -> refreshing;
        {refresh, stop} -> refreshed
    end,
    NodeJobDynState = NodeJobDyn#dperlNodeJobDyn.state,
    NewStateWithStats = maps:merge(NodeJobDynState#{Cycle=>CycleState3}, Stats1),
    dperl_dal:update_job_dyn(Job, NewStateWithStats, Status).

% default callbacks

%% default scheduling delays according to configured
-spec should_cleanup(ddTimestamp(), ddTimestamp(), 
                     scrBatchInterval(), scrCycleInterval()) -> true | false.
should_cleanup(LastAttempt, LastSuccess, BatchInterval, CycleInterval) ->
    if LastAttempt > LastSuccess ->
            % wait a short time between cleanup batches (100 items typically)
            imem_datatype:msec_diff(LastAttempt) > BatchInterval;
       true ->
            % wait a longer time between full cleanup cycles (all data)
            imem_datatype:msec_diff(LastSuccess) > CycleInterval
    end.

-spec should_refresh(ddTimestamp(), ddTimestamp(), scrBatchInterval(), 
                     scrCycleInterval(), scrHoursOfDay()) -> true | false.
should_refresh(LastAttempt, LastSuccess, BatchInterval, Interval, Hours) ->
    if LastAttempt > LastSuccess ->
            % wait a short time between refresh batches (100 items typically)
            imem_datatype:msec_diff(LastAttempt) > BatchInterval;
       true ->
            % wait a longer time between full cleanup cycles (all data)
            case imem_datatype:msec_diff(LastSuccess) > Interval of
               false -> false;
               true ->
                    if 
                        length(Hours) > 0 ->
                            %% only start new cycles during listed hours
                            {Hour,_,_} = erlang:time(),
                            case lists:member(Hour, Hours) of
                                true -> true;
                                _ ->    false
                            end;
                        true ->
                            % start new cycle after the configured delay
                            true
                    end
            end
    end.

%% implements a comparer for two objects with the same key.
%% Are the two object values equal (in the sense that they don't
%% need synchronisation?
%% Default semantics used here: exact match after sorting list components
%% The callback module can implement an override comparer which tolerate certain
%% differences.
-spec is_equal(scrAnyKey(), scrAnyVal(), scrAnyVal(), scrState()) -> boolean().
is_equal(_Key, S, S, _State) -> true;
is_equal(_Key, S, D, _State) when is_map(S), is_map(D) ->
    dperl_dal:normalize_map(S) == dperl_dal:normalize_map(D);
is_equal(_Key, S, D, _State) when is_list(S), is_list(D) ->
    lists:sort(S) == lists:sort(D);
is_equal(_Key, _, _, _State) -> false.

-spec cleanup_log(string(), scrAnyKeys()) -> no_op | ok.
cleanup_log(_Msg, []) -> no_op;
cleanup_log(Msg, [K | _] = Keys) when is_integer(K) ->
    ?JWarn("~s (~p) ~w", [Msg, length(Keys), Keys]);
cleanup_log(Msg, Keys) ->
    ?JWarn("~s (~p) ~p", [Msg, length(Keys), Keys]).

-spec sync_log(scrOperation(), scrAnyKey() | {scrAnyKey(),term()}, boolean()) -> no_op | ok.
sync_log(_, _, false) -> no_op;
sync_log(Msg, {Key, _}, ShouldLog) -> sync_log(Msg, Key, ShouldLog);
sync_log(Msg, Key, _) when is_binary(Key) -> ?JInfo("~s : ~s", [Msg, Key]);
sync_log(Msg, Key, _) -> ?JInfo("~s : ~p", [Msg, Key]).

%% chunked cleanup, process possibly dirty keys (change events/differences) in the list
%% for c/r (where events consist of kv tuples) this must be done in callback module ????
-spec process_events(scrAnyKeys(), jobModule(), scrState()) -> {boolean(), scrState()}.
process_events(Keys, Mod, State) ->
    ShouldLog = case erlang:function_exported(Mod, should_sync_log, 1) of
        true -> Mod:should_sync_log(State);
        false -> true
    end,
    IsEqualFun = make_is_equal_fun(Mod, State), 
    %% Note: Mod:is_equal() will see the current (FROZEN!) scrState()
    process_events(Keys, Mod, State, ShouldLog, IsEqualFun, false).

-spec process_events(scrAnyKeys(), jobModule(), scrState(), boolean(), fun(), boolean()) -> 
        {boolean(), scrState()}.
process_events([], Mod, State, _ShouldLog, _IsEqualFun, IsError) ->
    case erlang:function_exported(Mod, finalize_src_events, 1) of
        true -> execute_prov_fun(no_log, Mod, finalize_src_events, [State], false, IsError);
        false -> {IsError, State}
    end;
process_events([Key | Keys], Mod, State, ShouldLog, IsEqualFun, IsError) ->
    % Both values/KVPs are fetched again in order to avoid race conditions
    %?Info("Difference src: ~p", [Mod:fetch_src(Key, State)]),
    %?Info("Difference dst: ~p", [Mod:fetch_dst(Key, State)]),
    {NewIsError, NewState} = case {Mod:fetch_src(Key, State), Mod:fetch_dst(Key, State)} of
        {S, S} ->                               % exactly equal Erlang terms, nothing to do
            Mod:report_status(Key, no_op, State),
            {IsError, State};
        {{protected, _}, ?NOT_FOUND} ->         % pusher protection
            ?JError("Protected ~p is not found on target", [Key]),
            Error = <<"Protected key is not found on target">>,
            Mod:report_status(Key, Error, State),
            dperl_dal:job_error(Key, <<"sync">>, <<"process_events">>, Error),
            {true, State};
        {{protected, S}, D} ->                  % pusher protection
            execute_prov_fun("Protected", Mod, update_channel, [Key, true, S, D, State], ShouldLog, IsError, check);
        {{protected, IsSamePlatform, S}, D} ->  % puller protection
            execute_prov_fun("Protected", Mod, update_channel, [Key, IsSamePlatform, S, D, State], ShouldLog, IsError, check);
        {?NOT_FOUND, _D} ->                     % orphan 
            execute_prov_fun("Deleted", Mod, delete_dst, [Key, State], ShouldLog, IsError);
        {S, ?NOT_FOUND} ->                      % missing
            execute_prov_fun("Inserted", Mod, insert_dst, [Key, S, State], ShouldLog, IsError);
        {error, _} -> {true, State};            % src fetch error
        {{error, _} = Error, _} ->              % src fetch {error, term()}
            ?JError("Fetch src ~p : ~p", [Key, Error]),
            Mod:report_status(Key, Error, State),
            {true, State};
        {{error, Error, State1}, _} ->          % src fetch {error, term(), scrState()}
            ?JError("Fetch src ~p : ~p", [Key, Error]),
            Mod:report_status(Key, {error, Error}, State1),
            {true, State1};
        {_, error} -> {true, State};            % dst fetch error
        {_, {error, _} = Error} ->              % dst fetch {error, term()}
            ?JError("Fetch dst ~p : ~p", [Key, Error]),
            Mod:report_status(Key, Error, State),
            {true, State};
        {_, {error, Error, State1}} ->          % dst fetch {error, term(), scrState()}
            ?JError("Fetch dst ~p : ~p", [Key, Error]),
            Mod:report_status(Key, {error, Error}, State1),
            {true, State1};
        {S, D} ->                               % need to invoke is_equal()
            case IsEqualFun(Key, S, D) of
                false ->
                    % ?Info("process_events diff detected key=~p~n~p~n~p",[Key, S, D]), 
                    execute_prov_fun("Updated", Mod, update_dst, [Key, S, State], ShouldLog, IsError);
                true ->
                    Mod:report_status(Key, no_op, State),
                    {IsError, State}
            end
    end,
    process_events(Keys, Mod, NewState, ShouldLog, IsEqualFun, NewIsError).

-spec make_is_equal_fun(module(), scrState()) -> fun().
make_is_equal_fun(Mod, State) ->
    case erlang:function_exported(Mod, is_equal, 4) of
        true -> fun(Key,S,D) -> Mod:is_equal(Key, S, D, State) end;
        false -> fun(Key,S,D) -> is_equal(Key, S, D, State) end
    end.

-spec execute_prov_fun(scrOperation(), jobModule(), atom(), list(), boolean(), boolean()) -> 
        {scrSoftError(), scrState() | term()}.
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

-spec execute_prov_fun(scrOperation(), jobModule(), atom(), list(), boolean(), boolean(), check) -> 
        {true|false|idle, scrState()|term()}.
execute_prov_fun(Op, Mod, Fun, Args, ShouldLog, IsError, check) ->
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true -> 
            execute_prov_fun(Op, Mod, Fun, Args, ShouldLog, IsError);
        false ->
            ?Error("Function : ~p not exported in mod : ~p", [Fun, Mod]),
            {true, lists:last(Args)}
    end.

-spec cleanup_refresh_collect(jobModule(), #cleanup_ctx{}, scrState()) ->
    {#{deletes => scrAnyKeyVals(), inserts => scrAnyKeyVals(), lastKey => scrAnyKeyVal()}, scrState()}.
cleanup_refresh_collect(Mod, CleanupCtx, State) ->
    % note: Key used here in the sense of key-value-pair (KVP) where the value can itself
    % be structured (e.g. {Content::map(), Meta::map()} in Office365 contact sync)   
    #cleanup_ctx{minKey=MinKey, maxKey=MaxKey, lastKey=LastKey, bulkCount=BulkCnt} = CleanupCtx,
    CurKey = if
                 LastKey =< MinKey -> MinKey;  % throw to cycle start if getting
                 LastKey >= MaxKey -> MinKey;  % out of key bounds by re-config
                 true -> LastKey
             end,
    {SrcKeys, State2} = 
    case Mod:load_src_after_key(CurKey, BulkCnt, State) of
        {ok, SKeys, State1} -> {SKeys, State1};
        {error, Error, State1} ->
            ?JError("cleanup failed at load_src_after_key : ~p", [Error]),
            dperl_dal:job_error(<<"cleanup">>, <<"load_src_after_key">>, Error),
            error({step_failed, State1});
        SKeys -> {SKeys, State} % deprecated simple API which returns the kv-pairs only
    end,
    {DstKeys, State4} =
    case Mod:load_dst_after_key(CurKey, BulkCnt, State2) of
        {ok, DKeys, State3} -> {DKeys, State3};
        {error, Error1, State3} ->
            ?JError("cleanup failed at load_dst_after_key : ~p", [Error1]),
            dperl_dal:job_error(<<"cleanup">>, <<"load_dst_after_key">>, Error1),
            error({step_failed, State3});
        DKeys -> {DKeys, State2} % deprecated simple API which returns the kv-pairs only
    end,
    CpResult = cleanup_refresh_compare(
        make_is_equal_fun(Mod, State), 
        CleanupCtx#cleanup_ctx{ srcKeys=SrcKeys, srcCount=length(SrcKeys)
                              , dstKeys=DstKeys, dstCount=length(DstKeys)
                              , lastKey=CurKey}),
    {CpResult, State4}.

-spec cleanup_refresh_compare( fun(), #cleanup_ctx{}) -> #{ deletes=>scrAnyKeys()
                                                          , differences=>scrAnyKeys()
                                                          , inserts=>scrAnyKeys()
                                                          , lastKey=>scrAnyKey()}.
cleanup_refresh_compare(_, #cleanup_ctx{ srcKeys=SrcKeys, dstKeys=[]
                                       , deletes=Deletes, inserts=Inserts, differences=Diffs
                                       , srcCount=SrcCount, dstCount=DstCount, bulkCount=BulkCnt
                                       , minKey=MinKey})
        when DstCount < BulkCnt, SrcCount < BulkCnt ->
    Remaining = take_keys(SrcKeys),    % no more dstKeys and all srcKeys fetched -> end of cycle
    #{deletes=>Deletes, differences=>Diffs, inserts=>Inserts++Remaining, lastKey=>MinKey};

cleanup_refresh_compare(_, #cleanup_ctx{ srcKeys=SrcKeys, dstKeys=[]
                                       , deletes=Deletes, inserts=Inserts, differences=Diffs
                                       , dstCount=DstCount})
        when DstCount == 0 ->
    Remaining = take_keys(SrcKeys),    % no dstKeys but more srcKeys -> another batch needed 
    #{deletes=>Deletes, differences=>Diffs, inserts=>Inserts++Remaining, lastKey=>last_key(SrcKeys)};

cleanup_refresh_compare(_, #cleanup_ctx{ dstKeys=[]
                                       , deletes=Deletes, inserts=Inserts, differences=Diffs
                                       , lastKey=LK}) ->
                                       % no more data but not complete, another batch needed
    #{deletes=>Deletes, differences=>Diffs, inserts=>Inserts, lastKey=>LK};

cleanup_refresh_compare(_, #cleanup_ctx{ srcKeys=[], dstKeys=DstKeys, minKey=MinKey
                                       , deletes=Deletes, inserts=Inserts, differences=Diffs
                                       , srcCount=SrcCount, dstCount=DstCount, bulkCount=BulkCnt})
        when SrcCount < BulkCnt, DstCount < BulkCnt ->
    Remaining = take_keys(DstKeys),    % no more srcKeys and all dstKeys fetched -> end of cycle
    #{deletes=>Deletes++Remaining, differences=>Diffs, inserts=>Inserts, lastKey=>MinKey};

cleanup_refresh_compare(_, #cleanup_ctx{ srcKeys=[], dstKeys=DstKeys
                                       , deletes=Deletes, inserts=Inserts, differences=Diffs
                                       , srcCount=SrcCount}) 
        when SrcCount == 0 ->
    Remaining = take_keys(DstKeys),    % no srcKeys but more dstKeys -> another batch needed
    #{deletes=>Deletes++Remaining, differences=>Diffs, inserts=>Inserts, lastKey=>last_key(DstKeys)};

cleanup_refresh_compare(_, #cleanup_ctx{ srcKeys=[]
                                       , deletes=Deletes, inserts=Inserts, differences=Diffs
                                       , lastKey=LK}) ->
                                       % no more data but not complete, another batch needed
    #{deletes=>Deletes, differences=>Diffs, inserts=>Inserts, lastKey=>LK};

cleanup_refresh_compare(IE, #cleanup_ctx{srcKeys=[K|SrcKeys], dstKeys=[K|DstKeys]} = CleanupCtx) ->
                                       % exact kv-match, no_diff, recurse one item forward
    cleanup_refresh_compare(IE, CleanupCtx#cleanup_ctx{ srcKeys=SrcKeys, dstKeys=DstKeys
                                                      , lastKey=last_key([K])});

cleanup_refresh_compare(IE, #cleanup_ctx{ srcKeys=[{K,S}|SrcKeys], dstKeys=[{K,D}|DstKeys]
                                        , differences=Diffs} = CleanupCtx) ->
                                       % key match but no exact value match
    case IE(K, {K,S}, {K,D}) of 
        true ->                        % values compare equal -> recurse one item forward
            cleanup_refresh_compare(IE, CleanupCtx#cleanup_ctx{ srcKeys=SrcKeys, dstKeys=DstKeys
                                                              , lastKey=K, differences=Diffs});
        false ->                       % values different -> add diff and recurse one item forward
            cleanup_refresh_compare(IE, CleanupCtx#cleanup_ctx{ srcKeys=SrcKeys, dstKeys=DstKeys
                                                              , lastKey=K, differences=[K|Diffs]})
    end;

cleanup_refresh_compare(IE, #cleanup_ctx{ srcKeys=[SK|SrcKeys], dstKeys=[DK|DstKeys]
                                        , inserts=Inserts, deletes=Deletes} = CleanupCtx) ->
                                       % keys are different, compare keys
    K1 = take_key(SK), 
    K2 = take_key(DK),
    if 
        K1 < K2 -> cleanup_refresh_compare(IE,
            CleanupCtx#cleanup_ctx{srcKeys=SrcKeys, inserts=[K1|Inserts], lastKey=K1});
        K2 < K1 -> cleanup_refresh_compare(IE,
            CleanupCtx#cleanup_ctx{dstKeys=DstKeys, deletes=[K2|Deletes], lastKey=K2})
    end.

-spec take_key(scrAnyKey()|scrAnyKeyVal()) -> scrAnyKey().
take_key({K, _}) -> K;
take_key(K) ->      K.

-spec take_keys(scrAnyKeys()|scrAnyKeyVals()) -> scrAnyKeys().
take_keys(KVs) -> [take_key(KV) || KV <- KVs].

-spec last_key(scrAnyKeys()|scrAnyKeyVals()) -> scrAnyKey().
last_key(KVs) -> take_key(lists:last(KVs)).

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
    {#{deletes := Dels, inserts := Ins}, _} =
    cleanup_refresh_collect(?MODULE,
                #cleanup_ctx{minKey=?SCR_MIN_KEY, maxKey=?SCR_MAX_KEY, lastKey=?SCR_INIT_KEY, bulkCount=BulkCnt},
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
    MaxKey = ?SCR_MAX_KEY,
    Ctx = #cleanup_ctx{minKey=?SCR_MIN_KEY, maxKey=MaxKey, lastKey=?SCR_INIT_KEY, bulkCount=BulkCnt},
    #{deletes:=Dels, differences:=Diffs, inserts:=Ins} = 
            cleanup_refresh_loop(Ctx, ?SCR_INIT_KEY, {AllSrcKeys, AllDstKeys}, #{}),
    Cleaned = lists:usort(lists:foldr(fun(K, Acc) ->
                case lists:member(K, Dels) of
                    true -> 
                        lists:delete(K, Acc);
                    false -> Acc
                end
            end, take_keys(AllDstKeys), Dels) ++ Ins),
    ?assertEqual(Cleaned, lists:usort(take_keys(AllSrcKeys))),
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
    MaxKey = ?SCR_MAX_KEY,
    Ctx = #cleanup_ctx{minKey=?SCR_MIN_KEY, maxKey=MaxKey, lastKey=?SCR_INIT_KEY, bulkCount=BulkCnt},
    cleanup_refresh_collect(?MODULE, Ctx, {AllSrcKeys, AllDstKeys}).

cleanup_refresh_loop(_, ?SCR_MIN_KEY, _, Acc) -> Acc;
cleanup_refresh_loop(Ctx, CurKey, AllKeys, Acc) ->
    {#{deletes:=Dels, differences:=Diffs, inserts:=Ins, lastKey:=LastKey}, _} = 
        cleanup_refresh_collect(?MODULE, Ctx#cleanup_ctx{lastKey=CurKey}, AllKeys),
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
    {#{inserts := Ins, deletes := Dels, differences := Diffs}, _} = complete_cleanup_refresh(AllKeys, AllKeys ++ DeleteKeys, 2000),
    ?assertEqual([], Diffs),
    ?assertEqual([], Ins),
    ?assertEqual([6000, 7000, 8000], Dels).    

cleanup_refresh_only_ins_test() ->
    AllKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    InsertKeys = [{6000, 6}, {7000, 7}, {8000, 8}],
    {#{inserts := Ins, deletes := Dels, differences := Diffs}, _} = complete_cleanup_refresh(AllKeys ++ InsertKeys, AllKeys, 2000),
    ?assertEqual([], Diffs),
    ?assertEqual([6000, 7000, 8000], Ins),
    ?assertEqual([], Dels). 

cleanup_refresh_no_op_test() ->
    AllKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), rand:uniform(5000)} || _ <- lists:seq(1, 2000)]))),
    {#{inserts := Ins, deletes := Dels, differences := Diffs}, _} = complete_cleanup_refresh(AllKeys, AllKeys, 2000),
    ?assertEqual([], Diffs),
    ?assertEqual([], Ins),
    ?assertEqual([], Dels).

cleanup_refresh_no_diff_test() ->
    AllSrcKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), 1} || _ <- lists:seq(1, 2000)]))),
    AllDstKeys = lists:sort(maps:to_list(maps:from_list([{rand:uniform(5000), 1} || _ <- lists:seq(1, 2000)]))),
    {#{differences := Diffs}, _} = complete_cleanup_refresh(AllSrcKeys, AllDstKeys, 2000),
    ?assertEqual([], Diffs).

-endif.
