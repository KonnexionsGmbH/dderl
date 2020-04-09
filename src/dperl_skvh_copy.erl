-module(dperl_skvh_copy).

-include("dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

% dperl_job exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, format_status/2, get_status/1, init_state/1]).

-record(state, {name, srcImemSess, srcActLink = 1, srcCred, srcLinks, sbucket,
                maxKey, minKey, dstImemSess, dstActLink = 1, dstCred, dstLinks, 
                dbucket, dbucketOpts = [], isDLocal = false, isSLocal = false, 
                first_sync = true, rows = [], audit_start_time = {0,0}}).

% dperl_strategy_scr export
-export([connect_check_src/1, get_source_events/2, connect_check_dst/1,
         do_cleanup/5, do_refresh/2, load_src_after_key/3, load_dst_after_key/3,
         fetch_src/2, fetch_dst/2, delete_dst/2, insert_dst/3, update_dst/3,
         report_status/3]).

connect_check_src(#state{isSLocal = true} = State) -> {ok, State};
connect_check_src(#state{srcActLink = ActiveLink, srcLinks = Links,
                         srcCred = #{user := User, password := Password},
                         srcImemSess = OldSession} = State) ->
    case connect_imem(ActiveLink, Links, User, Password, OldSession) of
        {ok, OldSession} -> {ok, State};
        {ok, Session} -> {ok, State#state{srcImemSess = Session}};
        {error, Error, NewActiveLink} ->
            {error, Error, State#state{srcActLink = NewActiveLink}}
    end.

connect_check_dst(#state{isDLocal = true, dbucketOpts = DBucketOpts,
                         dbucket = DstBucket} = State) ->
    check_channel(undefined, true, DstBucket, DBucketOpts),
    {ok, State};
connect_check_dst(#state{dstActLink = ActiveLink, dstLinks = Links,
                         dstCred = #{user := User, password := Password},
                         dstImemSess = OldSession, dbucketOpts = DBucketOpts,
                         dbucket = DstBucket, isDLocal = IsDLocal} = State) ->
    case connect_imem(ActiveLink, Links, User, Password, OldSession) of
        {ok, OldSession} -> {ok, State};
        {ok, Session} ->
            check_channel(Session, IsDLocal, DstBucket, DBucketOpts),
            {ok, State#state{dstImemSess = Session}};
        {error, Error, NewActiveLink} ->
            {error, Error, State#state{dstActLink = NewActiveLink}}
    end.

get_source_events(#state{srcImemSess = Session, sbucket = SrcBucket, isSLocal = IsSLocal,
                          audit_start_time = LastStartTime} = State, BulkSize) ->
    case length(State#state.rows) > 0 of
        true -> {ok, State#state.rows, State#state{rows = []}};
        _ ->
            case catch run_cmd(audit_readGT, [SrcBucket, LastStartTime, BulkSize], 
                               IsSLocal, Session) of
                {error, _} = Error -> Error;
                {'EXIT', Error} -> {error, Error};
                [] ->
                    if State#state.first_sync == true -> 
                          ?JInfo("Audit rollup is complete"),
                          {ok, sync_complete, State#state{first_sync = false}};
                       true -> {ok, sync_complete, State}
                    end;
                Audits ->
                    #{time := NextStartTime} = lists:last(Audits),
                    {ok, filter_keys(Audits, State), State#state{
                           audit_start_time = NextStartTime}}
            end
    end.

load_src_after_key(CurKey, BlkCount,
                   #state{srcImemSess = SSession, sbucket = SrcBucket,
                          maxKey = MaxKey, minKey = MinKey, isSLocal = IsSLocal}) ->
    load_after_key(SrcBucket, CurKey, MaxKey, MinKey, BlkCount, IsSLocal, SSession).

load_dst_after_key(CurKey, BlkCount,
                   #state{dstImemSess = DSession, dbucket = DstBucket,
                          maxKey = MaxKey, minKey = MinKey, isDLocal = IsDLocal}) ->
    load_after_key(DstBucket, CurKey, MaxKey, MinKey, BlkCount, IsDLocal, DSession).

do_cleanup(Deletes, Inserts, Diffs, IsFinished, State) ->
    NewState = State#state{rows = Inserts ++ Diffs ++ Deletes},
    if IsFinished -> {ok, finish, NewState};
       true -> {ok, NewState}
    end.

do_refresh(_State, _) -> error(not_implemented).

fetch_src(Key, #state{srcImemSess = Session, sbucket = Bucket, isSLocal = IsLocal}) ->
    dperl_dal:job_error_close(Key),
    get_value(<<"fetch_src">>, Session, IsLocal, Bucket, Key).

fetch_dst(Key, #state{dstImemSess = Session, dbucket = Bucket, isDLocal = IsLocal}) ->
    get_value(<<"fetch_src">>, Session, IsLocal, Bucket, Key).

delete_dst(Key, #state{dstImemSess = Session, dbucket = Bucket, isDLocal = IsLocal} = State) ->
    case run_cmd(delete, [Bucket, imem_datatype:term_to_io(Key)], IsLocal, Session) of
        {error, Error} ->
            ?JError("imem_dal_skvh:delete(~p,[~p]) ~p",
                    [Bucket, imem_datatype:term_to_io(Key), Error]),
            dperl_dal:job_error(Key, <<"sync">>, <<"delete">>, Error),
            {true, State};
        _ -> {false, State}
    end.

insert_dst(Key, Val, State) -> update_dst(Key, Val, State).

update_dst(Key, Val, #state{dstImemSess = Session, dbucket = Bucket, isDLocal = IsLocal} = State) ->
    case catch run_cmd(write, [Bucket, Key, Val], IsLocal, Session) of
        {error, Error} ->
            ?JError("imem_dal_skvh:write(~p,~p,~p) ~p",
                    [Bucket, Key, Val, Error]),
            dperl_dal:job_error(Key, <<"sync">>, <<"update_dst">>, Error),
            {true, State};
        _ -> {false, State}
    end.

report_status(_Key, _Status, _State) -> no_op.

get_status(#state{audit_start_time = LastAuditTime}) ->
    #{lastAuditTime => LastAuditTime}.

init_state([]) -> #state{};
init_state([#dperlNodeJobDyn{state = #{lastAuditTime := LastAuditTime}} | _]) ->
    #state{audit_start_time = LastAuditTime};
init_state([_ | Others]) ->
    init_state(Others).

init({#dperlJob{name=Name, srcArgs = SrcArgs, dstArgs = DstArgs}, State}) ->
    ?JInfo("Starting ~s...", [Name]),
    DBucketOpts = maps:get(dbucketOpts, DstArgs, [audit,history]),
    State1 = load_src_args(SrcArgs, State),
    State2 = load_dst_args(DstArgs, State1),
    MinKey = maps:get(minKey, SrcArgs, -1),
    MaxKey = maps:get(maxKey, SrcArgs, [<<255>>]),
    {ok, State2#state{
            name = Name, maxKey = MaxKey, minKey = MinKey,
            dbucketOpts = DBucketOpts}};
init({Args, _}) ->
    ?JError("bad start parameters ~p", [Args]),
    {stop, badarg}.

handle_call(Request, _From, State) ->
    ?JWarn("Unsupported handle_call ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?JWarn("Unsupported handle_cast ~p", [Request]),
    {noreply, State}.

handle_info(Request, State) ->
    ?JWarn("Unsupported handle_info ~p", [Request]),
    {noreply, State}.

terminate(Reason, #state{srcImemSess = SrcSession, dstImemSess = DstSession}) ->
    try
        [Session:close() || Session <- [SrcSession, DstSession], Session /= undefined],
        ?JInfo("terminate ~p", [Reason])
    catch
        _:Error ->
        dperl_dal:job_error(<<"terminate">>, <<"terminate">>, Error),
        ?JError("terminate ~p:~p ~p",
                [Reason, Error, erlang:get_stacktrace()])
    end.

code_change(OldVsn, State, Extra) ->
    ?JInfo("code_change ~p: ~p", [OldVsn, Extra]),
    {ok, State}.

format_status(Opt, [PDict, State]) ->
    ?JInfo("format_status ~p: ~p", [Opt, PDict]),
    State.

connect_imem(_ActiveLink, [], _User, _Password, _OldSession) -> {error, no_links, -1};
connect_imem(ActiveLink, Links, User, Password, OldSession) ->
    #{schema := Schema} = lists:nth(ActiveLink, Links),
    case catch OldSession:run_cmd(schema, []) of
        Schema -> {ok, OldSession};
        _ ->
            catch OldSession:close(),
            case dperl_dal:connect_imem_link(
                   ActiveLink, Links, User, Password) of
                {ok, Session, _} ->
                    {ok, Session};
                {Error, NewActiveLink} ->
                    {error, Error, NewActiveLink}
            end
    end.

check_channel(Session, IsLocal, Bucket, BucketOpts) ->
    case catch run_cmd(create_check_skvh, [Bucket, BucketOpts], IsLocal, Session) of
        {'EXIT', Error} -> {error, Error};
        {error, Error} ->
            ?JError("imem_dal_skvh:create_check_skvh(~p,~p) ~p",
                    [Bucket, BucketOpts, Error]),
            {error, Error};
        _ -> ok
    end.

get_value(Op, Session, IsLocal, Bucket, Key) ->
    case run_cmd(read, [Bucket, [Key]], IsLocal, Session) of
        [] -> ?NOT_FOUND;
        [#{cvalue := Value}] -> Value;
        {error, Error} ->
            ?JError("imem_dal_skvh:read(~p,~p) ~p", [Bucket, [Key], Error]),
            dperl_dal:job_error(Key, <<"sync">>, Op, Error),
            error
    end.

load_src_args(#{channel := SChannel}, State) ->
    State#state{sbucket = to_binary(SChannel), isSLocal = true};
load_src_args(#{credential := SrcCred, links := SrcLinks, sbucket := SBucket}, State) ->
    State#state{sbucket = to_binary(SBucket), srcCred = SrcCred, srcLinks = SrcLinks}.

load_dst_args(#{channel := DChannel}, State) ->
    State#state{dbucket = to_binary(DChannel), isDLocal = true};
load_dst_args(#{credential := DstCred, links := DstLinks, dbucket := DBucket}, State) ->
    State#state{dbucket = to_binary(DBucket), dstCred = DstCred, dstLinks = DstLinks}.

to_binary(Channel) when is_list(Channel) -> list_to_binary(Channel);
to_binary(Channel) when is_binary(Channel) -> Channel;
to_binary(Channel) -> Channel.

run_cmd(Cmd, Args, true, _) -> erlang:apply(imem_dal_skvh, Cmd, [system | Args]);
run_cmd(Cmd, Args, _, Session) -> Session:run_cmd(dal_exec, [imem_dal_skvh, Cmd, Args]).

load_after_key(Bucket, CurKey, MaxKey, MinKey, BlkCount, IsLocal, Session) when MinKey == CurKey ->
    get_key_hashes(Bucket, CurKey, MaxKey, BlkCount, IsLocal, Session, MinKey == CurKey);
load_after_key(Bucket, CurKey, MaxKey, MinKey, BlkCount, IsLocal, Session) when is_integer(CurKey) ->
    load_after_key(Bucket, MinKey, MaxKey, MinKey, BlkCount, IsLocal, Session);
load_after_key(Bucket, CurKey, MaxKey, MinKey, BlkCount, IsLocal, Session) ->
    get_key_hashes(Bucket, CurKey, MaxKey, BlkCount, IsLocal, Session, MinKey == CurKey).

get_key_hashes(Bucket, FromKey, MaxKey, Count, IsLocal, Session, IsFirstTime) ->
    case catch run_cmd(readGELTHashes, [Bucket, FromKey, MaxKey, Count + 1], 
                       IsLocal, Session) of
        [{FromKey, _} | Keys] = KeyHashes -> 
            if IsFirstTime -> lists:sublist(KeyHashes, Count);
               true -> Keys
            end;
        KeyHashes when is_list(KeyHashes) -> lists:sublist(KeyHashes, Count);
        Error -> error(Error)
    end.

filter_keys(Keys, #state{minKey = MinKey, maxKey = MaxKey}) ->
    lists:foldr(
        fun(#{ckey := Key}, Acc) when Key >= MinKey andalso Key =< MaxKey ->
                case lists:member(Key, Acc) of
                    true -> Acc;
                    false -> [Key | Acc]
                end;
           (_, Acc) -> Acc
        end, [], Keys).