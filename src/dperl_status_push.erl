-module(dperl_status_push).

-include("dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

% dperl_worker exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, format_status/2, get_status/1, init_state/1]).

-record(state, {cred, links, imem_sess, name, channel, first_sync = true,
                audit_start_time = {0,0}, chunk_size = 200, provs = [],
                active_link = 1, mod, func, imem_connected = false}).

% dperl_strategy_scr export
-export([connect_check_src/1, get_source_events/2, connect_check_dst/1,
         do_cleanup/2, do_refresh/2, fetch_src/2, fetch_dst/2, delete_dst/2,
         insert_dst/3, update_dst/3, report_status/3, is_equal/4]).

get_source_events(#state{channel = Channel,
                          audit_start_time = LastStartTime} = State, BulkSize) ->
    case length(State#state.provs) > 0 of
        true -> {ok, State#state.provs, State#state{provs = []}};
        _ ->
            case dperl_dal:read_audit(Channel, LastStartTime, BulkSize) of
                {LastStartTime, LastStartTime, []} ->
                    if State#state.first_sync == true -> 
                          ?JInfo("Audit rollup is complete"),
                          {ok, sync_complete, State#state{first_sync = false}};
                       true -> {ok, sync_complete, State}
                    end;
                {_StartTime, NextStartTime, []} ->
                    {ok, [], State#state{audit_start_time = NextStartTime}};
                {_StartTime, NextStartTime, Statuses} ->
                    {ok, [K || #{key := K, nval := NV} <- Statuses, NV /= undefined],
                        State#state{audit_start_time = NextStartTime}}
            end
    end.

connect_check_src(State) -> {ok, State}.

connect_check_dst(State) -> ?CONNECT_CHECK_IMEM_LINK(State).

do_cleanup(_, _) -> error(sync_only_job).

do_refresh(_, _) -> error(sync_only_job).

fetch_src(Key, #state{channel = Channel}) ->
    dperl_dal:job_error_close(Key),
    dperl_dal:read_channel(Channel, Key).

fetch_dst(_Key, _State) -> ?NOT_FOUND.

delete_dst(_Key, State) -> {false, State}.

insert_dst([JMod, Chn, SId, Job] = Key, Val, #state{mod = Mod, func = Fun, imem_sess = Sess, 
                                                    channel = Channel} = State) ->
    Count = dperl_dal:count_sibling_jobs(list_to_existing_atom(JMod), Chn),
    case Val of
        #{auditTime := '$do_not_log'} -> {false, State};
        #{auditTime := Time} ->
            FormattedTime = case Time of
                undefined ->
                    undefined;
                Time when is_list(Time) ->
                    list_to_tuple(Time)
            end,
            Status = #{channel => Chn, shortid => SId, job => Job,
                       at => FormattedTime},
            NewStatus = case Val of
                #{error := Error} -> Status#{error => Error};
                _ -> Status
            end,
            case catch Sess:run_cmd(dal_exec, [Mod, Fun, [Count, [NewStatus]]]) of
                {'EXIT', Err} -> 
                    self() ! connect_src_link,
                    dperl_dal:job_error(Key, <<"sync">>, <<"process_staupdate_dsttus">>, Err),
                    ?JError("Status update error ~p", [Err]),
                    {true, State};
                ok -> 
                    case maps:is_key(error, Val) of
                        false -> dperl_dal:remove_from_channel(Channel, Key);
                        true -> no_op
                    end,
                    {false, State};
                Other -> 
                    dperl_dal:job_error(Key, <<"sync">>, <<"process_status">>, Other),
                    ?JWarn("Status update bad return ~p", [Other]),
                    {true, State}
            end
    end.

update_dst(_Key, _Val, State) -> {false, State}.

report_status(_Key, _, _State) -> no_op.

is_equal(_Key, ?NOT_FOUND, _,  _State) -> true;
is_equal(_Key, _Src, _, _State) -> false.

get_status(#state{audit_start_time = LastAuditTime}) ->
    #{lastAuditTime => LastAuditTime}.

init_state([]) -> #state{};
init_state([#dperlNodeJobDyn{state = #{lastAuditTime := LastAuditTime}} | _]) ->
    #state{audit_start_time = LastAuditTime};
init_state([_ | Others]) ->
    init_state(Others).

init({#dperlJob{name=Name, args = _Args, srcArgs = #{channel := Channel},
              dstArgs = #{credential := Credential, links := Links,
              mod := Mod, func := Fun}}, State}) ->
    dperl_dal:create_check_channel(Channel, [audit, {type,map}]),
    ?JInfo("Starting at audit ~s", [dperl_dal:ts_str(State#state.audit_start_time)]),
    {ok, State#state{name=Name, cred = Credential, links = Links,
                     channel = Channel, mod = Mod, func = Fun}};
init({Args, _}) ->
    ?JError("bad start parameters ~p", [Args]),
    {stop, badarg}.

handle_call(Request, _From, State) ->
    ?JWarn("handle_call ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?JWarn("handle_cast ~p", [Request]),
    {noreply, State}.

handle_info(Request, State) ->
    ?JWarn("handle_info ~p", [Request]),
    {noreply, State}.

terminate(Reason, #state{imem_sess = undefined}) ->
    ?JInfo("terminate ~p", [Reason]);
terminate(Reason, #state{imem_sess = Session}) ->
    ?JInfo("terminate ~p", [Reason]),
    try
        Session:close()
    catch
        _:Error ->
        dperl_dal:job_error(<<"terminate">>, <<"terminate">>, Error),
        ?JError("terminate ~p:~p", [Reason, Error])
    end.

code_change(OldVsn, State, Extra) ->
    ?JInfo("code_change ~p: ~p", [OldVsn, Extra]),
    {ok, State}.

format_status(Opt, [PDict, State]) ->
    ?JInfo("format_status ~p: ~p", [Opt, PDict]),
    State.
