-module(dperl_worker).

-include("dperl.hrl").

-behavior(gen_server).

% behavior export
-export([behaviour_info/1]).

% interface export
-export([start_link/3, start/1, stop/2, list/1, child_spec/2, child_id/2,
         is_alive/2]).

% gen_server exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, format_status/2]).

-record(st, {n, m, ms, js}).

behaviour_info(callbacks) -> [{get_status,1}, {init_state,1}
                              | gen_server:behaviour_info(callbacks)];
behaviour_info(Type) -> gen_server:behaviour_info(Type).

start(JobOrService) when is_record(JobOrService, dperlJob);
                         is_record(JobOrService, dperlService) ->

    Mod  = ?G(JobOrService,module),
    Name = ?G(JobOrService,name),
    Behaviors =
    lists:reverse(
      lists:foldl(
        fun({behavior,[Behavior]}, Acc) ->
                case code:ensure_loaded(Behavior) == {module, Behavior} andalso
                     erlang:function_exported(Behavior, execute, 4) of
                    true -> [Behavior|Acc];
                    _ -> Acc
                end;
           (_, Acc) -> Acc
        end, [], Mod:module_info(attributes))),
    Sup = ?SUP(JobOrService),
    ChildId = {Mod, Name},
    ChildSpec = #{id => ChildId, restart => transient, shutdown => 30000,
                  type => worker, modules => [Mod],
                  start => {?MODULE, start_link, [JobOrService, Behaviors, Sup]}},
    case supervisor:get_childspec(Sup, ChildId) of
        {ok, ChildSpec} -> supervisor:restart_child(Sup, ChildId);
        {ok, _} ->
            supervisor:terminate_child(Sup, ChildId),
            supervisor:delete_child(Sup, ChildId),
            start(JobOrService);
        {error, not_found} ->
            case supervisor:start_child(Sup, ChildSpec) of
                {ok, _} = Result -> Result;
                {ok, _, _} = Result -> Result;
                {error, {Error, _Child}} ->
                    ?Error([{enum, dperl_dal:to_atom(Name)}],
                           "~p starting ~p_~s: ~p", [Sup, Mod, Name, Error], []),
                    error(Error);
                {error, Error} ->
                    ?Error([{enum, dperl_dal:to_atom(Name)}],
                           "~p starting ~p_~s: ~p", [Sup, Mod, Name, Error], []),
                    error(Error)
            end
    end.

stop(Mod, Name) ->
    case get_supervisor(Mod, Name) of
        not_found ->
            {error, not_found};
        Sup ->
            case supervisor:terminate_child(Sup, {Mod, Name}) of
                ok ->
                    case supervisor:delete_child(Sup, {Mod, Name}) of
                        ok -> ok;
                        {error, Error} ->
                            ?Error("~p stop ~p_~s (delete_child) : ~p",
                                   [Sup, Mod, Name, Error]),
                            {error, Error}
                    end;
                {error, not_found} -> {error, not_found};
                {error, Error} ->
                    ?Error("~p stop ~p_~s (terminate_child) : ~p",
                           [Sup, Mod, Name, Error]),
                    error(Error)
            end
    end.

get_supervisor(Mod, Name) ->
    case whereis(child_id(Mod, Name)) of
        Pid when is_pid(Pid) ->
            {dictionary, Dictionary} = erlang:process_info(Pid, dictionary),
            case lists:keyfind(supervisor, 1, Dictionary) of
                {supervisor, Sup} ->
                    Sup;
                _ ->
                    not_found
            end;
        undefined ->
            not_found
    end.

list(job) ->
    list(whereis(dperl_job_sup));
list(service) ->
    list(whereis(dperl_service_sup));
list(undefined) ->
    [];
list(Pid) when is_pid(Pid) ->
    case erlang:process_info(Pid, links) of
        {links, [_]} -> [];
        {links, Links} ->
            lists:map(
                fun(ChildPid) ->
                    {dictionary, Dictionary} = erlang:process_info(ChildPid, dictionary),
                    {name, Name} = lists:keyfind(name, 1, Dictionary),
                    {module, Mod} = lists:keyfind(module, 1, Dictionary),
                    {Mod, Name}
                end, Links -- [whereis(dperl_sup)]);
        _ -> []
    end.

child_spec(job, Id) -> supervisor:get_childspec(dperl_job_sup, Id);
child_spec(service, Id) -> supervisor:get_childspec(dperl_service_sup, Id).

is_alive(Type, Name) when Type == job; Type == service ->
    lists:any(
        fun({_, JobOrServiceName}) ->
            Name == JobOrServiceName
        end, list(Type)
    ).

start_link(JobOrService, Behaviors, Sup)
    when is_record(JobOrService, dperlJob);
         is_record(JobOrService, dperlService) ->
    Mod  = ?G(JobOrService,module),
    Name = ?G(JobOrService,name),
    Opts = ?G(JobOrService,opts),
    Sup  = ?SUP(JobOrService),
    case gen_server:start_link({local, child_id(Mod, Name)}, ?MODULE,
                               {JobOrService, Behaviors, Sup},
                               Opts) of
        {ok, Pid} ->
            ?Debug([{enum, dperl_dal:to_atom(Name)}],
                   "~p started ~p_~s: ~p", [Sup, Mod, Name, Pid]),
            {ok, Pid};
        {error, Error} ->
            ?Error([{enum, dperl_dal:to_atom(Name)}],
                   "~p starting ~p_~s: ~p", [Sup, Mod, Name, Error], []),
            {error, Error};
        Other ->
            ?Error([{enum, dperl_dal:to_atom(Name)}],
                   "~p starting ~p_~s: ~p", [Sup, Mod, Name, Other], []),
            Other
    end.

-spec child_id(atom() | list(), list() | binary()) -> atom().
child_id(M, C) when is_atom(M) -> child_id(atom_to_list(M), C);
child_id(M, C) when is_binary(C) -> child_id(M, binary_to_list(C));
child_id(M, C) when is_list(M), is_list(C) -> list_to_atom(M++"_"++C).

init({JobOrService, Behaviors, Sup})
    when is_record(JobOrService, dperlJob);
         is_record(JobOrService, dperlService) ->
    Mod  = ?G(JobOrService,module),
    Name = ?G(JobOrService,name),
    Args = ?G(JobOrService,args),
    %% to get the following keys in oci process add it explicitly in
    %% dperl_dal:oci_opts
    put(debug, maps:get(debug, Args, false)),
    put(jstate, s),
    put(module, Mod),
    put(name, Name),
    put(jname, dperl_dal:to_atom(Name)),
    put(supervisor, Sup),
    ?Debug("starting ~p_~s", [Mod, Name]),
    process_flag(trap_exit, true),
    Bhaves = [self() ! {internal, {behavior, B, Args}} || B <- Behaviors],
    DefaultModState = Mod:init_state(dperl_dal:job_state(Name)),
    Type = ?RC(JobOrService),
    JobOrServiceSortedLinks =
    case JobOrService of
        #dperlJob{srcArgs = #{links := UnsortedSrcLinks} = SrcArgs,
                 dstArgs = #{links := UnsortedDstLinks} = DstArgs} ->
            JobOrService#dperlJob{
              srcArgs = SrcArgs#{links => dperl_dal:sort_links(UnsortedSrcLinks)},
              dstArgs = DstArgs#{links => dperl_dal:sort_links(UnsortedDstLinks)}};
        #dperlJob{dstArgs = #{links := UnsortedLinks} = DstArgs} ->
            JobOrService#dperlJob{dstArgs = DstArgs#{links => dperl_dal:sort_links(UnsortedLinks)}};
        #dperlJob{srcArgs = #{links := UnsortedLinks} = SrcArgs} ->
            JobOrService#dperlJob{srcArgs = SrcArgs#{links => dperl_dal:sort_links(UnsortedLinks)}};
        _ -> JobOrService
    end,
    case catch Mod:init({JobOrServiceSortedLinks, DefaultModState}) of
        {'EXIT', Reason} ->
            {stop, {shutdown, Reason}};
        {ok, ModState} ->
            update_state(Type, Name, Mod, Bhaves, ModState),
            dperl_dal:set_running(JobOrService, true),
            {ok, #st{n = Name, m = Mod, ms = ModState, js = JobOrService}};
        {ok, ModState, Timeout} ->
            update_state(Type, Name, Mod, Bhaves, ModState),
            dperl_dal:set_running(JobOrService, true),
            {ok, #st{n = Name, m = Mod, ms = ModState, js = JobOrService}, Timeout};
        Error ->
            dperl_dal:worker_error(Type, <<"init">>, <<"init">>, Error),
            Error
    end.

handle_call({internal, Args}, From, St) -> handle_call_i(Args, From, St);
handle_call(Normal, From, #st{m = Mod, ms = ModState} = St) ->
    case catch Mod:handle_call(Normal, From, ModState) of
        {'EXIT', Reason} ->
            {stop, {shutdown, Reason}, St};
        {reply, Reply, NewModState} ->
            {reply, Reply, St#st{ms = NewModState}};
        {reply, Reply, NewModState, Timeout} ->
            {reply,Reply,St#st{ms = NewModState},Timeout};
        {noreply, NewModState} ->
            {noreply, St#st{ms = NewModState}};
        {noreply, NewModState, Timeout} ->
            {noreply, St#st{ms = NewModState}, Timeout};
        {stop,Reason,Reply,NewModState} ->
            {stop,Reason,Reply,St#st{ms = NewModState}};
        {stop, Reason, NewModState} ->
            {stop, Reason, St#st{ms = NewModState}}
    end.

handle_cast({internal, Args}, St) -> handle_cast_i(Args, St);
handle_cast(Normal, #st{m = Mod, ms = ModState} = St) ->
    case catch Mod:handle_cast(Normal, ModState) of
        {'EXIT', Reason} ->
            {stop, {shutdown, Reason}, St};
        {noreply, NewModState} ->
            {noreply, St#st{ms = NewModState}};
        {noreply, NewModState, Timeout} ->
            {noreply, St#st{ms = NewModState}, Timeout};
        {stop, Reason, NewModState} ->
            {stop, Reason, St#st{ms = NewModState}}
    end.

handle_info({internal, Args}, St) -> handle_info_i(Args, St);
handle_info(Normal, #st{m = Mod, ms = ModState} = St) ->
    case catch Mod:handle_info(Normal, ModState) of
        {'EXIT', Reason} ->
            {stop, {shutdown, Reason}, St};
        {noreply, NewModState} ->
            {noreply, St#st{ms = NewModState}};
        {noreply, NewModState, Timeout} ->
            {noreply, St#st{ms = NewModState}, Timeout};
        {stop, Reason, NewModState} ->
            {stop, Reason, St#st{ms = NewModState}}
    end.

code_change(OldVsn, #st{m = Mod, ms = ModState} = St, Extra) ->
    case catch Mod:code_change(OldVsn, ModState, Extra) of
        {'EXIT', Reason} ->
            {error, Reason};
        {ok, NewModState} ->
            {ok, St#st{ms = NewModState}};
        {error, Reason} ->
            {error, Reason}
    end.

format_status(Opt, [PDict, #st{m = Mod, ms = ModState}]) ->
    case catch Mod:format_status(Opt, [PDict, ModState]) of
        {'EXIT', Reason} ->
            ?JError("format_status ~p", [Reason]);
        Result -> Result
    end.

terminate(Reason, #st{m = Mod, ms = ModState, n = Name, js = JobOrService}) ->
    State = Mod:get_status(ModState),
    case ?RC(JobOrService) of
        job -> dperl_dal:update_job_dyn(Name, State, stopped);
        service -> dperl_dal:update_service_dyn(Name, State, stopped)
    end,
    dperl_dal:set_running(JobOrService, false),
    case catch Mod:terminate(Reason, ModState) of
        {'EXIT', Reason} ->
            ?JError("terminate ~p", [Reason]);
        Result -> Result
    end.

handle_call_i(Args, _From, St) ->
    ?JError("handle_call_i(~p)", [Args]),
    {reply, ok, St}.

handle_cast_i(Args, St) ->
    ?JError("handle_cast_i(~p)", [Args]),
    {noreply, St}.

handle_info_i({behavior, BMod, Args}, St) ->
    case catch BMod:execute(St#st.m, St#st.n, St#st.ms, Args) of
        {'EXIT', Reason} -> {stop, {shutdown, Reason}, St};
        Ms -> {noreply, St#st{ms = Ms}}
    end;
handle_info_i(Args, St) ->
    ?JError("handle_info_i(~p)", [Args]),
    {noreply, St}.

update_state(job, Name, Mod, Bhaves, ModuleState) ->
    dperl_dal:job_error_close(),     
    if length(Bhaves) < 1 ->
           dperl_dal:update_job_dyn(Name, Mod:get_status(ModuleState), synced);
       true -> 
           dperl_dal:update_job_dyn(Name, Mod:get_status(ModuleState), undefined)
    end;
update_state(service, _Name, _Mod, _Behaves, _ModuleState) -> no_op.
