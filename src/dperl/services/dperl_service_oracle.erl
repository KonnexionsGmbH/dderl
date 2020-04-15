-module(dperl_service_oracle).

-include("dperl_service_oracle.hrl").

-behavior(dperl_worker).
-behavior(cowboy_middleware).
-behavior(cowboy_loop).

% dperl_worker exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, format_status/2, get_status/1, init_state/1]).

-record(state, {name, resource, baseUrl, active_link = 1, stmt_profile_del,
                stmt_ts_get, stmt_ts_post, stmt_ts_put, stmt_ts_reset,
                stmt_ts_revert, stmt_bar_get, stmt_bar_put, stmt_bar_post,
                pool, c_whitelist = #{}, listeners = [], tenants = #{}, port,
                close_statement = [], close_session = [], dynstate = #{}}).

% cowboy rest exports
-export([init/2, info/3, execute/2]).

-define(API_VERSION, "1.0.0").

init_state(_) -> #state{}.

get_status(#state{dynstate = DynState}) -> DynState.

init({#dperlService{name = SName, args = Args, resource = Resource,
                   interface = Interface},
      State}) ->
    case init_resources(
           Resource,
           State#state{name = SName, resource = Resource,
                       dynstate = #{start => imem_meta:time(), req => 0,
                                    error => #{}}}) of
        {ok, State1} ->
            Interface1 = maps:merge(Interface, Args),
            case init_interface(Interface1, State1) of
                {ok, State2} ->
                    erlang:send_after(?SERVICE_UPDATE_PERIOD(SName), self(), update_dyn),
                    erlang:send_after(?SERVICE_STATUS_RESET_PERIOD(SName), self(), reset_dyn),
                    dperl_dal:update_service_dyn(
                      State2#state.name, State2#state.dynstate,
                      ?SERVICE_ACTIVE_THRESHOLD(SName), ?SERVICE_OVERLOAD_THRESHOLD(SName)),
                    {ok, State2};
                Error ->
                    erlocipool:del(State1#state.pool),
                    {stop, Error}
            end;
        Error -> {stop, Error}
    end;
init({Args, _}) ->
    ?SError("bad start parameters ~p", [Args]),
    {stop, badarg}.

handle_call(Request, _From, State) ->
    ?SWarn("Unsupported handle_call ~p", [Request]),
    {reply, ok, State}.

% common
handle_cast(#{operation := {profile, delete}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"comment">> := Comment,
                        <<"requestor">> := Requestor}},
            #state{stmt_profile_del = ProfileDelStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH,
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH ->
    Resp = db_call({stmt_profile_del, ProfileDelStmt},
                    [Tenant, Msisdn, Requestor, Comment]),
    RespPid ! {reply, Resp},
    {noreply, State};
%% topstopper
handle_cast(#{operation := {topstopper, get}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant}, State) ->
    Resp = db_call({stmt_ts_get, State#state.stmt_ts_get}, [Tenant, Msisdn]),
    RespPid ! {reply, Resp},
    {noreply, State};
handle_cast(#{operation := {topstopper, post}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"comment">> := Comment,
                        <<"requestor">> := Requestor}},
            #state{stmt_ts_post = TsPostStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH,
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH ->
    Resp = db_call({stmt_ts_post, TsPostStmt}, [Tenant, Msisdn, Requestor,
                                                Comment]),
    RespPid ! {reply, Resp},
    {noreply, State};
handle_cast(#{operation := {topstopper, put}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"comment">> := Comment,
                        <<"requestor">> := Requestor,
                        <<"type">> := Type,
                        <<"limit">> := Limit}},
            #state{stmt_ts_put = TsPutStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH andalso
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH andalso
       (Type == <<"swisscom">> orelse Type == <<"customer">>) andalso 
       (Limit >= -1 andalso Limit =< 100000) ->
    Resp = db_call({stmt_ts_put, TsPutStmt},
                   [Tenant, Msisdn, Type,
                    dderloci_utils:oranumber_encode(
                      list_to_binary(io_lib:format("~p", [Limit]))),
                    Requestor, Comment]),
    RespPid ! {reply, Resp},
    {noreply, State};
handle_cast(#{operation := {topstopper, reset}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"comment">> := Comment,
                       <<"requestor">> := Requestor,
                       <<"type">> := Type}},
            #state{stmt_ts_reset = TsResetStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH andalso
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH andalso
       (Type == <<"swisscom">> orelse Type == <<"customer">>) ->
    Resp = db_call({stmt_ts_reset, TsResetStmt}, [Tenant, Msisdn, Type,
                                                  Requestor, Comment]),
    RespPid ! {reply, Resp},
    {noreply, State};
handle_cast(#{operation := {topstopper, revert}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"comment">> := Comment,
                        <<"requestor">> := Requestor,
                        <<"type">> := Type}},
            #state{stmt_ts_revert = TsRevertStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH andalso
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH andalso
       (Type == <<"swisscom">> orelse Type == <<"customer">>) ->
    Resp = db_call({stmt_ts_revert, TsRevertStmt}, [Tenant, Msisdn, Type,
                                                    Requestor, Comment]),
    RespPid ! {reply, Resp},
    {noreply, State};
%% barring
handle_cast(#{operation := {barring, get}, reply := RespPid, 
              msisdn := Msisdn, tenant := Tenant}, State) ->
    Resp = db_call({stmt_bar_get, State#state.stmt_bar_get}, [Tenant, Msisdn]),
    RespPid ! {reply, Resp},
    {noreply, State};
handle_cast(#{operation := {barring, post}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"comment">> := Comment,
                        <<"requestor">> := Requestor}},
            #state{stmt_bar_post = BarPostStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH,
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH ->
    Resp = db_call({stmt_bar_post, BarPostStmt}, [Tenant, Msisdn, Comment, Requestor]),
    RespPid ! {reply, Resp},
    {noreply, State};
handle_cast(#{operation := {barring, put}, reply := RespPid,
              msisdn := Msisdn, tenant := Tenant,
              data := #{<<"type">> := Type, <<"barring">> := Barring,
                        <<"comment">> := Comment, <<"requestor">> := Requestor}},
            #state{stmt_bar_put = BarPutStmt} = State)
  when byte_size(Comment) =< ?MAX_COMMENT_LENGTH andalso
       byte_size(Requestor) =< ?MAX_REQUESTOR_LENGTH andalso
       (Type == <<"swisscom">> orelse Type == <<"customer">>) andalso
       (Barring == 0 orelse Barring == 6 orelse Barring == 9) ->
    Resp = db_call({stmt_bar_put, BarPutStmt}, [Tenant, Msisdn, Type, Barring, Comment, Requestor]),
    RespPid ! {reply, Resp},
    {noreply, State};
%% 
handle_cast(#{reply := RespPid}, State) ->
    RespPid ! {reply, bad_req},
    {noreply, State};
handle_cast(Request, State) ->
    ?SWarn("Unsupported handle_cast ~p", [Request]),
    {noreply, State}.

-define(STMT_REBUILD(__SQL, __BINDS, __POOL, __STMT, __STATE),
        (__STATE#state.__STMT):close(),
        __STATE#state{__STMT = create_stmt(__POOL, __SQL, __BINDS)}).

handle_info(update_dyn, #state{dynstate = Ds, name = SName} = State) ->
    dperl_dal:update_service_dyn(
      State#state.name, Ds, ?SERVICE_ACTIVE_THRESHOLD(SName),
      ?SERVICE_OVERLOAD_THRESHOLD(SName)),
    erlang:send_after(?SERVICE_UPDATE_PERIOD(SName), self(), update_dyn),
    {noreply, State};
handle_info(reset_dyn, #state{name = SName} = State) ->
    NewDynState = (State#state.dynstate)#{req => 0, error => #{}},
    dperl_dal:update_service_dyn(
      State#state.name, NewDynState, ?SERVICE_ACTIVE_THRESHOLD(SName),
      ?SERVICE_OVERLOAD_THRESHOLD(SName)),
    erlang:send_after(?SERVICE_STATUS_RESET_PERIOD(SName), self(), reset_dyn),
    {noreply, State#state{dynstate = NewDynState}};
handle_info({error, StmtType, Code, Message},
            #state{close_session = CloseSessionsErrors, listeners = Listeners,
                   close_statement = CloseStatementErrors, port = Port,
                   name = SName, pool = Pool} = State) ->
    case lists:member(Code, CloseStatementErrors) of
        true ->
            ?SError("statement rebuild : ORA-~p ~s", [Code, Message]),
            {noreply,
             case StmtType of
                 stmt_profile_del ->
                     ?STMT_REBUILD(?PROFILE_DELETE_SQL,
                                   ?PROFILE_DELETE_TOPSTOPPER_POST_BINDS,
                                   Pool, stmt_profile_del, State);
                 stmt_ts_get ->
                     ?STMT_REBUILD(?TOPSTOPPER_GET_SQL,
                                   ?TOPSTOPPER_BARRING_GET_BINDS,
                                   Pool, stmt_ts_get, State);
                 stmt_ts_post ->
                     ?STMT_REBUILD(?TOPSTOPPER_POST_SQL,
                                   ?PROFILE_DELETE_TOPSTOPPER_POST_BINDS,
                                   Pool, stmt_ts_post, State);
                 stmt_ts_put -> 
                     ?STMT_REBUILD(?TOPSTOPPER_PUT_SQL,
                                   ?TOPSTOPPER_PUT_BINDS,
                                   Pool, stmt_ts_put, State);
                 stmt_ts_reset -> 
                     ?STMT_REBUILD(?TOPSTOPPER_RESET_SQL,
                                   ?TOPSTOPPER_RESET_REVERT_BINDS,
                                   Pool, stmt_ts_reset, State);
                 stmt_ts_revert -> 
                     ?STMT_REBUILD(?TOPSTOPPER_REVERT_SQL,
                                   ?TOPSTOPPER_RESET_REVERT_BINDS,
                                   Pool, stmt_ts_revert, State);
                 stmt_bar_get ->
                     ?STMT_REBUILD(?BARRING_GET_SQL,
                                   ?TOPSTOPPER_BARRING_GET_BINDS,
                                   Pool, stmt_bar_get, State);
                 stmt_bar_post ->
                     ?STMT_REBUILD(?BARRING_POST_SQL,
                                   ?BARRING_POST_BINDS,
                                   Pool, stmt_bar_post, State);
                 stmt_bar_put ->
                     ?STMT_REBUILD(?BARRING_PUT_SQL,
                                   ?BARRING_PUT_BINDS,
                                   Pool, stmt_bar_put, State)
             end};
        _ ->
            case lists:member(Code, CloseSessionsErrors) of
                true ->
                    ?SError("pool restart ORA-~p ~s", [Code, Message]),
                    lists:map(
                      fun(Ip) ->
                              case catch ranch:set_max_connections(
                                     {Ip, Port}, 0) of
                                  ok -> ok;
                                  Error ->
                                      ?SError("stop accept ~p on port ~p : ~p",
                                              [Ip, Port, Error])
                              end
                      end, Listeners),
                    erlocipool:del(Pool),
                    case init_resources(State#state.resource, State) of
                        {ok, State1} ->
                            lists:map(
                              fun(Ip) ->
                                      case catch ranch:set_max_connections(
                                             {Ip, Port}, ?SERVICE_MAXCONNS(SName)) of
                                          ok -> ok;
                                          Error ->
                                              ?SError("start accept ~p on port ~p : ~p",
                                                      [Ip, Port, Error])
                                      end
                              end, Listeners),
                            {noreply, State1};
                        Error -> {stop, {resource,  Error}, State}
                    end;
                _ ->
                    ?SError("Unhandled ~p : ~s", [Code, Message]),
                    {noreply, State}
            end
    end;
handle_info(count_request, #state{dynstate = Ds} = State) ->
    NewDs = Ds#{req => maps:get(req, Ds, 0) + 1},
    {noreply, State#state{dynstate = NewDs}};
handle_info({count_error, HttpRInt},
            #state{dynstate = #{error := Error} = Ds} = State) ->
    NewDs = Ds#{error => Error#{HttpRInt => maps:get(HttpRInt, Error, 0) + 1}},
    {noreply, State#state{dynstate = NewDs}};
handle_info(stop, State) ->
    {stop, normal, State};
handle_info(Request, State) ->
    ?SWarn("Unsupported handle_info ~p", [Request]),
    {noreply, State}.

terminate(Reason, #state{listeners = Listeners, port = Port, pool = Pool}) ->
    erlocipool:del(Pool),
    stop_listeners(Reason, Listeners, Port),
    ?SInfo("terminate ~p", [Reason]).

code_change(OldVsn, State, Extra) ->
    ?SInfo("code_change ~p: ~p", [OldVsn, Extra]),
    {ok, State}.

format_status(Opt, [PDict, State]) ->
    ?SInfo("format_status ~p: ~p", [Opt, PDict]),
    State.

db_call({StmtType, Stmt}, Params) ->
    self() ! count_request,
    case Stmt:exec_stmt([list_to_tuple([?RESP_BUFFER|Params])]) of
        {executed, _, [{_,<<"{\"errorCode\":",_:8,HttpR:3/binary,_/binary>>=Resp}]} ->
            HttpRInt = binary_to_integer(HttpR),
            if HttpRInt >= 400 andalso HttpRInt < 500 ->
                   ?SInfo("~p: ~s", [HttpRInt, Resp]);
               HttpRInt >= 500 ->
                   ?SWarn("~p: ~s", [HttpRInt, Resp])
            end,
            self() ! {count_error, HttpRInt},
            {HttpRInt, Resp};
        {executed, _, [{_,Resp}]} -> {200, Resp};
        {error, {Code, Message}} ->
            self() ! {error, StmtType, Code, Message},
            {500,
              #{errorCode => 2500,
                errorMessage => <<"Internal Server Error">>,
                errorDetails => Message}};
        {error, Reason} ->
            ?SError("~p (~p) : ~p", [StmtType, Params, Reason]),
            self() ! stop,
            {500,
              #{errorCode => 2500,
                errorMessage => <<"Internal Server Error">>,
                errorDetails => <<"See server error logs for details">>}}
    end.

init_interface(#{baseUrl := BaseUrl, commonWhitelist := CWhiteList,
                 listenerAddresses := LAddresses, tenants := Tenants,
                 port := Port, ssl := #{cert := Cert, key := Key}} = Intf,
               #state{name = SName} = State) ->
    MaxAcceptors = maps:get(max_acceptors, Intf, ?SERVICE_MAXACCEPTORS(SName)),
    MaxConnections = maps:get(max_connections, Intf, ?SERVICE_MAXCONNS(SName)),
    Opts = #{resource => self(), whitelist => maps:keys(CWhiteList),
             tenants => Tenants, name => State#state.name},
    FilteredListenerIps = local_ips(LAddresses),
    Base =
    case hd(BaseUrl) of
        $/ -> BaseUrl;
        _ -> "/" ++ BaseUrl
    end,
    try
        lists:map(fun(Ip) ->
            Dispatch =
            cowboy_router:compile(
                [{'_',
                 [{Base++"/swagger/", ?MODULE, {swagger, Base, SName}},
                  {Base++"/swagger/brand.json", cowboy_static, {priv_file, dperl, "brand.json"}},
                  {Base++"/swagger/swisscom.png", cowboy_static, {priv_file, dperl, "swisscom.png"}},
                  {Base++"/swagger/[...]", cowboy_static, {swagger_static, SName}},
                  {Base++"/" ++ ?SERVICE_PROBE_URL(SName), ?MODULE, {'$probe', SName}},
                  {Base, ?MODULE, {spec, SName}},
                  {Base++"/:class/:msisdn", [{class, fun class_constraint/2}], ?MODULE, Opts},
                  {Base++"/:msisdn", ?MODULE, Opts}
                 ]}]
            ),
            TransOpts = [{ip, Ip}, {port, Port},
                         {num_acceptors, MaxAcceptors},
                         {max_connections, MaxConnections},
                         {versions, ['tlsv1.2','tlsv1.1',tlsv1]}
                         | imem_server:get_cert_key(Cert)
                         ++ imem_server:get_cert_key(Key)],
            ProtoOpts = #{env => #{dispatch => Dispatch},
                          middlewares => [cowboy_router, ?MODULE, cowboy_handler],
                          stream_handlers => [cowboy_compress_h, cowboy_stream_h]},
            {ok, P} = cowboy:start_tls({Ip, Port}, TransOpts, ProtoOpts),
            ?SInfo("[~p] Activated https://~s:~p~s",
                    [P, inet:ntoa(Ip), Port, Base])
        end, FilteredListenerIps),
        {ok, State#state{listeners = FilteredListenerIps, port = Port}}
    catch
        error:{badmatch,{error,{already_started,_}}} = Error:Stacktrace ->
            ?SError("error:~p~n~p", [Error, Stacktrace]),
            stop_listeners(Error, FilteredListenerIps, Port),
            init_interface(Intf, State);
        Class:Error:Stacktrace ->
            ?SError("~p:~p~n~p", [Class, Error, Stacktrace]),
            {error, Error}
    end.

class_constraint(format_error, Value) -> io_lib:format("The class ~p is not an valid.", [Value]);
class_constraint(_Type, <<"topstopper">>) -> {ok, topstopper};
class_constraint(_Type, <<"barring">>) -> {ok, barring};
class_constraint(_Type, _) -> {error, not_valid}.

stop_listeners(Reason, Listerns, Port) ->
    lists:map(
      fun(Ip) ->
              case catch cowboy:stop_listener({Ip, Port}) of
                  ok -> ok;
                  Error ->
                      ?SError("[~p] stopping listener ~p on port ~p : ~p",
                              [Reason, Ip, Port, Error])
              end
      end, Listerns).

local_ips(ListenerAddresses) ->
    IntfIps = dderl:local_ipv4s(),
    maps:fold(
        fun({_, _, _, _} = Ip, _, Acc) ->
            case lists:member(Ip, IntfIps) of
                true -> [Ip | Acc];
                false -> Acc
            end;
           (_, _, Acc) -> Acc
        end, [], ListenerAddresses
    ).

init_resources(#{credential := #{user := User, password := Password},
                 links := Links} = Resources,
               #state{active_link = ActiveLink} = State) ->
    #{opt := Opts, tns := TNS} = lists:nth(ActiveLink, Links),
    {error, unimplemented}.
    %% TODO : reimplement without erlocipool
    %Options = dperl_dal:oci_opts(?ERLOCIPOOL_LOG_CB, Opts),
    %Pool = dperl_dal:get_pool_name(Resources, State#state.name),
    %case proplists:get_value(sess_min, Options) of
    %    0 -> {error, invalid_dbconn_pool_size};
    %    _ ->
    %        case erlocipool:new(Pool, TNS, User, Password, Options) of
    %            {ok, _PoolPid} ->
    %                try
    %                    CloseStatementErrors = maps:get(close_statement,
    %                                                    Resources, []),
    %                    CloseSessionsErrors = maps:get(close_session,
    %                                                   Resources, []),
    %                    if is_list(CloseStatementErrors) andalso
    %                       is_list(CloseSessionsErrors) ->
    %                           {ok,
    %                            State#state{
    %                              stmt_profile_del
    %                              = create_stmt(Pool, ?PROFILE_DELETE_SQL,
    %                                            ?PROFILE_DELETE_TOPSTOPPER_POST_BINDS),
    %                              stmt_ts_get
    %                              = create_stmt(Pool, ?TOPSTOPPER_GET_SQL,
    %                                            ?TOPSTOPPER_BARRING_GET_BINDS),
    %                              stmt_ts_post
    %                              = create_stmt(Pool, ?TOPSTOPPER_POST_SQL,
    %                                            ?PROFILE_DELETE_TOPSTOPPER_POST_BINDS),
    %                              stmt_ts_put
    %                              = create_stmt(Pool, ?TOPSTOPPER_PUT_SQL,
    %                                            ?TOPSTOPPER_PUT_BINDS),
    %                              stmt_ts_reset
    %                              = create_stmt(Pool, ?TOPSTOPPER_RESET_SQL,
    %                                            ?TOPSTOPPER_RESET_REVERT_BINDS),
    %                              stmt_ts_revert
    %                              = create_stmt(Pool, ?TOPSTOPPER_REVERT_SQL,
    %                                            ?TOPSTOPPER_RESET_REVERT_BINDS),
    %                              stmt_bar_get
    %                              = create_stmt(Pool, ?BARRING_GET_SQL,
    %                                            ?TOPSTOPPER_BARRING_GET_BINDS),
    %                              stmt_bar_post
    %                              = create_stmt(Pool, ?BARRING_POST_SQL,
    %                                            ?BARRING_POST_BINDS),
    %                              stmt_bar_put
    %                              = create_stmt(Pool, ?BARRING_PUT_SQL,
    %                                            ?BARRING_PUT_BINDS),
    %                              pool = Pool, close_statement = CloseStatementErrors,
    %                              close_session = CloseSessionsErrors}};
    %                       true ->
    %                           {badarg, [close_statement, close_session]}
    %                    end
    %                catch
    %                    Class:Error ->
    %                        ?SError("create statements failed : ~p. Deleing pool.", [{Class, Error}]),
    %                        erlocipool:del(Pool),
    %                        {Class, Error}
    %                end;
    %            {error, {already_started,_} = Error} ->
    %                ?SError("Pool ~p exists, restarting...", [Pool]),
    %                erlocipool:del(Pool),
    %                {error, Error};
    %            {error, Error} ->
    %                erlocipool:del(Pool),
    %                init_resources(
    %                  Resources,
    %                  State#state{active_link =
    %                              if length(Links) > ActiveLink ->
    %                                     ?SWarn("Pool ~p start : ",
    %                                            [Pool, Error]),
    %                                     ActiveLink + 1;
    %                                 true ->
    %                                     ?SError("Pool ~p start : ",
    %                                             [Pool, Error]),
    %                                     1
    %                              end})
    %        end
    %end.

-spec create_stmt(atom(), binary(), list()) -> tuple().
create_stmt(Pool, Sql, Binds) ->
    {error, unimplemented}.
    %% TODO
    % ?OciStmt(Pool, Sql, Binds, Stmt),
    % Stmt.

%%
%% Cowboy REST resource
%%

-define(SERVER,     "dperl AAA").
-define(SPEC_FILE,  "aaa.json").
-include_lib("dderl/src/dderl_rest.hrl").

-define(E2400,
        #{errorCode => 2400,
          errorMessage => <<"Missing body">>,
          errorDetails => <<"Missing request payload">>}).
-define(E1404,
        #{errorCode => 1404,
          errorMessage => <<"Not Found">>,
          errorDetails => <<"Ressource not found, no AAA-Profile exists."
                            " Consider creating a default profile with"
                            " POST">>}).
-define(E1405,
        #{errorCode => 1405,
          errorMessage => <<"Method Not Allowed">>,
          errorDetails => <<"HTTP method isn't allowed on this resource">>}).

-define(E1403,
        #{errorCode => 1403,
          errorMessage => <<"Forbidden">>,
          errorDetails => <<"No access to the requested service">>}).

-define(JSON(__BODY), imem_json:encode(__BODY)).

% applying Swagger whitelist through middleware
execute(Req, Env) ->
    case maps:get(handler_opts, Env, none) of
        {swagger_static, SName} ->
            apply_swagger_whitelist(
                Req, SName, Env#{handler_opts => {priv_dir, dderl, "public/swagger"}});
        {swagger, _, SName} -> apply_swagger_whitelist(Req, SName, Env);
        {spec, SName} -> apply_swagger_whitelist(Req, SName, Env);
        _ -> {ok, Req, Env}
    end.

apply_swagger_whitelist(Req, SName, Env) ->
    {Ip, _Port} = cowboy_req:peer(Req),
    case ?SWAGGER_WHITELIST(SName) of
        #{Ip := _} -> {ok, Req, Env};
        WL when map_size(WL) == 0 -> {ok, Req, Env};
        _ ->
            Req1 = cowboy_req:reply(403, ?REPLY_JSON_HEADERS,
                                          ?JSON(?E1403), Req),
            {stop, Req1}
    end.

init(Req, {'$probe', SName}) ->
    {Code, Resp} = ?SERVICE_PROBE_RESP(SName),
    Req1 = cowboy_req:reply(Code, ?REPLY_HEADERS, Resp, Req),
    {ok, Req1, undefined};
init(Req, {swagger, Base, _SName}) ->
    Url = iolist_to_binary(cowboy_req:uri(Req)),
    LastAt = byte_size(Url) - 1,
    Req1 =
    cowboy_req:reply(
      302, #{<<"cache-control">> => <<"no-cache">>,
             <<"pragma">> => <<"no-cache">>,
             <<"location">> =>
             list_to_binary(
               [Url, case Url of
                         <<_:LastAt/binary, "/">> -> "";
                         _ -> "/"
                     end,
                "index.html?url="++ Base])},
      <<"Redirecting...">>, Req),
    {ok, Req1, #state{}};
init(Req, {spec, _SName}) ->
    Req1 =
    case cowboy_req:method(Req) of
        <<"GET">> ->
            {ok, Content} = file:read_file(
                              filename:join(dderl:priv_dir(dperl),
                                            ?SPEC_FILE)),
            cowboy_req:reply(200, ?REPLY_JSON_SPEC_HEADERS, Content, Req);
        <<"OPTIONS">> ->
            ACRHS = cowboy_req:header(<<"access-control-request-headers">>, Req),
            cowboy_req:reply(200, 
                             maps:merge(#{<<"allow">> => <<"GET,OPTIONS">>,
                                          <<"access-control-allow-headers">> => ACRHS},
                                        ?REPLY_OPT_HEADERS), <<>>, Req);
        Method->
            ?Error("~p not supported", [Method]),
            cowboy_req:reply(405, ?REPLY_JSON_HEADERS, ?JSON(?E1405), Req)
    end,
    {ok, Req1, #state{}};
init(Req0, #{whitelist := CWhiteList, tenants := Tenants,
             name := ServiceName} = Opts)->
    put(name, ServiceName),
    Req = Req0#{req_time => os:timestamp()},
    {Ip, _Port} = cowboy_req:peer(Req),
    IpStr = inet:ntoa(Ip),
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Tenant, Password} ->
            case Tenants of % user:password authorization check
                #{Tenant := #{password := Password, permissions := Permissions,
                              whitelist := TWhiteLists}} ->
                    % whitelists check
                    case {is_ip_allowed(Ip, maps:keys(TWhiteLists)),
                          is_ip_allowed(Ip, CWhiteList)} of
                        {false, false} ->
                            ?SError("~s (~s) is not in tenants' whitelist",
                                    [IpStr, Tenant]),
                            unauthorized(Req);
                        _ ->
                            Class = cowboy_req:binding(class, Req, <<>>),
                            Msisdn = cowboy_req:binding(msisdn, Req, <<>>),
                            Op = cowboy_req:method(Req),
                            Operation = get_operation(Class, Op, Req),
                            % operation permission check
                            case Permissions of
                                #{Operation := _} ->
                                    push_request(Operation, Msisdn, Tenant, Req, Opts);
                                _ ->
                                    ?SError("~s (~s) operation ~p not authorized",
                                            [IpStr, Tenant, Operation]),
                                    unauthorized(Req)
                            end
                    end;
                Tenants ->
                    ?SError("~s (~s:~s) is not configured in tenants",
                            [IpStr, Tenant, Password]),
                    unauthorized(Req)
            end;
        Auth ->
            ?SError("~s provided unsupported or bad authorization ~p", [IpStr, Auth]),
            unauthorized(Req)
    end.

-spec is_ip_allowed(tuple(), list()) -> true | false.
is_ip_allowed(_Ip, []) -> true;
is_ip_allowed(Ip, WhiteList) -> lists:member(Ip, WhiteList).

unauthorized(Req) ->
    Req1 = cowboy_req:reply(403, ?REPLY_JSON_HEADERS, ?JSON(?E1403), Req),
    {ok, Req1, undefined}.

get_operation(<<>>,       <<"DELETE">>,  _Req) -> {profile,   delete};
get_operation(barring,    <<"GET">>,     _Req) -> {barring,      get};
get_operation(barring,    <<"POST">>,    _Req) -> {barring,      post};
get_operation(barring,    <<"PUT">>,     _Req) -> {barring,      put};
get_operation(topstopper, <<"GET">>,     _Req) -> {topstopper,   get};
get_operation(topstopper, <<"PATCH">>,    Req) ->
    case cowboy_req:match_qs([{action, [], none}], Req) of
        #{action := <<"reset">>} -> {topstopper, reset};
        #{action := <<"revert">>} -> {topstopper, revert};
        #{action := Other} -> {topstopper, Other}
    end;
get_operation(topstopper, <<"POST">>,    _Req) -> {topstopper,   post};
get_operation(topstopper, <<"PUT">>,     _Req) -> {topstopper,   put};
get_operation(Class,      Op,               _) -> {Class,        Op}.

push_request({_, Op} = Operation, Msisdn, Tenant, Req0, #{resource := Service} = Opts) ->
    CastReq = #{reply => self(), operation => Operation,
                msisdn => Msisdn, tenant => Tenant},
    Req = Req0#{db_call => os:timestamp()},
    case cowboy_req:has_body(Req) of
        false when Op == put; Op == post; Op == patch; Op == delete ->
            Req1 = cowboy_req:reply(400, ?REPLY_JSON_HEADERS, ?JSON(?E2400), Req),
            {ok, Req1, undefined};
        false when Op == get ->
            ok = gen_server:cast(Service, CastReq),
            {cowboy_loop, Req, Opts, hibernate};
        true ->
            {ok, Body, Req1} = cowboy_req:read_body(Req),
            case catch imem_json:decode(Body, [return_maps]) of
                {'EXIT', Error} ->
                    ?SError("~p malformed with ~s : ~p", [Operation, Body, Error]),
                    Req2 = cowboy_req:reply(400, ?REPLY_JSON_HEADERS, ?JSON(?E2400), Req1),
                    {ok, Req2, undefined};
                BodyMap ->
                    ok = gen_server:cast(Service, CastReq#{data => BodyMap}),
                    {cowboy_loop, Req1, Opts, hibernate}
            end;
        HasBody ->
            ?SError("~p with body ~p is not supported", [Operation, HasBody]),
            Req1 = cowboy_req:reply(400, ?REPLY_JSON_HEADERS, ?JSON(?E2400), Req),
            {ok, Req1, undefined}
    end.

info({reply, bad_req}, Req, State) -> info({reply, {400, <<>>}}, Req, State);
info({reply, not_found}, Req, State) ->
    info({reply, {404, ?JSON(?E1404)}}, Req, State);
info({reply, {Code, Body}}, Req, State) when is_integer(Code), is_map(Body) ->
    info({reply, {Code, imem_json:encode(Body)}}, Req, State);
info({reply, {Code, Body}}, Req, State) when is_integer(Code), is_binary(Body) ->
    Req1 = cowboy_req:reply(Code, ?REPLY_JSON_HEADERS, Body, Req),
    ReqTime = maps:get(req_time, Req1),
    DbCall = maps:get(db_call, Req1),
    Now = os:timestamp(),
    Total = timer:now_diff(Now, ReqTime),
    if Total > 0 ->
           Op = cowboy_req:method(Req),
           Class = cowboy_req:binding(class, Req, <<>>),
           Msisdn = cowboy_req:binding(msisdn, Req, <<>>),
           {Ip, Port} = cowboy_req:peer(Req),
           IpStr = inet:ntoa(Ip),
           if is_tuple(DbCall) ->
                  ?SDebug("~s:~p ~s ~p ~s : TotalTime (micros) ~p = ~p (ReqTime) + ~p (DBTime)",
                          [IpStr, Port, Op, Class, Msisdn, Total,
                           timer:now_diff(DbCall, ReqTime),
                           timer:now_diff(Now, DbCall)]);
              true ->
                  ?SDebug("~s:~p ~s ~p ~s : TotalTime ~p micros",
                          [IpStr, Port, Op, Class, Msisdn, Total])
           end;
       true -> ok
    end,
    {stop, Req1, State}.
