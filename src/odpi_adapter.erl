-module(odpi_adapter).

% all the valid type specifiers for parameterized binds
-define(CLM_TYPES, [
'INT',
'VARCHAR2',
'NUMBER',
'DATE',
'BLOB',
'CLOB'
]).


-include("dderlodpi.hrl").
-include("gres.hrl").

-include_lib("imem/include/imem_sql.hrl").

-export([ init/0
        , process_cmd/6
        , disconnect/1
        , rows/2
        , get_deps/0
        , rows_limit/3
        , bind_arg_types/0
        , add_conn_info/2
        , connect_map/1
        ]).

-record(priv, {connections = [], stmts_info = []}).

-define(E2B(__T), gen_adapter:encrypt_to_binary(__T)).
-define(D2T(__B), gen_adapter:decrypt_to_term(__B)).

bind_arg_types() ->
    [atom_to_binary(T,utf8) || T <- ?CLM_TYPES].

-spec init() -> ok.
init() ->
    ?Info("Init called"),
    dderl_dal:add_adapter(odpi, <<"Oracle/ODPI">>),
    gen_adapter:add_cmds_views(undefined, system, odpi, false, [
        { <<"Remote Users">>
        , <<"select USERNAME from ALL_USERS">>
        , [] },
        { <<"Remote Tables">>
        , <<"select concat(OWNER,concat('.', TABLE_NAME)) as QUALIFIED_TABLE_NAME from ALL_TABLES where OWNER=user order by TABLE_NAME">>
        , [] },
        { <<"Remote Views">>
        , <<"select concat(OWNER,concat('.', VIEW_NAME)) as QUALIFIED_TABLE_NAME from ALL_VIEWS where OWNER=user order by VIEW_NAME">>
        , [] }
    ]).

-spec add_conn_info(any(), any()) -> any().
add_conn_info(Priv, _ConnInfo) -> Priv.

-spec connect_map(#ddConn{}) -> map().
connect_map(#ddConn{adapter = odpi} = C) ->
    add_conn_extra(C, #{id => C#ddConn.id,
                        name => C#ddConn.name,
                        adapter => <<"odpi">>,
                        owner => dderl_dal:user_name(C#ddConn.owner)}).

add_conn_extra(#ddConn{access = Access}, Conn)
  when is_map(Access), is_map(Conn) ->
       	maps:merge(Conn, maps:remove(owner,maps:remove(<<"owner">>,Access)));
add_conn_extra(#ddConn{access = Access}, Conn0) when is_list(Access), is_map(Conn0) ->
    Conn = Conn0#{user => proplists:get_value(user, Access, <<>>),
                  charset => proplists:get_value(charset, Access, <<>>),
                  tns => proplists:get_value(tnsstr, Access, <<>>),
                  service => proplists:get_value(service, Access, <<>>),
                  sid => proplists:get_value(sid, Access, <<>>),
                  host => proplists:get_value(ip, Access, <<>>),
                  port => proplists:get_value(port, Access, <<>>)},
    case proplists:get_value(type, Access, service) of
        Type when Type == tns; Type == <<"tns">> ->
            Conn#{method => <<"tns">>};
        Type when Type == service; Type == <<"service">>; Type == <<"DB Name">> ->
            Conn#{method => <<"service">>};
        Type when Type == sid; Type == <<"sid">> ->
            Conn#{method => <<"sid">>}
    end.

-spec process_cmd({[binary()], term()}, {atom(), pid()}, ddEntityId(), pid(),
                  undefined | #priv{}, pid()) -> #priv{}.
                
process_cmd({[<<"connect">>], ReqBody, _SessionId}, Sess, UserId, From,
            undefined, SessPid) ->
    process_cmd({[<<"connect">>], ReqBody, _SessionId}, Sess, UserId, From,
                #priv{connections = []}, SessPid);
process_cmd({[<<"connect">>], BodyJson5, _SessionId}, Sess, UserId, From,
            #priv{connections = Connections} = Priv, _SessPid) ->
    {value, {<<"password">>, Password}, BodyJson4} = lists:keytake(<<"password">>, 1, BodyJson5),
    {value, {<<"owner">>, _Owner}, BodyJson3} = lists:keytake(<<"owner">>, 1, BodyJson4),
    {value, {<<"id">>, Id}, BodyJson2} = lists:keytake(<<"id">>, 1, BodyJson3),
    {value, {<<"name">>, Name}, BodyJson1} = lists:keytake(<<"name">>, 1, BodyJson2),
    {value, {<<"adapter">>, <<"odpi">>}, BodyJson} = lists:keytake(<<"adapter">>, 1, BodyJson1),

    Method    = proplists:get_value(<<"method">>, BodyJson, <<"service">>),
    User      = proplists:get_value(<<"user">>, BodyJson, <<>>),

    TNS = case Method of
        <<"tns">> ->
            Tns = proplists:get_value(<<"tns">>, BodyJson, <<>>),
            ?Info("user ~p, TNS ~p", [User, Tns]),
            Tns;
        ServiceOrSid when ServiceOrSid == <<"service">>; ServiceOrSid == <<"sid">> ->
            IpAddr   = proplists:get_value(<<"host">>, BodyJson, <<>>),
            Port     = binary_to_integer(proplists:get_value(<<"port">>, BodyJson, <<>>)),
            NewTnsstr
            = list_to_binary(
                io_lib:format(
                "(DESCRIPTION="
                "  (ADDRESS_LIST="
                "      (ADDRESS=(PROTOCOL=tcp)"
                "          (HOST=~s)"
                "          (PORT=~p)"
                "      )"
                "  )"
                "  (CONNECT_DATA=("++
                case ServiceOrSid of
                    <<"service">> -> "SERVICE_NAME";
                    <<"sid">> -> "SID"
                end
                ++"=~s)))",
                [IpAddr, Port,
                    case ServiceOrSid of
                        <<"service">> -> proplists:get_value(<<"service">>, BodyJson, <<>>);
                        <<"sid">> -> proplists:get_value(<<"sid">>, BodyJson, <<>>)
                    end])),
            ?Info("user ~p, TNS ~p", [User, NewTnsstr]),
            NewTnsstr
    end,    
    try
        % One slave per userid
        Node = case dpi_load(build_slave_name(UserId)) of
            N when is_atom(N) ->
                case lists:member(N, nodes(hidden)) of
                    true -> N;
                    _ -> error({dpi_slave_crash, N})
                end;
            ErrorLoad -> error([ErrorLoad])
        end,
        Context = case dpi:safe(
            Node, dpi, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
        ) of
            Ctx when is_reference(Ctx) -> Ctx;
            {error, Src, L, ErrorCtx} -> error([{Src, L, ErrorCtx}, Node])
        end,
        Connection = case dpi:safe(
            Node, dpi, conn_create,
            [
                Context, User, Password, TNS,
                % Hard coded utf8 as we don't support other encodings yet
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
                #{}
            ]
        ) of
            Conn when is_reference(Conn) -> Conn;
            {error, SrcConnect, LConnect, ErrorConnect} ->
                error([{SrcConnect, LConnect, ErrorConnect}, Node, Context])
        end,
        ConnRef = #odpi_conn{
            node = Node,
            context = Context,
            connection = Connection
        },
        ?Info("DPI loaded and connected! ~p", [ConnRef]),

        Con = #ddConn{
            id = Id, name = Name, owner = UserId, adapter = odpi,
            access  = jsx:decode(jsx:encode(BodyJson), [return_maps])
        },
        ?Debug([{user, User}], "may save/replace new connection ~p", [Con]),
        case dderl_dal:add_connect(Sess, Con) of
            {error, Msg} ->
                conn_close_and_destroy(ConnRef),
                From ! {reply, jsx:encode(#{connect => #{error => Msg}})};
            #ddConn{owner = Owner} = NewConn ->
                From ! {
                    reply,
                    jsx:encode(
                        #{connect =>
                            #{
                                conn_id => NewConn#ddConn.id,
                                owner => Owner,
                                conn => ?E2B(ConnRef)
                            }
                        }
                    )
                }
        end,
        Priv#priv{connections = [ConnRef | Connections]}
    catch Class:Exception:Stacktrace ->
        Error0 = case Exception of
            [E, SlaveNode, Cntx] ->
                spawn(
                    fun() ->
                        try
                            dpi:context_destroy(Cntx),
                            dpi:unload(SlaveNode)
                        catch
                            Class:Exception:ST ->
                                ?Error(
                                    "destroy context ~p and shutdown slave "
                                    "~p~n~p:~p~n~p",
                                    [Cntx, SlaveNode, Class, Exception, ST]
                                )
                        end
                    end
                ),
                E;
            [E, SlaveNode] ->
                spawn(
                    fun() ->
                        try dpi:unload(SlaveNode) catch
                            Class:Exception:ST ->
                                ?Error(
                                    "shutdown slave ~p~n~p:~p~n~p",
                                    [SlaveNode, Class, Exception, ST]
                                )
                        end
                    end
                ),
                E;
            E -> E
        end,
        Error = case Error0 of
            {S, Line, #{message := Message} = Err} ->
                ?Error("ORANIF: ~s:~p => ~p", [S, Line, Err]),
                list_to_binary(io_lib:format("~p: ~s", [Class, Message]));
            Err -> list_to_binary(io_lib:format("~p:~p", [Class, Err]))
        end,
        ?Error("DB connect error ~p:~s~n~p", [Class, Error, Stacktrace]),
        From ! {reply, jsx:encode(#{connect => #{error => Error}})},
        Priv
    end;

process_cmd({[<<"change_conn_pswd">>], BodyJson}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    User     = proplists:get_value(<<"user">>, BodyJson, <<>>),
    Password = list_to_binary(proplists:get_value(<<"password">>, BodyJson, <<>>)),
    NewPassword = proplists:get_value(<<"new_password">>, BodyJson, <<>>),
    case lists:member(Connection, Connections) of
        true ->
            case dderlodpi:change_password(Connection, User, Password, NewPassword) of
                {error, Error} ->
                    ?Error("change password exception ~n~p~n", [Error]),
                    Err = iolist_to_binary(io_lib:format("~p", [Error])),
                    From ! {reply, jsx:encode([{<<"change_conn_pswd">>,[{<<"error">>, Err}]}])},
                    Priv;
                ok ->
                    From ! {reply, jsx:encode([{<<"change_conn_pswd">>,<<"ok">>}])},
                    Priv
            end;
        false ->
            From ! {reply, jsx:encode([{<<"error">>, <<"Connection not found">>}])},
            Priv
    end;

process_cmd({[<<"disconnect">>], ReqBody, _SessionId}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    [{<<"disconnect">>, BodyJson}] = ReqBody,
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    case lists:member(Connection, Connections) of
        true ->
            case conn_close_and_destroy(Connection) of
                ok ->
                    RestConnections = lists:delete(Connection, Connections),
                    From ! {reply, jsx:encode([{<<"disconnect">>, <<"ok">>}])},
                    Priv#priv{connections = RestConnections};
                {error, Error} ->
                    ?Error("Unable to close connection ~p", [Error]),
                    From ! {reply, jsx:encode([{<<"error">>, <<"Unable to close connection">>}])},
                    Priv
            end;
        false ->
            From ! {reply, jsx:encode([{<<"error">>, <<"Connection not found">>}])},
            Priv
    end;
process_cmd({[<<"remote_apps">>], ReqBody}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    [{<<"remote_apps">>, BodyJson}] = ReqBody,
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    case lists:member(Connection, Connections) of
        true ->
            % odpi instance is always in local node
            Apps = application:which_applications(),
            Versions = dderl_session:get_apps_version(Apps, []),
            From ! {reply, jsx:encode([{<<"remote_apps">>, Versions}])},
            Priv;
        false ->
            From ! {reply, jsx:encode([{<<"error">>, <<"Connection not found">>}])},
            Priv
    end;

process_cmd({[<<"query">>], ReqBody}, Sess, _UserId, From, #priv{connections = Connections} = Priv, SessPid) ->
    [{<<"query">>,BodyJson}] = ReqBody,
    case make_binds(proplists:get_value(<<"binds">>, BodyJson, null)) of
        {error, Error} -> From ! {reply, jsx:encode([{<<"error">>, Error}])};
        BindVals ->
            Query = proplists:get_value(<<"qstr">>, BodyJson, <<>>),
            Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
            ConnId = proplists:get_value(<<"conn_id">>, BodyJson, <<>>),
            case lists:member(Connection, Connections) of
                true ->
                    R = case dderl_dal:is_local_query(Query) of
                            true -> gen_adapter:process_query(Query, Sess, {ConnId, odpi}, SessPid);
                            _ -> process_query({Query, BindVals}, Connection, SessPid)
                        end,
                    From ! {reply, jsx:encode([{<<"query">>,[{<<"qstr">>, Query} | R]}])};
                false ->
                    From ! {reply, error_invalid_conn(Connection, Connections)}
            end
    end,
    Priv;

process_cmd({[<<"browse_data">>], ReqBody}, Sess, _UserId, From, #priv{connections = Connections} = Priv, SessPid) ->
    [{<<"browse_data">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    ConnId = proplists:get_value(<<"conn_id">>, BodyJson, <<>>), %% This should be change to params...
    Row = proplists:get_value(<<"row">>, BodyJson, 0),
    Col = proplists:get_value(<<"col">>, BodyJson, 0),
    R = dderl_fsm:row_with_key(Statement, Row),
    IsView = try
        Tables = [element(1,T) || T <- tuple_to_list(element(3, R)), size(T) > 0],
        _IsView = lists:any(fun(E) -> E =:= ddCmd end, Tables),
        ?Debug("browse_data (view ~p) ~p - ~p", [_IsView, Tables, {R, Col}]),
        _IsView
    catch
        _:_ -> false
    end,
    if
        IsView ->
            ?Debug("Row with key ~p",[R]),
            {_,#ddView{name=Name,owner=Owner},#ddCmd{}=OldC} = element(3, R),
            V = dderl_dal:get_view(Sess, Name, odpi, Owner),
            C = dderl_dal:get_command(Sess, OldC#ddCmd.id),
            ?Debug("Cmd ~p Name ~p", [C#ddCmd.command, Name]),
            case C#ddCmd.conns of
                'local' ->
                    Resp = gen_adapter:process_query(C#ddCmd.command, Sess, {ConnId, odpi}, SessPid),
                    RespJson = jsx:encode([{<<"browse_data">>,
                        [{<<"content">>, C#ddCmd.command}
                         ,{<<"name">>, Name}
                         ,{<<"table_layout">>, (V#ddView.state)#viewstate.table_layout}
                         ,{<<"column_layout">>, (V#ddView.state)#viewstate.column_layout}
                         ,{<<"view_id">>, V#ddView.id}] ++ Resp}]),
                    ?Debug("loading ~p at ~p", [Name, (V#ddView.state)#viewstate.table_layout]);
                _ ->
                    case lists:member(Connection, Connections) of
                        true ->
                            ?Debug("ddView ~p", [V]),
                            CmdBinds = gen_adapter:opt_bind_json_obj(C#ddCmd.command, odpi),
                            ClientBinds = make_binds(proplists:get_value(<<"binds">>, BodyJson, null), CmdBinds),
                            case {CmdBinds, ClientBinds} of
                                {[], _} ->
                                    Resp = process_query(C#ddCmd.command, Connection, SessPid),
                                    RespJson = jsx:encode(
                                                 [{<<"browse_data">>,[{<<"content">>, C#ddCmd.command},
                                                                      {<<"name">>, Name},
                                                                      {<<"table_layout">>, (V#ddView.state)#viewstate.table_layout},
                                                                      {<<"column_layout">>, (V#ddView.state)#viewstate.column_layout},
                                                                      {<<"view_id">>, V#ddView.id}] ++ Resp}]
                                                ),
                                    ?Debug("loading ~p at ~p", [Name, (V#ddView.state)#viewstate.table_layout]);
                                {JsonBindInfo, Binds} when Binds == undefined; element(1, Binds) == error ->
                                    RespJson = jsx:encode(
                                                 [{<<"browse_data">>, [{<<"content">>, C#ddCmd.command},
                                                                       {<<"name">>, Name},
                                                                       {<<"table_layout">>, (V#ddView.state)#viewstate.table_layout},
                                                                       {<<"column_layout">>, (V#ddView.state)#viewstate.column_layout},
                                                                       {<<"view_id">>, V#ddView.id} | JsonBindInfo]}]
                                                );
                                {_, Binds} ->
                                    Resp = process_query({C#ddCmd.command, Binds}, Connection, SessPid),
                                    RespJson = jsx:encode(
                                                 [{<<"browse_data">>,[{<<"content">>, C#ddCmd.command},
                                                                      {<<"name">>, Name},
                                                                      {<<"table_layout">>, (V#ddView.state)#viewstate.table_layout},
                                                                      {<<"column_layout">>, (V#ddView.state)#viewstate.column_layout},
                                                                      {<<"view_id">>, V#ddView.id}] ++ Resp}]
                                                ),
                                    ?Debug("loading ~p at ~p", [Name, (V#ddView.state)#viewstate.table_layout])
                            end;
                        false ->
                            RespJson = error_invalid_conn(Connection, Connections)
                    end
            end,
            From ! {reply, RespJson};
        true ->
            case lists:member(Connection, Connections) of
                true ->
                    Name = element(3 + Col, R),
                    Query = <<"select * from ", Name/binary>>,
                    Resp = process_query(Query, Connection, SessPid),
                    RespJson = jsx:encode([{<<"browse_data">>,
                        [{<<"content">>, Query}
                         ,{<<"name">>, Name}] ++ Resp }]),
                    From ! {reply, RespJson};
                false ->
                    From ! {reply, error_invalid_conn(Connection, Connections)}
            end
    end,
    Priv;

% views
process_cmd({[<<"views">>], ReqBody}, Sess, UserId, From, Priv, SessPid) ->
    ?Info("Process command: ~p~n", [views]),
    [{<<"views">>, BodyJson}] = ReqBody,
    %% This should be change to params...
    ConnId = proplists:get_value(<<"conn_id">>, BodyJson, <<>>),
    case dderl_dal:get_view(Sess, <<"All ddViews">>, odpi, UserId) of
        {error, _} = Error->
            F = Error;
        undefined ->
            ?Debug("Using system view All ddViews"),
            F = dderl_dal:get_view(Sess, <<"All ddViews">>, odpi, system);
        UserView ->
            ?Debug("Using a personalized view All ddViews"),
            F = UserView
    end,
    case F of
        {error, Reason} ->
            RespJson = jsx:encode([{<<"error">>, Reason}]);
        _ ->
            C = dderl_dal:get_command(Sess, F#ddView.cmd),
            Resp = gen_adapter:process_query(C#ddCmd.command, Sess, {ConnId, odpi}, SessPid),
            ?Debug("ddViews ~p~n~p", [C#ddCmd.command, Resp]),
            RespJson = jsx:encode([{<<"views">>,
                [{<<"content">>, C#ddCmd.command}
                ,{<<"name">>, <<"All ddViews">>}
                ,{<<"table_layout">>, (F#ddView.state)#viewstate.table_layout}
                ,{<<"column_layout">>, (F#ddView.state)#viewstate.column_layout}
                ,{<<"view_id">>, F#ddView.id}]
                ++ Resp
            }])
    end,
    From ! {reply, RespJson},
    Priv;

%  system views
process_cmd({[<<"system_views">>], ReqBody}, Sess, _UserId, From, Priv, SessPid) ->
    [{<<"system_views">>,BodyJson}] = ReqBody,
    ConnId = proplists:get_value(<<"conn_id">>, BodyJson, <<>>), %% This should be change to params...
    case dderl_dal:get_view(Sess, <<"All ddViews">>, odpi, system) of
        {error, Reason} ->
            RespJson = jsx:encode([{<<"error">>, Reason}]);
        F ->
            C = dderl_dal:get_command(Sess, F#ddView.cmd),
            Resp = gen_adapter:process_query(C#ddCmd.command, Sess, {ConnId, odpi}, SessPid),
            ?Debug("ddViews ~p~n~p", [C#ddCmd.command, Resp]),
            RespJson = jsx:encode([{<<"system_views">>,
                [{<<"content">>, C#ddCmd.command}
                ,{<<"name">>, <<"All ddViews">>}
                ,{<<"table_layout">>, (F#ddView.state)#viewstate.table_layout}
                ,{<<"column_layout">>, (F#ddView.state)#viewstate.column_layout}
                ,{<<"view_id">>, F#ddView.id}]
                ++ Resp
            }])
    end,
    From ! {reply, RespJson},
    Priv;

% open view by id
process_cmd({[<<"open_view">>], ReqBody}, Sess, _UserId, From, #priv{connections = Connections} = Priv, SessPid) ->
    [{<<"open_view">>, BodyJson}] = ReqBody,
    ConnId = proplists:get_value(<<"conn_id">>, BodyJson, <<>>),
    ViewId = proplists:get_value(<<"view_id">>, BodyJson),
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    case lists:member(Connection, Connections) of
        true ->
            View = dderl_dal:get_view(Sess, ViewId),
            Binds = make_binds(proplists:get_value(<<"binds">>, BodyJson, null)),
            Res = open_view(Sess, Connection, SessPid, ConnId, Binds, View),
            %% We have to add the supported types so edit sql can be prefilled with the parameters.
            Result = [{<<"bind_types">>, bind_arg_types()} | Res],
            From ! {reply, jsx:encode(#{<<"open_view">> => Result})};
        false ->
            From ! {reply, error_invalid_conn(Connection, Connections)}
    end,
    Priv;

% open view by name from inside a d3 graph
process_cmd({[<<"open_graph_view">>], ReqBody}, Sess, UserId, From, #priv{connections = Connections} = Priv, SessPid) ->
    [{<<"open_graph_view">>, BodyJson}] = ReqBody,
    ConnId = proplists:get_value(<<"conn_id">>, BodyJson, <<>>),
    ViewName = proplists:get_value(<<"view_name">>, BodyJson),
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    case lists:member(Connection, Connections) of
        true ->
            %% We need to check first for owner views, and then for the rest...
            View = case dderl_dal:get_view(Sess, ViewName, odpi, UserId) of
                undefined ->
                    dderl_dal:get_view(Sess, ViewName, odpi, '_');
                VRes -> VRes
            end,
            Binds = make_binds(proplists:get_value(<<"binds">>, BodyJson, null)),
            Res = open_view(Sess, Connection, SessPid, ConnId, Binds, View),
            From ! {reply, jsx:encode(#{<<"open_graph_view">> => Res})};
        false ->
            From ! {reply, error_invalid_conn(Connection, Connections)}
    end,
    Priv;

% events
process_cmd({[<<"sort">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"sort">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    SrtSpc = proplists:get_value(<<"spec">>, BodyJson, []),
    SortSpec = sort_json_to_term(SrtSpc),
    ?Debug("The sort spec from json: ~p", [SortSpec]),
    dderl_fsm:gui_req(Statement, sort, SortSpec, gui_resp_cb_fun(<<"sort">>, Statement, From)),
    Priv;
process_cmd({[<<"filter">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"filter">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    FltrSpec = proplists:get_value(<<"spec">>, BodyJson, []),
    FilterSpec = filter_json_to_term(FltrSpec),
    dderl_fsm:gui_req(Statement, filter, FilterSpec, gui_resp_cb_fun(<<"filter">>, Statement, From)),
    Priv;
process_cmd({[<<"reorder">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"reorder">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    ColumnOrder = proplists:get_value(<<"column_order">>, BodyJson, []),
    dderl_fsm:gui_req(Statement, reorder, ColumnOrder, gui_resp_cb_fun(<<"reorder">>, Statement, From)),
    Priv;
process_cmd({[<<"drop_table">>], ReqBody}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    [{<<"drop_table">>, BodyJson}] = ReqBody,
    TableNames = proplists:get_value(<<"table_names">>, BodyJson, []),
    Results = [process_table_cmd(drop_table, TableName, BodyJson, Connections) || TableName <- TableNames],
    send_result_table_cmd(From, <<"drop_table">>, Results),
    Priv;
process_cmd({[<<"truncate_table">>], ReqBody}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    [{<<"truncate_table">>, BodyJson}] = ReqBody,
    TableNames = proplists:get_value(<<"table_names">>, BodyJson, []),
    Results = [process_table_cmd(truncate_table, TableName, BodyJson, Connections) || TableName <- TableNames],
    send_result_table_cmd(From, <<"truncate_table">>, Results),
    Priv;
process_cmd({[<<"snapshot_table">>], ReqBody}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    [{<<"snapshot_table">>, BodyJson}] = ReqBody,
    TableNames = proplists:get_value(<<"table_names">>, BodyJson, []),
    Results = [process_table_cmd(snapshot_table, TableName, BodyJson, Connections) || TableName <- TableNames],
    send_result_table_cmd(From, <<"snapshot_table">>, Results),
    Priv;
process_cmd({[<<"restore_table">>], ReqBody}, _Sess, _UserId, From, #priv{connections = Connections} = Priv, _SessPid) ->
    [{<<"restore_table">>, BodyJson}] = ReqBody,
    TableNames = proplists:get_value(<<"table_names">>, BodyJson, []),
    Results = [process_table_cmd(restore_table, TableName, BodyJson, Connections) || TableName <- TableNames],
    send_result_table_cmd(From, <<"restore_table">>, Results),
    Priv;

% gui button events
process_cmd({[<<"button">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"button">>,BodyJson}] = ReqBody,
    FsmStmt = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    case proplists:get_value(<<"btn">>, BodyJson, <<">">>) of
        <<"restart">> ->
            Query = dderl_fsm:get_query(FsmStmt),
            case dderl_dal:is_local_query(Query) of
                true ->
                    dderl_fsm:gui_req(FsmStmt, button, <<"restart">>, gui_resp_cb_fun(<<"button">>, FsmStmt, From));
                _ ->
                    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
                    %% TODO: Fix restart if there is a need to link again.
                    BindVals = case make_binds(proplists:get_value(<<"binds">>, BodyJson, null)) of
                                   {error, _Error} -> undefined;
                                   BindVals0 -> BindVals0
                               end,
                    case dderlodpi:exec(Connection, Query, BindVals, imem_sql_expr:rownum_limit()) of
                        {ok, #stmtResults{} = StmtRslt, TableName} ->
                            dderlodpi:add_fsm(StmtRslt#stmtResults.stmtRefs, FsmStmt),
                            FsmCtx = generate_fsmctx(StmtRslt, Query, BindVals, Connection, TableName),
                            dderl_fsm:gui_req(FsmStmt, button, <<"restart">>, gui_resp_cb_fun(<<"button">>, FsmStmt, From)),
                            dderl_fsm:refresh_session_ctx(FsmStmt, FsmCtx);
                        _ ->
                            From ! {reply, jsx:encode([{<<"button">>, [{<<"error">>, <<"unable to refresh the table">>}]}])}
                    end
            end;
        ButtonInt when is_integer(ButtonInt) ->
            dderl_fsm:gui_req(FsmStmt, button, ButtonInt, gui_resp_cb_fun(<<"button">>, FsmStmt, From));
        ButtonBin when is_binary(ButtonBin) ->
            case string:to_integer(binary_to_list(ButtonBin)) of
                {error, _} -> Button = ButtonBin;
                {Target, []} -> Button = Target
            end,
            dderl_fsm:gui_req(FsmStmt, button, Button, gui_resp_cb_fun(<<"button">>, FsmStmt, From))
    end,
    Priv;
process_cmd({[<<"update_data">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"update_data">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    RowId = proplists:get_value(<<"rowid">>, BodyJson, <<>>),
    CellId = proplists:get_value(<<"cellid">>, BodyJson, <<>>),
    Value = proplists:get_value(<<"value">>, BodyJson, <<>>),
    dderl_fsm:gui_req(Statement, update, [{RowId,upd,[{CellId,Value}]}], gui_resp_cb_fun(<<"update_data">>, Statement, From)),
    Priv;
process_cmd({[<<"delete_row">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"delete_row">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    RowIds = proplists:get_value(<<"rowids">>, BodyJson, []),
    DelSpec = [{RowId,del,[]} || RowId <- RowIds],
    ?Debug("delete ~p ~p", [RowIds, DelSpec]),
    dderl_fsm:gui_req(Statement, update, DelSpec, gui_resp_cb_fun(<<"delete_row">>, Statement, From)),
    Priv;
process_cmd({[<<"insert_data">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"insert_data">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    ClmIdx = proplists:get_value(<<"col">>, BodyJson, <<>>),
    Value =  proplists:get_value(<<"value">>, BodyJson, <<>>),
    dderl_fsm:gui_req(Statement, update, [{undefined,ins,[{ClmIdx,Value}]}], gui_resp_cb_fun(<<"insert_data">>, Statement, From)),
    Priv;
process_cmd({[<<"paste_data">>], ReqBody}, _Sess, _UserId, From, Priv, _SessPid) ->
    [{<<"paste_data">>, BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    ReceivedRows = proplists:get_value(<<"rows">>, BodyJson, []),
    Rows = gen_adapter:extract_modified_rows(ReceivedRows),
    dderl_fsm:gui_req(Statement, update, Rows, gui_resp_cb_fun(<<"paste_data">>, Statement, From)),
    Priv;
process_cmd({[<<"download_query">>], ReqBody}, _Sess, UserId, From, Priv, SessPid) ->
    [{<<"download_query">>, BodyJson}] = ReqBody,
    FileName = proplists:get_value(<<"fileToDownload">>, BodyJson, <<>>),
    Query = proplists:get_value(<<"queryToDownload">>, BodyJson, <<>>),
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    BindVals = case make_binds(proplists:get_value(<<"binds">>, BodyJson, null)) of
                                   {error, _Error} -> undefined;
                                   BindVals0 -> BindVals0
                                end,
    Id = proplists:get_value(<<"id">>, BodyJson, <<>>),
    case dderlodpi:exec(Connection, Query, BindVals, imem_sql_expr:rownum_limit()) of
        {ok, #stmtResults{rowCols = Clms, stmtRefs = StmtRef, rowFun = RowFun}, _} ->
            Columns = gen_adapter:build_column_csv(UserId, odpi, Clms),
            From ! {reply_csv, FileName, Columns, first},
            ProducerPid = spawn(fun() ->
                produce_csv_rows(UserId, From, StmtRef, RowFun)
            end),
            dderlodpi:add_fsm(StmtRef, {?MODULE, ProducerPid}),
            dderlodpi:fetch_recs_async(StmtRef, [{fetch_mode, push}], 0),
            ?Debug("process_query created statement ~p for ~p", [ProducerPid, Query]);
        Error ->
            ?Error("query error ~p", [Error]),
            Error = if is_binary(Error) -> Error;
                true -> list_to_binary(lists:flatten(io_lib:format("~p", [Error])))
            end,
            From ! {reply_csv, FileName, Error, single}
    end,
    SessPid ! {download_done, Id},
    Priv;

process_cmd({[<<"term_diff">>], ReqBody}, Sess, _UserId, From, Priv, SessPid) ->
    [{<<"term_diff">>, BodyJson}] = ReqBody,
    % Can't be handled directly as SessPid is not given to gen_adapter.
    gen_adapter:term_diff(BodyJson, Sess, SessPid, From),
    Priv;

% unsupported gui actions
process_cmd({Cmd, BodyJson}, _Sess, _UserId, From, Priv, _SessPid) ->
    ?Error("unsupported command ~p content ~p and priv ~p", [Cmd, BodyJson, Priv]),
    CmdBin = lists:last(Cmd),
    From ! {reply, jsx:encode([{CmdBin,[{<<"error">>, <<"command '", CmdBin/binary, "' is unsupported">>}]}])},
    Priv.

% dderl_fsm like row receive interface for compatibility
rows(Rows, {?MODULE, Pid}) ->  Pid ! Rows.
rows_limit(_NRows, Rows, {?MODULE, Pid}) ->  Pid ! {Rows, true}. %% Fake a completed to send the last cvs part.
produce_csv_rows(UserId, From, StmtRef, RowFun) when is_function(RowFun) andalso is_pid(From) ->
    receive
        Data ->
            case erlang:process_info(From) of
                undefined ->
                    ?Error("Request aborted (response pid ~p invalid)", [From]),
                    dderlodpi:close(StmtRef);
                _ ->
                    produce_csv_rows_result(Data, UserId, From, StmtRef, RowFun)
            end
    end.

produce_csv_rows_result({error, Error}, _UserId, From, StmtRef, _RowFun) ->
    From ! {reply_csv, <<>>, list_to_binary(io_lib:format("Error: ~p", [Error])), last},
    dderlodpi:close(StmtRef);
produce_csv_rows_result({Rows, false}, UserId, From, StmtRef, RowFun) when is_list(Rows), is_function(RowFun) ->
    CsvRows = gen_adapter:make_csv_rows(UserId, Rows, RowFun, odpi),
    From ! {reply_csv, <<>>, CsvRows, continue},
    produce_csv_rows(UserId, From, StmtRef, RowFun);
produce_csv_rows_result({Rows, true}, UserId, From, StmtRef, RowFun) when is_list(Rows), is_function(RowFun) ->
    CsvRows = gen_adapter:make_csv_rows(UserId, Rows, RowFun, odpi),
    From ! {reply_csv, <<>>, CsvRows, last},
    dderlodpi:close(StmtRef).

-spec disconnect(#priv{}) -> #priv{}.
disconnect(#priv{connections = Connections} = Priv) ->
    ?Debug("closing the connections ~p", [Connections]),
    [conn_close_and_destroy(ConnRef) || ConnRef <- Connections],
    Priv#priv{connections = []}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec gui_resp_cb_fun(binary(), {atom(), pid()}, pid()) -> fun().
gui_resp_cb_fun(Cmd, Statement, From) ->
    Clms = dderl_fsm:get_columns(Statement),
    gen_adapter:build_resp_fun(Cmd, Clms, From).

-spec sort_json_to_term(list()) -> [tuple()].
sort_json_to_term([]) -> [];
sort_json_to_term([[{C,T}|_]|Sorts]) ->
    case string:to_integer(binary_to_list(C)) of
        {Index, []} -> Index;
        {error, _R} -> Index = C
    end,
    [{Index, if T -> <<"asc">>; true -> <<"desc">> end}|sort_json_to_term(Sorts)].

-spec filter_json_to_term([{binary(), term()} | [{binary(), term()}]]) -> [{atom() | integer(), term()}].
filter_json_to_term([{<<"undefined">>,[]}]) ->  {'undefined', []};
filter_json_to_term([{<<"and">>,Filters}]) ->  {'and', filter_json_to_term(Filters)};
filter_json_to_term([{<<"or">>,Filters}]) ->  {'or', filter_json_to_term(Filters)};
filter_json_to_term([]) ->  [];
filter_json_to_term([[{C,Vs}]|Filters]) ->
    [{binary_to_integer(C), Vs} | filter_json_to_term(Filters)].

-spec open_view({atom(), pid()}, {atom(), pid()}, pid(), binary(), [tuple()], undefined | {error, binary()}) -> list().
open_view(_Sess, _Connection, _SessPid, _ConnId, _Binds, undefined) ->  [{<<"error">>, <<"view not found">>}];
open_view(_Sess, _Connection, _SessPid, _ConnId, _Binds, {error, Reason}) ->  [{<<"error">>, Reason}];
open_view(Sess, Connection, SessPid, ConnId, Binds, #ddView{id = Id, name = Name, cmd = CmdId, state = ViewState}) ->
    C = dderl_dal:get_command(Sess, CmdId),
    Resp =
    case C#ddCmd.conns of
        local -> gen_adapter:process_query(C#ddCmd.command, Sess, {ConnId, odpi}, SessPid);
        _ ->
            case {gen_adapter:opt_bind_json_obj(C#ddCmd.command, odpi), Binds} of
                {[], _} -> process_query(C#ddCmd.command, Connection, SessPid);
                {JsonBindInfo, B} when B == undefined; element(1, B) == error -> JsonBindInfo;
                {_, Binds} -> process_query({C#ddCmd.command, Binds}, Connection, SessPid)
            end
    end,
    [{<<"content">>, C#ddCmd.command}
    ,{<<"name">>, Name}
    ,{<<"table_layout">>,ViewState#viewstate.table_layout}
    ,{<<"column_layout">>, ViewState#viewstate.column_layout}
    ,{<<"view_id">>, Id} | Resp].


-spec process_query(tuple()|binary(), tuple(), pid()) -> list().
process_query({Query, BindVals}, Connection, SessPid) ->
    Result = dderlodpi:exec(Connection, Query, BindVals, imem_sql_expr:rownum_limit()),
    CheckFuns = check_funs(Result),
    process_query(CheckFuns, Query, BindVals, Connection, SessPid);
process_query(Query, Connection, SessPid) ->
    process_query(check_funs(dderlodpi:exec(Connection, Query, imem_sql_expr:rownum_limit())),
                  Query, [], Connection, SessPid).

-spec process_query(term(), binary(), list(), tuple(), pid()) -> list().
process_query(ok, Query, BindVals, Connection, SessPid) ->
    ?Debug([{session, Connection}], "query ~p -> ok", [Query]),
    SessPid ! {log_query, Query, process_log_binds(BindVals)},
    [{<<"result">>, <<"ok">>}];
process_query({ok, #stmtResults{sortSpec = SortSpec, rowCols = Clms} = StmtRslt, TableName},
              Query, BindVals, #odpi_conn{} = Connection, SessPid) ->
    SessPid ! {log_query, Query, process_log_binds(BindVals)},
    FsmCtx = generate_fsmctx(StmtRslt, Query, BindVals, Connection, TableName),
    StmtFsm = dderl_fsm:start(FsmCtx, SessPid),
    dderlodpi:add_fsm(StmtRslt#stmtResults.stmtRefs, StmtFsm),
    ?Debug("StmtRslt ~p ~p", [Clms, SortSpec]),
    Columns = gen_adapter:build_column_json(lists:reverse(Clms)),
    JSortSpec = build_srtspec_json(SortSpec),
    ?Debug("JColumns~n ~s~n JSortSpec~n~s", [jsx:prettify(jsx:encode(Columns)), jsx:prettify(jsx:encode(JSortSpec))]),
    ?Debug("process_query created statement ~p for ~p", [StmtFsm, Query]),
    [{<<"columns">>, Columns},
     {<<"sort_spec">>, JSortSpec},
     {<<"statement">>, base64:encode(term_to_binary(StmtFsm))},
     {<<"connection">>, ?E2B(Connection)}];
process_query({ok, Values}, Query, BindVals, #odpi_conn{} = Connection, SessPid) ->
    SessPid ! {log_query, Query, process_log_binds(BindVals)},
    [{<<"data">>, Values},
     {<<"connection">>, ?E2B(Connection)}];
process_query({error, Msg}, Query, BindVals, _Connection, _SessPid) when is_binary(Msg) ->
    ?Error("query error ~p for ~p whith bind values ~p", [Msg, Query, BindVals]),
    [{<<"error">>, Msg}];
process_query(Error, Query, BindVals, _Connection, _SessPid) ->
    ?Error("query error ~p for ~p whith bind values ~p", [Error, Query, BindVals]),
    if
        is_binary(Error) ->
            [{<<"error">>, Error}];
        true ->
            Err = list_to_binary(lists:flatten(io_lib:format("~p", [Error]))),
            [{<<"error">>, Err}]
    end.

-spec send_result_table_cmd(pid(), binary(), list()) -> ok.
send_result_table_cmd(From, BinCmd, Results) ->
    TableErrors = [TableName || {error, TableName} <- Results],
    case TableErrors of
        [] ->
            From ! {reply, jsx:encode([{BinCmd, [{<<"result">>, <<"ok">>}]}])};
        [invalid_connection | _Rest] ->
            From ! {reply, error_invalid_conn()};
        _ ->
            ListNames = [binary_to_list(X) || X <- TableErrors],
            BinTblError = list_to_binary(string:join(ListNames, ",")),
            [CmdSplit|_] = binary:split(BinCmd, <<"_">>),
            Err = iolist_to_binary([<<"Unable to ">>, CmdSplit, <<" the following tables: ">>,  BinTblError]),
            ?Error("Error: ~p",  [Err]),
            From ! {reply, jsx:encode([{BinCmd, [{<<"error">>, Err}]}])}
    end,
    ok.

process_table_cmd(Cmd, TableName, BodyJson, Connections) ->
    Connection = ?D2T(proplists:get_value(<<"connection">>, BodyJson, <<>>)),
    case lists:member(Connection, Connections) of
        true ->
            case dderlodpi:run_table_cmd(Connection, Cmd, TableName) of
                ok -> ok;
                {error, Error} ->
                    ?Error("query error ~p", [Error]),
                    {error, TableName}
            end;
        false ->
            {error, invalid_connection}
    end.

-spec process_log_binds({list(), list()} | [] | undefined) -> list().
process_log_binds(undefined) ->  [];
process_log_binds([]) ->  [];
process_log_binds({[], _}) ->  [];
process_log_binds({[{Name, Dir, Type} | Vars], [Val | Values]}) ->
    [{Name, Dir, Type, Val} | process_log_binds({Vars, Values})].

-spec error_invalid_conn() -> term().
error_invalid_conn() ->
    Err = <<"Trying to process a query with an unowned connection">>,
    jsx:encode([{<<"error">>, Err}]).

-spec build_srtspec_json([{integer()| binary(), boolean()}]) -> list().
build_srtspec_json(SortSpecs) ->
    [build_srtspec_json(SP, AscDesc) || {SP, AscDesc} <- SortSpecs].

build_srtspec_json(SP, <<"asc">>) ->
    build_srtspec_json(SP, true);
build_srtspec_json(SP, <<"desc">>) ->
    build_srtspec_json(SP, false);
build_srtspec_json(SP, IsAsc) when is_integer(SP) ->
    {integer_to_binary(SP), [{<<"id">>, SP}, {<<"asc">>, IsAsc}]};
build_srtspec_json(SP, IsAsc) when is_binary(SP) ->
    case string:to_integer(binary_to_list(SP)) of
        {SPInt, []} ->
            {SP, [{<<"id">>, SPInt}, {<<"asc">>, IsAsc}]};
        _ ->
            {SP, [{<<"id">>, -1}, {<<"asc">>, IsAsc}]}
    end.

-spec error_invalid_conn({atom(), pid()}, [{atom(), pid()}]) -> term().
error_invalid_conn(Connection, Connections) ->
    Err = <<"Trying to process a query with an unowned connection">>,
    ?Error("~s: ~p~n connections list: ~p", [Err, Connection, Connections]),
    jsx:encode([{<<"error">>, Err}]).

-spec check_fun_vsn(fun()) -> boolean().
check_fun_vsn(Fun) when is_function(Fun)->
    {module, Mod} = erlang:fun_info(Fun, module),
    ?Debug("The module: ~p", [Mod]),
    [ModVsn] = proplists:get_value(vsn, Mod:module_info(attributes)),
    ?Debug("The Module version: ~p~n", [ModVsn]),
    {new_uniq, <<FunVsn:16/unit:8>>} = erlang:fun_info(Fun, new_uniq),
    ?Debug("The function version: ~p~n", [FunVsn]),
    ModVsn =:= FunVsn;
check_fun_vsn(Something) ->
    ?Error("Not a function ~p", [Something]),
    false.

-spec check_funs(term()) -> term().
check_funs({ok, #stmtResults{rowFun = RowFun, sortFun = SortFun} = StmtRslt, TableName}) ->
    ValidFuns = check_fun_vsn(RowFun) andalso check_fun_vsn(SortFun),
    if
        ValidFuns -> {ok, StmtRslt, TableName};
        true -> <<"Unsupported target database version">>
    end;
check_funs(Error) ->
    Error.

-spec generate_fsmctx(#stmtResults{}, binary(), list(), tuple(), term()) -> #fsmctxs{}.
generate_fsmctx(#stmtResults{
                  rowCols = Clms
                , rowFun   = RowFun
                , stmtRefs = StmtRef
                , sortFun  = SortFun
                , sortSpec = SortSpec}, Query, BindVals, #odpi_conn{} = Connection, TableName) ->
    #fsmctxs{rowCols      = Clms
           ,stmtRefs      = [StmtRef]
           ,rowFun        = RowFun
           ,sortFun       = SortFun
           ,sortSpec      = SortSpec
           ,orig_qry      = Query
           ,bind_vals     = BindVals
           ,stmtTables    = [TableName]
           ,block_length  = ?DEFAULT_ROW_SIZE
           ,fetch_recs_async_funs = [fun(Opts, Count) -> dderlodpi:fetch_recs_async(StmtRef, Opts, Count) end]
           ,fetch_close_funs = [fun() -> dderlodpi:fetch_close(StmtRef) end]
           ,stmt_close_funs  = [fun() -> dderlodpi:close(StmtRef) end]
           ,filter_and_sort_funs =
                [fun(FilterSpec, SrtSpec, Cols) ->
                        dderlodpi:filter_and_sort(StmtRef, Connection, FilterSpec, SrtSpec, Cols, Query)
                end]
           ,update_cursor_prepare_funs =
                [fun(ChangeList) ->
                        dderlodpi_stmt:prepare(TableName, ChangeList, Connection, Clms)
                end]
           ,update_cursor_execute_funs =
                [fun(_Lock, PrepStmt) ->
                        dderlodpi_stmt:execute(PrepStmt)
                end]
           }.

build_slave_name(system) ->  odpi_node_system;
build_slave_name(UserId) when is_integer(UserId) -> 
    list_to_atom("odpi_node_" ++ integer_to_list(UserId)).

dpi_load(SlaveName) ->
    Master = self(),
    spawn_link(
        fun() ->
            Master ! {oranif_result, dpi:load(SlaveName)},
            timer:sleep(infinity)
        end
    ),
    receive {oranif_result, Result} -> Result end.

-spec get_deps() -> [atom()].
get_deps() ->  [oranif].

make_binds(Binds) ->  make_binds(Binds, []).

make_binds(null, []) ->  undefined;
%make_binds(null, [{<<"binds">>, ParamsProp}]) ->
%    case proplists:get_value(<<"pars">>, ParamsProp, []) of
%        [] -> undefined;
%        ParameterList ->
%            % Convert parameter properties to map to make it easier to extract
%            extract_rset_out([{Name, maps:from_list(Value)} || {Name, Value} <- ParameterList])
%    end;
make_binds(null, _CmdBinds) ->  undefined;
make_binds(Binds, _CmdBinds) ->
    try
        {Vars, Values} = lists:foldr(
            fun({B, TV}, {NewBinds, NewVals}) ->
                Typ = binary_to_existing_atom(proplists:get_value(<<"typ">>, TV), utf8),
                Dir = case proplists:get_value(<<"dir">>, TV) of
                          <<"in">> -> in;
                          <<"out">> -> out;
                          <<"inout">> -> inout
                      end,
                Val = case proplists:get_value(<<"val">>, TV, <<>>) of
                          V when byte_size(V) == 0 ->
                              if Dir == out orelse Dir == inout ->
                                 list_to_binary(lists:duplicate(4400, 0));
                                 true -> V
                              end;
                          V -> V
                      end,
                {[{B, Dir, Typ} | NewBinds],
                 [Val | NewVals]}
            end,
            {[], []}, Binds),
        {Vars, Values}
    catch
        _:Exception:Stacktrace ->
            ?Error("ST ~p", [Stacktrace]),
            {error, list_to_binary(io_lib:format("bind process error : ~p", [Exception]))}
    end.

conn_close_and_destroy(#odpi_conn{context = Ctx, connection = Conn, node = Node}) ->
    dpi:safe(Node, fun() ->
        ok = dpi:conn_close(Conn, [], <<>>),
        ok = dpi:context_destroy(Ctx)
    end),
    dpi:unload(Node).

%-------------------------------------------------------------------------------
% TESTS
%-------------------------------------------------------------------------------

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

build_slave_name_test_() ->
    [
        ?_assertEqual(odpi_node_system, build_slave_name(system)),
        ?_assertEqual(odpi_node_123456, build_slave_name(123456))
    ].

-endif.
