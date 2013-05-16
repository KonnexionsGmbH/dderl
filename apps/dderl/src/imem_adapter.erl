-module(imem_adapter).
-author('Bikram Chatterjee <bikram.chatterjee@k2informatics.ch>').

-include("dderl.hrl").
-include_lib("erlimem/src/gres.hrl").

-export([ init/0
        , process_cmd/3
        ]).

-record(priv, { sess
              , stmts
       }).

init() ->
    dderl_dal:add_adapter(imem, "IMEM DB"),
    dderl_dal:add_connect(#ddConn{ id = erlang:phash2(make_ref())
                                 , name = "local imem"
                                 , adapter = imem
                                 , access = [{ip, "local"}, {user, "admin"}]
                                 }),
    gen_adapter:add_cmds_views(imem, [
        { <<"All Tables">>
        , <<"select name(qname) from all_tables order by qname">>
        , remote},
        %{<<"All Tables">>, <<"select name(qname) from all_tables where not is_member(\"{virtual, true}\", opts)">>},
        { <<"All Views">>
        , <<"select
                c.owner,
                v.name
            from
                ddView as v,
                ddCmd as c
            where
                c.id = v.cmd
                and c.adapters = \"[imem]\"
                and (c.owner = user or c.owner = system)
            order by
                v.name,
                c.owner">>
        , local}
        %{<<"All Views">>, <<"select v.name from ddView as v, ddCmd as c where c.id = v.cmd and c.adapters = \"[imem]\" and (c.owner = system)">>}
        %{<<"All Views">>, <<"select name, owner, command from ddCmd where adapters = '[imem]' and (owner = user or owner = system)">>}
    ]).

process_cmd({[<<"connect">>], ReqBody}, From, _) ->
    [{<<"connect">>,BodyJson}] = ReqBody,
    Schema = binary_to_list(proplists:get_value(<<"service">>, BodyJson, <<>>)),
    Port   = binary_to_list(proplists:get_value(<<"port">>, BodyJson, <<>>)),
    Ip     = binary_to_list(proplists:get_value(<<"ip">>, BodyJson, <<>>)),
    case Ip of
        Ip when Ip =:= "local_sec" ->
            Type    = local_sec,
            Opts    = {Schema};
        Ip when Ip =:= "local" ->
            Type    = local,
            Opts    = {Schema};
        Ip when Ip =:= "rpc" ->
            Type    = rpc,
            Opts    = {list_to_existing_atom(Port), Schema};
        Ip ->
            Type    = tcp,
            Opts    = {Ip, list_to_integer(Port), Schema}
    end,
    User = proplists:get_value(<<"user">>, BodyJson, <<>>),
    Password = list_to_binary(hexstr_to_list(binary_to_list(proplists:get_value(<<"password">>, BodyJson, <<>>)))),
    ?Debug("session:open ~p", [{Type, Opts, {User, Password}}]),
    case erlimem:open(Type, Opts, {User, Password}) of
        {error, Error} ->
            ?Error("DB connect error ~p", [Error]),
            Err = list_to_binary(lists:flatten(io_lib:format("~p", [Error]))),
            From ! {reply, jsx:encode([{<<"connect">>,[{<<"error">>, Err}]}])},
            #priv{};
        {ok, {_,ConPid} = Connection} ->
            ?Debug("session ~p", [Connection]),
            ?Debug("connected to params ~p", [{Type, Opts}]),
            Statements = [],
            Con = #ddConn { id       = erlang:phash2(make_ref())
                          , name     = binary_to_list(proplists:get_value(<<"name">>, BodyJson, <<>>))
                          , adapter  = imem
                          , access   = [ {ip,   Ip}
                                       , {port, Port}
                                       , {type, Type}
                                       , {user, User}
                                       ]
                          , schema   = list_to_atom(Schema)
                          },
            ?Debug([{user, User}], "may save/replace new connection ~p", [Con]),
            dderl_dal:add_connect(Con),
            From ! {reply, jsx:encode([{<<"connect">>,list_to_binary(?EncryptPid(ConPid))}])},
            #priv{sess=Connection, stmts=Statements}
    end;
process_cmd({[<<"query">>], ReqBody}, From, #priv{sess=Connection}=Priv) ->
    [{<<"query">>,BodyJson}] = ReqBody,
    Query = binary_to_list(proplists:get_value(<<"qstr">>, BodyJson, <<>>)),
    {NewPriv, R} = case dderl_dal:is_local_query(Query) of
        true -> process_query(Query, dderl_dal:get_session(), Priv);
        _ -> process_query(Query, Connection, Priv)
    end,
    ?Info("query ~p", [Query]),
    From ! {reply, jsx:encode([{<<"query">>,R}])},
    NewPriv;

process_cmd({[<<"browse_data">>], ReqBody}, From, #priv{sess={_,ConnPid}} = Priv) ->
    [{<<"browse_data">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    Connection = {erlimem_session, ConnPid},
    Row = proplists:get_value(<<"row">>, BodyJson, <<>>),
    Col = proplists:get_value(<<"col">>, BodyJson, <<>>),
    R = Statement:row_with_key(Row),
    ?Debug("Row with key ~p",[R]),
    Tables = [element(1,T) || T <- tuple_to_list(element(3, R)), size(T) > 0],
    IsView = lists:any(fun(E) -> E =:= ddCmd end, Tables),
    ?Debug("browse_data (view ~p) ~p - ~p", [IsView, Tables, {R, Col}]),
    if IsView ->
        {#ddView{name=Name,owner=Owner},#ddCmd{}=C,_} = element(3, R),
        Name = element(5, R),
        V = dderl_dal:get_view(Name, Owner),
        ?Debug("Cmd ~p Name ~p", [C#ddCmd.command, Name]),
        AdminConn =
            case C#ddCmd.conns of
            'local' -> dderl_dal:get_session();
            _ -> Connection
        end,
        {NewPriv, Resp} = process_query(C#ddCmd.command, AdminConn, Priv),
        RespJson = jsx:encode([{<<"browse_data">>,
            [{<<"content">>, C#ddCmd.command}
            ,{<<"name">>, Name}
            ,{<<"table_layout">>, (V#ddView.state)#viewstate.table_layout}
            ,{<<"column_layout">>, (V#ddView.state)#viewstate.column_layout}] ++
            Resp
        }]),
        ?Debug("loading ~p at ~p", [Name, (V#ddView.state)#viewstate.table_layout]),
        From ! {reply, RespJson};
    true ->                
        Name = lists:last(tuple_to_list(R)),
        Query = <<"SELECT * FROM ", Name/binary>>,
        {NewPriv, Resp} = process_query(Query, Connection, Priv),
        RespJson = jsx:encode([{<<"browse_data">>,
            [{<<"content">>, Query}
            ,{<<"name">>, Name}] ++
            Resp
        }]),
        From ! {reply, RespJson}
    end,
    NewPriv;

% views
process_cmd({[<<"views">>], _}, From, Priv) ->
    [F|_] = dderl_dal:get_view(<<"All Views">>),
    C = dderl_dal:get_command(F#ddView.cmd),
    AdminSession = dderl_dal:get_session(),
    {NewPriv, Resp} = process_query(C#ddCmd.command, AdminSession, Priv),
    ?Debug("Views ~p~n~p", [C#ddCmd.command, Resp]),
    RespJson = jsx:encode([{<<"views">>,
        [{<<"content">>, C#ddCmd.command}
        ,{<<"name">>, <<"All Views">>}
        ,{<<"table_layout">>, (F#ddView.state)#viewstate.table_layout}
        ,{<<"column_layout">>, (F#ddView.state)#viewstate.column_layout}]
        ++ Resp
    }]),
    From ! {reply, RespJson},
    NewPriv;

% commands handled generically
% TODO may be moved to dderl_session
process_cmd({[C], _} = Cmd, From, Priv)
    when    (C =:= <<"save_view">>)
    % query
    orelse  (C =:= <<"get_query">>)
    orelse  (C =:= <<"parse_stmt">>)
    ->
    gen_adapter:process_cmd(Cmd, From),
    Priv;

% sort and filter
process_cmd({[<<"sort">>], ReqBody}, From, Priv) ->
    [{<<"sort">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    SrtSpc = proplists:get_value(<<"spec">>, BodyJson, []),
    SortSpec = sort_json_to_term(SrtSpc),
    Statement:gui_req(sort, SortSpec, gui_resp_cb_fun(<<"sort">>, Statement, From)),
    Priv;
process_cmd({[<<"filter">>], ReqBody}, From, Priv) ->
    [{<<"filter">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    FltrSpec = proplists:get_value(<<"spec">>, BodyJson, []),
    FilterSpec = filter_json_to_term(FltrSpec),
    Statement:gui_req(filter, FilterSpec, gui_resp_cb_fun(<<"filter">>, Statement, From)),
    Priv;

% gui button events
process_cmd({[<<"button">>], ReqBody}, From, Priv) ->
    [{<<"button">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    Button = proplists:get_value(<<"btn">>, BodyJson, <<">">>),
    Statement:gui_req(button, Button, gui_resp_cb_fun(<<"button">>, Statement, From)),
    Priv;
process_cmd({[<<"update_data">>], ReqBody}, From, Priv) ->
    [{<<"update_data">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    RowId = proplists:get_value(<<"rowid">>, BodyJson, <<>>),
    CellId = proplists:get_value(<<"cellid">>, BodyJson, <<>>),
    Value = binary_to_list(proplists:get_value(<<"value">>, BodyJson, <<>>)),
    Statement:gui_req(update, [{RowId,upd,[{CellId,Value}]}], gui_resp_cb_fun(<<"update_data">>, Statement, From)),
    Priv;
process_cmd({[<<"delete_row">>], ReqBody}, From, Priv) ->
    [{<<"delete_row">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    RowIds = proplists:get_value(<<"rowids">>, BodyJson, []),
    DelSpec = [{RowId,del,[]} || RowId <- RowIds],
    ?Debug("delete ~p ~p", [RowIds, DelSpec]),
    Statement:gui_req(update, DelSpec, gui_resp_cb_fun(<<"delete_row">>, Statement, From)),
    Priv;
process_cmd({[<<"insert_data">>], ReqBody}, From, Priv) ->
    [{<<"insert_data">>,BodyJson}] = ReqBody,
    Statement = binary_to_term(base64:decode(proplists:get_value(<<"statement">>, BodyJson, <<>>))),
    ClmIdx = proplists:get_value(<<"col">>, BodyJson, <<>>),
    Value =  binary_to_list(proplists:get_value(<<"value">>, BodyJson, <<>>)),
    Statement:gui_req(update, [{undefined,ins,[{ClmIdx,Value}]}], gui_resp_cb_fun(<<"insert_data">>, Statement, From)),
    Priv;

% unsupported gui actions
process_cmd({Cmd, BodyJson}, From, Priv) ->
    ?Error("unsupported command ~p content ~p and priv ~p", [Cmd, BodyJson, Priv]),
    CmdBin = lists:last(Cmd),
    From ! {reply, jsx:encode([{CmdBin,[{<<"error">>, <<"command ", CmdBin/binary, " is unsupported">>}]}])},
    Priv.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
gui_resp_cb_fun(Cmd, Statement, From) ->
    fun(#gres{} = GuiResp) ->
        GuiRespJson = gen_adapter:gui_resp(GuiResp, Statement:get_columns()),
        case (catch jsx:encode([{Cmd,GuiRespJson}])) of
            {'EXIT', Error} -> ?Error("Encoding problem ~p ~p~n~p~n~p", [Cmd, Error, GuiResp, GuiRespJson]);
            Resp -> From ! {reply, Resp}
        end
    end.

sort_json_to_term([]) -> [];
sort_json_to_term([[{C,T}|_]|Sorts]) ->
    [{binary_to_integer(C), if T -> <<"asc">>; true -> <<"desc">> end}|sort_json_to_term(Sorts)].

filter_json_to_term([{<<"undefined">>,[]}]) -> {'undefined', []};
filter_json_to_term([{<<"and">>,Filters}]) -> {'and', filter_json_to_term(Filters)};
filter_json_to_term([{<<"or">>,Filters}]) -> {'or', filter_json_to_term(Filters)};
filter_json_to_term([]) -> [];
filter_json_to_term([[{C,Vs}]|Filters]) ->
    Tail = filter_json_to_term(Filters),
    [{binary_to_integer(C), Vs} | Tail].

process_query(Query, {_,ConPid}=Connection, Priv) ->
    case Connection:exec(Query, ?DEFAULT_ROW_SIZE) of
        {ok, StmtRslt, {_,_,ConPid}=Statement} ->
            Clms = proplists:get_value(cols, StmtRslt, []),
            SortSpec = proplists:get_value(sort_spec, StmtRslt, []),
            ?Debug("StmtRslt ~p ~p", [Clms, SortSpec]),
            Columns = build_column_json(lists:reverse(Clms), []),
            JSortSpec = build_srtspec_json(SortSpec),
            ?Debug("JColumns~n"++binary_to_list(jsx:prettify(jsx:encode(Columns)))++
                   "~n JSortSpec~n"++binary_to_list(jsx:prettify(jsx:encode(JSortSpec)))),
            ?Debug("process_query created statement ~p for ~p", [Statement, Query]),
            {Priv, [{<<"columns">>, Columns}
                   ,{<<"sort_spec">>, JSortSpec}
                   ,{<<"statement">>, base64:encode(term_to_binary(Statement))}
                   ,{<<"connection">>, list_to_binary(?EncryptPid(ConPid))}
                   ]};
        ok ->
            ?Info([{session, Connection}], "query ~p -> ok", [Query]),
            {Priv, [{<<"result">>, <<"ok">>}]};
        {error, {{Ex, M}, _Stacktrace} = Error} ->
            ?Error([{session, Connection}], "query error ~p", [Error]),
            Err = list_to_binary(atom_to_list(Ex) ++ ": " ++ element(1, M)),
            {Priv, [{<<"error">>, Err}]};
        {error, {Ex,M}} ->
            ?Error([{session, Connection}], "query error ~p", [{Ex,M}]),
            Err = list_to_binary(atom_to_list(Ex) ++ ": " ++ element(1, M)),
            {Priv, [{<<"error">>, Err}]};
        Error ->
            ?Error([{session, Connection}], "query error ~p", [Error]),
            Err = list_to_binary(lists:flatten(io_lib:format("~p", [Error]))),
            {Priv, [{<<"error">>, Err}]}
    end.

build_srtspec_json(SortSpecs) ->
    [{if is_integer(SP) -> integer_to_binary(SP); true -> SP end
     , [{<<"id">>, if is_integer(SP) -> SP; true -> -1 end}
       ,{<<"asc">>, if AscDesc =:= <<"asc">> -> true; true -> false end}]
     } || {SP,AscDesc} <- SortSpecs].

build_column_json([], JCols) ->
    [[{<<"id">>, <<"sel">>},
      {<<"name">>, <<"">>},
      {<<"field">>, <<"id">>},
      {<<"behavior">>, <<"select">>},
      {<<"cssClass">>, <<"cell-selection">>},
      {<<"width">>, 38},
      {<<"minWidth">>, 2},
      {<<"cannotTriggerInsert">>, true},
      {<<"resizable">>, true},
      {<<"sortable">>, false},
      {<<"selectable">>, false}] | JCols];
build_column_json([C|Cols], JCols) ->
    Nm = C#stmtCol.alias,
    Nm1 = if Nm =:= <<"id">> -> <<"_id">>; true -> Nm end,
    JC = [{<<"id">>, Nm1},
          {<<"name">>, Nm},
          {<<"field">>, Nm1},
          {<<"resizable">>, true},
          {<<"sortable">>, false},
          {<<"selectable">>, true}],
    JCol = if C#stmtCol.readonly =:= false -> [{<<"editor">>, <<"true">>} | JC]; true -> JC end,
    build_column_json(Cols, [JCol | JCols]).

int(C) when $0 =< C, C =< $9 -> C - $0;
int(C) when $A =< C, C =< $F -> C - $A + 10;
int(C) when $a =< C, C =< $f -> C - $a + 10.

hexstr_to_list([]) -> [];
hexstr_to_list([X,Y|T]) -> [int(X)*16 + int(Y) | hexstr_to_list(T)].