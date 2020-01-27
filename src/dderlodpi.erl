-module(dderlodpi).
-behaviour(gen_server).

-include("dderlodpi.hrl").

-define(TRACE_FUNCTION_CALLS, '_').
-ifdef(TRACE_FUNCTION_CALLS).
    -define (TR, io:format("dderlodpi call ~p/~p~n", [?FUNCTION_NAME, ?FUNCTION_ARITY])).
    -define (TR(__V), io:format("dderlodpi call ~p/~p (~p)~n", [?FUNCTION_NAME, ?FUNCTION_ARITY, __V])).
-else.
    -define (TR, nop).
    -define (TR(__V), nop).
-endif.

%% API
-export([
    exec/3,                 % TODO: Cover all this functions with test cases.
    exec/4,                 %
    change_password/4,      %
    add_fsm/2,              %
    fetch_recs_async/3,     %
    fetch_close/1,          %
    filter_and_sort/6,      %
    close/1,                %
    close_port/1,           %
    run_table_cmd/3,        %
    cols_to_rec/2,          %
    get_alias/1,            %
    fix_row_format/4,       %
    create_rowfun/3
]).

%% helper functions for odpi_stmt
-export([
    dpi_conn_prepareStmt/2,
    dpi_conn_commit/1,
    dpi_conn_rollback/1,
    dpi_conn_newVar/2,
    dpi_conn_newVar/3,
    dpi_conn_newVar/5,
    dpi_stmt_bindByName/4,
    dpi_stmt_execute/3,
    dpi_stmt_executeMany/4,
    dpi_var_set_many/3,
    dpi_stmt_close/2,
    dpi_var_getReturnedData/3,
    dpi_var_get_rowids/3,
    dpi_var_release/2,
    dpi_data_release/2,
%% data conversion
    dpi_to_dderltime/1,
    dpi_to_dderlts/1,
    dpi_to_dderltstz/1,
    number_to_binary/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(qry, {select_sections
             ,contain_rowid
             ,stmt_result
             ,fsm_ref
             ,max_rowcount
             ,pushlock
             ,contain_rownum
             ,connection
             }).

-define(PREFETCH_SIZE, 250).

%% ===================================================================
%% Exported functions
%% ===================================================================
-spec exec(map(), binary(), integer()) -> ok | {ok, pid()} | {error, term()}.
exec(Connection, Sql, MaxRowCount) ->
    ?TR,
    exec(Connection, Sql, undefined, MaxRowCount).

-spec exec(map(), binary(), tuple(), integer()) -> ok | {ok, pid()} | {error, term()}.
exec(Connection, OrigSql, Binds, MaxRowCount) ->
    ?TR,
    {Sql, NewSql, TableName, RowIdAdded, SelectSections} =
        parse_sql(sqlparse:parsetree(OrigSql), OrigSql),
    case catch run_query(Connection, Sql, Binds, NewSql, RowIdAdded, SelectSections) of
        {'EXIT', {{error, Error}, ST}} ->
            ?Error("run_query(~s,~p,~s)~n{~p,~p}", [Sql, Binds, NewSql, Error, ST]),
            {error, Error};
        {'EXIT', {Error, ST}} ->
            ?Error("run_query( SQL: ~s, Binds: ~p, NewSQL: ~s)~n{Error: ~p, ST: ~p}", [Sql, Binds, NewSql, Error, ST]),
            {error, Error};
        {ok, #stmtResults{} = StmtResult, ContainRowId} ->
            LowerSql = string:to_lower(binary_to_list(Sql)),
            case string:str(LowerSql, "rownum") of
                0 -> ContainRowNum = false;
                _ -> ContainRowNum = true
            end,
            {ok, Pid} = gen_server:start(?MODULE, [
                SelectSections, StmtResult, ContainRowId, MaxRowCount, ContainRowNum, Connection
            ], []),
            SortSpec = gen_server:call(Pid, build_sort_spec, ?ExecTimeout),
            %% Mask the internal stmt ref with our pid.
            {ok, StmtResult#stmtResults{stmtRefs = Pid, sortSpec = SortSpec}, TableName};
        NoSelect ->
            NoSelect
    end.

-spec append_semicolon(binary(), integer()) -> binary().
append_semicolon(Sql, $;) -> ?TR, Sql;
append_semicolon(Sql, _) -> <<Sql/binary, $;>>.

-spec change_password(tuple(), binary(), binary(), binary()) -> ok | {error, term()}.
change_password({oci_port, _, _} = Connection, User, OldPassword, NewPassword) ->
    ?TR,
    run_table_cmd(Connection, iolist_to_binary(["ALTER USER ", User, " IDENTIFIED BY ", NewPassword, " REPLACE ", OldPassword])).

-spec add_fsm(pid(), term()) -> ok.
add_fsm(Pid, FsmRef) ->
    ?TR,
    gen_server:cast(Pid, {add_fsm, FsmRef}).

-spec fetch_recs_async(pid(), list(), integer()) -> ok.
fetch_recs_async(Pid, Opts, Count) ->
    ?TR,
    gen_server:cast(Pid, {fetch_recs_async, lists:member({fetch_mode, push}, Opts), Count}).

-spec fetch_close(pid()) -> ok.
fetch_close(Pid) ->
    ?TR,
    gen_server:call(Pid, fetch_close, ?ExecTimeout).

-spec filter_and_sort(pid(), tuple(), list(), list(), list(), binary()) -> {ok, binary(), fun()}.
filter_and_sort(Pid, Connection, FilterSpec, SortSpec, Cols, Query) ->
    gen_server:call(Pid, {filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}, ?ExecTimeout).

-spec close(pid()) -> term().
close(Pid) ->
    ?TR,
    gen_server:call(Pid, close, ?ExecTimeout).

-spec close_port(tuple()) -> term().
close_port({OciMod, PortPid, _Conn}) -> ?TR, close_port({OciMod, PortPid});
close_port({_OciMod, _PortPid} = Port) -> ?TR, oci_port:close(Port).

%% Gen server callbacks
init([SelectSections, StmtResult, ContainRowId, MaxRowCount, ContainRowNum, Connection]) ->
    ?TR,
    {ok, #qry{
            select_sections = SelectSections,
            stmt_result = StmtResult,
            contain_rowid = ContainRowId,
            max_rowcount = MaxRowCount,
            contain_rownum = ContainRowNum,
            connection = Connection}}.

handle_call({filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}, _From, #qry{stmt_result = StmtResult} = State) ->
    ?TR(a),
    #stmtResults{rowCols = StmtCols} = StmtResult,
    %% TODO: improve this to use/update parse tree from the state.
    Res = filter_and_sort_internal(Connection, FilterSpec, SortSpec, Cols, Query, StmtCols),
    {reply, Res, State};
handle_call(build_sort_spec, _From, #qry{stmt_result = StmtResult, select_sections = SelectSections} = State) ->
    ?TR(b),
    #stmtResults{rowCols = StmtCols} = StmtResult,
    SortSpec = build_sort_spec(SelectSections, StmtCols),
    {reply, SortSpec, State};
handle_call(get_state, _From, State) ->
    ?TR(c),
    {reply, State, State};
handle_call(fetch_close, _From, #qry{} = State) ->
    ?TR(d),
    {reply, ok, State#qry{pushlock = true}};
handle_call(close, _From, #qry{connection = Connection, stmt_result = StmtResult} = State) ->
    ?TR(e),
    #stmtResults{stmtRefs = StmtRef} = StmtResult,
    dpi_stmt_close(Connection, StmtRef),
    {stop, normal, ok, State#qry{stmt_result = StmtResult#stmtResults{stmtRefs = undefined}}};
handle_call(_Ignored, _From, State) ->
    ?TR(f),
    {noreply, State}.

handle_cast({add_fsm, FsmRef}, #qry{} = State) -> ?TR, {noreply, State#qry{fsm_ref = FsmRef}};

handle_cast({fetch_recs_async, _, _}, #qry{pushlock = true} = State) ->
    ?TR,
    {noreply, State};
handle_cast({fetch_push, _, _}, #qry{pushlock = true} = State) ->
    ?TR,
    {noreply, State};
handle_cast({fetch_recs_async, true, FsmNRows}, #qry{max_rowcount = MaxRowCount} = State) ->
    ?TR,
    case FsmNRows rem MaxRowCount of
        0 -> RowsToRequest = MaxRowCount;
        Result -> RowsToRequest = MaxRowCount - Result
    end,
    gen_server:cast(self(), {fetch_push, 0, RowsToRequest}),
    {noreply, State};
handle_cast({fetch_recs_async, false, _}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult,
        contain_rowid = ContainRowId, connection = Connection} = State) ->
    ?TR,
    #stmtResults{stmtRefs = Statement, rowCols = Clms} = StmtResult,
    Res = dpi_fetch_rows(Connection, Statement, ?DEFAULT_ROW_SIZE),
    case Res of
        {error, Error} -> FsmRef:rows({error, Error});
        {error, _DpiNifFile, _Line, #{message := Msg}} -> FsmRef:rows({error, Msg});
        {Rows, Completed} when is_list(Rows), is_boolean(Completed) ->
            Rowargs = {fix_row_format(Statement, Rows, Clms, ContainRowId), Completed},
            try FsmRef:rows(Rowargs) of
                ok -> ok
            catch
                _Class:Result ->
                    FsmRef:rows({error, Result})
            end
    end,
    {noreply, State};
handle_cast({fetch_push, NRows, Target}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult} = State) ->
    ?TR,
    #qry{contain_rowid = ContainRowId, contain_rownum = ContainRowNum} = State,
    #stmtResults{stmtRefs = StmtRef, rowCols = Clms} = StmtResult,
    MissingRows = Target - NRows,
    if
        MissingRows > ?DEFAULT_ROW_SIZE ->
            RowsToFetch = ?DEFAULT_ROW_SIZE;
        true ->
            RowsToFetch = MissingRows
    end,
    case StmtRef:fetch_rows(RowsToFetch) of
        {{rows, Rows}, Completed} ->
            RowsFixed = fix_row_format(StmtRef, Rows, Clms, ContainRowId),
            NewNRows = NRows + length(RowsFixed),
            if
                Completed -> FsmRef:rows({RowsFixed, Completed});
                (NewNRows >= Target) andalso (not ContainRowNum) -> FsmRef:rows_limit(NewNRows, RowsFixed);
                true ->
                    FsmRef:rows({RowsFixed, false}),
                    gen_server:cast(self(), {fetch_push, NewNRows, Target})
            end;
        {error, Error} ->
            FsmRef:rows({error, Error})
    end,
    {noreply, State};
handle_cast(_Ignored, State) ->
    ?TR,
    {noreply, State}.

handle_info(_Info, State) ->
    ?TR,
    {noreply, State}.

terminate(_Reason, #qry{stmt_result = #stmtResults{stmtRefs = undefined}}) -> ?TR, ok;
terminate(_Reason, #qry{connection = Connection, stmt_result = #stmtResults{stmtRefs = Stmt}}) ->
    ?TR,
    dpi_stmt_close(Connection, Stmt).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions %%%
-spec select_type(list()) -> atom().
select_type(Args) ->
    ?TR,
    Opts = proplists:get_value(opt, Args, <<>>),
    GroupBy = proplists:get_value('group by', Args),
    NotAgregation = case proplists:get_value(from, Args) of
        [] -> false;
        FromTargets -> not is_agregation(FromTargets)
    end,
    if Opts =:= <<>> andalso
       GroupBy =:= [] andalso
       NotAgregation -> select;
       true -> agregation
    end.

-spec is_agregation([binary() | tuple()]) -> boolean().
is_agregation([]) -> ?TR, false;
is_agregation([Table | Rest]) when is_binary(Table) ->
    ?TR,
    is_agregation(Rest);
is_agregation([{as, Table, Alias} | Rest]) when is_binary(Alias), is_binary(Table) ->
    ?TR,
    is_agregation(Rest);
is_agregation(_) -> ?TR, true.

-spec inject_rowid(atom(), list(), binary()) -> {binary(), binary(), boolean()}.
inject_rowid(agregation, Args, Sql) ->
    ?TR,
    {from, [FirstTable|_]=_Forms} = lists:keyfind(from, 1, Args),
    %% Do not add rowid on agregation.
    {FirstTable, Sql, false};
inject_rowid(select, Args, Sql) ->
    ?TR,
    {fields, Flds} = lists:keyfind(fields, 1, Args),
    {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
    NewFields = expand_star(Flds, Forms) ++ [add_rowid_field(FirstTable)],
    NewArgs = lists:keyreplace(fields, 1, Args, {fields, NewFields}),
    NPT = {select, NewArgs},
    case sqlparse_fold:top_down(sqlparse_format_flat, NPT, []) of
        {error, _Reason} ->
            {FirstTable, Sql, false};
        NewSql ->
            {FirstTable, NewSql, true}
    end.

-spec add_rowid_field(tuple() | binary()) -> binary().
add_rowid_field(Table) -> ?TR, qualify_field(Table, "ROWID").

-spec qualify_field(tuple() | binary(), binary() | list()) -> binary().
qualify_field(Table, Field) -> ?TR, iolist_to_binary(add_field(Table, Field)).

-spec add_field(tuple() | binary(), binary() | list()) -> iolist().
add_field({as, _, Alias}, Field) -> ?TR, [Alias, ".", Field];
add_field({{as, _, Alias}, _}, Field) -> ?TR, [Alias, ".", Field];
add_field({Tab, _}, Field) when is_binary(Tab) ->
    ?TR,
    include_at(binary:split(Tab, <<"@">>), Field);
add_field(Tab, Field) when is_binary(Tab) ->
    ?TR,
    include_at(binary:split(Tab, <<"@">>), Field).

-spec include_at(list(), binary() | list()) -> iolist().
include_at([TabName, TabLocation], Field) ->
    ?TR,
    [TabName, ".", Field, $@, TabLocation];
include_at([TabName], Field) ->
    ?TR,
    [TabName, ".", Field].

-spec expand_star(list(), list()) -> list().
expand_star([<<"*">>], Forms) -> ?TR, qualify_star(Forms);
expand_star(Flds, _Forms) -> ?TR, Flds.

-spec qualify_star(list()) -> list().
qualify_star([]) -> ?TR, [];
qualify_star([Table | Rest]) -> ?TR, [qualify_field(Table, "*") | qualify_star(Rest)].

bind_exec_stmt(Conn, Stmt, undefined) ->
    ?TR,
    dpi_stmt_execute(Conn, Stmt);
bind_exec_stmt(Conn, Stmt, {BindsMeta, BindVal}) ->
    ?TR,
    BindVars = bind_vars(Conn, Stmt, BindsMeta),
    {cols, Cols} = execute_with_binds(Conn, Stmt, BindVars, BindVal),

    dpi:safe(Conn#odpi_conn.node, fun() ->
        [ dpi:var_release(maps:get(var, B))|| B <- BindVars]
    end),
    Cols.

% given the atom that specifies the type of the bind, finds the respective ora/dpi types and returns them as a tuple
bindTypeMapping(OraType)->
    ?TR,
    case OraType of
        'INTEGER' -> { 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64' };
        'STRING' -> { 'DPI_ORACLE_TYPE_CHAR', 'DPI_NATIVE_TYPE_BYTES' };
        'FLOAT' -> { 'DPI_ORACLE_TYPE_NUMBER', 'DPI_NATIVE_TYPE_DOUBLE' };
        'TIMESTAMP' -> { 'DPI_ORACLE_TYPE_DATE', 'DPI_NATIVE_TYPE_TIMESTAMP' };
        'BLOB' -> { 'DPI_ORACLE_TYPE_BLOB', 'DPI_NATIVE_TYPE_LOB' };
        'CLOB' -> { 'DPI_ORACLE_TYPE_CLOB', 'DPI_NATIVE_TYPE_LOB' };
        Else ->       { error, {"Unknown Type", Else}}
    end.


bind_vars(Conn, Stmt, BindsMeta)->
    ?TR,
    [begin
        {OraNative, DpiNative} = bindTypeMapping(BindType),
        Var = (catch dpi:safe(Conn#odpi_conn.node, fun() ->
        #{var := VarReturned, data := [FirstData | _Rest]} =
            dpi:conn_newVar(Conn#odpi_conn.connection, OraNative, DpiNative, 100, 4000, false, false, null),
            dpi:stmt_bindByName(Stmt, BindName, VarReturned),
        
        {VarReturned, FirstData}
        end)),
        {Var, DpiNative, BindType}
    end || {BindName, _Direction, BindType} <- BindsMeta].
 

execute_with_binds(#odpi_conn{context = _Ctx, connection = Conn, node = Node}, Stmt, BindVars, Binds) ->
    ?TR,
    [   
        begin
        % turn BindTuple into a list for the next list comprehension to work
        BindList = if
            is_list(BindTuple) -> BindTuple; % already a list: leave it as it is
            is_tuple(BindTuple) -> tuple_to_list(BindTuple); % a tuple: turn it into a list
            true -> [BindTuple] % something else: wrap it into a list
        end,
        [   begin
            dpi:safe(Node, fun() ->
                case BindType of 
                    'INTEGER' ->
                        % since everything is a binary now, even the ints need to be converted first
                        ok = dpi:data_setInt64(Data, list_to_integer(binary_to_list(Bind)));
                    'FLOAT' ->
                        % doubles are handled as binaries now to avoid precision loss, so the double that is to be inserted has to be turned back from binary to double here
                        ok = dpi:data_setDouble(Data, list_to_float(binary_to_list(Bind)));
                    'STRING' ->
                        ok = dpi:var_setFromBytes(Var, 0, Bind);
                    'TIMESTAMP' ->
                        {{Y,M,D},{Hh,Mm,Ss}} = imem_datatype:io_to_datetime(Bind), % extract values out of timestamp binary
                        ok = dpi:data_setTimestamp(Data, Y, M, D, Hh, Mm, Ss, 0, 0, 0); % fsecond and timezone hour/minute offset not supported, so they are set to 0
                    'CLOB' ->
                        io:format("CLOBotomy CLOB ~p~n", [Bind]),
                        LOB = dpi:conn_newTempLob(Conn, 'DPI_ORACLE_TYPE_CLOB'),
                        ok = dpi:lob_setFromBytes(LOB, Bind),
                        ok = dpi:var_setFromLob(Var, 0, LOB),
                        ok = dpi:lob_release(LOB);
                    'BLOB' ->
                        io:format("BLOBotomy BLOB ~p~n", [Bind]),
                        LOB = dpi:conn_newTempLob(Conn, 'DPI_ORACLE_TYPE_BLOB'),
                        io:format("LOBotomy pass ~p~n", [LOB]),
                        Bind2 = (catch imem_datatype:io_to_binary(Bind, byte_size(Bind)/2)),
                        io:format("gear 2nd ~p~n", [Bind2]),
                        ok = dpi:lob_setFromBytes(LOB, Bind2),
                        ok = dpi:var_setFromLob(Var, 0, LOB),
                        ok = dpi:lob_release(LOB);
                    Else -> io:format("Error! Unsupported bind type ~p~n", [Else])
                %% TODO: find out what other types are needed and implement those
                end
             end)
        end
        || {Bind, {{Var, Data}, _VarType, BindType}} <- lists:zip(BindList, BindVars)
        ] end
    || BindTuple <- Binds],
    Cols = dpi:safe(Node, fun() ->
        dpi:stmt_execute(Stmt, [])
    end),
    {cols,Cols}.
   
run_query(Connection, Sql, Binds, NewSql, RowIdAdded, SelectSections) ->
    ?TR,
    %% For now only the first table is counted.
    case dpi_conn_prepareStmt(Connection, NewSql) of
        Statement when is_reference(Statement) ->
            StmtExecResult = bind_exec_stmt(Connection, Statement, Binds),
            case dpi_stmt_getInfo(Connection, Statement) of
                #{isQuery := true} ->
                    result_exec_query(
                        StmtExecResult,
                        Statement,
                        Sql,
                        Binds,
                        NewSql,
                        RowIdAdded,
                        Connection,
                        SelectSections
                    );
                _GetInfo ->
                    StmtExecResult1 = case StmtExecResult of 0-> {executed, 0}; Else -> Else end,
                    result_exec_stmt(StmtExecResult1,Statement,Sql,Binds,NewSql,RowIdAdded,Connection,
                             SelectSections)
            end;
        {error, _DpiNifFile, _Line, #{message := Msg}} -> error(list_to_binary(Msg));
        Error -> error(Error)
    end.

result_exec_query(NColumns, Statement, _Sql, _Binds, NewSql, RowIdAdded, Connection,
                    SelectSections) when is_integer(NColumns), NColumns > 0 ->
                        ?TR,
    Clms = dpi_query_columns(Connection, Statement, NColumns),
    if
        RowIdAdded -> % ROWID is hidden from columns
            [_|ColumnsR] = lists:reverse(Clms),
            Columns = lists:reverse(ColumnsR);
        true ->
            Columns = Clms
    end,
    Fields = proplists:get_value(fields, SelectSections, []),
    NewClms = cols_to_rec(Columns, Fields),
    SortFun = build_sort_fun(NewSql, NewClms),
    R= {ok
     , #stmtResults{ rowCols = NewClms
                    , rowFun   =
                        fun({{}, Row}) ->
                                if
                                    RowIdAdded ->
                                        [_|NewRowR] = lists:reverse(tuple_to_list(Row)),
                                        translate_datatype(Statement, lists:reverse(NewRowR), NewClms);
                                    true ->
                                        translate_datatype(Statement, tuple_to_list(Row), NewClms)
                                end
                        end
                    , stmtRefs  = Statement
                    , sortFun  = SortFun
                    , sortSpec = []}
     , RowIdAdded},
    R;
result_exec_query(RowIdError, OldStmt, Sql, Binds, NewSql, _RowIdAdded, Connection,
        SelectSections) when Sql =/= NewSql ->
            ?TR,
    ?Debug("RowIdError ~p", [RowIdError]),
    dpi_stmt_close(Connection, OldStmt),
    case dpi_conn_prepareStmt(Connection, Sql) of
        Stmt when is_reference(Stmt) ->
            Result = bind_exec_stmt(Connection, Stmt, Binds),
            result_exec_query(Result, Stmt, Sql, Binds, Sql, false, Connection, SelectSections);
        {error, _DpiNifFile, _Line, #{message := Msg}} -> error(list_to_binary(Msg));
        Error -> error(Error)
    end;
result_exec_query(Error, Stmt, _Sql, _Binds, _NewSql, _RowIdAdded, Connection, _SelectSections) ->
    ?TR,
    result_exec_error(Error, Stmt, Connection).

result_exec_stmt({rowids, _}, Statement, _Sql, _Binds, _NewSql, _RowIdAdded, Connection, _SelectSections) ->
    ?TR,
    dpi_stmt_close(Connection, Statement),
    ok;
result_exec_stmt({executed, _}, Statement, _Sql, _Binds, _NewSql, _RowIdAdded, Connection, _SelectSections) ->
    ?TR,
    dpi_stmt_close(Connection, Statement),
    ok;
result_exec_stmt({executed, 1, [{Var, Val}]}, Statement, Sql, {Binds, _}, NewSql, false, Connection, _SelectSections) ->
    ?TR,
    dpi_stmt_close(Connection, Statement),
    case lists:keyfind(Var, 1, Binds) of
        {Var,out,'SQLT_RSET'} ->
            result_exec_stmt(Val:exec_stmt(), Val, Sql, undefined, NewSql, false, Connection, []);
        {Var,out,'SQLT_VNU'} ->
            {ok, [{Var, list_to_binary(oci_util:from_num(Val))}]};
        _ ->
            {ok, [{Var, Val}]}
    end;
result_exec_stmt({executed,_,Values}, Statement, _Sql, {Binds, _BindValues}, _NewSql, _RowIdAdded, Connection, _SelectSections) ->
    ?TR,
    NewValues =
    lists:foldl(
      fun({Var, Val}, Acc) ->
              [{Var,
                case lists:keyfind(Var, 1, Binds) of
                    {Var,out,'SQLT_VNU'} -> list_to_binary(oci_util:from_num(Val));
                    _ -> Val
                end} | Acc]
      end, [], Values),
    ?Debug("Values ~p", [Values]),
    ?Debug("Binds ~p", [Binds]),
    dpi_stmt_close(Connection, Statement),
    {ok, NewValues}.

result_exec_error({error, _DpiNifFile, _Line, #{message := Msg}}, Statement, Connection) ->
    ?TR,
    dpi_stmt_close(Connection, Statement),
    error(list_to_binary(Msg));
result_exec_error(Result, Statement, Connection) ->
    ?TR,
    ?Error("Result with unrecognized format ~p", [Result]),
    dpi_stmt_close(Connection, Statement),
    error(Result).

-spec create_rowfun(boolean(), list(), term()) -> fun().
create_rowfun(RowIdAdded, Clms, Stmt) ->
    ?TR,
    fun({{}, Row}) ->
            if
                RowIdAdded ->
                    [_|NewRowR] = lists:reverse(tuple_to_list(Row)),
                    translate_datatype(Stmt, lists:reverse(NewRowR), Clms);
                true ->
                    translate_datatype(Stmt, tuple_to_list(Row), Clms)
            end
    end.

expand_fields([<<"*">>], _, AllFields, Cols, Sections) ->
    ?TR,
    NewFields = [lists:nth(N, AllFields) || N <- Cols],
    lists:keyreplace('fields', 1, Sections, {'fields', NewFields});
expand_fields(QryFields, Tables, AllFields, Cols, Sections) ->
    ?TR,
    NormQryFlds = normalize_pt_fields(QryFields, #{}),
    LowerAllFields = [string:to_lower(binary_to_list(X)) || X <- AllFields],
    case can_expand(maps:keys(NormQryFlds), Tables, LowerAllFields) of
        true ->
            Keys = [lists:nth(N,LowerAllFields) || N <- Cols],
            NewFields = [maps:get(K, NormQryFlds) || K <- Keys],
            lists:keyreplace('fields', 1, Sections, {'fields',NewFields});
        false ->
            Sections
    end.

can_expand(LowerSelectFields, [TableName], LowerAllFields) when is_binary(TableName) ->
    ?TR,
    length(LowerSelectFields) =:= length(LowerAllFields) andalso [] =:= (LowerSelectFields -- LowerAllFields);
can_expand(_, _, _) -> ?TR, false.

normalize_pt_fields([], Result) -> ?TR, Result;
normalize_pt_fields([{as, _Field, Alias} = Fld | Rest], Result) when is_binary(Alias) ->
    ?TR,
    Normalized = string:to_lower(binary_to_list(Alias)),
    normalize_pt_fields(Rest, Result#{Normalized => Fld});
normalize_pt_fields([TupleField | Rest], Result) when is_tuple(TupleField) ->
    ?TR,
    case element(1, TupleField) of
        'fun' ->
            BinField = sqlparse_fold:top_down(sqlparse_format_flat, TupleField, []),
            Normalized = string:to_lower(binary_to_list(BinField)),
            normalize_pt_fields(Rest, Result#{Normalized => TupleField});
        _ ->
            normalize_pt_fields(Rest, Result)
    end;
normalize_pt_fields([Field | Rest], Result) when is_binary(Field) ->
    ?TR,
    Normalized = string:to_lower(binary_to_list(Field)),
    normalize_pt_fields(Rest, Result#{Normalized => Field});
normalize_pt_fields([_Ignored | Rest], Result) ->
    ?TR,
    normalize_pt_fields(Rest, Result).

build_sort_spec(SelectSections, StmtCols) ->
    ?TR,
    FullMap = build_full_map(StmtCols),
    case lists:keyfind('order by', 1, SelectSections) of
        {'order by', OrderBy} ->
            [process_sort_order(ColOrder, FullMap) || ColOrder <- OrderBy];
        _ ->
            []
    end.

process_sort_order({Name, <<>>}, Map) ->
    ?TR,
    process_sort_order({Name, <<"asc">>}, Map);
process_sort_order({Name, Dir}, []) when is_binary(Name)-> ?TR, {Name, Dir};
process_sort_order({Name, Dir}, [#bind{alias = Alias, cind = Pos} | Rest]) when is_binary(Name) ->
    ?TR,
    case string:to_lower(binary_to_list(Name)) =:= string:to_lower(binary_to_list(Alias)) of
        true -> {Pos, Dir};
        false -> process_sort_order({Name, Dir}, Rest)
    end;
process_sort_order({Fun, Dir}, Map) ->
    ?TR,
    process_sort_order({sqlparse_fold:top_down(sqlparse_format_flat, Fun, []), Dir}, Map).


%%% Model how imem gets the new filter and sort results %%%%
%       NewSortFun = imem_sql:sort_spec_fun(SortSpec, FullMaps, ColMaps),
%       %?Debug("NewSortFun ~p~n", [NewSortFun]),
%       OrderBy = imem_sql:sort_spec_order(SortSpec, FullMaps, ColMaps),
%       %?Debug("OrderBy ~p~n", [OrderBy]),
%       Filter =  imem_sql:filter_spec_where(FilterSpec, ColMaps, WhereTree),
%       %?Debug("Filter ~p~n", [Filter]),
%       Cols1 = case Cols0 of
%           [] ->   lists:seq(1,length(ColMaps));
%           _ ->    Cols0
%       end,
%       AllFields = imem_sql:column_map_items(ColMaps, ptree),
%       % ?Debug("AllFields ~p~n", [AllFields]),
%       NewFields =  [lists:nth(N,AllFields) || N <- Cols1],
%       % ?Debug("NewFields ~p~n", [NewFields]),
%       NewSections0 = lists:keyreplace('fields', 1, SelectSections, {'fields',NewFields}),
%       NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',Filter}),
%       %?Debug("NewSections1 ~p~n", [NewSections1]),
%       NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
%       %?Debug("NewSections2 ~p~n", [NewSections2]),
%       NewSql = sqlparse_fold:top_down(sqlparse_format_flat, {select,NewSections2}, []),     % sql_box:flat_from_pt({select,NewSections2}),
%       %?Debug("NewSql ~p~n", [NewSql]),
%       {ok, NewSql, NewSortFun}

filter_and_sort_internal(_Connection, FilterSpec, SortSpec, Cols, Query, StmtCols) ->
    ?TR,
    FullMap = build_full_map(StmtCols),
    case Cols of
        [] ->   Cols1 = lists:seq(1,length(FullMap));
        _ ->    Cols1 = Cols
    end,
    % AllFields = imem_sql:column_map_items(ColMaps, ptree), %%% This should be the correct way if doing it.
    AllFields = [C#bind.alias || C <- FullMap],
    SortSpecExplicit = [{Col, Dir} || {Col, Dir} <- SortSpec, is_integer(Col)],
    NewSortFun = imem_sql_expr:sort_spec_fun(SortSpecExplicit, FullMap, FullMap),
    case sqlparse:parsetree(Query) of
        {ok,[{{select, SelectSections},_}]} ->
            {fields, Flds} = lists:keyfind(fields, 1, SelectSections),
            {from, Tables} = lists:keyfind(from, 1, SelectSections),
            {where, WhereTree} = lists:keyfind(where, 1, SelectSections),
            NewSections0 = expand_fields(Flds, Tables, AllFields, Cols1, SelectSections),
            Filter = imem_sql_expr:filter_spec_where(FilterSpec, FullMap, WhereTree),
            FilterEmptyAsNull = filter_replace_empty(Filter),
            NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',FilterEmptyAsNull}),
            OrderBy = imem_sql_expr:sort_spec_order(SortSpec, FullMap, FullMap),
            NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
            NewSql = sqlparse_fold:top_down(sqlparse_format_flat, {select, NewSections2}, []);
        _->
            NewSql = Query
    end,
    {ok, NewSql, NewSortFun}.

filter_replace_empty({'=', Column, <<"''">>}) -> ?TR, {is, Column, <<"null">>};
filter_replace_empty({in, Column, {list, List}} = In) ->
    ?TR,
    EmptyRemoved = [E || E <- List, E =/= <<"''">>],
    case length(EmptyRemoved) =:= length(List) of
        true -> In; % Nothing to do
        false -> {'or', {in, Column, {list, EmptyRemoved}}, {is, Column, <<"null">>}}
    end;
filter_replace_empty({Op, Parameter1, Parameter2}) ->
    ?TR,
    {Op, filter_replace_empty(Parameter1), filter_replace_empty(Parameter2)};
filter_replace_empty(Condition) -> ?TR, Condition.

-spec to_imem_type(atom()) -> atom().
to_imem_type('SQLT_NUM') -> ?TR, number;
to_imem_type(_) -> ?TR, binstr.

build_full_map(Clms) ->
    ?TR,
    [#bind{ tag = list_to_atom([$$|integer_to_list(T)])
              , name = Alias
              , alias = Alias
              , tind = 2
              , cind = T
              , type = to_imem_type(OciType)
              , len = Len
              , prec = undefined }
     || {T, #rowCol{alias = Alias, type = OciType, len = Len}} <- lists:zip(lists:seq(1,length(Clms)), Clms)].

build_sort_fun(_Sql, _Clms) ->
    ?TR,
    fun(_Row) -> {} end.

-spec cols_to_rec([map()], list()) -> [#rowCol{}].
cols_to_rec([], _) -> ?TR, [];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := Type,
        fsPrecision := FsPrec
    }} | Rest], Fields
) when Type =:= 'DPI_ORACLE_TYPE_TIMESTAMP_TZ'; Type =:= 'DPI_ORACLE_TYPE_TIMESTAMP' ->
    ?TR,
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#rowCol{ tag = Tag
             , alias = Alias
             , type = Type
             , prec = FsPrec
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := 'DPI_ORACLE_TYPE_NUMBER',
        precision := 63,
        scale := -127 
    }} | Rest], Fields
) ->
    ?TR,
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#rowCol{ tag = Tag
             , alias = Alias
             , type = 'DPI_ORACLE_TYPE_NUMBER'
             , len = 19
             , prec = dynamic
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := 'DPI_ORACLE_TYPE_NUMBER',
        scale := -127 
    }} | Rest], Fields
) ->
    ?TR,
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#rowCol{ tag = Tag
             , alias = Alias
             , type = 'DPI_ORACLE_TYPE_NUMBER'
             , len = 38
             , prec = dynamic
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := Type
    }} | Rest], Fields
) when Type =:= 'DPI_ORACLE_TYPE_NATIVE_DOUBLE'; Type =:= 'DPI_ORACLE_TYPE_NATIVE_FLOAT' ->
    ?TR,
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#rowCol{ tag = Tag
             , alias = Alias
             , type = Type
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := Type, 
        clientSizeInBytes := Len,
        precision := Prec
    }} | Rest],
    Fields
) ->
    ?TR,
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#rowCol{ tag = Tag
             , alias = Alias
             , type = Type
             , len = Len
             , prec = Prec
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)].

-spec get_alias([#rowCol{}]) -> [binary()].
get_alias([]) -> ?TR, [];
get_alias([#rowCol{alias = A} | Rest]) ->
    ?TR,
    [A | get_alias(Rest)].

translate_datatype(_Stmt, [], []) -> ?TR, [];
translate_datatype(Stmt, [Bin | RestRow], [#rowCol{} | RestCols]) when is_binary(Bin) ->
    ?TR(a),
    [Bin | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [null | RestRow], [#rowCol{} | RestCols]) ->
    ?TR(b),
    [<<>> | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#rowCol{type = 'DPI_ORACLE_TYPE_TIMESTAMP_TZ'} | RestCols]) ->
    ?TR(c),
    [dpi_to_dderltstz(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#rowCol{type = 'DPI_ORACLE_TYPE_TIMESTAMP'} | RestCols]) ->
    ?TR(d),
    [dpi_to_dderlts(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#rowCol{type = 'DPI_ORACLE_TYPE_DATE'} | RestCols]) ->
    ?TR(e),
    [dpi_to_dderltime(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [Number | RestRow], [#rowCol{type = Type} | RestCols]) when
        %Type =:= 'DPI_ORACLE_TYPE_NUMBER';
        Type =:= 'DPI_ORACLE_TYPE_NATIVE_DOUBLE';
        Type =:= 'DPI_ORACLE_TYPE_NATIVE_FLOAT' ->
            ?TR(f),
    Result = dderloci_utils:clean_dynamic_prec(number_to_binary(Number)),
    [Result | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [{_Pointer, Size, Path, Name} | RestRow], [#rowCol{type = 'SQLT_BFILEE'} | RestCols]) ->
    ?TR(g),
    SizeBin = integer_to_binary(Size),
    [<<Path/binary, $#, Name/binary, 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [{Pointer, Size} | RestRow], [#rowCol{type = 'SQLT_BLOB'} | RestCols]) ->
    ?TR(h),
    if
        Size > ?PREFETCH_SIZE ->
            {lob, Trunc} = Stmt:lob(Pointer, 1, ?PREFETCH_SIZE),
            SizeBin = integer_to_binary(Size),
            AsIO = imem_datatype:binary_to_io(Trunc),
            [<<AsIO/binary, $., $., 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
        true ->
            {lob, Full} = Stmt:lob(Pointer, 1, Size),
            AsIO = imem_datatype:binary_to_io(Full),
            [AsIO | translate_datatype(Stmt, RestRow, RestCols)]
    end;
translate_datatype(Stmt, [Raw | RestRow], [#rowCol{type = 'SQLT_BIN'} | RestCols]) ->
    ?TR(i),
    [imem_datatype:binary_to_io(Raw) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#rowCol{} | RestCols]) ->
    ?TR(j),
    [R | translate_datatype(Stmt, RestRow, RestCols)].

-spec fix_row_format(term(), [list()], [#rowCol{}], boolean()) -> [tuple()].
fix_row_format(_Stmt, [], _, _) -> ?TR, [];
fix_row_format(Stmt, [Row | Rest], Columns, ContainRowId) ->
    ?TR,
    %% TODO: we have to add the table name at the start of the rows i.e
    %  rows [
    %        {{temp,1,2,3},{}},
    %        {{temp,4,5,6},{}}
    %  ]

    %% TODO: Convert the types to imem types??
    % db_to_io(Type, Prec, DateFmt, NumFmt, _StringFmt, Val),
    % io_to_db(Item,Old,Type,Len,Prec,Def,false,Val) when is_binary(Val);is_list(Val)
    if
        ContainRowId ->
            {RestRow, [RowId]} = lists:split(length(Row) - 1, Row),
            [{{}, list_to_tuple(fix_format(Stmt, RestRow, Columns) ++ [RowId])} | fix_row_format(Stmt, Rest, Columns, ContainRowId)];
        true ->
            [{{}, list_to_tuple(fix_format(Stmt, Row, Columns))} | fix_row_format(Stmt, Rest, Columns, ContainRowId)]
    end.

fix_format(_Stmt, [], []) -> ?TR, [];
fix_format(Stmt, [<<0:8, _/binary>> | RestRow], [#rowCol{type = 'SQLT_NUM'} | RestCols]) ->
    ?TR,
    [null | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [Number | RestRow], [#rowCol{type = 'SQLT_NUM', len = Scale, prec = dynamic} | RestCols]) ->
    ?TR,
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(Number),
    FormattedNumber = imem_datatype:decimal_to_io(Mantissa, Exponent),
    [imem_datatype:io_to_decimal(FormattedNumber, undefined, Scale) | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [Number | RestRow], [#rowCol{type = 'SQLT_NUM', len = Len,  prec = Prec} | RestCols]) ->
    ?TR,
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(Number),
    FormattedNumber = imem_datatype:decimal_to_io(Mantissa, Exponent),
    [imem_datatype:io_to_decimal(FormattedNumber, Len, Prec) | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<0, 0, 0, 0, 0, 0, 0, _/binary>> | RestRow], [#rowCol{type = 'SQLT_DAT'} | RestCols]) -> %% Null format for date.
?TR,
    [<<>> | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<Date:7/binary, _/binary>> | RestRow], [#rowCol{type = 'SQLT_DAT'} | RestCols]) -> %% Trim to expected binary size.
?TR,
    [Date | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<0,0,0,0,0,0,0,0,0,0,0,_/binary>> | RestRow], [#rowCol{type = 'SQLT_TIMESTAMP'} | RestCols]) -> %% Null format for timestamp.
?TR,
    [<<>> | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<TimeStamp:11/binary, _/binary>> | RestRow], [#rowCol{type = 'SQLT_TIMESTAMP'} | RestCols]) -> %% Trim to expected binary size.
?TR,
    [TimeStamp | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<0,0,0,0,0,0,0,0,0,0,0,0,0,_/binary>> | RestRow], [#rowCol{type = 'SQLT_TIMESTAMP_TZ'} | RestCols]) -> %% Null format for timestamp.
?TR,
    [<<>> | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<TimeStampTZ:13/binary, _/binary>> | RestRow], [#rowCol{type = 'SQLT_TIMESTAMP_TZ'} | RestCols]) -> %% Trim to expected binary size.
?TR,
    [TimeStampTZ | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [{Pointer, Size} | RestRow], [#rowCol{type = 'SQLT_CLOB'} | RestCols]) ->
    ?TR,
%% TODO: This is a workaround as there is no real support for CLOB in dderl or the current
%%       driver, so we read full text here and treat it as a normal STR, Oracle is smart
%%       to do the conversion into CLOB on the way in.
    {lob, Full} = Stmt:lob(Pointer, 1, Size),
    [Full | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [Cell | RestRow], [#rowCol{} | RestCols]) ->
    ?TR,
    [Cell | fix_format(Stmt, RestRow, RestCols)].

-spec run_table_cmd(tuple(), atom(), binary()) -> ok | {error, term()}. %% %% !! Fix this to properly use statements.
run_table_cmd(#odpi_conn{}, restore_table, _TableName) -> ?TR, {error, <<"Command not implemented">>};
run_table_cmd(#odpi_conn{}, snapshot_table, _TableName) -> ?TR, {error, <<"Command not implemented">>};
run_table_cmd(#odpi_conn{} = Connection, truncate_table, TableName) ->
    ?TR,
    run_table_cmd(Connection, iolist_to_binary([<<"truncate table ">>, TableName]));
run_table_cmd(#odpi_conn{} = Connection, drop_table, TableName) ->
    ?TR,
    run_table_cmd(Connection, iolist_to_binary([<<"drop table ">>, TableName])).

-spec run_table_cmd(reference(), binary()) -> ok | {error, term()}.
run_table_cmd(Connection, SqlCmd) ->
    ?TR,
    Stmt = dpi_conn_prepareStmt(Connection, SqlCmd),
    Result = case dpi_stmt_execute(Connection, Stmt) of
        0 -> ok; % 0 rows available is the success response.
        Error ->
            ?Error("Error running table command ~p, result ~p", [SqlCmd, Error]),
            {error, <<"Table command failed">>}
    end,
    dpi_stmt_close(Connection, Stmt),
    Result.

-spec find_original_field(binary(), list()) -> {binary(), boolean(), list()}.
find_original_field(Alias, []) -> ?TR, {Alias, false, []};
find_original_field(Alias, [<<"*">>]) -> ?TR, {Alias, false, []};
find_original_field(Alias, [Field | Fields]) when is_binary(Field) ->
    ?TR,
    compare_alias(Alias, Field, Fields, Field, {Alias, false, Fields});
find_original_field(Alias, [{as, Name, Field} = CompleteAlias | Fields])
  when is_binary(Name),
       is_binary(Field) ->
           ?TR,
    compare_alias(Alias, Field, Fields, CompleteAlias, {Name, false, Fields});
find_original_field(Alias, [{as, _Expr, Field} = CompleteAlias | Fields])
  when is_binary(Field) ->
      ?TR,
    compare_alias(Alias, Field, Fields, CompleteAlias, {Alias, true, Fields});
find_original_field(Alias, [Field | Fields]) ->
    ?TR,
    {ResultName, ReadOnly, RestFields} = find_original_field(Alias, Fields),
    {ResultName, ReadOnly, [Field | RestFields]}.

-spec compare_alias(binary(), binary(), list(), term(), binary()) -> {binary(), boolean(), list()}.
compare_alias(Alias, Field, Fields, OrigField, Result) ->
    ?TR,
    LowerAlias = string:to_lower(binary_to_list(Alias)),
    LowerField = string:to_lower(binary_to_list(Field)),
    AliasQuoted = [$" | LowerAlias] ++ [$"],
    if
        LowerAlias =:= LowerField -> Result;
        AliasQuoted =:= LowerField -> Result;
        true ->
            {ResultName, ReadOnly, RestFields} = find_original_field(Alias, Fields),
            {ResultName, ReadOnly, [OrigField | RestFields]}
    end.

-spec parse_sql(tuple(), binary()) -> {binary(), binary(), binary(), boolean(), list()}.
parse_sql({ok, [{{select, SelectSections},_}]}, Sql) ->
    ?TR,
    {TableName, NewSql, RowIdAdded} = inject_rowid(select_type(SelectSections), SelectSections, Sql),
    {Sql, NewSql, TableName, RowIdAdded, SelectSections};
parse_sql({ok, [{{'begin procedure', _},_}]}, Sql) ->
    ?TR,
    %% Old sql is replaced by the one with the correctly added semicolon, issue #401
    NewSql = append_semicolon(Sql, binary:last(Sql)),
    {NewSql, NewSql, <<"">>, false, []};
parse_sql(_UnsuportedSql, Sql) ->
    ?TR,
    {Sql, Sql, <<"">>, false, []}.

%%%% Dpi data helper functions

dpi_to_dderltime(#{day := Day, month := Month, year := Year, hour := Hour, minute := Min, second := Sec}) ->
    ?TR,
    iolist_to_binary([
        pad(Day), ".",
        pad(Month), ".",
        integer_to_list(Year), " ",
        pad(Hour), ":",
        pad(Min), ":", pad(Sec)
    ]).

dpi_to_dderlts(#{fsecond := FSecond} = DpiTs) ->
    ?TR,
    ListFracSecs = case integer_to_list(FSecond) of
        NeedPad when length(NeedPad) < 9 -> pad(NeedPad, 9);
        FullPrec -> FullPrec
    end,
    case string:trim(ListFracSecs, trailing, "0") of
        [] -> dpi_to_dderltime(DpiTs);
        FracSecs -> iolist_to_binary([dpi_to_dderltime(DpiTs), $., FracSecs])
    end.

dpi_to_dderltstz(#{tzHourOffset := H,tzMinuteOffset := M} = DpiTsTz) ->
    ?TR,
    iolist_to_binary([dpi_to_dderlts(DpiTsTz), format_tz(H, M)]).

format_tz(TZOffset, M) when TZOffset > 0 ->
    ?TR,
    [$+ | format_tz_internal(TZOffset, M)];
format_tz(TZOffset, M) when TZOffset =:= 0, M >= 0 ->
    ?TR,
    [$+ | format_tz_internal(TZOffset, M)];
format_tz(TZOffset, M) ->
    ?TR,
    [$- | format_tz_internal(abs(TZOffset), abs(M))].

format_tz_internal(TZOffset, M) ->
    ?TR,
    [pad_tz(TZOffset), integer_to_list(TZOffset), $:, pad_tz(M), integer_to_list(M)].

pad_tz(TzDigit) when TzDigit < 10 -> ?TR, [$0];
pad_tz(_) -> ?TR, [].

pad(ListValue, Size) ->
    ?TR,
    lists:duplicate(Size - length(ListValue), $0) ++ ListValue.

pad(IntValue) ->
    ?TR,
    Value = integer_to_list(IntValue),
    pad(Value, 2).

number_to_binary(Int) when is_integer(Int) -> ?TR, integer_to_binary(Int);
number_to_binary(Float) when is_float(Float) -> ?TR, float_to_binary(Float, [{decimals,20}, compact]);
number_to_binary(Binary) when is_binary(Binary) -> ?TR, Binary; % already a binary, so just leave it
number_to_binary(Else) -> ?TR, io:format("ERROR: Tried to convert bad term to binary: ~p~n", [Else]).

%%%% Dpi safe functions executed on dpi slave node

dpi_conn_prepareStmt(#odpi_conn{node = Node, connection = Conn}, Sql) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:conn_prepareStmt(Conn, false, Sql, <<"">>) end).

dpi_conn_commit(#odpi_conn{node = Node, connection = Conn}) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:conn_commit(Conn) end).

dpi_conn_rollback(#odpi_conn{node = Node, connection = Conn}) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:conn_rollback(Conn) end).

dpi_conn_newVar(#odpi_conn{node = Node, connection = Conn} = Connection, Count, Type) ->
    ?TR,

    % encapsulates the dpi:safe call call that creates the variable and returns the variable/data
    % because the call is the same for most variable types except the ora/dpi types
    MakeVar = fun(NativeType, DpiType)->
    dpi:safe(Node, fun() ->
        #{var := Var, data := DataList} =
            dpi:conn_newVar(Conn, NativeType, DpiType, Count, 1, false, false, null),
        {Var, DataList}
    end) end,

    case Type of    'DPI_ORACLE_TYPE_DATE' -> MakeVar('DPI_ORACLE_TYPE_DATE', 'DPI_NATIVE_TYPE_TIMESTAMP');
                    'DPI_ORACLE_TYPE_TIMESTAMP' -> MakeVar('DPI_ORACLE_TYPE_TIMESTAMP', 'DPI_NATIVE_TYPE_TIMESTAMP');
                    'DPI_ORACLE_TYPE_TIMESTAMP_LTZ' -> MakeVar('DPI_ORACLE_TYPE_TIMESTAMP_LTZ', 'DPI_NATIVE_TYPE_TIMESTAMP');
                    'DPI_ORACLE_TYPE_TIMESTAMP_TZ' -> MakeVar('DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP');

    _Else -> dpi_conn_newVar(Connection, Count) end.

dpi_conn_newVar(Connection, Count) ->
    ?TR,
    dpi_conn_newVar(Connection, Count, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 4000).

dpi_conn_newVar(#odpi_conn{node = Node, connection = Conn}, Count, OracleType, NativeType, Size) ->
    ?TR,
    dpi:safe(Node, fun() ->
        #{var := Var, data := DataList} =
            dpi:conn_newVar(Conn, OracleType, NativeType, Count, Size, false, false, null),
        {Var, DataList}
    end).

dpi_stmt_bindByName(#odpi_conn{node = Node}, Stmt, Name, Var) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:stmt_bindByName(Stmt, Name, Var) end).

dpi_stmt_execute(Connection, Stmt) ->
    ?TR,
    % Commit automatically for any dderl queries.
    dpi_stmt_execute(Connection, Stmt, ['DPI_MODE_EXEC_COMMIT_ON_SUCCESS']).

dpi_stmt_execute(#odpi_conn{node = Node}, Stmt, Mode) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:stmt_execute(Stmt, Mode) end).

dpi_stmt_executeMany(#odpi_conn{node = Node}, Stmt, Count, Mode) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:stmt_executeMany(Stmt, Mode, Count) end).

dpi_stmt_getInfo(#odpi_conn{node = Node}, Stmt) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:stmt_getInfo(Stmt) end).

dpi_stmt_close(#odpi_conn{node = Node}, Stmt) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:stmt_close(Stmt) end).

dpi_var_getReturnedData(#odpi_conn{node = Node}, Var, Index) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:var_getReturnedData(Var, Index) end).

dpi_var_release(#odpi_conn{node = Node}, Var) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:var_release(Var) end).

dpi_data_release(#odpi_conn{node = Node}, DataList) when is_list(DataList) ->
    ?TR,
    dpi:safe(Node, fun() -> [dpi:data_release(Data) || Data <- DataList] end);
dpi_data_release(#odpi_conn{node = Node}, Data) ->
    ?TR,
    dpi:safe(Node, fun() -> dpi:data_release(Data) end).

% This is not directly dpi but seems this is the best place to declare as it is rpc...
dpi_query_columns(#odpi_conn{node = Node}, Stmt, NColumns) ->
    ?TR,
    dpi:safe(Node, fun() -> get_column_info(Stmt, 1, NColumns) end).

get_column_info(_Stmt, ColIdx, Limit) when ColIdx > Limit -> ?TR, [];
get_column_info(Stmt, ColIdx, Limit) ->
    ?TR,
    QueryInfo = dpi:stmt_getQueryInfo(Stmt, ColIdx),
    InnerMap = maps:get(typeInfo, QueryInfo),
    Type = case maps:get(defaultNativeTypeNum, InnerMap) of 'DPI_NATIVE_TYPE_DOUBLE' -> 'DPI_NATIVE_TYPE_BYTES'; A -> A end,
    InnerMap2 = InnerMap#{defaultNativeTypeNum := Type},
    QueryInfo2 = QueryInfo#{typeInfo := InnerMap2},
    [QueryInfo2 | get_column_info(Stmt, ColIdx + 1, Limit)].

dpi_fetch_rows( #odpi_conn{node = Node, connection = Conn}, Statement, BlockSize) ->
    ?TR,
    dpi:safe(Node, fun() -> get_rows_prepare(Conn, Statement, BlockSize, []) end).

%% initalizes things that need to be done before getting the rows
%% it finds out how many columns they are and what types all those columns are
%% then it makes and defines dpiVars for every column where it's necessary because stmt_getQueryValue() can't be used for those cols
%% and calls get_rows to fetch all the results of the query
get_rows_prepare(Conn, Stmt, NRows, Acc)->
    NumCols = dpi:stmt_getNumQueryColumns(Stmt),    % get number of cols returned by the Stmt
    Types = [
        begin
            Qinfo = maps:get(typeInfo, dpi:stmt_getQueryInfo(Stmt, Col)), % get the info and extract the map within the map
            #{defaultNativeTypeNum := DefaultNativeTypeNum,
            oracleTypeNum := OracleTypeNum} = Qinfo,    % match the map to get the wanted native/ora data type atoms
            {OracleTypeNum, DefaultNativeTypeNum, Col} % put those types into a tuple and add the column number
        end
        || Col <- lists:seq(1, NumCols)], % make a list of types that each row has. Each entry is a tuple of Oratype and nativetype. Also includes the col count

    VarsDatas = [
            begin
                io:format("taipuuuu ~p~n", [NativeType]),
                case NativeType of 'DPI_NATIVE_TYPE_DOUBLE' ->  % if the type is a double, make a variable for it, but say that the native type is bytes
                        % if stmt_getQueryValue() is used to get the values, then they will have their "correct" type. But doubles need to be
                        % fetched as a binary in order to avoid a rounding error that would occur if they were transformed from their internal decimal
                        % representation to double. Therefore, stmt_getQueryValue() can't be used for this, so a variable needs to be made because
                        % the data has to be fetched using define so the value goes into the data and then retrieving the values from the data
                        #{var := Var, data := Datas} = dpi:conn_newVar(Conn, OraType, 'DPI_NATIVE_TYPE_BYTES', 100, 0, false, false, null),
                        ok = dpi:stmt_define(Stmt, Col, Var),    %% results will be fetched to the vars and go into the data
                        {Var, Datas}; % put the variable and its data list into a tuple
                    'DPI_NATIVE_TYPE_LOB' ->
                        #{var := Var, data := Datas} = dpi:conn_newVar(Conn, OraType, 'DPI_NATIVE_TYPE_LOB', 100, 0, false, false, null),
                        ok = dpi:stmt_define(Stmt, Col, Var),    %% results will be fetched to the vars and go into the data
                        {Var, Datas, OraType}; % put the variable and its data list into a tuple
                    _else -> noVariable % when no variable needs to be made for the type, just put an atom signlizing that no variable was made and stmt_getQueryValue() can be used to get the values
                end
            end
            || {OraType, NativeType, Col} <- Types], % make a list of {Var, Datas} tuples. Var is the dpiVar handle, Datas is the list of Data handles in that respective Var  
R = get_rows(Conn, Stmt, NRows, Acc, VarsDatas), % gets all the results from the query
    [begin
        case VarDatas of {Var, Datas} -> % if there is a variable (which was made to fetch a double as a binary)
                [dpi:data_release(Data)|| Data <- Datas], % loop through the list of datas and release them all
                dpi:var_release(Var); % now release the variable
            _else -> nop % if no variable was made, then nothing needs to be done here
        end
    end || VarDatas <- VarsDatas], % clean up eventual variables that may have been made
R. % return query results

%% this recursive function fetches all the rows. It does so by calling yet another recursive function that fetches all the fields in a row.
get_rows(_Conn, _, 0, Acc, _VarsDatas) -> ?TR, {lists:reverse(Acc), false};
get_rows(Conn, Stmt, NRows, Acc, VarsDatas) ->
    ?TR,
    case dpi:stmt_fetch(Stmt) of % try to fetch a row
        #{found := true} -> % got a row: get the values in that row and then do the recursive call to try to get another row
    io:format("found: YES~n"),
            get_rows(Conn, Stmt, NRows -1, [get_column_values(Conn, Stmt, 1, VarsDatas, length(Acc)+1) | Acc], VarsDatas); % recursive call
        #{found := false} -> % no more rows: that was all of them
    io:format("found: NOPE~n"),
            {lists:reverse(Acc), true} % reverse the list so it's in the right order again after it was pieced together the other way around
    end.

%% get all the fields in one row
get_column_values(_Conn, _Stmt, ColIdx, VarsDatas, _RowIndex) when ColIdx > length(VarsDatas) -> ?TR(1), [];
get_column_values(Conn, Stmt, ColIdx, VarsDatas, RowIndex) ->
    ?TR(2),
    case lists:nth(ColIdx, VarsDatas) of % get the entry that is either a {Var, Datas} tuple or noVariable if no variable was made for this column
        {_Var, Datas, OraType} -> % if a variable was made for this column: the value was fetched into the variable's data object, so get it from there
            Value = dpi:data_get(lists:nth(RowIndex, Datas)), % get the value out of that data variable
        io:format("Value ~p Oratype ~p~n", [Value, OraType]),
            ValueFixed = case OraType of % depending on the ora type, the value might have to be changed into a different format so it displays properly
                'DPI_ORACLE_TYPE_BLOB' -> list_to_binary(lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Value)])); % turn binary to hex string
                Else -> Else end, % the value is already in the correct format for most types, so do nothing

            [ValueFixed | get_column_values(Conn, Stmt, ColIdx + 1, VarsDatas, RowIndex)]; % recursive call
        noVariable -> % if no variable has been made then that means that the value can be fetched with stmt_getQueryValue()
            #{data := Data} = dpi:stmt_getQueryValue(Stmt, ColIdx), % get the value 
            Value = dpi:data_get(Data), % take the value from this freshly made data
            dpi:data_release(Data), % release this new data object
            [Value | get_column_values(Conn, Stmt, ColIdx + 1, VarsDatas, RowIndex)]; % recursive call
        Else ->
            io:format("ERROR! Invalid variable term of ~p~n!", [Else])
    end.

% Helper function to avoid many rpc calls when binding a list of variables.
dpi_var_set_many(#odpi_conn{node = Node}, Vars, Rows) ->
    ?TR,
    dpi:safe(Node, fun() -> var_bind_many(Vars, Rows, 0) end).

var_bind_many(_Vars, [], _) -> ?TR, ok;
var_bind_many(Vars, [Row | Rest], Idx) ->
    ?TR,
    ok = var_bind_row(Vars, Row, Idx),
    var_bind_many(Vars, Rest, Idx + 1).

var_bind_row([], [], _Idx) -> ?TR, ok;
var_bind_row([], _Row, _Idx) -> ?TR, {error, <<"Bind variables does not match the given data">>};
var_bind_row(_Vars, [], _Idx) -> ?TR, {error, <<"Bind variables does not match the given data">>};
var_bind_row([{Var, Data, Type} | RestVars], [Bytes | RestRow], Idx) ->
    ?TR,
    case Type of 
        Atom when % dates and timestamp are the same internally so dpi:data_setTimestamp is good for all of them
        Atom =:= 'DPI_ORACLE_TYPE_DATE';
        Atom =:= 'DPI_ORACLE_TYPE_TIMESTAMP';
        Atom =:= 'DPI_ORACLE_TYPE_TIMESTAMP_TZ';
        Atom =:= 'DPI_ORACLE_TYPE_TIMESTAMP_LTZ'
        ->
            [Date] = Data,
            {{Y,M,D},{Hh,Mm,Ss}} = imem_datatype:io_to_datetime(Bytes),
            dpi:data_setTimestamp(Date, Y, M, D, Hh, Mm, Ss, 0, 0, 0);
        _Else ->
            ok = dpi:var_setFromBytes(Var, Idx, Bytes)
    end,
    var_bind_row(RestVars, RestRow, Idx);
var_bind_row([Var | RestVars], [Bytes | RestRow], Idx) ->
    ?TR,
    var_bind_row([{Var, undefined, undefined} | RestVars], [Bytes | RestRow], Idx).

dpi_var_get_rowids(#odpi_conn{node = Node}, Var, Count) when Count > 0->
    ?TR,
    dpi:safe(Node, fun() -> var_get_rowids(Var, Count, 0) end).

var_get_rowids(_, Count, Count) -> ?TR, [];
var_get_rowids(Var, Count, Idx) ->
    ?TR,
    #{numElements := 1, data  := [D]} = dpi:var_getReturnedData(Var, Idx),
    RowId = dpi:data_get(D),
    ok = dpi:data_release(D),
    [RowId | var_get_rowids(Var, Count, Idx + 1)].
