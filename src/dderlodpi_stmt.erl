-module(dderlodpi_stmt).
-behaviour(gen_server).

-include("dderlodpi.hrl").

-export([prepare/4,
    execute/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(NoCommit, 0).
-define(AutoCommit, 1).

-record(stmt, {columns = [],
               del_rows = [],
               upd_rows = [],
               ins_rows = [],
               del_stmt,
               upd_stmt,
               ins_stmt,
               connection}).

-record(row, {index, id, pos, op, values}).

-record(binds, {stmt, var, data}).

%%% API Implementation

-spec prepare(binary(), list(), tuple(), list()) -> {error, term()} | {ok, pid()}.
prepare(TableName, ChangeList, Connection, Columns) ->
    gen_server:start(?MODULE, [TableName, ChangeList, Connection, Columns], [{timeout, ?InitTimeout}]).

-spec execute(pid()) -> {error, term()} | list().
execute(Pid) ->
    gen_server:call(Pid, execute, ?ExecTimeout).

%%% gen_server callbacks
init([TableName, ChangeList, Connection, Columns]) ->
    case create_stmts(TableName, Connection, ChangeList, Columns) of
        {ok, Stmt} -> {ok, Stmt};
        {error, Error} -> {stop, Error}
    end.

handle_call(execute, _From, #stmt{columns = Columns, connection = Connection} = Stmt) ->
    try
        case process_delete(Connection, Stmt#stmt.del_stmt, Stmt#stmt.del_rows, Columns) of
            {ok, DeleteChangeList} ->
                case process_update(Connection, Stmt#stmt.upd_stmt, Stmt#stmt.upd_rows, Columns) of
                    {ok, UpdateChangeList} ->
                        case process_insert(Connection, Stmt#stmt.ins_stmt, Stmt#stmt.ins_rows, Columns) of
                            {ok, InsertChangeList} ->
                                dderlodpi:dpi_conn_commit(Connection),
                                {stop, normal, DeleteChangeList ++ UpdateChangeList ++ InsertChangeList, Stmt};
                            Error ->
                                dderlodpi:dpi_conn_rollback(Connection),
                                {stop, normal, Error, Stmt}
                        end;
                    Error ->
                        dderlodpi:dpi_conn_rollback(Connection),
                        {stop, normal, Error, Stmt}
                end;
            Error ->
                dderlodpi:dpi_conn_rollback(Connection),
                {stop, normal, Error, Stmt}
        end
    catch _Class:Error2 ->
            dderlodpi:dpi_conn_rollback(Connection),
            {stop, normal, {error, Error2}, Stmt}
    end.

handle_cast(_Ignored, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #stmt{del_stmt=DelStmt, upd_stmt=UpdStmt, ins_stmt=InsStmt, connection=Conn}) ->
    %% Delete is not a list since it is always only one.
    close_stmts(Conn, [Stmt || Stmt <- lists:flatten([DelStmt,UpdStmt,InsStmt]), Stmt =/= undefined]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Private functions

-spec table_name(binary() | {as, binary(), binary()}) -> binary().
table_name(TableName) when is_binary(TableName) -> TableName;
table_name({as, TableName, Alias}) -> iolist_to_binary([TableName, " ", Alias]).

-spec alias_name(binary() | {as, binary(), binary()}) -> binary().
alias_name(TableName) when is_binary(TableName) -> TableName;
alias_name({as, _TableName, Alias}) -> Alias.

-spec create_stmts(binary(), tuple(), list(), list()) -> {ok, #stmt{}} | {error, term()}.
create_stmts(TableName, Connection, ChangeList, Columns) ->
    {DeleteList, UpdateListTotal, InsertListTotal} = split_changes(ChangeList),
    UpdateList = split_by_columns_mod(UpdateListTotal, Columns, []),
    InsertList = split_by_non_empty(InsertListTotal, []),
    case create_stmts([{del, DeleteList}, {upd, UpdateList}, {ins, InsertList}], TableName, Connection, Columns, []) of
        {ok, Stmt} ->
            {ok, Stmt#stmt{del_rows = DeleteList, upd_rows = UpdateList, ins_rows = InsertList, connection = Connection}};
        Error ->
            Error
    end.

-spec create_stmts([{atom(), list()}], binary(), tuple(), list(), list()) -> {ok, #stmt{}} | {error, term()}.
create_stmts([], _TableName, _Connection, _Columns, []) ->
    {error, <<"empty change list">>};
create_stmts([], _TableName, _Connection, Columns, ResultStmts) ->
    DelStmt = proplists:get_value(del, ResultStmts),
    UpdStmt = proplists:get_value(upd, ResultStmts),
    InsStmt = proplists:get_value(ins, ResultStmts),
    {ok, #stmt{columns = Columns, del_stmt = DelStmt, upd_stmt = UpdStmt, ins_stmt = InsStmt}};
create_stmts([{del, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
create_stmts([{del, DelList} | Rest], TableName, Connection, Columns, ResultStmt) ->
    Sql = iolist_to_binary([<<"delete from ">>, table_name(TableName), " where ", alias_name(TableName), ".ROWID = :IDENTIFIER"]),
    case dderlodpi:dpi_conn_prepareStmt(Connection, Sql) of
        Stmt when is_reference(Stmt) ->
            {Var, DataList} = dderlodpi:dpi_conn_newVar(Connection, length(DelList)),
            ok = dderlodpi:dpi_stmt_bindByName(Connection, Stmt, <<"IDENTIFIER">>, Var),
            StmtBind = #binds{stmt = Stmt, var = Var, data = DataList},
            create_stmts(Rest, TableName, Connection, Columns, [{del, StmtBind} | ResultStmt]);
        Error ->
            ?Error("Error preparing delete stmt: ~p", [Error]),
            close_stmts(Connection, ResultStmt),
            {error, iolist_to_binary(["Unable to prepare stmt: ", Sql])}
    end;
create_stmts([{upd, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
create_stmts([{upd, UpdList} | Rest], TableName, Connection, Columns, ResultStmts) ->
    [{ModifiedCols, Rows} | RestUpdList] = UpdList,
    FilterColumns = filter_columns(ModifiedCols, Columns),
    UpdVars = create_upd_vars(FilterColumns),
    Sql = iolist_to_binary([<<"update ">>, table_name(TableName), " set ", UpdVars, " where ", alias_name(TableName), ".ROWID = :IDENTIFIER"]),
    ?Info("The update sql ~p and the length rows ~p", [Sql, length(Rows)]), %% TODO: Remove
    case dderlodpi:dpi_conn_prepareStmt(Connection, Sql) of
        Stmt when is_reference(Stmt) ->
            {RowIdVar, RowIdData} = dderlodpi:dpi_conn_newVar(Connection, length(Rows)),
            ok = dderlodpi:dpi_stmt_bindByName(Connection, Stmt, <<"IDENTIFIER">>, RowIdVar),
            {Vars, DataLists} = create_and_bind_vars(Connection, Stmt, FilterColumns, length(Rows)),
            StmtBind = #binds{stmt = Stmt, var = [RowIdVar | Vars], data = [RowIdData | DataLists]},
            NewResultStmts = case proplists:get_value(upd, ResultStmts) of
                undefined -> [{upd, [StmtBind]} | ResultStmts];
                UpdtStmts -> lists:keyreplace(upd, 1, ResultStmts, {upd, UpdtStmts ++ [StmtBind]})
            end,
            create_stmts([{upd, RestUpdList} | Rest], TableName, Connection, Columns, NewResultStmts);
        Error ->
            ?Error("Error preparing update stmt: ~p", [Error]),
            close_stmts(Connection, ResultStmts),
            {error, iolist_to_binary(["Unable to prepare stmt: ", Sql])}
    end;
create_stmts([{ins, []} | Rest], TableName, Connection, Columns, ResultStmt) ->
    create_stmts(Rest, TableName, Connection, Columns, ResultStmt);
create_stmts([{ins, InsList} | Rest], TableName, Connection, Columns, ResultStmts) ->
    [{NonEmptyCols, Rows} | RestInsList] = InsList,
    FilterColumns = filter_columns(NonEmptyCols, Columns),
    InsColumns = ["(", create_ins_columns(FilterColumns), ")"],
    Sql = iolist_to_binary(["insert into ", table_name(TableName), " ", InsColumns, " values ", "(", create_ins_vars(FilterColumns), ") returning rowid into :IDENTIFIER"]),
    ?Info("The insert sql ~p", [Sql]), %% TODO: Remove
    case dderlodpi:dpi_conn_prepareStmt(Connection, Sql) of
        Stmt when is_reference(Stmt) ->
            {RowIdVar, RowIdData} =
                dderlodpi:dpi_conn_newVar(
                    Connection,
                    length(Rows),
                    'DPI_ORACLE_TYPE_ROWID',
                    'DPI_NATIVE_TYPE_ROWID',
                    0
                ),
            ok = dderlodpi:dpi_stmt_bindByName(Connection, Stmt, <<"IDENTIFIER">>, RowIdVar),
            {Vars, DataLists} = create_and_bind_vars(Connection, Stmt, FilterColumns, length(Rows)),
            StmtBind = #binds{stmt = Stmt, var = [RowIdVar | Vars], data = [RowIdData | DataLists]},
            NewResultStmts = case proplists:get_value(ins, ResultStmts) of
                undefined -> [{ins, [StmtBind]} | ResultStmts];
                InsStmts -> lists:keyreplace(ins, 1, ResultStmts, {ins, InsStmts ++ [StmtBind]})
            end,
            create_stmts([{ins, RestInsList} | Rest], TableName, Connection, Columns, NewResultStmts);
        Error ->
            ?Error("Error preparing insert stmt: ~p", [Error]),
            close_stmts(Connection, ResultStmts),
            {error, iolist_to_binary(["Unable to prepare stmt: ", Sql])}
    end.

-spec process_delete(term(), term(), list(), list()) -> {ok, list()} | {error, term()}.
process_delete(_Conn, undefined, [], _Columns) -> {ok, []};
process_delete(Connection, #binds{stmt = Stmt, var = Var}, Rows, _Columns) ->
    RowsToDelete = [[Row#row.id] || Row <- Rows],
    %% Delete is always only one column (Rowid), so wrap the var in a list.
    ok = dderlodpi:dpi_var_set_many(Connection, [Var], RowsToDelete),
    case dderlodpi:dpi_stmt_executeMany(Connection, Stmt, length(RowsToDelete), []) of
        {error, _DpiNifFile, _Line, #{message := Msg}} ->
            {error, list_to_binary(Msg)};
        ok ->
            ChangedKeys = [{Row#row.pos, {{}, {}}} || Row <- Rows],
            {ok, ChangedKeys}
    end.

-spec process_update(term(), list(), list(), [#rowCol{}]) -> {ok, list()} | {error, term()}.
process_update(_Conn, undefined, [], _Columns) -> {ok, []};
process_update(_Conn, [], [], _Colums) -> {ok, []};
process_update(Connection, [PrepStmt | RestStmts], [{ModifiedCols, Rows} | RestRows], Columns) ->
    FilterRows = [Row#row{values = filter_columns(ModifiedCols, Row#row.values)} || Row <- Rows],
    case process_one_update(Connection, PrepStmt, FilterRows, Rows, Columns) of
        {ok, ChangedKeys} ->
            case process_update(Connection, RestStmts, RestRows, Columns) of
               {ok, RestChangedKeys} ->
                    {ok, ChangedKeys ++ RestChangedKeys};
               ErrorRest ->
                    ErrorRest
            end;
        Error ->
            Error
    end.

-spec process_one_update(term(), term(), [#row{}], [#row{}], [#rowCol{}]) -> {ok, list()} | {error, term()}.
process_one_update(Connection, #binds{stmt = Stmt, var = Var}, FilterRows, Rows, Columns) ->
    RowsToUpdate = [[Row#row.id | Row#row.values] || Row <- FilterRows],
    ok = dderlodpi:dpi_var_set_many(Connection, Var, RowsToUpdate),
    case dderlodpi:dpi_stmt_executeMany(Connection, Stmt, length(RowsToUpdate), []) of
        {error, _DpiNifFile, _Line, #{message := Msg}} ->
            ?Info("ERROR: ~p ~n", [Msg]),
            {error, list_to_binary(Msg)};
        ok ->
            ChangedKeys = [{Row#row.pos, {{}, list_to_tuple(create_changedkey_vals(Row#row.values ++ [Row#row.id], Columns ++ [#rowCol{type = 'DPI_ORACLE_TYPE_ROWID'}]))}} || Row <- Rows],
            ?Info("The changed keys ~p", [ChangedKeys]),
            {ok, ChangedKeys}
    end.

-spec process_insert(term(), term(), list(), list()) -> {ok, list()} | {error, term()}.
process_insert(_Conn, undefined, [], _Columns) -> {ok, []};
process_insert(_Conn, [], [], _Columns) -> {ok, []};
process_insert(Connection, [PrepStmt | RestStmts], [{NonEmptyCols, Rows} | RestRows], Columns) ->
    FilterRows = [Row#row{values = filter_columns(NonEmptyCols, Row#row.values)} || Row <- Rows],
    case process_one_insert(Connection, PrepStmt, FilterRows, Rows, Columns) of
        {ok, ChangedKeys} ->
            case process_insert(Connection, RestStmts, RestRows, Columns) of
                {ok, RestChangedKeys} ->
                    {ok, ChangedKeys ++ RestChangedKeys};
                ErrorRest ->
                    ErrorRest
            end;
        Error ->
            Error
    end.

-spec process_one_insert(term(), term(), [#row{}], [#row{}], [#rowCol{}]) -> {ok, list()} | {error, term()}.
process_one_insert(Connection, #binds{stmt = Stmt, var = [RowIdVar | Vars]}, FilterRows, Rows, Columns) ->
    RowsToInsert = [Row#row.values || Row <- FilterRows],
    ok = dderlodpi:dpi_var_set_many(Connection, Vars, RowsToInsert),
    case dderlodpi:dpi_stmt_executeMany(Connection, Stmt, length(RowsToInsert), []) of
        {error, _DpiNifFile, _Line, #{message := Msg}} ->
            {error, list_to_binary(Msg)};
        ok ->
            RowIds = dderlodpi:dpi_var_get_rowids(Connection, RowIdVar, length(RowsToInsert)),
            case inserted_changed_keys(RowIds, Rows, Columns) of
                {error, ErrorMsg} -> {error, ErrorMsg};
                ChangedKeys -> {ok, ChangedKeys}
            end
    end.

-spec split_changes(list()) -> {[#row{}], [#row{}], [#row{}]}.
split_changes(ChangeList) ->
    split_changes(ChangeList, {[], [], []}).

split_changes([], Result) -> Result;
split_changes([ListRow | ChangeList], Result) ->
    [Pos, Op, Index | Values] = ListRow,
    case Index of
        {{}, {}}  -> RowId = undefined;
        {{}, Idx} -> RowId = element(tuple_size(Idx), Idx);
        _ ->         RowId = undefined
    end,
    Row = #row{index  = Index,
               id     = RowId,
               pos    = Pos,
               op     = Op,
               values = Values},
    NewResult = add_to_split_result(Row, Result),
    split_changes(ChangeList, NewResult).

%%TODO: Change for less verbose option setelement...
add_to_split_result(#row{op = del} = Row, {DeleteRows, UpdateRows, InsertRows}) ->
    {[Row | DeleteRows], UpdateRows, InsertRows};
add_to_split_result(#row{op = upd} = Row, {DeleteRows, UpdateRows, InsertRows}) ->
    {DeleteRows, [Row | UpdateRows], InsertRows};
add_to_split_result(#row{op = ins} = Row, {DeleteRows, UpdateRows, InsertRows}) ->
    {DeleteRows, UpdateRows, [Row | InsertRows]}.

filter_columns(ModifiedCols, Columns) ->
    ModifiedColsList = tuple_to_list(ModifiedCols),
    [lists:nth(ColIdx, Columns) || ColIdx <- ModifiedColsList].

create_upd_vars([#rowCol{} = Col]) -> [Col#rowCol.tag, " = :", "\"", Col#rowCol.tag, "\""];
create_upd_vars([#rowCol{} = Col | Rest]) -> [Col#rowCol.tag, " = :", "\"", Col#rowCol.tag, "\"", ", ", create_upd_vars(Rest)].

create_ins_columns([#rowCol{} = Col]) -> [Col#rowCol.tag];
create_ins_columns([#rowCol{} = Col | Rest]) -> [Col#rowCol.tag, ", ", create_ins_columns(Rest)].

create_ins_vars([#rowCol{} = Col]) -> [":", "\"", Col#rowCol.tag, "\""];
create_ins_vars([#rowCol{} = Col | Rest]) -> [":", "\"", Col#rowCol.tag, "\"", ", ", create_ins_vars(Rest)].

create_changedkey_vals([], _Cols) -> [];
create_changedkey_vals([Val | Rest], [#rowCol{} | RestCols]) ->
    [Val | create_changedkey_vals(Rest, RestCols)].

-spec inserted_changed_keys([binary()], [#row{}], list()) -> [tuple()].
inserted_changed_keys([], [], _) -> [];
inserted_changed_keys([RowId | RestRowIds], [Row | RestRows], Columns) ->
    [{Row#row.pos, {{}, list_to_tuple(create_changedkey_vals(Row#row.values ++ [RowId], Columns ++ [#rowCol{type = 'DPI_ORACLE_TYPE_ROWID'}]))}} | inserted_changed_keys(RestRowIds, RestRows, Columns)];
inserted_changed_keys(_, _, _) ->
    {error, <<"Invalid row keys returned by the oracle driver">>}.

-spec split_by_columns_mod([#row{}], [#rowCol{}], [{tuple(), [#row{}]}]) -> [{tuple(), [#row{}]}].
split_by_columns_mod([], _Columns, Result) -> Result;
split_by_columns_mod([#row{} = Row | RestRows], Columns, Result) ->
    case list_to_tuple(get_modified_cols(Row, Columns)) of
        {} -> %% No changes in the row, nothing to do
            NewResult = Result;
        ModifiedCols ->
            case proplists:get_value(ModifiedCols, Result) of
                undefined ->
                    NewResult = [{ModifiedCols, [Row]} | Result];
                RowsSameCol ->
                    NewResult = lists:keyreplace(ModifiedCols, 1, Result, {ModifiedCols, [Row | RowsSameCol]})
            end
    end,
    split_by_columns_mod(RestRows, Columns, NewResult).

-spec get_modified_cols(#row{}, [#rowCol{}]) -> [integer()].
get_modified_cols(#row{index = Index, values = Values}, Columns) ->
    {{}, OriginalValuesTuple} = Index,
    [_RowId | OriginalValuesR] = lists:reverse(tuple_to_list(OriginalValuesTuple)),
    OriginalValues = lists:reverse(OriginalValuesR),
    %% If we dont have rowid should be read only field.
    LengthOrig = length(OriginalValues),
    LengthOrig = length(Values),
    get_modified_cols(OriginalValues, Values, Columns, 1).

%% TODO: This should apply the same functions used by the rowfun to avoid repeating the code here again
%% null -> <<>> should be the default convertion .
-spec get_modified_cols([binary()], [binary()], [#rowCol{}], pos_integer()) -> [integer()].
get_modified_cols([], [], [], _) -> [];
get_modified_cols([_Orig | RestOrig], [_Value | RestValues], [#rowCol{readonly=true} | Columns], Pos) ->
    get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
get_modified_cols([Null | RestOrig], [Value | RestValues], [#rowCol{} | Columns], Pos) when 
        Null =:= null; Null =:= <<>> ->
    case Value of
        <<>> -> get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
        _ -> [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)]
    end;
get_modified_cols([OrigVal | RestOrig], [Value | RestValues], [#rowCol{type = 'DPI_ORACLE_TYPE_DATE'} | Columns], Pos) ->
    case dderlodpi:dpi_to_dderltime(OrigVal) of
        Value ->
            get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
        _ ->
            [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)]
    end;
get_modified_cols([OrigVal | RestOrig], [Value | RestValues], [#rowCol{type = 'DPI_ORACLE_TYPE_TIMESTAMP'} | Columns], Pos) ->
    case dderlodpi:dpi_to_dderlts(OrigVal) of
        Value ->
            get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
        _ ->
            [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)]
    end;
get_modified_cols([OrigVal | RestOrig], [Value | RestValues], [#rowCol{type = 'DPI_ORACLE_TYPE_TIMESTAMP_TZ'} | Columns], Pos) ->
    case dderlodpi:dpi_to_dderltstz(OrigVal) of
        Value ->
            get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
        _ ->
            [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)]
    end;
get_modified_cols([OrigVal | RestOrig], [OrigVal | RestValues], [#rowCol{} | Columns], Pos) ->
    get_modified_cols(RestOrig, RestValues, Columns, Pos + 1);
get_modified_cols([_OrigVal | RestOrig], [_Value | RestValues], [#rowCol{} | Columns], Pos) ->
    [Pos | get_modified_cols(RestOrig, RestValues, Columns, Pos + 1)].

-spec split_by_non_empty([#row{}], [{tuple(),[#row{}]}]) -> [{tuple(), [#row{}]}].
split_by_non_empty([], Result) -> Result;
split_by_non_empty([#row{values = Values} = Row | RestRows], Result) ->
    NonEmptyCols = list_to_tuple(get_non_empty_cols(Values, 1)),
    case proplists:get_value(NonEmptyCols, Result) of
        undefined ->
            NewResult = [{NonEmptyCols, [Row]} | Result];
        RowsSameCol ->
            NewResult = lists:keyreplace(NonEmptyCols, 1, Result, {NonEmptyCols, [Row | RowsSameCol]})
    end,
    split_by_non_empty(RestRows, NewResult).

-spec get_non_empty_cols([binary()], pos_integer()) -> [integer()].
get_non_empty_cols([], _) -> [];
get_non_empty_cols([<<>> | RestValues], Pos) ->
    get_non_empty_cols(RestValues, Pos + 1);
get_non_empty_cols([_Value | RestValues], Pos) ->
    [Pos | get_non_empty_cols(RestValues, Pos + 1)].

-spec close_stmts(term(), list() | undefined) -> ok.
close_stmts(_Conn, undefined) -> ok;
close_stmts(_Conn, []) -> ok;
close_stmts(Conn, [{del, Binds} | RestBinds]) ->
    close_and_release_binds(Conn, Binds),
    close_stmts(Conn, RestBinds);
close_stmts(Conn, [{upd, BindsList} | RestBinds]) ->
    [close_and_release_binds(Conn, Binds) || Binds <- BindsList],
    close_stmts(Conn, RestBinds);
close_stmts(Conn, [{ins, BindsList} | RestBinds]) ->
    [close_and_release_binds(Conn, Binds) || Binds <- BindsList],
    close_stmts(Conn, RestBinds);
close_stmts(Conn, [Binds | RestBinds]) ->
    close_and_release_binds(Conn, Binds),
    close_stmts(Conn, RestBinds).

-spec close_and_release_binds(reference(), #bind{}) -> ok.
close_and_release_binds(Conn, #binds{stmt = Stmt, var = VarList, data = DataList}) when is_list(VarList) ->
    dderlodpi:dpi_stmt_close(Conn, Stmt),
    [dderlodpi:dpi_var_release(Conn, Var) || Var <- VarList],
    [dderlodpi:dpi_data_release(Conn, Data) || Data <- DataList];
close_and_release_binds(Conn, #binds{stmt = Stmt, var = Var, data = Data}) ->
    dderlodpi:dpi_stmt_close(Conn, Stmt),
    dderlodpi:dpi_var_release(Conn, Var),
    dderlodpi:dpi_data_release(Conn, Data).

create_and_bind_vars(_Connection, _Stmt, [], _RowsCount) -> {[], []};
create_and_bind_vars(Connection, Stmt, [Col | RestCols], RowsCount) ->
     % This is probably missing type for dates / timestamp...
    ?Debug("create and bind vars. Connection ~p RowsCount ~p Col#rowCol.type~p~n", [Connection, RowsCount, Col#rowCol.type]),
    {Var, Data} = dderlodpi:dpi_conn_newVar(Connection, RowsCount, Col#rowCol.type),
    VarFmt = {Var, Data, Col#rowCol.type},
    %{Var, Data} = dderlodpi:dpi_conn_newVar(Connection, RowsCount),
    ok = dderlodpi:dpi_stmt_bindByName(Connection, Stmt, Col#rowCol.tag, Var),
    {AccVar, AccData} = create_and_bind_vars(Connection, Stmt, RestCols, RowsCount),
    {[VarFmt | AccVar], [Data | AccData]}.
