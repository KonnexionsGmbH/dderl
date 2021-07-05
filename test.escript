ng -*-
%%! -smp enable -pa _build/default/lib/oranif/ebin/

-define(TNS, <<
        "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
        "(PROTOCOL=TCP)(HOST=192.1681.160)(PORT=1521)))"
        "(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME=orclpdb1)))"
    >>).
-define(USER, <<"scott">>).
-define(PSWD, <<"regit">>).

-define(TEST_SQL, <<"select 1 from dual">>).
    

main(_) ->
    ok = dpi:load_unsafe(),
    Ctx = dpi:context_create(3, 0),
    Conn = dpi:conn_create(Ctx, ?USER, ?PSWD, ?TNS, #{}, #{}),
    Stmt = dpi:conn_prepareStmt(Conn, false, ?TEST_SQL, <<>>),
    1 = dpi:stmt_execute(Stmt, []),
    #{found := true} = dpi:stmt_fetch(Stmt),
    #{data := Result} =
        dpi:stmt_getQueryValue(Stmt, 1),
    1.0 = dpi:data_get(Result),
    io:format("done ~n", []),
    halt(1).


