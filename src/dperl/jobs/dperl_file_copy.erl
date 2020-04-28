-module(dperl_file_copy).

-include_lib("dperl/dperl.hrl").

-behavior(dperl_worker).

% dperl_job exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         get_status/1, init_state/1]).

-ifndef(TEST).

-define(SFTP_TIMEOUT,
          ?GET_CONFIG(sfhSftpTimeout, [get(name)], 3000,
                      "Delay in millisecond before waiting for response from"
                      " sftp after which {error, timeout} will be returned")
       ).

-define(PICK_PROFILE_FUN(_Default),
          ?GET_CONFIG(
                pickProfileFun, [get(name)],
                <<??_Default>>,
                "SFH Transfer profile election function"
            )
       ).

-else.

-define(SFTP_TIMEOUT, 3000).

-define(PICK_PROFILE_FUN(_Default), <<??_Default>>).

-endif.

-define(SSH_DEFAULT_PORT, 22).

-record(state, {name, src_ctx, file_info, src_root, srcs, dsts, src_conn,
                dst_conn, src_connected = false, dst_connected = false,
                status_path, status_extra, status_ctx, status_dir, dst_root,
                dst_ctx, is_list_dir = true, src_file_handle, src_position = 0,
                type, pick_profile_fun, pick_profile_fun_ctx}).

%-------------------------------------------------------------------------------
% dperl_worker
%-------------------------------------------------------------------------------

get_status(#state{}) -> #{}.

init_state([]) -> #state{};
init_state([#dperlNodeJobDyn{} | _]) -> #state{};
init_state([_ | Others]) -> init_state(Others).

init({#dperlJob{name=Name, dstArgs = DstArgs, args = Args,
               srcArgs = SrcArgs}, State}) ->
    ?JInfo("Starting"),
    PickProfileFunStr = ?PICK_PROFILE_FUN(
        fun(ProfileList, _Context) ->
            {Profile, _} = lists:nth(
                rand:uniform(length(ProfileList)),
                ProfileList
            ),
            NewContext = Profile,
            {Profile, NewContext}
        end
    ),
    ?JTrace("PickProfileFunStr ~s", [PickProfileFunStr]),
    try imem_compiler:compile(PickProfileFunStr) of
        PickProfileFun when is_function(PickProfileFun, 2) ->
            case catch parse_args(Args, SrcArgs, DstArgs, State) of
                {ok, #state{status_path = SPath, status_dir = SDir} = State1} ->
                    SDir1 = path_join([SPath, SDir]),
                    case connect_check_dir(SDir1, SDir1) of
                        {ok, SCtx} ->
                            dperl_dal:activity_logger(SCtx, SDir,
                                                     State1#state.status_extra),
                            self() ! execute,
                            {ok,
                             State1#state{
                                name=Name, status_ctx = SCtx,
                                pick_profile_fun = PickProfileFun
                             }};
                        {error, SErr} ->
                            ?JError("Status dir not accessible : ~p", [SErr]),
                            {stop, SErr}
                    end;
                Error ->
                    ?JError("Invalid job parameters : ~p", [Error]),
                    {stop, badarg}
            end
    catch
        _:Error ->
            ?JError("Bad configuration pickProfileFun : ~p", [Error]),
            {stop, Error}
    end;
init({Args, _}) ->
    ?JError("bad start parameters ~p", [Args]),
    {stop, badarg}.

handle_call(Request, _From, State) ->
    ?JWarn("Unsupported handle_call ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?JWarn("Unsupported handle_cast ~p", [Request]),
    {noreply, State}.

handle_info(execute, #state{name = Job} = State) ->
    try
        {ok, State1} = connect_check_src(State),
        case get_source_files(State1) of
            {none, State2} ->
                {ok, State2};
            {ok, State2} ->
                {ok, State3} = connect_check_dst(State2),
                {ok, State4} = open_tmp_files(State3),
                process_files(State4)
        end 
    of
        {ok, #state{is_list_dir = true} = State5} ->
            %% all the files in the state has been processed
            dperl_dal:update_job_dyn(Job, #{}, idle),
            dperl_dal:job_error_close(),
            erlang:send_after(?CYCLE_IDLE_WAIT(?MODULE, Job), self(), execute),
            {noreply, State5};
        {ok, State5} ->
            dperl_dal:update_job_dyn(Job, #{}, synced),
            dperl_dal:job_error_close(),
            erlang:send_after(?CYCLE_ALWAYS_WAIT(?MODULE, Job), self(), execute),
            {noreply, State5}
    catch
        error:{sfh_error, Error, State5}:Stacktrace ->
            ?JError("~p ~p step_failed~n~p", [?MODULE, Error, Stacktrace]),
            dperl_dal:update_job_dyn(Job, #{}, error),
            erlang:send_after(?CYCLE_ERROR_WAIT(?MODULE, Job), self(), execute),
            dperl_dal:job_error(get(jstate), <<"step failed">>, Error),
            {noreply, State5};
        Class:Error:Stacktrace ->
            ?JError("~p ~p ~p~n~p", [?MODULE, Class, Error, Stacktrace]),
            dperl_dal:update_job_dyn(Job, error),
            erlang:send_after(?CYCLE_ERROR_WAIT(?MODULE, Job), self(), execute),
            dperl_dal:job_error(get(jstate), atom_to_binary(Class, utf8), Error),
            {noreply, State}
    end;
handle_info(Msg, State) ->
    ?JWarn("Unhandled msg : ~p", [Msg]),
    {noreply, State}.

terminate(Reason, State) ->
    check_close_conn(State#state.src_ctx, State#state.srcs),
    check_close_conn(State#state.dst_ctx, State#state.dsts),
    ?JInfo("terminate ~p", [Reason]).

%-------------------------------------------------------------------------------
% private
%-------------------------------------------------------------------------------

process_files(#state{name = Job} = State) ->
    case get_source_data(State, ?MAX_BULK_COUNT(?MODULE, Job)) of
        {ok, Data, State1} ->
            {ok, State2} = write_tmp(Data, State1),
            process_files(State2);
        {eof, Data, State1} ->
            {ok, State2} = write_tmp(Data, State1),
            {ok, State3} = rename_tmp_files(State2),
            {ok, State3}
    end.

connect_check_src(#state{src_connected = true} = State) ->
    dperl_dal:activity_logger(State#state.status_ctx, State#state.status_dir,
                             State#state.status_extra),
    case check_dir(State#state.src_ctx, State#state.src_root) of
        ok ->
            {ok, State};
        {error, Error} ->
            error({sfh_error, Error, State#state{src_connected = false}})
    end;
connect_check_src(#state{src_root = Root, src_conn = Conn} = State) ->
    case connect_check_dir(Conn, Root) of
        {ok, Ctx} ->
            ?JInfo("Connected to source"),
            {ok, State#state{src_ctx = Ctx, src_connected = true}};
        {error, Error} ->
            ?JError("Connecting to src : ~p", [Error]),
            error({sfh_error, Error})
    end.

connect_check_dst(#state{dst_connected = true} = State) ->
    case check_dir(State#state.dst_ctx, State#state.dst_root) of
        ok ->
            {ok, State};
        {error, Error} ->
            error({sfh_error, Error,
                   check_error(Error, dst, State#state{dst_connected = false})})
    end;
connect_check_dst(#state{dst_conn = Conn, dst_root = Root} = State) ->
    case connect_check_dir(Conn, Root) of
        {ok, Ctx} ->
            ?JInfo("Connected to destination"),
            {ok, State#state{dst_ctx = Ctx, dst_connected = true}};
        {error, Error} ->
            ?JError("Connecting to dst : ~p", [Error]),
            error({sfh_error, Error, check_error(Error, dst, State)})
    end.

get_source_files(#state{is_list_dir = true, srcs = Srcs} = State) ->
    Fun = fun(K, #{path := Path, match := Match} = Src, Ctx, STimeout) ->
        case imem_file:list_dir(Ctx, Path, STimeout) of
            {error, Error} ->
                ?JError("Listing dir ~p ~s failed : ~p", [K, Path, Error]),
                {error, Error};
            {ok, Files} ->
                ?JTrace("list dir ~p ~s result : ~p", [K, Path, Files]),
                {ok, Src#{files => filter_files(Files, Match),
                          matched => false}}
        end
    end,
    case maps_map(Fun, [State#state.src_ctx, ?SFTP_TIMEOUT], Srcs) of
        {error, Error, Srcs1} ->
            error({sfh_error, Error, is_conn_closed(Error, src, State#state{srcs = Srcs1})});
        {ok, Srcs1} ->
            get_source_files(State#state{srcs = Srcs1, is_list_dir = false})
    end;
get_source_files(#state{file_info = undefined, srcs = Srcs} = State) ->
    case maps:fold(fun(_Profile, #{files := []}, Acc) -> Acc;
                      (Profile, #{files := Files}, Acc) ->
                        [{Profile, Files} | Acc]
                   end, [], Srcs) of
        [] ->
            ?JTrace("No matches found"),
            {none, State#state{is_list_dir = true}};
        ProfileList ->
            {K, NewProfileFunCtx} =
                (State#state.pick_profile_fun)(
                    ProfileList, State#state.pick_profile_fun_ctx
                ),
            #{K := #{path := Path, files := [File|Files]} = Src} = Srcs,
            FileInfo = {K, Path, File},
            ?JTrace("File match found : ~p", [FileInfo]),
            Srcs1 = Srcs#{K => Src#{files => Files}},
            case is_file_in_hist(File, K, State) of
                true ->
                    ?JTrace("Skipping file ~p, since its already in hst file", [FileInfo]),
                    get_source_files(
                        State#state{srcs = Srcs1,
                                    pick_profile_fun_ctx = NewProfileFunCtx});
                false ->
                    {ok,
                     State#state{
                        file_info = FileInfo, srcs = Srcs1,
                        pick_profile_fun_ctx = NewProfileFunCtx
                     }}
            end
    end.

get_source_data(#state{file_info = {_, Path, File}, src_file_handle = undefined} = State, BulkSize) ->
    case imem_file:open(State#state.src_ctx, path_join([Path, File]), [read], ?SFTP_TIMEOUT) of
        {ok, FileHandle} ->
            ?JTrace("Opened src file ~s", [File]),
            get_source_data(State#state{src_file_handle = FileHandle}, BulkSize);
        {error, Error} ->
            ?JError("Opening file ~s error : ~p path : ~p", [File, Error, Path]),
            error({sfh_error, Error, check_error(Error, src, State)})
    end;
get_source_data(#state{src_file_handle = FileHandle, src_ctx = Ctx} = State, BulkSize) ->
    case read(Ctx, FileHandle, State#state.src_position, BulkSize * 1024) of
        {ok, Data, Position} ->
            {ok, Data, State#state{src_position = Position}};
        {eof, Data} ->
            ?JTrace("End of file ~p reached", [State#state.file_info]),
            {eof, Data, State#state{src_file_handle = close_file(Ctx, FileHandle)}};
        {error, Error} ->
            ?JError("Reading ~p : ~p", [State#state.file_info, Error]),
            error({sfh_error, Error, check_error(Error, src, State)})
    end.

open_tmp_files(#state{type = multipleOneToOne, file_info = {K, _, File},
                      dsts = Dsts, dst_ctx = DCtx} = State) ->
    #{K := Dst} = Dsts,
    case open_tmp_file(K, Dst, DCtx, tmp_file_name(File), ?SFTP_TIMEOUT) of
        {error, Error} ->
            error({sfh_error, Error, check_error(Error, dst, State)});
        {ok, Dst1} ->
            {ok, State#state{dsts = Dsts#{K => Dst1}}}
    end;
open_tmp_files(#state{file_info = {_, _, File}, dsts = Dsts} = State) ->
    case maps_map(fun open_tmp_file/5, [State#state.dst_ctx,
                  tmp_file_name(File), ?SFTP_TIMEOUT], Dsts) of
        {error, Error, Dsts1} ->
            error({sfh_error, Error, check_error(Error, dst, State#state{dsts = Dsts1})});
        {ok, Dsts1} ->
            {ok, State#state{dsts = Dsts1}}
    end.

write_tmp(_, #state{file_info = undefined} = State) ->
    ?JError("Src file already closed"),
    {ok, State};
write_tmp(<<>>, State) -> {ok, State};
% N to N copy - srcs and dsts names should match to copy
write_tmp(Data, #state{type = multipleOneToOne, file_info = {K, _, File},
                              dsts = Dsts, dst_ctx = DCtx} = State) ->
    #{K := Dst} = Dsts,
    case write_tmp(K, Dst, DCtx, Data, File, ?SFTP_TIMEOUT) of
        {error, Error} ->
            error({sfh_error, Error, check_error(Error, dst, State)});
        {ok, Dst1} ->
            {ok, State#state{dsts = Dsts#{K => Dst1}}}
    end;
% 1 to 1 and 1 to N
write_tmp(Data, #state{file_info = {_, _, File}, dsts = Dsts,
                              dst_ctx = DCtx} = State) ->
    case maps_map(fun write_tmp/6, [DCtx, Data, File, ?SFTP_TIMEOUT], Dsts) of
        {error, Error, Dsts1} ->
            error({sfh_error, Error, check_error(Error, dst, State#state{dsts = Dsts1})});
        {ok, Dsts1} ->
            {ok, State#state{dsts = Dsts1}}
    end.

check_dir(Ctx, Path) ->
    case imem_file:is_dir(Ctx, Path, ?SFTP_TIMEOUT) of
        false ->
            imem_file:disconnect(Ctx),
            {error, Path ++ " dir not found"};
        true -> ok
    end.

close_file(Ctx, #{file_handle := FileHandle}) -> close_file(Ctx, FileHandle);
close_file(_Ctx, undefined) -> undefined;
close_file(Ctx, FileHandle) ->
    catch imem_file:close(Ctx, FileHandle, ?SFTP_TIMEOUT),
    undefined.

check_args(Srcs, Dsts, State) ->
    case check_arg_counts(lists:sort(maps:keys(Srcs)), lists:sort(maps:keys(Dsts))) of
        invalid ->
            {error, "Src and Dst args does not match"};
        Type ->
            {ok, State#state{dsts = Dsts, srcs = Srcs, type = Type}}
    end.

check_arg_counts(S, D) when length(S) == 1, length(D) == 1 ->
    ?JTrace("Single source and single destination conf"),
    oneToOne;
check_arg_counts(S, D) when length(S) == 1 ->
    ?JTrace("Single source and multiple destination (~p) conf", [length(D)]),
    oneToMany;
check_arg_counts(S, D) when length(D) == 1 ->
    ?JTrace("Multiple source (~p) and single destination conf", [length(S)]),
    manyToOne;
check_arg_counts(N, N) ->
    ?JTrace("Multiple source (~p) and multiple destination (~p) conf", [length(N), length(N)]),
    multipleOneToOne;
check_arg_counts(S, D) ->
    ?JError("Source (~p) and Destination (~p) conf mismatch", [S, D]),
    invalid.

parse_args(#{status_path := StatusPath, status_extra := Extra,
             status_dir := SName}, SrcArg, DstArg, State) ->
    case parse_args(SrcArg) of
        {ok, #{root := SrcRoot} = SrcConn, Srcs} ->
            case parse_args(DstArg) of
                {ok, #{root := DstRoot} = DstConn, Dsts} ->
                    State1 = State#state{status_path = StatusPath,
                                         status_extra = Extra,
                                         status_dir = SName, src_root = SrcRoot,
                                         dst_root = DstRoot, src_conn = SrcConn,
                                         dst_conn = DstConn},
                    check_args(Srcs, Dsts, State1);
                Error -> Error
            end;
        Error -> Error
    end.

parse_args(#{default := _} = Args) ->
    {Default, Rest} = maps:take(default, Args),
    Default1 = maps:merge(#{mask => "", path => "",
                            backup => undefined, tmp_dir => undefined},
                          Default),
    case process_default(Default1) of
        {ok, Default2} ->
            {ok, Conf} = parse_args_rest(Default2, Rest),
            {ok, Default2, Conf};
        {error, _} = Error ->
            Error
    end.

process_default(#{proto := sftp, host := _, user := _, password := _,
                  root := _} = Default) ->
    {ok, Default#{opts => maps:get(opts, Default, []),
                  port => maps:get(port, Default, ?SSH_DEFAULT_PORT)}};
process_default(#{proto := sftp, host := _, user := _, key := _,
                  root := _} = Default) ->
    {ok, Default#{port => maps:get(port, Default, ?SSH_DEFAULT_PORT),
                  opts => maps:get(opts, Default, [])}};
process_default(#{proto := local, root := _} = Default) -> {ok, Default};
process_default(Default) ->
    ?JError("Invalid Default : ~p", [Default]),
    {error, badarg}.

parse_args_rest(#{mask := Mask, path := Path, backup := Backup,
                  tmp_dir := TmpDir}, Rest) when map_size(Rest) == 0 ->
    HistFile = <<(get(name))/binary, ".hst">>,
    {ok, #{"1" => #{mask => Mask, path => Path, backup => Backup,
                    match => transform_mask(Mask), hist_file => HistFile,
                    tmp_dir => TmpDir}}};
parse_args_rest(#{mask := Mask, path := Path, backup := Backup,
                  tmp_dir := TmpDir}, Rest) ->
    DefaultConf = #{mask => Mask, path => Path, backup => Backup,
                    tmp_dir => TmpDir},
    Confs = maps:map(
        fun(K, V) ->
            #{mask := Mask1} = V1 = maps:merge(DefaultConf, V),
            KBin =
                if not (is_list(K) orelse is_binary(K)) ->
                        list_to_binary(io_lib:format("~p", [K]));
                    is_list(K) -> list_to_binary(K);
                    true -> K
                end,
            HistFile = <<(get(name))/binary, "(", KBin/binary, ").hst">>,
            V1#{match => transform_mask(Mask1), hist_file => HistFile}
        end, Rest),
    {ok, Confs}.

tmp_file_name(File) -> "tmp_" ++ File.

transform_mask(Mask) ->
    %% replace all `?` with .
    Mask1 = re:replace(Mask, "[?]", ".", [global, {return, binary}]),
    %% move all hashes into parenthesis
    Mask2 = re:replace(Mask1, "[#]+", "(&)", [global, {return, binary}]),
    %% replace all hashes with \\d to match integers
    Mask3 = re:replace(Mask2, "[#]", "\\\\d", [global, {return, binary}]),
    %% make it a full line match
    <<"^", Mask3/binary, "$">>.

read(Ctx, FileHandle, Position, Size) ->
    read(Ctx, FileHandle, Position, Size, <<>>).
 
read(Ctx, FileHandle, Position, Size, Acc) ->
    case imem_file:pread(Ctx, FileHandle, Position, Size, ?SFTP_TIMEOUT) of
        {ok, Data} when size(Data) < Size ->
            DataSize = size(Data),
            NewPosition = Position + DataSize,
            NewLen = Size - DataSize,
            read(Ctx, FileHandle, NewPosition, NewLen,
                      <<Acc/binary, Data/binary>>);
        {ok, Data} -> {ok, <<Acc/binary, Data/binary>>, Position + Size};
        eof -> {eof, Acc};
        {error, Error} -> {error, Error}
    end.

rename_tmp_files(#state{type = multipleOneToOne, file_info = {K, _, File}, dsts = Dsts,
                        dst_ctx = DCtx, dst_conn = #{root := DstRoot}} = State) ->
    #{K := #{mask := SrcMask}} = State#state.srcs,
    #{K := Dst} = Dsts,
    case rename_tmp_file(K, Dst, DCtx, SrcMask, DstRoot, File, ?SFTP_TIMEOUT) of
        {error, Error} ->
            error({sfh_error, Error, check_error(Error, dst, State)});
        {ok, Dst1} ->
            add_to_hist(State),
            delete_or_backup_src_file(State#state{dsts = Dsts#{K => Dst1}})
    end;
rename_tmp_files(#state{file_info = {K, _, File}, dsts = Dsts, dst_ctx = DCtx,
                        dst_conn = #{root := DstRoot}} = State) ->
    #{K := #{mask := SrcMask}} = State#state.srcs,
    case maps_map(fun rename_tmp_file/7,
                  [DCtx, SrcMask, DstRoot, File, ?SFTP_TIMEOUT], Dsts) of
        {error, Error, Dsts1} ->
            error({sfh_error, Error, check_error(Error, dst, State#state{dsts = Dsts1})});
        {ok, Dsts1} ->
            add_to_hist(State),
            delete_or_backup_src_file(State#state{dsts = Dsts1})
    end.

delete_or_backup_src_file(#state{file_info = {K, _Path, _File}} = State) ->
    case maps:get(K, State#state.srcs) of
        #{backup := undefined} ->
            delete_src_file(State);
        #{backup := BackupDir} ->
            backup_src_file(BackupDir, State)
    end.

delete_src_file(#state{file_info = {_, Path, File}} = State) ->
    case imem_file:delete(State#state.src_ctx, path_join([Path, File]), ?SFTP_TIMEOUT) of
        ok ->
            ?JInfo("Deleted file : ~s", [File]),
            {ok, State#state{file_info = undefined, src_position = 0}};
        {error, Error} ->
            ?JError("Deleting file ~s : ~p", [File, Error]),
            error({sfh_error, Error, check_error(Error, src, State)})
    end.

backup_src_file(BackupDir, #state{file_info = {_, Path, File}, src_ctx = Ctx} = State) ->
    FilePath = path_join([State#state.src_root, Path, File]),
    BackupPath = path_join([State#state.src_root, BackupDir, File]),
    case imem_file:rename(Ctx, FilePath, BackupPath, ?SFTP_TIMEOUT) of
        ok ->
            ?JInfo("Backed up file : ~s", [File]),
            {ok, State#state{file_info = undefined, src_position = 0}};
        {error, Error} ->
            ?JError("Backing up file ~s failed : ~p", [BackupPath, Error]),
            error({sfh_error, Error, check_error(Error, src, State)})
    end.

check_error(Err, Type, #state{src_ctx = SCtx, src_file_handle = SFileHandle,
                              dsts = Dsts, dst_ctx = DCtx} = State) ->
    Dsts1 = maps:map(
        fun(_K, V) ->
            V#{file_handle => close_file(DCtx, V)}
        end, Dsts),
    is_conn_closed(Err, Type,
                   State#state{file_info = undefined, src_position = 0,
                               src_file_handle = close_file(SCtx, SFileHandle),
                               dsts = Dsts1}).

is_conn_closed(closed, src, State) ->
    Srcs = check_close_conn(State#state.src_ctx, State#state.srcs),
    State#state{src_connected = false, srcs = Srcs};
is_conn_closed(closed, dst, State) ->
    Dsts = check_close_conn(State#state.dst_ctx, State#state.dsts),
    State#state{dst_connected = false, dsts = Dsts};
is_conn_closed(_Err, _Type, State) -> State.

check_close_conn(Ctx, Confs) ->
    Conf1 = maps:map(
        fun(_K, M) ->
            close_file(Ctx, M),
            M#{file_handle => undefined, files => []}
        end, Confs),
    imem_file:disconnect(Ctx),
    Conf1.

connect_check_dir(Conn, Path) ->
    Conn1 = case is_map(Conn) of
                true -> Conn#{path => Path};
                false -> Conn
            end,
    case imem_file:connect(Conn1) of
        {error, _} = Error -> Error;
        Ctx ->
            case check_dir(Ctx, Path) of
                {error, _} = Err -> Err;
                ok -> {ok, Ctx}
            end
    end.

maps_map(Fun, Args, Map) when is_map(Map) ->
    maps_map(Fun, Args, maps:keys(Map), Map).

maps_map(_Fun, _Args, [], Map) -> {ok, Map};
maps_map(Fun, Args, [K | Keys], Map) ->
    case apply(Fun, [K, maps:get(K, Map) | Args]) of
        {ok, V} ->
            maps_map(Fun, Args, Keys, Map#{K => V});
        {error, Error} ->
            {error, Error, Map}
    end.

get_tmp_path(#{path := Path, tmp_dir := undefined}) -> Path;
get_tmp_path(#{tmp_dir := TmpDir}) -> TmpDir.

open_tmp_file(K, Dst, Ctx, File, Timeout) ->
    TmpFilePath = path_join([get_tmp_path(Dst), File]),
    case imem_file:open(Ctx, TmpFilePath, [write], Timeout) of
        {error, Error} ->
            ?JError("Opening ~s error : ~p", [File, Error]),
            {error, Error};
        {ok, FileHandle} ->
            ?JTrace("Opened dst file ~s to write for conn : ~p", [File, K]),
            {ok, Dst#{file_handle => FileHandle}}
    end.

write_tmp(K, #{file_handle := FileHandle} = Dst, Ctx, Data, File, STimeout) ->
    case imem_file:write(Ctx, FileHandle, Data, STimeout) of
        {error, Error} ->
            ?JError("Writing tmp(~p) file error : ~p", [K, Error]),
            {error, Error};
        ok ->
            ?JTrace("Copied ~p kb from ~s", [byte_size(Data) / 1024, File]),
            {ok, Dst}
    end.

rename_tmp_file(K, #{file_handle := FileHandle, mask := DMask, path := Path} = Dst, Ctx,
                SMask, DRoot, SFile, Timeout) ->
    imem_file:close(Ctx, FileHandle, Timeout),
    DstFile = dstFileName(SFile, SMask, DMask),
    TmpFile = tmp_file_name(SFile),
    TmpPath = path_join([DRoot, get_tmp_path(Dst), TmpFile]),
    NewPath = path_join([DRoot, Path, DstFile]),
    case imem_file:rename(Ctx, TmpPath, NewPath, Timeout) of
        ok ->
            ?JTrace("Renamed ~s to ~s for ~p", [TmpFile, DstFile, K]),
            {ok, Dst#{file_handle => undefined}};
        {error, Error} ->
            ?JError("Renaming ~s error : ~p", [TmpFile, Error]),
            {error, Error}
    end.

is_file_in_hist(File, Key, State) ->
    #{Key := #{hist_file := HistFile}} = State#state.srcs,
    case imem_file:read_file(State#state.status_ctx, HistFile, ?SFTP_TIMEOUT) of
        {error, Error} ->
            ?JError("Reading hist file error : ~p", [Error]),
            false;
        {ok, Hist} ->
            lists:member(list_to_binary(File), re:split(Hist, "\n"))
    end.

add_to_hist(#state{file_info={K, _, File}, srcs = Srcs, status_ctx = SCtx}) ->
    #{K := #{hist_file := HistFile}} = Srcs,
    Timeout = ?SFTP_TIMEOUT,
    Data = [list_to_binary(File), <<"\n">>],
    case imem_file:read_file(SCtx, HistFile, Timeout) of
        {error, Error} ->
            ?JError("Reading hist file ~s error : ~p", [HistFile, Error]),
            imem_file:write_file(SCtx, HistFile, Data, Timeout);
        {ok, Hist} ->
            Lines = re:split(string:trim(Hist), "\n", [{return, list}]),
            Lines1 =
            case length(Lines) of
                L when L < 100 ->
                    Lines ++ [File];
                L ->
                    {_, List2} = lists:split(L - 99, Lines),
                    List2 ++ [File]
            end,
            imem_file:write_file(SCtx, HistFile, string:join(Lines1, "\n"), Timeout)
    end,
    ?JTrace("Filename ~s written to hst file ~s", [File, HistFile]).

filter_files([], _) -> [];
filter_files([File | Rest], Match) ->
    lists:sort(
        case re:run(File, Match, [{capture, all_but_first, binary}]) of
            {match, _} -> [File | filter_files(Rest, Match)];
            nomatch -> filter_files(Rest, Match)
        end
    ).

path_join([[], File]) -> File;
path_join(Paths) -> filename:join(Paths).

%% -----------------------------------------------------------------------------
%% dstFileName
%% -----------------------------------------------------------------------------

dstFileName(SrcFN, _, "") -> SrcFN;
dstFileName(SrcFN, _, "*") -> SrcFN;
dstFileName(SrcFN, _, ".*") -> SrcFN;
dstFileName(SrcFN, _, "*.*") -> SrcFN;
dstFileName(SrcFN, SrcFM, DstFM)
 when is_list(SrcFN), is_list(SrcFM), is_list(DstFM) ->
    NewDstFM = hash(SrcFN, SrcFM, DstFM),
    dstFN(SrcFN, NewDstFM, []).

dstFN(_SrcFN, [], DstFile) -> lists:reverse(DstFile);
dstFN(SrcFN, [$<|DstFM], DstFile) ->
    dstFN(SrcFN, dt(DstFM), DstFile);
dstFN([FC|SrcFN], [C|DstFM], DstFile) when C == $?; C == $# ->
    dstFN(SrcFN, DstFM, [FC|DstFile]);
dstFN([_|SrcFN], [C|DstFM], DstFile) ->
    dstFN(SrcFN, DstFM, [C|DstFile]);
dstFN([], [C|DstFM], DstFile) ->
    dstFN([], DstFM, [C|DstFile]).

dt(DstFM) ->
    {{Year, Month, Day}, {Hour, Minute, Second}}
    = calendar:now_to_local_time(os:timestamp()),
    dt(
        DstFM,
        {io_lib:format("~4..0B", [Year]), io_lib:format("~2..0B", [Month]),
         io_lib:format("~2..0B", [Day])},
        {io_lib:format("~2..0B", [Hour]), io_lib:format("~2..0B", [Minute]),
         io_lib:format("~2..0B", [Second])},
        []
    ).

dt([$>|DstFM], _, _, Buf) -> lists:flatten([Buf, DstFM]);
dt([$M,$M|DstFM], {_,M,_} = Dt, T, Buf) -> dt(DstFM, Dt, T, Buf ++ M);
dt([$D,$D|DstFM], {_,_,D} = Dt, T, Buf) -> dt(DstFM, Dt, T, Buf ++ D);
dt([$h,$h|DstFM], Dt, {H,_,_} = T, Buf) -> dt(DstFM, Dt, T, Buf ++ H);
dt([$m,$m|DstFM], Dt, {_,M,_} = T, Buf) -> dt(DstFM, Dt, T, Buf ++ M);
dt([$s,$s|DstFM], Dt, {_,_,S} = T, Buf) -> dt(DstFM, Dt, T, Buf ++ S);
dt([$Y,$Y,$Y,$Y|DstFM], {Y,_,_} = Dt, T, Buf) -> dt(DstFM, Dt, T, Buf ++ Y).

hash(_SrcFN, _SrcFM, DstFM) -> DstFM.

%% -----------------------------------------------------------------------------
%% TESTS
%% -----------------------------------------------------------------------------

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    Ctx = #{proto => local, path => "test"},
    {inparallel,
        [ {"init bad arg test1", ?_assertEqual({stop, badarg}, init({#dperlJob{name = <<"test">>}, test}))}
        , {"init bad arg test2", ?_assertEqual({stop, badarg}, init({test, test}))}
        , {"init state", ?_assertEqual(#state{}, init_state([5, #dperlNodeJobDyn{state = #{}}]))}
        , {"handle_call", ?_assertEqual({reply, ok, st}, handle_call(req, self(), st))}
        , {"handle_cast", ?_assertEqual({noreply, st}, handle_cast(req, st))}
        , {"close_file", ?_assertEqual(undefined, close_file(ctx, undefined))}
        , {"oneToOne", ?_assertEqual(oneToOne, check_arg_counts(["default"], ["test"]))}
        , {"oneToMany", ?_assertEqual(oneToMany, check_arg_counts(["1"], ["test1", "test2"]))}
        , {"manyToOne", ?_assertEqual(manyToOne, check_arg_counts(["test1","test2"], ["test"]))}
        , {"multipleOneToOne", ?_assertEqual(multipleOneToOne, check_arg_counts(["1", "2"], ["1", "2"]))}
        , {"invalid1", ?_assertEqual(invalid, check_arg_counts(["1", "2"], ["1", "2", "3"]))}
        , {"invalid2", ?_assertEqual(invalid, check_arg_counts(["1", "2"], ["2", "1"]))}
        , {"check args", ?_assertEqual({error, "Src and Dst args does not match"}, check_args(#{a => 1, b => 2}, #{c => 2, a => 1}, #state{}))}
        , {"check dir invalid", ?_assertEqual({error, badarg}, connect_check_dir("test", "test"))}
        , {"check dir invalid path", ?_assertMatch({error, _}, connect_check_dir("/home", "test"))}
        , {"parse args invalid default", ?_assertEqual({error, badarg}, parse_args(#{status_dir => s, status_path => s, status_extra => e}, #{default => #{}}, d, s))}
        , {"parse default sftp1", ?_assertMatch({ok, _}, process_default(#{proto => sftp, host => h, root => r, user => u, key => k}))}
        , {"parse default sftp2", ?_assertMatch({ok, _}, process_default(#{proto => sftp, host => h, root => r, user => u, password => p}))}
        , {"connect check src error", ?_assertMatch({_, {{sfh_error, _}, _}}, catch connect_check_src(#state{src_root = "test", src_conn = #{proto => local}}))}
        , {"connect check dst error", ?_assertMatch({_, {{sfh_error, _, _}, _}}, catch connect_check_dst(#state{dst_root = "test", dst_conn = #{proto => local}, dsts = #{}}))}
        , {"connect check src conn error", ?_assertMatch({_, {{sfh_error, _, _}, _}}, catch connect_check_src(#state{src_root = "test", src_connected = true, src_ctx = Ctx}))}
        , {"connect check dst conn error", ?_assertMatch({_, {{sfh_error, _, _}, _}}, catch connect_check_dst(#state{dst_root = "test", dst_connected = true, dst_ctx = Ctx, dsts = #{}}))}
        , {"unhandled info", ?_assertEqual({noreply, test}, handle_info(test, test))}
        , {"write dst with file closed", ?_assertMatch({ok, _}, write_tmp(test, #state{file_info = undefined}))}
        ]
    }.

parse_args_invalid_dst_test() ->
    put(name, <<"test">>),
    ?assertEqual({error, badarg}, parse_args(#{status_dir => s, status_path => s, status_extra => e}, #{default => #{proto => local, root => r, mask => ""}}, #{default => #{}}, s)).

init_error_test() ->
    put(name, <<"test">>),
    ?assertEqual({stop, badarg},
                 init({#dperlJob{name = <<"test">>, args = #{status_dir => "test", status_path => "config", status_extra => "e"},
                                dstArgs = #{default => #{proto => local, root => r, mask => ""}},
                                srcArgs = #{default => #{proto => local, root => r, mask => ""}}}, #state{}})).

dstFileName_test_() ->
    {inparallel, [
        {T, ?_assertEqual(DF, dstFileName(SF, SM, DM))}
        || {T, SF, SM, DM, DF} <-
            [
                {"no change", "ABCD1234.txt", "ABCD????.txt", "", "ABCD1234.txt"},
                {"no change *", "ABCD1234.txt", "ABCD????.txt", "*", "ABCD1234.txt"},
                {"no change .*", "ABCD1234.txt", "ABCD????.txt", ".*", "ABCD1234.txt"},
                {"no change *.*", "ABCD1234.txt", "ABCD????.txt", "*.*", "ABCD1234.txt"},
                {"reduce", "ABCD1234.txt", "ABCD????.txt", "????12.txt", "ABCD12.txt"},
                {"remove_extn", "ABCD1234.txt", "ABCD????.txt", "????12", "ABCD12"},
                {"reduce_replace_extn", "ABCD1234.txt", "ABCD????.txt", "????12.csv", "ABCD12.csv"},
                {"extend", "ABCD1234.txt", "ABCD????.txt", "?????SomthingElse", "ABCD1SomthingElse"},
                {"date", "SMCH80-1234-abcdefgh.ascii", "SMCH90-????-########.ascii",
                 "T<YYYY>080_SBS_SMS_SMSC.csv", "T2020080_SBS_SMS_SMSC.csv"},
                {"MMSC", "F-miopoltmmstn00-20190127234100.123.dat", "F-miop??????????-??????????????.???.dat",
                 "F-miop??????????-??????????????","F-miopoltmmstn00-20190127234100"},
                {"Hotbill", "HB-SMCH80-abcd-12345678-RAP.xml", "HB-SMCH80-????-########-RAP.xml",
                 "HB-SMCH80-????-########.xml", "HB-SMCH80-abcd-12345678.xml"}
            ]
       ]
    }.

dt_test_() ->
    {{Y, M, D}, {H, Min, S}}
    = calendar:now_to_local_time(os:timestamp()),
    Year = lists:flatten(io_lib:format("~4..0B", [Y])),
    Month = lists:flatten(io_lib:format("~2..0B", [M])),
    Day = lists:flatten(io_lib:format("~2..0B", [D])),
    Date = Year ++ Month ++ Day,
    Hour = lists:flatten(io_lib:format("~2..0B", [H])),
    Minute = lists:flatten(io_lib:format("~2..0B", [Min])),
    Second = lists:flatten(io_lib:format("~2..0B", [S])),
    Time = Hour ++ Minute ++ Second,
    {inparallel, [
        {T, ?_assertEqual(E, dt(DM, {Year, Month, Day}, {Hour, Minute, Second}, []))}
        || {T, DM, E} <-
            [
                {"all", "YYYYMMDDhhmmss>", Date ++ Time},
                {"date", "YYYYMMDD>", Date},
                {"time", "hhmmss>", Time},
                {"year", "YYYY>", Year},
                {"month", "MM>", Month},
                {"day", "DD>", Day},
                {"hour", "hh>", Hour},
                {"minute", "mm>", Minute},
                {"second", "ss>", Second},
                {"year_month", "YYYYMM>", Year ++ Month},
                {"month_day", "MMDD>", Month ++ Day},
                {"hour_minute", "hhmm>", Hour ++ Minute},
                {"minute_second", "mmss>", Minute ++ Second}
            ]
       ]
    }.

-endif.
