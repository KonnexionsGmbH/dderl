-module(dperl_file_copy_SUITE).

-include_lib("common_test/include/ct.hrl").

-include("../src/dperl/dperl.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).

-export([test/1]).

all() ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":all/0 - Start ===>~n", []),
    [test].

init_per_suite(Config) ->
    file:make_dir("Data"),
    file:make_dir("Data/test1"),
    file:make_dir("Data/test2"),
    file:make_dir("Data/test3"),
    file:make_dir("Data/test1/backup"),
    file:make_dir("Data/test2/backup"),
    file:make_dir("Data/test3/backup"),
    file:make_dir("Data/SMSC_CUC"),
    file:make_dir("Data/SMSC_CUC/smch40"),
    file:make_dir("Data/SMSC_CUC/smch40/tmp"),
    file:make_dir("Data/SMSC_CUC/smch40/test1"),
    file:make_dir("Data/SMSC_CUC/smch40/test2"),
    file:make_dir("Data/SMSC_CUC/smch40/test3"),
    file:make_dir("Programs"),
    file:make_dir("Programs/SFHSmscCUC"),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "Starting ensure dperl ===>~n", []),
    application:load(dderl),
    application:set_env(dderl, port, 8552),
    application:ensure_all_started(dderl),
    Config.

end_per_suite(_Config) ->
    application:stop(dperl).

test(_Config) ->
    {ok, Cwd} = file:get_cwd(),
    Path = "Data/SMSC_CUC/smch40",
    Data = "Data",
    Job = #dperlJob{name = <<"SFHSmscCUC1">>,module = dperl_file_copy,
                   args = #{cleanup => false,refresh => false,sync => true, debug => true,
                            status_dir => "SFHSmscCUC", status_extra => "WORKING\n600\n900",
                            status_path => filename:join(Cwd, "Programs")},
                   srcArgs = #{default => #{root => filename:join(Cwd, "Data"),
                                            proto => local, mask =>
                                    "CUCA_vimszmos-smin?1_1_########_??????????????.CSV"}},
                   dstArgs = #{default => #{proto => local, tmp_dir => "tmp",
                                            root => filename:join(Cwd, Path)}},
                   enabled = true,running = true,plan = at_most_once,
                   nodes = [], opts = []},
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "About to Start SFH SMSCUC job :)~n", []),
    dperl_dal:write(dperlJob, Job),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "Starting SFH SMSCUC job :)~n", []),
    ct:sleep({seconds, 3}),
    File1 = "CUCA_vimszmos-smin41_1_00000868_20181102205842.CSV",
    Header = <<"SubmitTime\tSubmitMicros\tSubmitGti\tSubmitApp\tOrigAddress\tOrigAddressTon\tOrigAddressNpi\tOrigCharset\t"
               "OrigGti\tOrigSca\tOrigImsi\tOrigEsmeId\tOrigIp\tOrigBillingId\tRecipAddress\tRecipAddressTon\t"
               "RecipAddressNpi\tRecipImsi\tDeliverTime\tDeliverMicros\tDeliverAttempt\tDeliverAddress\tDeliverAddressTon\t"
               "DeliverAddressNpi\tDeliverGti\tDeliverImsi\tDeliverEsmeId\tDeliverIp\tMsgReference\tMsgPid\tMsgReqType\t"
               "MsgScheduleTime\tMsgExpiryTime\tMsgStatus\tSegId\tSegCount\tSegLength\tErrorType\tErrorCode\tDiaResult\t"
               "OrigDcs\tOrigMessageId\tOrigImei\tDeliverImei\tOrigPaniHeader\tDeliverPaniHeader\tDeliverMapGti\r\n">>,
    Bin = <<"20181102205442+0100\t542176\t\t\tSwisscom\t5\t0\t0\t\t\t\t36530\t10.196.28.45/51096\t\t41794937801\t1\t1\t\t"
               "20181102205442+0100\t0\t1\t41794937801\t1\t1\t\t\t\t\t1541188482542176498fe861O\t0\t4356\t\t20181102215440\t"
               "9\t1\t1\t8\t2\t19733248\tNA\t4\t626090BD\t\t\t\t\t\r\n">>,
    file:write_file(filename:join(Data, File1), [Header, Bin]),
    ct:sleep({seconds, 5}),
    meck:new(imem_file, [passthrough]),
    meck:expect(imem_file, list_dir, 3, {error, enoent}),
    File2 = "CUCA_vimszmos-smin41_1_00000869_20181102205852.CSV",
    file:write_file(filename:join(Data, File2), [Header, [Bin || _ <- lists:seq(1, 2530)]]),
    ct:sleep({seconds, 5}),
    meck:delete(imem_file, list_dir, 3, false),
    meck:expect(imem_file, open,
                fun(#{path := Root}, File, Modes, _) ->
                    case re:run(File, "tmp") of
                        nomatch -> {error, enoent};
                        _ -> file:open(filename:join(Root, File), lists:usort([binary | Modes]))
                    end
                end),
    ct:sleep({seconds, 6}),
    meck:delete(imem_file, open, 4, false),
    ct:sleep({seconds, 1}),
    ok = file:write_file("Programs/SFHSmscCUC/SFHSmscCUC1.hst", string:join([File1 || _ <- lists:seq(1, 100)], "\n")),
    meck:expect(imem_file, delete, 3, {error, enoent}),
    ct:sleep({seconds, 8}),
    ok = file:delete("Programs/SFHSmscCUC/SFHSmscCUC1.hst"),
    meck:expect(imem_file, rename, 4, {error, enoent}),
    ct:sleep({seconds, 8}),
    meck:expect(imem_file, write, 4, {error, enoent}),
    ct:sleep({seconds, 8}),
    meck:expect(imem_file, write, 4, {error, closed}),
    ct:sleep({seconds, 8}),
    meck:expect(imem_file, pread, 5, {error, enoent}),
    ct:sleep({seconds, 8}),
    meck:expect(imem_file, pread, 5, {error, closed}),
    ct:sleep({seconds, 8}),
    meck:expect(imem_file, open, 4, {error, enoent}),
    ct:sleep({seconds, 8}),
    meck:unload(imem_file),
    ct:sleep({seconds, 9}),
    dperl_dal:disable(Job),
    {ok, Files} = file:list_dir(Path),
    true = lists:member(File1, Files),
    true = lists:member(File2, Files),
    {ok, StatusFiles} = file:list_dir("Programs/SFHSmscCUC"),
    true = lists:member("ServiceActivityLog.sal", StatusFiles),
    true = lists:member("SFHSmscCUC1.hst", StatusFiles),
    meck:new(imem_compiler, [passthrough]),
    meck:expect(imem_compiler, compile, fun(_) -> error(badfun) end),
    dperl_dal:write(dperlJob, Job),
    ct:sleep({seconds, 4}),
    meck:unload(imem_compiler),
    Job2 = Job#dperlJob{name = <<"SFHSmscCUC2">>,
                       srcArgs = #{default => #{root => filename:join(Cwd, "Data"), proto => local,
                                                mask => "CUCA_vimszmos-smin?1_1_########_??????????????.CSV"},
                                   test1 => #{path => "test1", backup => "test1/backup"},
                                   <<"test2">> => #{path => "test2", backup => "test2/backup"},
                                   "test3" => #{path => "test3", backup => "test3/backup"}},
                       dstArgs = #{default => #{proto => local, mask => [], root => filename:join(Cwd, Path)},
                                   test1 => #{path => "test1"},
                                   <<"test2">> => #{path => "test2"},
                                   "test3" => #{path => "test3"}}},
    dperl_dal:write(dperlJob, Job2),
    ct:sleep({seconds, 3}),
    file:write_file(filename:join([Data, "test1", File1]), []),
    file:write_file(filename:join([Data, "test2", File1]), []),
    file:write_file(filename:join([Data, "test1", File2]), [Header, Bin]),
    file:write_file(filename:join([Data, "test2", File2]), [Header, Bin]),
    ct:sleep({seconds, 4}),
    meck:new(imem_file, [passthrough]),
    meck:expect(imem_file, rename,
        fun(_Ctx, Src, Dst, _) ->
            case re:run(Dst, "backup") of
                nomatch -> file:rename(Src, Dst);
                _ -> {error, enoent}
            end
        end),
    file:write_file(filename:join([Data, "test3", File1]), [Header, Bin]),
    ct:sleep({seconds, 4}),
    meck:expect(imem_file, write, 4, {error, closed}),
    file:write_file(filename:join([Data, "test3", File2]), [Header, Bin]),
    ct:sleep({seconds, 8}),
    meck:expect(imem_file, open, 4, {error, closed}),
    ct:sleep({seconds, 8}),
    meck:delete(imem_file, open, 4, false),
    meck:delete(imem_file, write, 4, false),
    meck:expect(imem_file, rename, 4, {error, enoent}),
    ct:sleep({seconds, 8}),
    meck:unload(imem_file),
    ct:sleep({seconds, 8}),
    dperl_dal:disable(Job2),
    dperl_dal:write(dperlJob, Job2),
    ct:sleep({seconds, 2}),
    Pid = global:whereis_name("SFHSmscCUC"),
    exit(Pid, kill),
    meck:new(inet, [unstick, passthrough]),
    meck:expect(inet, gethostname, 0, {error, test}),
    ct:sleep({seconds, 5}),
    meck:unload(inet),
    dperl_dal:disable(Job2),
    {ok, Files1} = file:list_dir(filename:join(Path, "test1")),
    true = lists:member(File1, Files1),
    true = lists:member(File2, Files1),
    {ok, Files2} = file:list_dir(filename:join(Path, "test2")),
    true = lists:member(File1, Files2),
    true = lists:member(File2, Files2),
    {ok, BFiles1} = file:list_dir(filename:join([Data, "test1", "backup"])),
    true = lists:member(File1, BFiles1),
    true = lists:member(File2, BFiles1),
    {ok, BFiles2} = file:list_dir(filename:join([Data, "test2", "backup"])),
    true = lists:member(File1, BFiles2),
    true = lists:member(File2, BFiles2),
    {ok, StatusFiles2} = file:list_dir("Programs/SFHSmscCUC"),
    true = lists:member("SFHSmscCUC2(test1).hst", StatusFiles2),
    true = lists:member("SFHSmscCUC2(test2).hst", StatusFiles2),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "Disabled SFH SMSCUC job :)~n", []).
