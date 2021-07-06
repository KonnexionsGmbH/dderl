#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

-define(L(_F, _A),  io:format(user, "[~s:~p] "_F, [get(script), ?LINE | _A])).
-define(L(_S),      ?L(_S, [])).

main(#{git := true, profile := Profile}) ->
    Script = escript:script_name(),
    put(script, filename:rootname(filename:basename(Script))),
    PathRev = case lists:reverse(filename:split(filename:dirname(Script))) of
        ["."] -> [Profile, "_build"];
        [_, _ | Rest] -> Rest % REVISIT for dderl as app framework
    end,
    Dir = filename:join(lists:reverse(["lib" | PathRev])),
    Libs = lists:filtermap(
        fun(Lib) ->
            L = filename:join(Dir, Lib),
            case filelib:is_dir(L) of
                true -> {true, L};
                _ -> false
            end
        end, filelib:wildcard("*", Dir)
    ),
    ?L("processing ~p paths~n", [length(Libs)]),
    libs(Libs),
    halt(0); % `rebar3 as prod release` success
main(#{git := false}) ->
    ?L("command `git` isn't in path. Aborting release!~n"),
    % fails `rebar3 as prod release`
    halt(1);
main([Profile]) ->
     main(#{git => git(), profile => Profile}).

git() ->
    case os:find_executable("git") of
        false -> false;
        GitExe when is_list(GitExe) -> true
    end.

libs([]) -> all_done;
libs([Lib | Libs]) ->
    {ok, Cwd} = file:get_cwd(),
    case file:set_cwd(Lib) of
        ok ->
            case list_to_binary(os:cmd("git rev-parse HEAD")) of
                <<"fatal:", _>> ->
                    ?L("not_git ~p~n", [Lib]);
                Revision ->
                    case re:run(
                        os:cmd("git remote -v"),
                        "(http[^ ]+)", [{capture, [1], list}]
                    ) of
                        {match,[Url|_]} ->
                            lib(Url, Revision);
                        _ ->
                            ?L("no_remote ~p~n", [Lib])
                    end
            end;
        _ -> 
            ?L("not_found ~p~n", [Lib])
    end,
    ok = file:set_cwd(Cwd),
    libs(Libs).

lib(Url, Revision) ->
    CleanUrl = re:replace(Url, "\\.git", "", [{return, list}]),
    RawUrlPrefix = list_to_binary([CleanUrl, "/raw/", string:trim(Revision)]),
    Beams = filelib:wildcard("*.beam", "ebin"),
    git_inject_origin(Beams, RawUrlPrefix),
    Repo = lists:last(filename:split(CleanUrl)),
    ?L("gitOrigin inserted into ~p beams of ~s~n", [length(Beams), Repo]).

git_inject_origin([], _RawUrlPrefix) -> all_done;
git_inject_origin([Beam | Beams], RawUrlPrefix) ->
    Url = git_file(RawUrlPrefix, Beam),
    BeamFile = filename:join("ebin", Beam),
    {ok, _, Chunks} = beam_lib:all_chunks(BeamFile),
    CInf = binary_to_term(proplists:get_value("CInf", Chunks)),
    CInfOpts = proplists:get_value(options, CInf),
    OptCInfo = proplists:get_value(compile_info, CInfOpts, [{gitOrigin, Url}]),
    OptCInfo1 = lists:keyreplace(gitOrigin, 1, OptCInfo, {gitOrigin, Url}),
    CInfOpts1 = lists:keystore(compile_info, 1, CInfOpts, {compile_info, OptCInfo1}),
    CInf1 = lists:keyreplace(options, 1, CInf, {options, CInfOpts1}),
    Chunks1 = lists:keyreplace("CInf", 1, Chunks, {"CInf", term_to_binary(CInf1)}),
    {ok, PatchBeamBin} = beam_lib:build_module(Chunks1),
    ok = file:write_file(BeamFile, PatchBeamBin),
    git_inject_origin(Beams, RawUrlPrefix).

git_file(RawUrlPrefix, BeamFile) ->
    SrcFile = re:replace(BeamFile, "\\.beam", ".erl", [{return, list}]),
    SrcFilePath = filename:join("src", SrcFile),
    git_file(RawUrlPrefix, BeamFile, SrcFilePath).

git_file(RawUrlPrefix, BeamFile, SrcFilePath) ->
    case {
        list_to_binary(os:cmd("git ls-files --error-unmatch " ++ SrcFilePath)),
        filename:extension(SrcFilePath)
    } of
        {<<"src/", _/binary>>, _} -> uri_join(RawUrlPrefix, SrcFilePath);
        {<<"error: pathspec", _/binary>>, ".erl"} ->
            git_file(
                RawUrlPrefix, BeamFile,
                re:replace(
                    SrcFilePath, "\\.erl", "\\.yrl",
                    [{return, list}]
                )
            );
        {<<"error: pathspec", _/binary>>, ".yrl"} ->
            git_file(
                RawUrlPrefix, BeamFile,
                re:replace(
                    SrcFilePath, "\\.yrl", "\\.xrl",
                    [{return, list}]
                )
            );
        {<<"error: pathspec", _/binary>>, ".xrl"} ->
            {ok, Cwd} = file:get_cwd(),
            case lists:reverse(filename:split(Cwd)) of
                [Repo, _, _, _, _ | Root] ->
                    Alternate = filename:join(lists:reverse([Repo | Root])),
                    case filelib:is_dir(Alternate) of
                        true ->
                            ok = file:set_cwd(Alternate),
                            Res = git_file(
                                RawUrlPrefix, BeamFile,
                                re:replace(
                                    SrcFilePath, "\\.xrl", "\\.erl",
                                    [{return, list}]
                                )
                            ),
                            ok = file:set_cwd(Cwd),
                            Res;
                        _ ->
                            ?L(
                                "file not found ~s(~s) at ~s~n",
                                [SrcFilePath, BeamFile, Cwd]
                            ),
                            <<>>
                    end;
                _ ->
                    ?L(
                        "file not found ~s(~s) at ~s~n",
                        [SrcFilePath, BeamFile, Cwd]
                    ),
                    <<>>
            end;
        Other ->
            ?L("~s(~s) ~p~n", [SrcFilePath, BeamFile, Other]),
            <<>>
    end.

uri_join(Url, Src) ->
    uri_join(binary:last(Url), Url, Src).
uri_join($/, Url, [$/|Src])     -> list_to_binary([Url, Src]);
uri_join($/, Url, Src)          -> list_to_binary([Url, Src]);
uri_join(_, Url, [$/|_] = Src)  -> list_to_binary([Url, Src]);
uri_join(_, Url, Src)           -> list_to_binary([Url, "/", Src]).