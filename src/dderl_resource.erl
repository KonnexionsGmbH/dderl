%% @doc POST ajax handler.
-module(dderl_resource).
-author('Bikram Chatterjee <bikram.chatterjee@k2informatics.ch>').

-behaviour(cowboy_loop_handler).
 
-include("dderl.hrl").

-export([init/3]).
-export([info/3]).
-export([terminate/3]).


%-define(DISP_REQ, 1).

init({ssl, http}, Req, []) ->
    display_req(Req),
    {_Method, Req0} = cowboy_req:method(Req),
    case cowboy_req:has_body(Req0) of
    true ->
        {Session, Req1} = cowboy_req:header(<<"dderl_sess">>,Req0),
        {Adapter, Req2} = cowboy_req:header(<<"adapter">>,Req1),
        {Typ, Req3} = cowboy_req:path_info(Req2),
        %?Info("DDerl {session, adapter} from header ~p", [{Session,Adapter,Typ}]),
        process_request(Session, Adapter, Req3, Typ);
    _Else ->
        ?Error("DDerl request ~p, error ~p", [Req0, _Else]),
        self() ! {reply, <<"{}">>},
        {loop, Req, <<>>, 5000, hibernate}
    end.

read_multipart(Req) -> read_multipart(Req, <<>>).
read_multipart({done, Req}, Body) -> {ok, Body, Req};
read_multipart({ok, Data, Req}, Body) ->
    read_multipart(cowboy_req:stream_body(Req), list_to_binary([Body, Data])).

process_request(_, _, Req, [<<"upload">>]) ->
    {ok, ReqData, Req1} = read_multipart(cowboy_req:stream_body(Req)),
    %%{ok, ReqData, Req1} = cowboy_req:body(Req),
    ?Debug("Request ~p", [ReqData]),
    Resp = {reply, jsx:encode([{<<"upload">>, 
        case re:run(ReqData, ".*filename=[\"](.*)[\"].*", [{capture,[1],binary}]) of
            {match, [FileName]} -> [{<<"name">>, FileName}];
            _ -> []
        end ++
        case re:run(ReqData, "\r\n\r\n(.*)\r\n\r\n[-a-zA-Z0-9]+", [{capture,[1],binary},dotall]) of
            {match, [FileContent]} -> [{<<"content">>, FileContent}];
            _ -> []
        end
    }])},
    ?Debug("Responding ~p", [Resp]),
    self() ! Resp,
    {loop, Req1, <<>>, 5000, hibernate};
process_request(_, _, Req, [<<"download_query">>] = Typ) ->
    {ok, ReqDataList, Req1} = cowboy_req:body_qs(Req),
    Session = proplists:get_value(<<"dderl_sess">>, ReqDataList, <<>>),
    Adapter = proplists:get_value(<<"adapter">>, ReqDataList, <<>>),
    FileToDownload = proplists:get_value(<<"fileToDownload">>, ReqDataList, <<>>),
    QueryToDownload = proplists:get_value(<<"queryToDownload">>, ReqDataList, <<>>),
    Connection = proplists:get_value(<<"connection">>, ReqDataList, <<>>),
    process_request_low(Session, Adapter, Req1
                        , jsx:encode([{<<"download_query">>, [ {<<"connection">>, Connection}
                                        , {<<"fileToDownload">>, FileToDownload}
                                        , {<<"queryToDownload">>, QueryToDownload}
                                        ]}])
                        , Typ);
process_request(Session, Adapter, Req, Typ) ->
    {ok, Body, Req1} = cowboy_req:body(Req),
    process_request_low(Session, Adapter, Req1, Body, Typ).

process_request_low(Session, Adapter, Req, Body, Typ) ->
    case create_new_session(Session) of
        {ok, {_,DDerlSessPid} = DderlSess} ->
            AdaptMod = if
                is_binary(Adapter) -> list_to_existing_atom(binary_to_list(Adapter) ++ "_adapter");
                true -> undefined
            end,
            DderlSess:process_request(AdaptMod, Typ, Body, self()),
            {loop, Req, DDerlSessPid, 3600000, hibernate};
        {error, Reason} ->
            {{Ip, Port}, Req0} = cowboy_req:peer(Req),
            ?Info("session ~p doesn't exist (~p), from ~s:~p", [Session, Reason, imem_datatype:ipaddr_to_io(Ip), Port]),
            self() ! {reply, jsx:encode([{<<"error">>, <<"Session is not valid">>}])},
            {loop, Req0, Session, 5000, hibernate}
    end.

info({reply, Body}, Req, DDerlSessPid) ->
    ?NoDbLog(debug, [], "reply ~n~s", [jsx:prettify(Body)]),
    {ok, Req2} = reply_200_json(Body, DDerlSessPid, Req),
    {ok, Req2, DDerlSessPid};
info({reply_csv, FileName, Chunk, ChunkIdx}, Req, DDerlSessPid) ->
    ?Debug("reply csv FileName ~p, Chunk ~p, ChunkIdx ~p", [FileName, Chunk, ChunkIdx]),
    {ok, Req1} = reply_csv(FileName, Chunk, ChunkIdx, Req),
    case ChunkIdx of
        last -> {ok, Req1, DDerlSessPid};
        single -> {ok, Req1, DDerlSessPid};
        _ -> {loop, Req1, DDerlSessPid, hibernate}
    end;
info(Message, Req, State) ->
    ?Error("~p unknown message in loop ~p", [self(), Message]),
    {loop, Req, State, hibernate}.

terminate(_Reason, _Req, _State) ->
	ok.

%% Helper functions
-spec create_new_session(binary() | list()) -> {ok, {atom(), pid()}} | {error, term()}.
create_new_session(<<>>) ->
    DderlSess = dderl_session:start(),
    ?Debug("new dderl session ~p from ~p", [DderlSess, self()]),
    {ok, DderlSess};
create_new_session(DDerlSessStr) when is_list(DDerlSessStr) ->
    try
        {_, Pid} = _DDerlSess = ?DecryptPid(DDerlSessStr),
        %?Debug("existing session ~p", [DDerlSess]),
        case erlang:process_info(Pid) of
            undefined -> {error, <<"process not found">>};
            _ -> {ok, {dderl_session, Pid}}
        end
    catch
        Error:Reason ->  {error, {Error, Reason}}
    end;
create_new_session(S) when is_binary(S) -> create_new_session(binary_to_list(S));
create_new_session(_) -> create_new_session(<<>>).

% Reply templates
% cowboy_req:reply(400, [], <<"Missing echo parameter.">>, Req),
% cowboy_req:reply(200, [{<<"content-encoding">>, <<"utf-8">>}], Echo, Req),
% {ok, PostVals, Req2} = cowboy_req:body_qs(Req),
% Echo = proplists:get_value(<<"echo">>, PostVals),
% cowboy_req:reply(400, [], <<"Missing body.">>, Req)
reply_200_json(Body, DDerlSessPid, Req) when is_pid(DDerlSessPid) ->
    reply_200_json(Body, list_to_binary(?EncryptPid({dderl_session, DDerlSessPid})), Req);
reply_200_json(Body, EncryptedPid, Req) ->
	cowboy_req:reply(200, [
          {<<"content-encoding">>, <<"utf-8">>}
        , {<<"content-type">>, <<"application/json">>}
        , {<<"dderl_sess">>, EncryptedPid}
        ], Body, Req).

reply_csv(FileName, Chunk, ChunkIdx, Req) ->
    case ChunkIdx of
        first ->
            {ok, Req1} = cowboy_req:chunked_reply(200, [
                  {<<"content-encoding">>, <<"utf-8">>}
                , {<<"content-type">>, <<"text/csv">>}
                , {<<"Content-disposition">>, list_to_binary(["attachment;filename=", FileName])}
                ], Req),
            ok = cowboy_req:chunk(Chunk, Req1),
            {ok, Req1};
        single ->
            {ok, Req1} = cowboy_req:chunked_reply(200, [
                  {<<"content-encoding">>, <<"utf-8">>}
                , {<<"content-type">>, <<"text/csv">>}
                , {<<"Content-disposition">>, list_to_binary(["attachment;filename=", FileName])}
                ], Req),
            ok = cowboy_req:chunk(Chunk, Req1),
            {ok, Req1};
        _ ->
            ok = cowboy_req:chunk(Chunk, Req),
            {ok, Req}
    end.

-ifdef(DISP_REQ).
display_req(Req) ->
    ?Info("-------------------------------------------------------"),
    ?Info("method     ~p~n", [element(1,cowboy_req:method(Req))]),
    ?Info("version    ~p~n", [element(1,cowboy_req:version(Req))]),
    ?Info("peer       ~p~n", [element(1,cowboy_req:peer(Req))]),
    %?Info("peer_addr  ~p~n", [element(1,cowboy_req:peer_addr(Req))]),
    ?Info("host       ~p~n", [element(1,cowboy_req:host(Req))]),
    ?Info("host_info  ~p~n", [element(1,cowboy_req:host_info(Req))]),
    ?Info("port       ~p~n", [element(1,cowboy_req:port(Req))]),
    ?Info("path       ~p~n", [element(1,cowboy_req:path(Req))]),
    ?Info("path_info  ~p~n", [element(1,cowboy_req:path_info(Req))]),
    ?Info("qs         ~p~n", [element(1,cowboy_req:qs(Req))]),
    %?Info("qs_val     ~p~n", [element(1,cowboy_req:qs_val(Req))]),
    ?Info("qs_vals    ~p~n", [element(1,cowboy_req:qs_vals(Req))]),
    ?Info("fragment   ~p~n", [element(1,cowboy_req:fragment(Req))]),
    ?Info("host_url   ~p~n", [element(1,cowboy_req:host_url(Req))]),
    ?Info("url        ~p~n", [element(1,cowboy_req:url(Req))]),
    %?Info("binding    ~p~n", [element(1,cowboy_req:binding(Req))]),
    ?Info("bindings   ~p~n", [element(1,cowboy_req:bindings(Req))]),
    ?Info("hdr(ddls)  ~p~n", [element(1,cowboy_req:header(<<"dderl_sess">>,Req))]),
    ?Info("hdr(host)  ~p~n", [element(1,cowboy_req:header(<<"host">>,Req))]),
    %?Info("headers    ~p~n", [element(1,cowboy_req:headers(Req))]),
    %?Info("cookie     ~p~n", [element(1,cowboy_req:cookie(Req))]),
    ?Info("cookies    ~p~n", [element(1,cowboy_req:cookies(Req))]),
    %?Info("meta       ~p~n", [element(1,cowboy_req:meta(Req))]),
    ?Info("has_body   ~p~n", [cowboy_req:has_body(Req)]),
    ?Info("body_len   ~p~n", [element(1,cowboy_req:body_length(Req))]),
    ?Info("body_qs    ~p~n", [element(2,cowboy_req:body_qs(Req))]),
    ?Info("body       ~p~n", [element(2,cowboy_req:body(Req))]),
    ?Info("-------------------------------------------------------").
-else.
display_req(_) -> ok.
-endif.

%% - handle(Req, State) ->    
%% -    	{ok, Req2} =
%% -     case cowboy_req:header(<<"authorization">>, Req) of
%% -         {undefined, Req1} ->
%% -             cowboy_req:reply(401
%% -                              , [{<<"WWW-Authenticate">>, <<"NTLM">>}
%% -                                 , {<<"content-type">>, <<"text/html">>}]
%% -                              , <<"<html><body>Test</body></html>">>, Req1);
%% -         {Auth, Req1} ->
%% -             case authorize(Auth) of
%% -                 not_ntlm ->
%% -                     cowboy_req:reply(401
%% -                                      , [{<<"WWW-Authenticate">>, <<"NTLM">>}
%% -                                         , {<<"content-type">>, <<"text/html">>}]
%% -                                      , <<"<html><body>Request Type-1</body></html>">>, Req1);
%% -                 {unsupported, AuthBin} ->
%% -                     ?Info("Authorization unsupported ~p~n", [AuthBin]),
%% -                     cowboy_req:reply(404, [{<<"content-type">>, <<"text/html">>}]
%% -                                      , <<"<html><body>Login failed</body></html>">>, Req1);
%% -                 {ntlmssp, 1, Flags, Rest} ->
%% -                      % , cowboy_req:headers(Req1)
%% -                     ?Info("Authorization : 1~nFlags ~p~n", [Flags]),
%% -                     NTLMType2Bin = base64:encode(<<16#4e, 16#54, 16#4c, 16#4d,
%% -                                                    16#53, 16#53, 16#50, 16#00,
%% -                                                    16#02, 16#00, 16#00, 16#00,
%% -                                                    16#0c, 16#00, 16#0c, 16#00,
%% -                                                    16#30, 16#00, 16#00, 16#00,
%% -                                                    16#01, 16#02, 16#81, 16#00,
%% -                                                    16#01, 16#23, 16#45, 16#67,
%% -                                                    16#89, 16#ab, 16#cd, 16#ef,
%% -                                                    16#00, 16#00, 16#00, 16#00,
%% -                                                    16#00, 16#00, 16#00, 16#00,
%% -                                                    16#62, 16#00, 16#62, 16#00,
%% -                                                    16#3c, 16#00, 16#00, 16#00,
%% -                                                    16#44, 16#00, 16#4f, 16#00,
%% -                                                    16#4d, 16#00, 16#41, 16#00,
%% -                                                    16#49, 16#00, 16#4e, 16#00,
%% -                                                    16#02, 16#00, 16#0c, 16#00,
%% -                                                    16#44, 16#00, 16#4f, 16#00,
%% -                                                    16#4d, 16#00, 16#41, 16#00,
%% -                                                    16#49, 16#00, 16#4e, 16#00,
%% -                                                    16#01, 16#00, 16#0c, 16#00,
%% -                                                    16#53, 16#00, 16#45, 16#00,
%% -                                                    16#52, 16#00, 16#56, 16#00,
%% -                                                    16#45, 16#00, 16#52, 16#00,
%% -                                                    16#04, 16#00, 16#14, 16#00,
%% -                                                    16#64, 16#00, 16#6f, 16#00,
%% -                                                    16#6d, 16#00, 16#61, 16#00,
%% -                                                    16#69, 16#00, 16#6e, 16#00,
%% -                                                    16#2e, 16#00, 16#63, 16#00,
%% -                                                    16#6f, 16#00, 16#6d, 16#00,
%% -                                                    16#03, 16#00, 16#22, 16#00,
%% -                                                    16#73, 16#00, 16#65, 16#00,
%% -                                                    16#72, 16#00, 16#76, 16#00,
%% -                                                    16#65, 16#00, 16#72, 16#00,
%% -                                                    16#2e, 16#00, 16#64, 16#00,
%% -                                                    16#6f, 16#00, 16#6d, 16#00,
%% -                                                    16#61, 16#00, 16#69, 16#00,
%% -                                                    16#6e, 16#00, 16#2e, 16#00,
%% -                                                    16#63, 16#00, 16#6f, 16#00,
%% -                                                    16#6d, 16#00, 16#00, 16#00,
%% -                                                    16#00, 16#00>>),
%% -                     cowboy_req:reply(401
%% -                                      , [{<<"WWW-Authenticate">>
%% -                                          , <<"NTLM ", NTLMType2Bin/binary>>}
%% -                                         , {<<"content-type">>, <<"text/html">>}]
%% -                                      , <<"<html><body>Request Type-3</body></html>">>, Req1);
%% -                 {ntlmssp, 3, Flags, Rest} ->
%% -                     ?Info("Authorization : 3~nFlags ~p~nRest ~p~n", [Flags, Rest]),
%% -                     cowboy_req:reply(200
%% -                                      , [{<<"content-type">>, <<"text/html">>}]
%% -                                      , get_html(), Req1)
%% -             end
%% -     end,
%% - 	{ok, Req2, State}.
%% - 
%% - authorize(Auth) ->
%% -     case re:run(Auth, <<"NTLM (.*)">>, [{capture, [1], binary}]) of
%% -         nomatch -> not_ntlm;
%% -         {match,[AuthB64]} ->
%% -             AuthBin = base64:decode(AuthB64),
%% -             case AuthBin of
%% -                 <<"NTLMSSP", 0:8, 1:4/little-unsigned-integer-unit:8, T1Body/binary>> ->
%% -                     <<Flags:4/little-unsigned-integer-unit:8, T1BodyRest/binary>> = T1Body,
%% -                     {ntlmssp, 1, bitmask_parse(<< Flags:32 >>), T1BodyRest};
%% -                 <<"NTLMSSP", 0:8, 3:4/little-unsigned-integer-unit:8, T3Body/binary>> ->
%% -                     {ntlmssp, 3, [], T3Body};
%% -                 _ -> {unsupported, AuthBin}
%% -             end
%% -     end.
%% - 
%% - bitmask_parse(Flags) ->
%% -     bitmask_parse(Flags
%% -                   , [encrypt65, type3EncryptSessionKey, encrypt128, u1N2B4
%% -                      , u4N1B4, u3N1B4, u2N1B4, u1N1B4
%% -                      , targetInfo, nonNTSessKeyReq, acceptRespReq, initRespReq
%% -                      , negoNTLM2Key, targetTypeShare, targetTypeServer, targetTypeDomain
%% -                      , negoAlwaysSign, negoLocalCall, negoWorkStnSuppld, negoDomainSuppld
%% -                      , negoAnonym, u3N1B2, negoNTLM, negoNetware
%% -                      , negoLanManKey, negoDatagramStyle, negoSeal, negoSign
%% -                      , u4N2B1, targetReq, negoOEM, negoUnicode]
%% -                   , []).
%% - bitmask_parse(<<>>, [], Options) -> lists:reverse(Options);
%% - bitmask_parse(<<1:1, Rest/bitstring>>, [P|Props], Options) ->
%% -     bitmask_parse(Rest, Props, [P | Options]);
%% - bitmask_parse(<<0:1, Rest/bitstring>>, [_|Props], Options) ->
%% -     bitmask_parse(Rest, Props, Options).