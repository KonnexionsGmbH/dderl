-module(dderl_oauth).

-include("dderl.hrl").

-define(TOKEN_KEYPART, "#token#").

-export([ get_authorize_url/3
        , get_access_token/4
        , get_token_info/3
        , refresh_access_token/3
        ]).

get_token_info(AccountId, TokenPrefix, _SyncType) ->
    dderl_dal:read_from_avatar_channel(AccountId, TokenPrefix ++ [?TOKEN_KEYPART]).

set_token_info(AccountId, TokenPrefix, TokenInfo, SyncType) when is_map(TokenInfo) ->
    set_token_info(AccountId, TokenPrefix, imem_json:encode(TokenInfo), SyncType);
set_token_info(AccountId, TokenPrefix, TokenInfo, SyncType) when is_list(TokenInfo) ->
    set_token_info(AccountId, TokenPrefix, list_to_binary(TokenInfo), SyncType);
set_token_info(AccountId, TokenPrefix, TokenInfo, _SyncType) when is_binary(TokenInfo) ->
    ?Info("set_token_info using ~p",[imem_enc_mnesia:get_enc_hash()]),
    dderl_dal:write_to_avatar_channel(AccountId, TokenPrefix ++ [?TOKEN_KEYPART], TokenInfo).

get_authorize_url(XSRFToken, AuthConfig, SyncType) ->
    State = #{xsrfToken => XSRFToken, type => SyncType},
    #{auth_url:=Url, client_id:=ClientId, redirect_uri:=RedirectURI, scope:=Scope} = AuthConfig, 
    UrlParams = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "redirect_uri" => {enc, RedirectURI}
         ,"scope" => {enc, Scope}, "state" => {enc, imem_json:encode(State)}}),
    erlang:iolist_to_binary([Url, "&", UrlParams]).


%% get token info from web service using the configuration from callback module
%% store it in the avatar table of AccountId under the key TokenPrefix || "#token#"  
-spec get_access_token(ddEntityId(), list(), string(), module()) -> ok | {error, term()}.
get_access_token(AccountId, TokenPrefix, Code, SyncType) ->
    AuthConfig = try 
        SyncType:get_auth_config() % ToDo: AuthConfig may depend on JobName or TokenPrefix
    catch 
        _:E:S ->
            ?Error("Finding AuthConfig : ~p ñ~p", [E,S]),
            {error, E}
    end,
    %?Info("get_access_token AuthConfig: ~p",[AuthConfig]),
    #{token_url:=TUrl, client_id:=ClientId, redirect_uri:=RedirectURI
     ,client_secret:=Secret, grant_type:=GrantType
     ,scope := Scope} = AuthConfig,
    Body = dperl_dal:url_enc_params(
        #{ "client_id" => ClientId, "scope" => {enc, Scope}, "code" => Code
         , "redirect_uri" => {enc, RedirectURI}, "grant_type" => GrantType
         , "client_secret" => {enc, Secret}}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenInfo}} ->
            set_token_info(AccountId, TokenPrefix, TokenInfo, SyncType),
            ok;
        {ok, {{_, Code, _}, _, Error}} ->
            {error, Error};
        {error, Error} ->
            ?Error("Fetching access token : ~p", [Error]),
            {error, Error}
    end.

%% refresh access token from web service using the configuration from callback module
%% store it in the avatar table of AccountId under the key TokenPrefix || "#token#"  
-spec refresh_access_token(ddEntityId(), list(), module()) -> {ok, binary()} | {error, term()}.
refresh_access_token(AccountId, TokenPrefix, SyncType) ->
    #{token_url:=TUrl, client_id:=ClientId, scope:=Scope, client_secret:=Secret} 
        = SyncType:get_auth_config(),
    ?Info("refresh_access_token ~p ~p ~p",[AccountId, TokenPrefix, SyncType]),
    #{<<"refresh_token">>:=RefreshToken} = get_token_info(AccountId, TokenPrefix, SyncType),
    Body = dperl_dal:url_enc_params(#{ "client_id"=>ClientId, "client_secret"=>{enc, Secret}
                                     , "scope"=>{enc, Scope}, "refresh_token"=>RefreshToken
                                     , "grant_type"=>"refresh_token"}),
    ContentType = "application/x-www-form-urlencoded",
    ?Info("refresh_access_token TUrl=~p",[TUrl]),
    ?Info("refresh_access_token ContentType=~p",[ContentType]),
    ?Info("refresh_access_token Body=~p",[Body]),
    ?Info("refresh_access_token RefreshToken=~p",[RefreshToken]),
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenBody}} ->
            TokenInfo = imem_json:decode(list_to_binary(TokenBody), [return_maps]),
            set_token_info(AccountId, TokenPrefix, TokenBody, SyncType),
            #{<<"access_token">> := Token} = TokenInfo,
            {ok, Token};
        Error ->
            {error, Error}
    end.
