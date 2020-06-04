-module(dderl_oauth).

-include("dderl.hrl").
% -include("dderl/_checkouts/imem/include/imem_config.hrl").

-define(TOKEN_KEYPART, "#token#").

-export([ get_authorize_url/3
        , get_access_token/4
        , get_token_info/3
        , refresh_access_token/3
        ]).

get_token_info(AccountId, KeyPrefix, _SyncType) ->
    dderl_dal:read_from_avatar_channel(AccountId, KeyPrefix ++ [?TOKEN_KEYPART]).

set_token_info(AccountId, KeyPrefix, TokenInfo, SyncType) when is_map(TokenInfo) ->
    set_token_info(AccountId, KeyPrefix, imem_json:encode(TokenInfo), SyncType);
set_token_info(AccountId, KeyPrefix, TokenInfo, SyncType) when is_list(TokenInfo) ->
    set_token_info(AccountId, KeyPrefix, list_to_binary(TokenInfo), SyncType);
set_token_info(AccountId, KeyPrefix, TokenInfo, _SyncType) when is_binary(TokenInfo) ->
    dderl_dal:write_to_avatar_channel(AccountId, KeyPrefix ++ [?TOKEN_KEYPART], TokenInfo).

get_authorize_url(XSRFToken, AuthConfig, SyncType) ->
    State = #{xsrfToken => XSRFToken, type => SyncType},
    #{auth_url:=Url, client_id:=ClientId, redirect_uri:=RedirectURI, scope:=Scope} = AuthConfig, 
    UrlParams = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "redirect_uri" => {enc, RedirectURI},
          "scope" => {enc, Scope}, "state" => {enc, imem_json:encode(State)}}),
    erlang:iolist_to_binary([Url, "&", UrlParams]).

get_access_token(AccountId, KeyPrefix, Code, SyncType) ->
    AuthConfig = try 
        SyncType:get_auth_config() % ToDo: AuthConfig may depend on JobName or KeyPrefix
    catch 
        _:E:S ->
            ?Error("Finding AuthConfig : ~p ñ~p", [E,S]),
            {error, E}
    end,
    ?Info("get_access_token AuthConfig: ~p",[AuthConfig]),
    #{token_url := TUrl, client_id := ClientId, redirect_uri := RedirectURI,
            client_secret := Secret, grant_type := GrantType,
            scope := Scope} = AuthConfig,
    Body = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "scope" => {enc, Scope}, "code" => Code,
        "redirect_uri" => {enc, RedirectURI}, "grant_type" => GrantType,
        "client_secret" => {enc, Secret}}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenInfo}} ->
            set_token_info(AccountId, KeyPrefix, TokenInfo, SyncType),
            ok;
        {ok, {{_, Code, _}, _, Error}} ->
            ?Error("Fetching access token : ~p:~p", [Code, Error]),
            {error, Error};
        {error, Error} ->
            ?Error("Fetching access token : ~p", [Error]),
            {error, Error}
    end.

refresh_access_token(AccountId, KeyPrefix, SyncType) ->
    #{token_url := TUrl, client_id := ClientId, scope := Scope,
      client_secret := Secret} = SyncType:get_auth_config(),
    #{<<"refresh_token">> := RefreshToken} = get_token_info(AccountId, KeyPrefix, SyncType),
    Body = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "client_secret" => {enc, Secret}, "scope" => {enc, Scope},
          "refresh_token" => RefreshToken, "grant_type" => "refresh_token"}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenBody}} ->
            TokenInfo = imem_json:decode(list_to_binary(TokenBody), [return_maps]),
            set_token_info(AccountId, KeyPrefix, TokenBody, SyncType),
            #{<<"access_token">> := AccessToken} = TokenInfo,
            {ok, AccessToken};
        Error ->
            {error, Error}
    end.
