-module(dderl_oauth).

-include("dderl.hrl").

-define(OFFICE_365_AUTH_CONFIG,
        ?GET_CONFIG(office365AuthConfig,[],
            #{auth_url =>"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?response_type=code&response_mode=query",
              client_id => "12345", redirect_uri => "https://localhost:8443/dderl/", client_secret => "12345", grant_type => "authorization_code",
              token_url => "https://login.microsoftonline.com/common/oauth2/v2.0/token",
              scope => "offline_access https://graph.microsoft.com/people.read"},
            "Office 365 (Graph API) auth config")).

-define(OURA_RING_AUTH_CONFIG,
        ?GET_CONFIG(ouraRingAuthConfig,[],
            #{auth_url =>"https://cloud.ouraring.com/oauth/authorize?response_type=code",
              client_id => "12345", redirect_uri => "https://localhost:8443/dderl/",
              client_secret => "12345", grant_type => "authorization_code",
              token_url => "https://cloud.ouraring.com/oauth/token",
              scope => "email personal daily"},
            "Oura Ring auth config")).

-export([get_authorize_url/2, get_access_token/2, get_token_info/1, refresh_access_token/1]).

get_auth_config(?OFFICE365) -> ?OFFICE_365_AUTH_CONFIG;
get_auth_config(?OURARING) -> ?OURA_RING_AUTH_CONFIG.

get_token_info(Type) when Type == ?OFFICE365 orelse Type == ?OURARING ->
    dperl_dal:read_channel(<<"avatar">>, [binary_to_list(Type),"token"]).

set_token_info(TokenInfo, Type) when is_map(TokenInfo) ->
    set_token_info(imem_json:encode(TokenInfo), Type);
set_token_info(TokenInfo, Type) when is_list(TokenInfo) ->
    set_token_info(list_to_binary(TokenInfo), Type);
set_token_info(TokenInfo, Type) when is_binary(TokenInfo), (Type == ?OFFICE365 orelse Type == ?OURARING) ->
    dperl_dal:create_check_channel(<<"avatar">>),
    dperl_dal:write_channel(<<"avatar">>, [binary_to_list(Type), "token"], TokenInfo).

get_authorize_url(XSRFToken, Type) when Type == ?OFFICE365 orelse Type == ?OURARING ->
    State = #{xsrfToken => XSRFToken, type => Type},
    #{auth_url := Url, client_id := ClientId, redirect_uri := RedirectURI,
      scope := Scope} = get_auth_config(Type),
    UrlParams = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "redirect_uri" => {enc, RedirectURI},
          "scope" => {enc, Scope}, "state" => {enc, imem_json:encode(State)}}),
    erlang:iolist_to_binary([Url, "&", UrlParams]).

get_access_token(Code, Type) when Type == ?OFFICE365 orelse Type == ?OURARING ->
    #{token_url := TUrl, client_id := ClientId, redirect_uri := RedirectURI,
      client_secret := Secret, grant_type := GrantType,
      scope := Scope} = get_auth_config(Type),
    Body = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "scope" => {enc, Scope}, "code" => Code,
          "redirect_uri" => {enc, RedirectURI}, "grant_type" => GrantType,
          "client_secret" => {enc, Secret}}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenInfo}} ->
            set_token_info(TokenInfo, Type),
            ok;
        {ok, {{_, Code, _}, _, Error}} ->
            ?Error("Fetching access token : ~p:~p", [Code, Error]),
            {error, Error};
        {error, Error} ->
            ?Error("Fetching access token : ~p", [Error]),
            {error, Error}
    end.

refresh_access_token(Type) when Type == ?OFFICE365 orelse Type == ?OURARING ->
    #{token_url := TUrl, client_id := ClientId, scope := Scope,
      client_secret := Secret} = get_auth_config(Type),
    #{<<"refresh_token">> := RefreshToken} = get_token_info(Type),
    Body = dperl_dal:url_enc_params(
        #{"client_id" => ClientId, "client_secret" => {enc, Secret}, "scope" => {enc, Scope},
          "refresh_token" => RefreshToken, "grant_type" => "refresh_token"}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenBody}} ->
            TokenInfo = imem_json:decode(list_to_binary(TokenBody), [return_maps]),
            set_token_info(TokenBody, Type),
            #{<<"access_token">> := AccessToken} = TokenInfo,
            {ok, AccessToken};
        Error ->
            {error, Error}
    end.
