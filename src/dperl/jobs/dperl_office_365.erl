-module(dperl_office_365).

-include_lib("dperl/dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

-define(OFFICE_365_AUTH_CONFIG,
        ?GET_CONFIG(office365AuthConfig,[],
            #{auth_url =>"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?response_type=code&response_mode=query",
              client_id => "12345", redirect_uri => "https://localhost:8443/dderl/", client_secret => "12345", grant_type => "authorization_code",
              token_url => "https://login.microsoftonline.com/common/oauth2/v2.0/token",
              scope => "offline_access https://graph.microsoft.com/people.read"},
            "Office 365 (Graph API) auth config")).

% dperl_worker exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         get_status/1, init_state/1]).

-export([get_authorize_url/1, get_access_token/1]).

-record(state, {name, channel, is_connected = true, access_token, api_url,
                infos = [], key_prefix, fetch_url}).

% dperl_strategy_scr export
-export([connect_check_src/1, get_source_events/2, connect_check_dst/1,
         do_cleanup/5, do_refresh/2, load_src_after_key/3, load_dst_after_key/3,
         fetch_src/2, fetch_dst/2, delete_dst/2, insert_dst/3,
         update_dst/3, report_status/3]).

get_office_365_auth_config() ->
    ?OFFICE_365_AUTH_CONFIG.

get_token_info() ->
    dperl_dal:read_channel(<<"avatar">>, ["office365","token"]).

set_token_info(TokenInfo) when is_map(TokenInfo) ->
    set_token_info(imem_json:encode(TokenInfo));
set_token_info(TokenInfo) when is_list(TokenInfo) ->
    set_token_info(list_to_binary(TokenInfo));
set_token_info(TokenInfo) when is_binary(TokenInfo) ->
    dperl_dal:create_check_channel(<<"avatar">>),
    dperl_dal:write_channel(<<"avatar">>, ["office365","token"], TokenInfo).

get_authorize_url(XSRFToken) ->
    URLState = http_uri:encode(XSRFToken),
    #{auth_url := Url, client_id := ClientId, redirect_uri := RedirectURI,
      scope := Scope} = get_office_365_auth_config(),
    UrlParams = url_enc_params(#{"client_id" => ClientId, "redirect_uri" => {enc, RedirectURI},
                                 "scope" => {enc, Scope}, "state" => URLState}),
    erlang:iolist_to_binary([Url, "&", UrlParams]).

get_access_token(Code) ->
    #{token_url := TUrl, client_id := ClientId, redirect_uri := RedirectURI,
      client_secret := Secret, grant_type := GrantType,
      scope := Scope} = get_office_365_auth_config(),
    Body = url_enc_params(#{"client_id" => ClientId, "scope" => {enc, Scope}, "code" => Code,
                            "redirect_uri" => {enc, RedirectURI}, "grant_type" => GrantType,
                            "client_secret" => {enc, Secret}}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {_, _, TokenInfo}} ->
            set_token_info(TokenInfo),
            ok;
        {error, Error} ->
            ?Error("Fetching access token : ~p", [Error]),
            {error, Error}
    end.

connect_check_src(#state{is_connected = true} = State) ->
    {ok, State};
connect_check_src(#state{is_connected = false} = State) ->
    ?Info("Refreshing access token"),
    #{token_url := TUrl, client_id := ClientId, client_secret := Secret,
      scope := Scope} = get_office_365_auth_config(),
    #{<<"refresh_token">> := RefreshToken} = get_token_info(),
    Body = url_enc_params(#{"client_id" => ClientId, "scope" => {enc, Scope},
                            "refresh_token" => RefreshToken, "grant_type" => "refresh_token",
                            "client_secret" => {enc, Secret}}),
    ContentType = "application/x-www-form-urlencoded",
    case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
        {ok, {{_, 200, "OK"}, _, TokenBody}} ->
            TokenInfo = imem_json:decode(list_to_binary(TokenBody), [return_maps]),
            set_token_info(TokenBody),
            #{<<"access_token">> := AccessToken} = TokenInfo,
            {ok, State#state{access_token = AccessToken, is_connected = true}};
        Error ->
            ?JError("Unexpected response : ~p", [Error]),
            {ok, State}
    end.

get_source_events(#state{infos = []} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{infos = Infos} = State, _BulkSize) ->
    {ok, Infos, State#state{infos = []}}.

connect_check_dst(State) -> {ok, State}.

do_refresh(_State, _BulkSize) -> {error, cleanup_only}.

fetch_src(Key, #state{api_url = ApiUrl} = State) ->
    Id = Key -- State#state.key_prefix,
    FetchUrl = erlang:iolist_to_binary([ApiUrl, Id]),
    case exec_req(FetchUrl, State#state.access_token) of
        Value when is_map(Value) -> Value;
        {error, Error} -> {error, Error, State}
    end.

fetch_dst(Key, State) ->
    dperl_dal:read_channel(State#state.channel, Key).

insert_dst(Key, Val, State) ->
    update_dst(Key, Val, State).

report_status(_Key, _Status, _State) -> no_op.

load_dst_after_key(CurKey, BlkCount, #state{channel = Channel}) ->
    dperl_dal:read_gt(Channel, CurKey, BlkCount).

load_src_after_key(CurKey, BlkCount, #state{fetch_url = undefined} = State) ->
    % https://graph.microsoft.com/v1.0/me/contacts/?$top=100&$select=displayName&orderby=displayName
    UrlParams = url_enc_params(#{"$top" => integer_to_list(BlkCount), "$orderby" => "id"}),
    ContactsUrl = erlang:iolist_to_binary([State#state.api_url, "?", UrlParams]),
    load_src_after_key(CurKey, BlkCount, State#state{fetch_url = ContactsUrl});
load_src_after_key(_CurKey, _BlkCount, #state{fetch_url = FetchUrl, key_prefix = KeyPrefix} = State) ->
    case exec_req(FetchUrl, State#state.access_token) of
        #{<<"@odata.nextLink">> := NextLink, <<"value">> := Contacts} ->
            {ok, format_contacts(Contacts, KeyPrefix), State#state{fetch_url = NextLink}};
        #{<<"value">> := Contacts} ->
            {ok, format_contacts(Contacts, KeyPrefix), State#state{fetch_url = undefined}};
        {error, unauthorized} ->
            {error, unauthorized, State#state{is_connected = false}};
        {error, Error} ->
            {error, Error, State}
    end.

do_cleanup(Deletes, Inserts, Diffs, IsFinished, State) ->
    NewState = State#state{infos = Inserts ++ Diffs ++ Deletes},
    if IsFinished -> {ok, finish, NewState};
       true -> {ok, NewState}
    end.


% do_cleanup(State, BlkCount) ->
%     case fetch_contacts(State, BlkCount) of
%         {ok, State1} ->
%             {ok, State1};
%         {ok, finish, State1} ->
%             {ok, finish, State1};
%         {error, unauthorized} ->
%             ?Info("Access token has been expired"),
%             {ok, State#state{is_connected = false}};
%         {error, Error} ->
%             {error, Error, State#state{is_connected = false}}
%     end.

delete_dst(Key, #state{channel = Channel} = State) ->
    ?JInfo("Deleting : ~p", [Key]),
    dperl_dal:remove_from_channel(Channel, Key),
    {false, State}.

update_dst({Key, _}, Val, State) ->
    update_dst(Key, Val, State);
update_dst(Key, Val, #state{channel = Channel} = State) when is_binary(Val) ->
    dperl_dal:write_channel(Channel, Key, Val),
    {false, State};
update_dst(Key, Val, State) ->
    update_dst(Key, imem_json:encode(Val), State).

get_status(#state{}) -> #{}.

init_state(_) -> #state{}.

init({#dperlJob{name=Name, dstArgs = #{channel := Channel},
                srcArgs = #{api_url := ApiUrl} = SrcArgs}, State}) ->
    % case dperl_auth_cache:get_enc_hash(Name) of
    %     undefined ->
    %         ?JError("Encryption hash is not avaialable"),
    %         {stop, badarg};
    %     {User, EncHash} ->
            % ?JInfo("Starting with ~p's enchash...", [User]),
            % imem_sec_mnesia:put_enc_hash(EncHash),
    case get_token_info() of
        #{<<"access_token">> := AccessToken} ->
            ChannelBin = dperl_dal:to_binary(Channel),
            KeyPrefix = maps:get(key_prefix, SrcArgs, []),
            dperl_dal:create_check_channel(ChannelBin),
            {ok, State#state{channel = ChannelBin, name = Name, api_url = ApiUrl,
                             key_prefix = KeyPrefix, access_token = AccessToken}};
        _ ->
            ?JError("Access token not found"),
            {stop, badarg}                    
    end;
init(Args) ->
    ?JError("bad start parameters ~p", [Args]),
    {stop, badarg}.

handle_call(Request, _From, State) ->
    ?JWarn("Unsupported handle_call ~p", [Request]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?JWarn("Unsupported handle_cast ~p", [Request]),
    {noreply, State}.

handle_info(Request, State) ->
    ?JWarn("Unsupported handle_info ~p", [Request]),
    {noreply, State}.

terminate(Reason, _State) ->
    httpc:reset_cookies(?MODULE),
    ?JInfo("terminate ~p", [Reason]).

%% private functions

% fetch_contacts(#state{fetch_url = undefined} = State, BlkCount) ->
%     % https://graph.microsoft.com/v1.0/me/contacts/?$top=100&$select=displayName&orderby=displayName
%     UrlParams = url_enc_params(#{"$top" => integer_to_list(BlkCount), "$orderby" => "displayName"}),
%     ContactsUrl = erlang:iolist_to_binary([State#state.api_url, "?", UrlParams]),
%     fetch_contacts(State#state{fetch_url = ContactsUrl}, BlkCount);
% fetch_contacts(#state{fetch_url = FetchUrl, key_prefix = KeyPrefix} = State, _BlkCount) ->
%     case exec_req(FetchUrl, State#state.access_token) of
%         #{<<"@odata.nextLink">> := NextLink, <<"value">> := Contacts} ->
%             {ok, State#state{fetch_url = NextLink, infos = format_contacts(Contacts, KeyPrefix)}};
%         #{<<"value">> := Contacts} ->
%             {ok, finish, State#state{fetch_url = undefined, infos = format_contacts(Contacts, KeyPrefix)}};
%         Error ->
%             Error
% end.

format_contacts([], _) -> [];
format_contacts([#{<<"id">> := IdBin} = Contact | Contacts], KeyPrefix) ->
    Id = binary_to_list(IdBin),
    Key = KeyPrefix ++ [Id],
    [{Key, Contact} | format_contacts(Contacts, KeyPrefix)].

exec_req(Url, AccessToken) when is_binary(Url) ->
    exec_req(binary_to_list(Url), AccessToken);
exec_req(Url, AccessToken) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}],
    case httpc:request(get, {Url, AuthHeader}, [], []) of
        {ok, {{_, 200, "OK"}, _, Result}} ->
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok, {{_, 401, _}, _, _}} ->
            {error, unauthorized};
        Error ->
            {error, Error}
    end.

url_enc_params(Params) ->
    EParams = maps:fold(
        fun(K, {enc, V}, Acc) ->
            ["&", K, "=", http_uri:encode(V) | Acc];
           (K, V, Acc) ->
            ["&", K, "=", V | Acc]
        end, [], Params),
    erlang:iolist_to_binary([tl(EParams)]).
