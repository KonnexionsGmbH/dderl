-module(dperl_office_365).

-include_lib("dperl/dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

% dperl_worker exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         get_status/1, init_state/1]).

% contacts graph api
% https://docs.microsoft.com/en-us/graph/api/resources/contact?view=graph-rest-1.0

-record(state, {name, channel, is_connected = true, access_token, api_url,
                contacts = [], key_prefix, fetch_url, cl_contacts = [],
                is_cleanup_finished = true, push_channel, type = pull,
                audit_start_time = {0,0}, first_sync = true}).

% dperl_strategy_scr export
-export([connect_check_src/1, get_source_events/2, connect_check_dst/1,
         do_cleanup/5, do_refresh/2, load_src_after_key/3, load_dst_after_key/3,
         fetch_src/2, fetch_dst/2, delete_dst/2, insert_dst/3,
         update_dst/3, report_status/3]).

connect_check_src(#state{is_connected = true} = State) ->
    {ok, State};
connect_check_src(#state{is_connected = false} = State) ->
    ?JTrace("Refreshing access token"),
    case dderl_oauth:refresh_access_token(?OFFICE365) of
        {ok, AccessToken} ->
            ?Info("new access token fetched"),
            {ok, State#state{access_token = AccessToken, is_connected = true}};
        {error, Error} ->
            ?JError("Unexpected response : ~p", [Error]),
            {error, Error, State}
    end.
    % ?JTrace("Refreshing access token"),
    % #{token_url := TUrl, client_id := ClientId, client_secret := Secret,
    %   scope := Scope} = get_office_365_auth_config(),
    % #{<<"refresh_token">> := RefreshToken} = get_token_info(),
    % Body = dperl_dal:url_enc_params(
    %     #{"client_id" => ClientId, "scope" => {enc, Scope},
    %       "refresh_token" => RefreshToken, "grant_type" => "refresh_token",
    %       "client_secret" => {enc, Secret}}),
    % ContentType = "application/x-www-form-urlencoded",
    % case httpc:request(post, {TUrl, "", ContentType, Body}, [], []) of
    %     {ok, {{_, 200, "OK"}, _, TokenBody}} ->
    %         TokenInfo = imem_json:decode(list_to_binary(TokenBody), [return_maps]),
    %         set_token_info(TokenBody),
    %         #{<<"access_token">> := AccessToken} = TokenInfo,
    %         ?JInfo("new access token fetched"),
    %         {ok, State#state{access_token = AccessToken, is_connected = true}};
    %     Error ->
    %         ?JError("Unexpected response : ~p", [Error]),
    %         {error, Error, State}
    % end.

get_source_events(#state{audit_start_time = LastStartTime, type = push,
                         push_channel = PChannel} = State, BulkSize) ->
    case dperl_dal:read_audit_keys(PChannel, LastStartTime, BulkSize) of
        {LastStartTime, LastStartTime, []} ->
            if State#state.first_sync == true -> 
                    ?JInfo("Audit rollup is complete"),
                    {ok, sync_complete, State#state{first_sync = false}};
                true -> {ok, sync_complete, State}
            end;
        {_StartTime, NextStartTime, []} ->
            {ok, [], State#state{audit_start_time = NextStartTime}};
        {_StartTime, NextStartTime, Keys} ->
            UniqueKeys = lists:delete(undefined, lists:usort(Keys)),
            {ok, UniqueKeys, State#state{audit_start_time = NextStartTime}}
    end;
get_source_events(#state{contacts = []} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{contacts = Contacts} = State, _BulkSize) ->
    {ok, Contacts, State#state{contacts = []}}.

connect_check_dst(State) -> {ok, State}.

do_refresh(_State, _BulkSize) -> {error, cleanup_only}.

fetch_src(Key, #state{type = push} = State) ->
    dperl_dal:read_channel(State#state.push_channel, Key);
fetch_src(Key, #state{cl_contacts = Contacts, type = pull}) ->
    case lists:keyfind(Key, 1, Contacts) of
        {Key, Contact} -> Contact;
        false -> ?NOT_FOUND
    end.

fetch_dst(Key, #state{type = push, api_url = ApiUrl} = State) ->
    Id = Key -- State#state.key_prefix,
    ContactUrl = erlang:iolist_to_binary([ApiUrl, Id]),
    case exec_req(ContactUrl, State#state.access_token) of
        #{<<"id">> := _} = Contact ->
            format_contact(Contact);
        _ -> ?NOT_FOUND
    end; 
fetch_dst(Key, State) ->
    dperl_dal:read_channel(State#state.channel, Key).

insert_dst(Key, Val, #state{type = push, api_url = ApiUrl} = State) ->
    case exec_req(ApiUrl, State#state.access_token, Val, post) of
        #{<<"id">> := Id} = Contact ->
            NewKey = State#state.key_prefix ++ [binary_to_list(Id)],
            ContactBin = imem_json:encode(format_contact(Contact)),
            dperl_dal:remove_from_channel(State#state.push_channel, Key),
            dperl_dal:write_channel(State#state.channel, NewKey, ContactBin),
            dperl_dal:write_channel(State#state.push_channel, NewKey, ContactBin),
            {false, State};
        {error, unauthorized} ->
            reconnect_exec(State, insert_dst, [Key, Val]);
        {error, Error} ->
            {error, Error}
    end;
insert_dst(Key, Val, State) ->
    update_dst(Key, Val, State).

delete_dst(Key, #state{type = push, api_url = ApiUrl} = State) ->
    Id = Key -- State#state.key_prefix,
    ContactUrl = erlang:iolist_to_binary([ApiUrl, Id]),
    case exec_req(ContactUrl, State#state.access_token, #{}, delete) of
        ok ->
            dperl_dal:remove_from_channel(State#state.channel, Key),
            {false, State};
        {error, unauthorized} ->
            reconnect_exec(State, delete_dst, [Key]);
        Error ->
            Error
    end;
delete_dst(Key, #state{channel = Channel} = State) ->
    dperl_dal:remove_from_channel(Channel, Key),
    dperl_dal:remove_from_channel(State#state.push_channel, Key),
    {false, State}.

update_dst(Key, Val, #state{type = push, api_url = ApiUrl} = State) ->
    Id = Key -- State#state.key_prefix,
    ContactUrl = erlang:iolist_to_binary([ApiUrl, Id]),
    case exec_req(ContactUrl, State#state.access_token, Val, patch) of
        #{<<"id">> := _} = Contact ->
            ContactBin = imem_json:encode(format_contact(Contact)),
            dperl_dal:write_channel(State#state.channel, Key, ContactBin),
            dperl_dal:write_channel(State#state.push_channel, Key, ContactBin),
            {false, State};
        {error, unauthorized} ->
            reconnect_exec(State, update_dst, [Key, Val]);
        {error, Error} ->
            {error, Error}
    end;
update_dst(Key, Val, #state{channel = Channel} = State) when is_binary(Val) ->
    dperl_dal:write_channel(Channel, Key, Val),
    dperl_dal:write_channel(State#state.push_channel, Key, Val),
    {false, State};
update_dst(Key, Val, State) ->
    update_dst(Key, imem_json:encode(Val), State).

report_status(_Key, _Status, _State) -> no_op.

load_dst_after_key(CurKey, BlkCount, #state{channel = Channel}) ->
    dperl_dal:read_gt(Channel, CurKey, BlkCount).

load_src_after_key(CurKey, BlkCount, #state{fetch_url = undefined} = State) ->
    % https://graph.microsoft.com/v1.0/me/contacts/?$top=100&$select=displayName&orderby=displayName
    UrlParams = dperl_dal:url_enc_params(#{"$top" => integer_to_list(BlkCount)}),
    ContactsUrl = erlang:iolist_to_binary([State#state.api_url, "?", UrlParams]),
    load_src_after_key(CurKey, BlkCount, State#state{fetch_url = ContactsUrl});
load_src_after_key(CurKey, BlkCount, #state{is_cleanup_finished = true, key_prefix = KeyPrefix,
                                            access_token = AccessToken, fetch_url = FetchUrl} = State) ->
    % fetch all contacts
    case fetch_all_contacts(FetchUrl, AccessToken, KeyPrefix) of
        {ok, Contacts} ->
            load_src_after_key(CurKey, BlkCount, State#state{cl_contacts = Contacts, is_cleanup_finished = false});
        {error, unauthorized} ->
            reconnect_exec(State, load_src_after_key, [CurKey, BlkCount]);
        {error, Error} ->
            {error, Error, State}
    end;
load_src_after_key(CurKey, BlkCount, #state{cl_contacts = Contacts} = State) ->
    {ok, get_contacts_gt(CurKey, BlkCount, Contacts), State}.

reconnect_exec(State, Fun, Args) ->
    case connect_check_src(State#state{is_connected = false}) of
        {ok, State1} ->
            erlang:apply(?MODULE, Fun, Args ++ [State1]);
        {error, Error, State1} ->
            {error, Error, State1}
    end.

do_cleanup(_Deletes, _Inserts, _Diffs, _IsFinished, #state{type = push}) ->
    {error, <<"cleanup only for pull job">>};
do_cleanup(Deletes, Inserts, Diffs, IsFinished, State) ->
    NewState = State#state{contacts = Inserts ++ Diffs ++ Deletes},
    if IsFinished -> {ok, finish, NewState#state{is_cleanup_finished = true}};
       true -> {ok, NewState}
    end.

get_status(#state{}) -> #{}.

init_state(_) -> #state{}.

init({#dperlJob{name=Name, srcArgs = #{api_url := ApiUrl}, args = Args,
                dstArgs = #{channel := Channel, push_channel := PChannel} = DstArgs}, State}) ->
    case dperl_auth_cache:get_enc_hash(Name) of
        undefined ->
            ?JError("Encryption hash is not avaialable"),
            {stop, badarg};
        {User, EncHash} ->
            ?JInfo("Starting with ~p's enchash...", [User]),
            imem_sec_mnesia:put_enc_hash(EncHash),
            case dderl_oauth:get_token_info(?OFFICE365) of
                #{<<"access_token">> := AccessToken} ->
                    ChannelBin = dperl_dal:to_binary(Channel),
                    PChannelBin = dperl_dal:to_binary(PChannel),
                    KeyPrefix = maps:get(key_prefix, DstArgs, []),
                    Type = maps:get(type, Args, pull),
                    dperl_dal:create_check_channel(ChannelBin),
                    dperl_dal:create_check_channel(PChannelBin),
                    {ok, State#state{channel = ChannelBin, name = Name, api_url = ApiUrl,
                                    key_prefix = KeyPrefix, access_token = AccessToken,
                                    push_channel = PChannelBin, type = Type}};
                _ ->
                    ?JError("Access token not found"),
                    {stop, badarg}
            end
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

format_contacts([], _) -> [];
format_contacts([#{<<"id">> := IdBin} = Contact | Contacts], KeyPrefix) ->
    Id = binary_to_list(IdBin),
    Key = KeyPrefix ++ [Id],
    [{Key, format_contact(Contact)} | format_contacts(Contacts, KeyPrefix)].

format_contact(Contact) ->
    maps:without([<<"@odata.etag">>, <<"@odata.context">>], Contact).

fetch_all_contacts(Url, AccessToken, KeyPrefix) ->
    fetch_all_contacts(Url, AccessToken, KeyPrefix, []).

fetch_all_contacts(Url, AccessToken, KeyPrefix, AccContacts) ->
    ?JTrace("Fetching contacts with url : ~s", [Url]),
    ?JTrace("Fetched contacts : ~p", [length(AccContacts)]),
    case exec_req(Url, AccessToken) of
        #{<<"@odata.nextLink">> := NextUrl, <<"value">> := Contacts} ->
            FContacts = format_contacts(Contacts, KeyPrefix),
            fetch_all_contacts(NextUrl, AccessToken, KeyPrefix, lists:append(FContacts, AccContacts));
        #{<<"value">> := Contacts} ->
            FContacts = format_contacts(Contacts, KeyPrefix),
            {ok, lists:keysort(1, lists:append(FContacts, AccContacts))};
        {error, Error} ->
            {error, Error}
    end.

get_contacts_gt(CurKey, BlkCount, Contacts) ->
    get_contacts_gt(CurKey, BlkCount, Contacts, []).
    
get_contacts_gt(_CurKey, _BlkCount, [], Acc) -> lists:reverse(Acc);
get_contacts_gt(_CurKey, BlkCount, _Contacts, Acc) when length(Acc) == BlkCount ->
    lists:reverse(Acc);
get_contacts_gt(CurKey, BlkCount, [{Key, _} | Contacts], Acc) when Key =< CurKey ->
    get_contacts_gt(CurKey, BlkCount, Contacts, Acc);
get_contacts_gt(CurKey, BlkCount, [Contact | Contacts], Acc) ->
    get_contacts_gt(CurKey, BlkCount, Contacts, [Contact | Acc]).

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

exec_req(Url, AccessToken, Body, Method) when is_binary(Url) ->
    exec_req(binary_to_list(Url), AccessToken, Body, Method);
exec_req(Url, AccessToken, Body, Method) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}],
    % Headers = [AuthHeader, {"Contnet-type", "application/json"}],
    case httpc:request(Method, {Url, AuthHeader, "application/json", imem_json:encode(Body)}, [], []) of
        {ok, {{_, 201, _}, _, Result}} ->
            % create/post result
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok, {{_, 200, _}, _, Result}} ->
            % update/patch result
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok,{{_, 204, _}, _, _}} ->
            % delete result
            ok;
        {ok, {{_, 401, _}, _, Error}} ->
            ?JError("Unauthorized body : ~s", [Error]),
            {error, unauthorized};
        Error ->
            {error, Error}
    end.
