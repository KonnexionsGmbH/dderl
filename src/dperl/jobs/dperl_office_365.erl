-module(dperl_office_365).

-include_lib("../dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

-define(OAUTH2_CONFIG(__JOB_NAME), 
            ?GET_CONFIG(oAuth2Config,
            [__JOB_NAME],
            #{auth_url =>"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?response_type=code&response_mode=query"
             ,client_id => "12345"
             ,redirect_uri => "https://localhost:8443/dderl/"
             ,client_secret => "12345"
             ,grant_type => "authorization_code"
             ,token_url => "https://login.microsoftonline.com/common/oauth2/v2.0/token"
             ,scope => "offline_access https://graph.microsoft.com/people.read"
             },
            "Office 365 (Graph API) auth config"
            )
        ).

-define(KEY_PREFIX(__JOB_NAME),
            ?GET_CONFIG(keyPrefix,
            [__JOB_NAME],
            ["contact","Office365"],
            "Default KeyPrefix for Office365 data"
            )
       ).

-define(CONTACT_ATTRIBUTES(__JOB_NAME),
            ?GET_CONFIG(contactAttributes,
            [__JOB_NAME],
            [<<"businessPhones">>,<<"mobilePhone">>,<<"title">>,<<"personalNotes">>,<<"parentFolderId">>
            ,<<"companyName">>,<<"emailAddresses">>,<<"middleName">>,<<"businessHomePage">>,<<"id">>
            ,<<"assistantName">>,<<"department">>,<<"children">>,<<"officeLocation">>,<<"createdDateTime">>
            ,<<"profession">>,<<"givenName">>,<<"categories">>,<<"nickName">>,<<"jobTitle">>,<<"yomiGivenName">>
            ,<<"changeKey">>,<<"surname">>,<<"imAddresses">>,<<"spouseName">>,<<"yomiSurname">>,<<"businessAddress">>
            ,<<"lastModifiedDateTime">>,<<"generation">>,<<"manager">>,<<"initials">>,<<"displayName">>
            ,<<"homeAddress">>,<<"otherAddress">>,<<"homePhones">>,<<"fileAs">>,<<"yomiCompanyName">>,<<"birthday">>
            ], 
            "Attributes to be synced for Office365 contact data"
            )
       ).

% dperl_worker exports
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , get_status/1
        , init_state/1
        ]).

% contacts graph api
% https://docs.microsoft.com/en-us/graph/api/resources/contact?view=graph-rest-1.0

-record(state,  { name
                , channel
                , isConnected = true
                , accessToken
                , apiUrl
                , contacts = []         % 
                , keyPrefix
                , fetchUrl
                , clContacts = []       % 
                , isCleanupFinished = true
                , pushChannel
                , type = pull
                , auditStartTime = {0,0}
                , firstSync = true
                , accountId
            }).

% dperl_strategy_scr export
-export([ connect_check_src/1
        , get_source_events/2
        , connect_check_dst/1
        , do_cleanup/5
        , do_refresh/2
        , load_src_after_key/3
        , load_dst_after_key/3
        , fetch_src/2
        , fetch_dst/2
        , delete_dst/2
        , insert_dst/3
        , update_dst/3
        , report_status/3
        ]).

% dderl_oauth exports
-export([ get_auth_config/0
        , get_auth_config/1
        , get_key_prefix/0
        , get_key_prefix/1
        ]).

get_auth_config() -> ?OAUTH2_CONFIG(<<>>).

get_auth_config(JobName) -> ?OAUTH2_CONFIG(JobName).

get_key_prefix() -> ?KEY_PREFIX(<<>>).

get_key_prefix(JobName) -> ?KEY_PREFIX(JobName).

% determine the contact id (last piece of cekey) from ckey or from the remote id
% this id is a string representing a hash of the remote id
-spec local_contact_id(list()|binary()) -> string().
local_contact_id(Key) when is_list(Key) -> 
    lists:last(Key);
local_contact_id(Bin) when is_binary(Bin) -> 
    io_lib:format("~.36B",[erlang:phash2(Bin)]).

% convert list of remote values (already maps) to list of {Key,RemoteId,RemoteValue} triples
% which serves as a lookup buffer of the complete remote state, avoiding sorting issues
format_remote_values_to_kv(Values, KeyPrefix, JobName) ->
    format_remote_values_to_kv(Values, KeyPrefix, JobName, []).

format_remote_values_to_kv([], _KeyPrefix, _JobName, Acc) -> Acc;
format_remote_values_to_kv([Value|Values], KeyPrefix, JobName, Acc) -> 
    #{<<"id">> := RemoteId} = Value,        
    Key = KeyPrefix ++ [local_contact_id(RemoteId)],
    format_remote_values_to_kv(Values, KeyPrefix, JobName, [{Key,RemoteId,format_value(Value, JobName)}|Acc]).

% format remote or local value by projecting it down to configured list of synced attributes
format_value(Value, JobName) when is_map(Value) -> maps:with(?CONTACT_ATTRIBUTES(JobName), Value).

connect_check_src(#state{isConnected=true} = State) ->
    {ok, State};
connect_check_src(#state{isConnected=false, accountId=AccountId, keyPrefix=KeyPrefix} = State) ->
    ?JTrace("Refreshing access token"),
    case dderl_oauth:refresh_access_token(AccountId, KeyPrefix, ?SYNC_OFFICE365) of
        {ok, AccessToken} ->
            ?Info("new access token fetched"),
            {ok, State#state{accessToken=AccessToken, isConnected=true}};
        {error, Error} ->
            ?JError("Unexpected response : ~p", [Error]),
            {error, Error, State}
    end.

get_source_events(#state{auditStartTime=LastStartTime, type=push,
            pushChannel=PushChannel, firstSync=FirstSync} = State, BulkSize) ->
    case dperl_dal:read_audit_keys(PushChannel, LastStartTime, BulkSize) of
        {LastStartTime, LastStartTime, []} ->
            if
                FirstSync == true -> 
                    ?JInfo("Audit rollup is complete"),
                    {ok, sync_complete, State#state{firstSync=false}};
                true -> 
                    {ok, sync_complete, State}
            end;
        {_StartTime, NextStartTime, []} ->
            {ok, [], State#state{auditStartTime=NextStartTime}};
        {_StartTime, NextStartTime, Keys} ->
            UniqueKeys = lists:delete(undefined, lists:usort(Keys)),
            {ok, UniqueKeys, State#state{auditStartTime=NextStartTime}}
    end;
get_source_events(#state{contacts=[]} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{contacts=Contacts} = State, _BulkSize) ->
    {ok, Contacts, State#state{contacts=[]}}.

connect_check_dst(State) -> {ok, State}.    % Question: Why defaulted for push destination?

do_refresh(_State, _BulkSize) -> {error, cleanup_only}.

fetch_src(Key, #state{pushChannel=PushChannel, type=push}) ->
    dperl_dal:read_channel(PushChannel, Key);
fetch_src(Key, #state{clContacts=ClContacts, type=pull}) ->
    case lists:keyfind(Key, 1, ClContacts) of
        {Key, _RemoteId, Value} -> Value;
        false -> ?NOT_FOUND
    end.

fetch_dst(Key, #state{name=Name, clContacts=ClContacts, type=push, 
                apiUrl=ApiUrl, accessToken=AccessToken} = State) ->
    case lists:keyfind(Key, 1, ClContacts) of
        {Key, RemoteId, _Value} -> 
            ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
            case exec_req(ContactUrl, AccessToken) of
                #{<<"id">> := _} = RValue ->    format_value(RValue, Name);
                {error, unauthorized} ->        reconnect_exec(State, fetch_dst, [Key]);
                {error, Error} ->               {error, Error};
                _ ->                            ?NOT_FOUND
            end; 
        false -> 
            ?NOT_FOUND
    end;
fetch_dst(Key, #state{channel=Channel}) ->
    dperl_dal:read_channel(Channel, Key).

insert_dst(Key, Value, #state{name=Name, channel=Channel, pushChannel=PushChannel, type=push, 
                        keyPrefix=KeyPrefix, apiUrl=ApiUrl, accessToken=AccessToken} = State) ->
    case exec_req(ApiUrl, AccessToken, Value, post) of
        #{<<"id">> := Id} = RemoteValue ->
            NewKey = KeyPrefix ++ [local_contact_id(Id)],
            FormRemote = format_value(RemoteValue, Name),
            PushValue = dperl_dal:read_channel(PushChannel, Key),
            MergeValue = maps:merge(PushValue, FormRemote),
            MergedBin = imem_json:encode(MergeValue),
            dperl_dal:remove_from_channel(PushChannel, Key),
            dperl_dal:write_channel(Channel, NewKey, MergedBin),
            dperl_dal:write_channel(PushChannel, NewKey, MergedBin),
            {false, State};
        {error, unauthorized} ->
            reconnect_exec(State, insert_dst, [Key, Value]);
        {error, Error} ->
            {error, Error}
    end;
insert_dst(Key, Value, State) ->
    update_dst(Key, Value, State).

delete_dst(Key, #state{channel=Channel, type=push, clContacts=ClContacts, 
                apiUrl=ApiUrl, accessToken=AccessToken} = State) ->
    case lists:keyfind(Key, 1, ClContacts) of
        {Key, RemoteId, _Value} -> 
            ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
            case exec_req(ContactUrl, AccessToken, #{}, delete) of
                ok ->
                    dperl_dal:remove_from_channel(Channel, Key),
                    {false, State};
                {error, unauthorized} ->
                    reconnect_exec(State, delete_dst, [Key]);
                Error ->
                    Error
            end;
        false -> 
            {false, State}
    end;
delete_dst(Key, #state{channel=Channel, pushChannel=PushChannel} = State) ->
    dperl_dal:remove_from_channel(Channel, Key),
    dperl_dal:remove_from_channel(PushChannel, Key),
    {false, State}.

-spec update_dst(Key::list(), Value::map(), #state{}) -> tuple().
update_dst(Key, Value, #state{name=Name, channel=Channel, pushChannel=PushChannel, type=push, 
                        clContacts=ClContacts, apiUrl=ApiUrl, accessToken=AccessToken} = State) ->
    case lists:keyfind(Key, 1, ClContacts) of
        {Key, RemoteId, _Value} -> 
            ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
            case exec_req(ContactUrl, AccessToken, Value, patch) of
                #{<<"id">> := _} = RemoteValue ->
                    FormRemote = format_value(RemoteValue, Name),
                    OldValue = dperl_dal:read_channel(PushChannel, Key),
                    MergeValue = maps:merge(OldValue, FormRemote),
                    MergedBin = imem_json:encode(MergeValue),
                    dperl_dal:remove_from_channel(PushChannel, Key),
                    dperl_dal:write_channel(Channel, Key, MergedBin),
                    dperl_dal:write_channel(PushChannel, Key, MergedBin),
                    {false, State};
                {error, unauthorized} ->
                    reconnect_exec(State, update_dst, [Key, Value]);
                {error, Error} ->
                    {error, Error}
            end;
        false -> 
            {false, State}
    end;
update_dst(Key, Value, #state{channel=Channel, pushChannel=PushChannel} = State) when is_map(Value) ->
    OldValue = dperl_dal:read_channel(Channel, Key),
    MergeValue = maps:merge(OldValue, Value),
    MergedBin = imem_json:encode(MergeValue),
    dperl_dal:write_channel(Channel, Key, MergedBin),
    dperl_dal:write_channel(PushChannel, Key, MergedBin),
    {false, State}.

report_status(_Key, _Status, _State) -> no_op.

load_dst_after_key(CurKey, BlkCount, #state{channel = Channel}) ->
    dperl_dal:read_gt(Channel, CurKey, BlkCount).

load_src_after_key(CurKey, BlkCount, #state{type=pull, fetchUrl=undefined, apiUrl=ApiUrl} = State) ->
    UrlParams = dperl_dal:url_enc_params(#{"$top" => integer_to_list(BlkCount)}),
    ContactsUrl = erlang:iolist_to_binary([ApiUrl, "?", UrlParams]),
    load_src_after_key(CurKey, BlkCount, State#state{fetchUrl=ContactsUrl});
load_src_after_key(CurKey, BlkCount, #state{name=Name, type=pull, isCleanupFinished=true, 
                            keyPrefix=KeyPrefix, accessToken=AccessToken, fetchUrl=FetchUrl} = State) ->
    % fetch all contacts
    case fetch_all_contacts(FetchUrl, AccessToken, KeyPrefix, Name) of
        {ok, Contacts} ->
            load_src_after_key(CurKey, BlkCount, State#state{clContacts=Contacts, isCleanupFinished=false});
        {error, unauthorized} ->
            reconnect_exec(State, load_src_after_key, [CurKey, BlkCount]);
        {error, Error} ->
            {error, Error, State}
    end;
load_src_after_key(CurKey, BlkCount, #state{clContacts=Contacts} = State) ->
    {ok, get_contacts_gt(CurKey, BlkCount, Contacts), State}.

reconnect_exec(State, Fun, Args) ->
    case connect_check_src(State#state{isConnected = false}) of
        {ok, State1} ->
            erlang:apply(?MODULE, Fun, Args ++ [State1]);
        {error, Error, State1} ->
            {error, Error, State1}
    end.

do_cleanup(_Deletes, _Inserts, _Diffs, _IsFinished, #state{type = push}) ->
    {error, <<"cleanup only for pull job">>};
do_cleanup(Deletes, Inserts, Diffs, IsFinished, State) ->
    NewState = State#state{contacts = Inserts ++ Diffs ++ Deletes},
    if IsFinished ->    {ok, finish, NewState#state{isCleanupFinished=true}};
       true ->          {ok, NewState}
    end.

get_status(#state{}) -> #{}.

init_state(_) -> #state{}.

init({#dperlJob{name=Name, srcArgs=#{apiUrl:=ApiUrl}, args=Args,
                dstArgs=#{channel:=Channel, pushChannel:=PChannel} = DstArgs}, State}) ->
    case dperl_auth_cache:get_enc_hash(Name) of
        undefined ->
            ?JError("Encryption hash is not avaialable"),
            {stop, badarg};
        {AccountId, EncHash} ->
            ?JInfo("Starting with ~p's enchash...", [AccountId]),
            imem_enc_mnesia:put_enc_hash(EncHash),
            KeyPrefix = maps:get(keyPrefix, DstArgs, get_key_prefix(Name)),
            case dderl_oauth:get_token_info(AccountId, KeyPrefix, ?SYNC_OFFICE365) of
                #{<<"access_token">> := AccessToken} ->
                    ChannelBin = dperl_dal:to_binary(Channel),
                    PChannelBin = dperl_dal:to_binary(PChannel),
                    Type = maps:get(type, Args, pull),
                    dperl_dal:create_check_channel(ChannelBin),
                    dperl_dal:create_check_channel(PChannelBin),
                    {ok, State#state{channel = ChannelBin, name = Name, apiUrl = ApiUrl,
                                    keyPrefix = KeyPrefix, accessToken = AccessToken,
                                    pushChannel = PChannelBin, type = Type, accountId = AccountId}};
                _ ->
                    ?JError("Access token not found for ~p at ~p", [AccountId, KeyPrefix]),
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

%% Fetch all remote contacts, create 3-tuple {Key::list(), RemoteId::binary(), RemoteValue::map())
%% Sort by Key (needed for sync)
fetch_all_contacts(Url, AccessToken, KeyPrefix, JobName) ->
    fetch_all_contacts(Url, AccessToken, KeyPrefix, JobName, []).

fetch_all_contacts(Url, AccessToken, KeyPrefix, JobName, AccContacts) ->
    ?JTrace("Fetching contacts with url : ~s", [Url]),
    ?JTrace("Fetched contacts : ~p", [length(AccContacts)]),
    case exec_req(Url, AccessToken) of
        #{<<"@odata.nextLink">> := NextUrl, <<"value">> := MoreContacts} ->
            Contacts = format_remote_values_to_kv(MoreContacts, KeyPrefix, JobName),
            fetch_all_contacts(NextUrl, AccessToken, KeyPrefix, lists:append(Contacts, AccContacts));
        #{<<"value">> := MoreContacts} ->
            Contacts = format_remote_values_to_kv(MoreContacts, KeyPrefix, JobName),
            {ok, lists:keysort(1, lists:append(Contacts, AccContacts))};
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

-spec exec_req(Url::binary()|string(), AccessToken::binary()) -> tuple().
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

-spec exec_req(Url::binary()|string(), AccessToken::binary(), Body::map(), Method::atom()) -> tuple().
exec_req(Url, AccessToken, Body, Method) when is_binary(Url), is_map(Body) ->
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
