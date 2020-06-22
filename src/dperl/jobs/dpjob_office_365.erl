-module(dpjob_office_365).

-include("../dperl.hrl").
-include("../dperl_strategy_scr.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr). 

-type locKey() :: [string()].   % local key of a contact, e.g. ["contact","My","Ah2hA77a"]
-type locId()  :: string().     % last item in locKey() is called local id e.g. "Ah2hA77a"
-type locVal() :: map().        % local cvalue of a contact, converted to a map for processing
%-type locBin() :: binary().     % local cvalue of a contact in binary form (often stored like that)
-type remKey() :: binary().     % remote key of a contact, called <<"id">> in Office365
-type remKeys():: [remKey()].   % list of remKey() type (e.g. DirtyKeys)
-type remVal() :: map().        % remote value of a contact (relevant fields only)
%-type remBin() :: binary().     % remote value of a contract in raw binary JSON form
-type meta()   :: map().        % contact meta information with respect to this remote cloud


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

-define(OAUTH2_TOKEN_KEY_PREFIX(__JOB_NAME),
            ?GET_CONFIG(oAuth2KeyPrefix,
            [__JOB_NAME],
            ["dpjob","Office365"],
            "Default KeyPrefix for Office365 token cache"
            )
       ).

-define(KEY_PREFIX(__JOB_NAME),
            ?GET_CONFIG(keyPrefix,
            [__JOB_NAME],
            ["contact","Office365"],
            "Default KeyPrefix for Office365 contact data"
            )
       ).

-define(CONTACT_INDEXID,
            ?GET_CONFIG(contactIndexId,
            [],
            1,
            "Id for index on Office365 contact data"
            )
       ).

-define(CONTENT_ATTRIBUTES(__JOB_NAME),
            ?GET_CONFIG(contactAttributes,
            [__JOB_NAME],
            [<<"businessPhones">>,<<"mobilePhone">> %,<<"title">> ,<<"personalNotes">>
            ,<<"companyName">>,<<"emailAddresses">> % ,<<"middleName">>,<<"businessHomePage">>
            ,<<"assistantName">>,<<"department">> % ,<<"children">>,<<"officeLocation">>
            ,<<"profession">>,<<"givenName">>,<<"categories">>,<<"jobTitle">> % ,<<"nickName">>,<<"yomiGivenName">>
            ,<<"surname">>,<<"imAddresses">>,<<"businessAddress">> % ,<<"spouseName">>,<<"yomiSurname">>
            ,<<"manager">> % ,<<"generation">>,<<"initials">>,<<"displayName">>
            % ,<<"homeAddress">>,<<"otherAddress">>,<<"homePhones">>,<<"fileAs">>,<<"yomiCompanyName">>,<<"birthday">>
            ], 
            "Attributes to be synced for Office365 contact data"
            )
       ).

-define(META_ATTRIBUTES(__JOB_NAME),
            ?GET_CONFIG(contactAttributes,
            [__JOB_NAME],
            [<<"id">>
            ,<<"lastModifiedDateTime">>
            ,<<"changeKey">>            %,<<"parentFolderId">>,<<"createdDateTime">>
            ], 
            "Attributes used for Office365 contact change tracking"
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

%% remote item (single contact info in cache)
-record(remItem,    { remKey                    :: remKey() % remote key (id) 
                    , meta                      :: meta()   % META information (id, ...)
                    , content                   :: remVal() % relevant contact info to be synced
                    }).

%% scr processing state
-record(state,      { name                      :: jobName()
                    , type = pull               :: scrDirection()
                    , channel                   :: scrChannel()     % channel name
                    , keyPrefix                 :: locKey()         % key space prefix in channel
                    , tokenPrefix               :: locKey()         % without id #token#
                    , token                     :: map()            % token info as stored under #token#
                    , apiUrl                    :: binary()
                    , fetchUrl                  :: binary()
                    , dirtyKeys = []            :: remKeys()        % needing insert/update/delete
                    , remItems = []             :: list(#remItem{}) % cache for cleanup / ToDo: remove
                    , isConnected = true        :: boolean()
                    , isFirstSync = true        :: boolean()
                    , isCleanupFinished = true  :: boolean()
                    , auditStartTime = {0,0}    :: ddTimestamp()    % UTC timestamp {Sec,MicroSec}
                    , template  = ?NOT_FOUND    :: ?NOT_FOUND|map() % empty contact with default values 
                    , accountId                 :: ddEntityId()     % data owner
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
        , get_auth_token_key_prefix/0
        , get_auth_token_key_prefix/1
        , get_key_prefix/0
        , get_key_prefix/1
        ]).

get_auth_config() -> ?OAUTH2_CONFIG(<<>>).

get_auth_config(JobName) -> ?OAUTH2_CONFIG(JobName).

get_auth_token_key_prefix() -> ?OAUTH2_TOKEN_KEY_PREFIX(<<>>).

get_auth_token_key_prefix(JobName) -> ?OAUTH2_TOKEN_KEY_PREFIX(JobName).

get_key_prefix() -> ?KEY_PREFIX(<<>>).

get_key_prefix(JobName) -> ?KEY_PREFIX(JobName).

% determine the local id as the last piece of ckey (if available) 
%           or hash of remote id (if new to local store)
% this id is a string representing a hash of the remote id
-spec local_id(locKey()) -> locId().
local_id(Key) when is_list(Key) -> lists:last(Key).


% calculate a new local id as a string representing the hash of the remote key (id)
-spec new_local_id(remKey()) -> locKey().
new_local_id(RemKey) when is_binary(RemKey) -> io_lib:format("~.36B",[erlang:phash2(RemKey)]).

% convert list of remote values (already maps) to list of {Key,RemoteId,RemoteValue} triples
% which serves as a lookup buffer of the complete remote state, avoiding sorting issues
-spec format_remote_values_to_kv(remVal(), locKey(), jobName()) -> remVal(). 
format_remote_values_to_kv(Values, KeyPrefix, JobName) ->
    format_remote_values_to_kv(Values, KeyPrefix, JobName, []).

format_remote_values_to_kv([], _KeyPrefix, _JobName, Acc) -> Acc;
format_remote_values_to_kv([Value|Values], KeyPrefix, JobName, Acc) -> 
    #{<<"id">> := RemoteId} = Value,        
    Key = KeyPrefix ++ [new_local_id(RemoteId)],
    format_remote_values_to_kv(Values, KeyPrefix, JobName, [{Key,RemoteId,format_value(Value, JobName)}|Acc]).

% format remote or local value by projecting it down to configured list of synced (meta + content) attributes
format_value(Value, JobName) when is_map(Value) -> 
    maps:with(?META_ATTRIBUTES(JobName)++?CONTENT_ATTRIBUTES(JobName), Value).

-spec connect_check_src(#state{}) -> {ok,#state{}} | {error,any()} | {error,any(), #state{}}.
connect_check_src(#state{isConnected=true} = State) ->
    {ok, State};
connect_check_src(#state{isConnected=false, accountId=AccountId, tokenPrefix=TokenPrefix} = State) ->
    ?JTrace("Refreshing access token"),
    case dderl_oauth:refresh_access_token(AccountId, TokenPrefix, ?SYNC_OFFICE365) of
        {ok, Token} ->
            ?Info("new access token fetched"),
            {ok, State#state{token=Token, isConnected=true}};
        {error, Error} ->
            ?JError("Unexpected response : ~p", [Error]),
            {error, Error, State}
    end.

-spec get_source_events(#state{}, scrBatchSize()) -> 
        {ok,remKeys(),#state{}} | {ok,sync_complete,#state{}}. % {error,scrAnyKey()}
get_source_events(#state{auditStartTime=LastStartTime, type=push,
            channel=Channel, isFirstSync=IsFirstSync} = State, BulkSize) ->
    case dperl_dal:read_audit_keys(Channel, LastStartTime, BulkSize) of
        {LastStartTime, LastStartTime, []} ->
            if
                IsFirstSync -> 
                    ?JInfo("Audit rollup is complete"),
                    {ok, sync_complete, State#state{isFirstSync=false}};
                true -> 
                    {ok, sync_complete, State}
            end;
        {_StartTime, NextStartTime, []} ->
            {ok, [], State#state{auditStartTime=NextStartTime}};
        {_StartTime, NextStartTime, Keys} ->
            UniqueKeys = lists:delete(undefined, lists:usort(Keys)),
            {ok, UniqueKeys, State#state{auditStartTime=NextStartTime}}
    end;
get_source_events(#state{dirtyKeys=[]} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{dirtyKeys=DirtyKeys} = State, _BulkSize) ->
    ?Info("get_source_events result count ~p~n~p",[length(DirtyKeys), hd(DirtyKeys)]),
    {ok, DirtyKeys, State#state{dirtyKeys=[]}}.

- spec connect_check_dst(#state{}) -> {ok, #state{}}. % {error,any()} | {error,any(),#state{}}
connect_check_dst(State) -> {ok, State}.    % Question: Why defaulted for push destination?

do_refresh(_State, _BulkSize) -> {error, cleanup_only}. % using cleanup/refresh combined

-spec fetch_src(remKey(), #state{}) -> ?NOT_FOUND | locVal() | remVal().
fetch_src(Key, #state{channel=Channel, type=push}) ->
    dperl_dal:read_channel(Channel, Key);
fetch_src(Key, #state{remItems=RemItems, type=pull}) ->
    case lists:keyfind(Key, 1, RemItems) of
        {Key, _RemoteId, Value} -> Value;
        false -> ?NOT_FOUND
    end.

-spec fetch_dst(remKey(), #state{}) -> ?NOT_FOUND | locVal() | remVal().
fetch_dst(Key, #state{ name=Name, remItems=RemItems, type=push
                     , apiUrl=ApiUrl, token=Token} = State) ->
    case lists:keyfind(Key, 1, RemItems) of
        {Key, RemoteId, _Value} -> 
            ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
            case exec_req(ContactUrl, Token) of
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

-spec insert_dst(remKey(), remVal()|locVal(), #state{}) -> {error, any()}.
insert_dst(Key, Value, #state{type=push, apiUrl=ApiUrl, token=Token} = State) ->
    case exec_req(ApiUrl, Token, Value, post) of
        #{<<"id">> := _} = RemoteValue ->   merge_meta_to_local(Key, RemoteValue, State);
        {error, unauthorized} ->            reconnect_exec(State, insert_dst, [Key, Value]);
        {error, Error} ->                   {error, Error}
    end;
insert_dst(Key, Value, State) ->
    Result = update_dst(Key, Value, State),
    ?Info("insert_dst ~p~n~p~nresult ~p",[Key, Value, Result]),
    Result.

merge_meta_to_local(Key, RemoteValue, #state{channel=Channel, tokenPrefix=TokenPrefix} = State) ->
    AccessId = access_id(TokenPrefix),
    MetaItem = #{<<"id">> => maps:get(<<"id">>, RemoteValue)},
    case dperl_dal:read_channel(Channel, Key) of
        #{<<"META">> := Meta} = LocVal ->
            case maps:merge(Meta, #{AccessId => MetaItem}) of 
                Meta -> 
                    ok;     % RemoteMeta already there
                NewM ->
                    MergedBin = imem_json:encode(LocVal#{<<"META">> => NewM}), 
                    dperl_dal:write_channel(Channel, Key, MergedBin)
            end;
        LocVal ->
            MergedBin = imem_json:encode(LocVal#{<<"META">> => MetaItem}),
            dperl_dal:write_channel(Channel, Key, MergedBin)
    end,
    {false, State}.

access_id(TokenPrefix) ->
    list_to_binary(string:join(TokenPrefix,"/")).

-spec delete_dst(remKey(), #state{}) -> {scrSoftError(), #state{}}.
delete_dst(Key, #state{channel=Channel, type=push, remItems=RemItems, 
                apiUrl=ApiUrl, token=Token} = State) ->
    case lists:keyfind(Key, 1, RemItems) of
        {Key, RemoteId, _Value} -> 
            ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
            case exec_req(ContactUrl, Token, #{}, delete) of
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
delete_dst(Key, #state{channel=Channel} = State) ->
    ?Info("delete_dst ~p",[Key]),
    dperl_dal:remove_from_channel(Channel, Key),
    {false, State}.

-spec update_dst(remKey(), remVal()|locVal(), #state{}) -> {scrSoftError(), #state{}}.
update_dst(Key, Value, #state{name=Name, channel=Channel, type=push, 
                remItems=RemItems, apiUrl=ApiUrl, token=Token} = State) ->
    case lists:keyfind(Key, 1, RemItems) of
        {Key, RemoteId, _Value} -> 
            ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
            case exec_req(ContactUrl, Token, Value, patch) of
                #{<<"id">> := _} = RemoteValue ->
                    FormRemote = format_value(RemoteValue, Name),
                    OldValue = dperl_dal:read_channel(Channel, Key),
                    MergeValue = maps:merge(OldValue, FormRemote),
                    MergedBin = imem_json:encode(MergeValue),
                    dperl_dal:write_channel(Channel, Key, MergedBin),
                    {false, State};
                {error, unauthorized} ->
                    reconnect_exec(State, update_dst, [Key, Value]);
                {error, Error} ->
                    {error, Error}
            end;
        false -> 
            {false, State}
    end;
update_dst(Key, Value, #state{channel=Channel} = State) when is_map(Value) ->
    OldValue = dperl_dal:read_channel(Channel, Key),
    MergeValue = maps:merge(OldValue, Value),
    MergedBin = imem_json:encode(MergeValue),
    dperl_dal:write_channel(Channel, Key, MergedBin),
    {false, State}.

report_status(_Key, _Status, _State) -> no_op.

load_dst_after_key(CurKey, BlkCount, #state{type=pull, keyPrefix=KeyPrefix} = State) when CurKey < KeyPrefix ->
    load_dst_after_key(KeyPrefix, BlkCount, State);
load_dst_after_key(CurKey, BlkCount, #state{channel=Channel, type=pull, keyPrefix=KeyPrefix}) ->
    Filter = fun({K,_}) -> lists:prefix(KeyPrefix,K) end,
    lists:filter(Filter, dperl_dal:read_gt(Channel, CurKey, BlkCount)).

load_src_after_key(CurKey, BlkCount, #state{type=pull, fetchUrl=undefined, apiUrl=ApiUrl} = State) ->
    UrlParams = dperl_dal:url_enc_params(#{"$top" => integer_to_list(BlkCount)}),
    ContactsUrl = erlang:iolist_to_binary([ApiUrl, "?", UrlParams]),
    load_src_after_key(CurKey, BlkCount, State#state{fetchUrl=ContactsUrl});
load_src_after_key(CurKey, BlkCount, #state{name=Name, type=pull, isCleanupFinished=true, 
                            keyPrefix=KeyPrefix, token=Token, fetchUrl=FetchUrl} = State) ->
    case fetch_all_contacts(FetchUrl, Token, KeyPrefix, Name) of
        {ok, Contacts} ->
            load_src_after_key(CurKey, BlkCount, State#state{remItems=Contacts, isCleanupFinished=false});
        {error, unauthorized} ->
            reconnect_exec(State, load_src_after_key, [CurKey, BlkCount]);
        {error, Error} ->
            {error, Error, State}
    end;
load_src_after_key(CurKey, BlkCount, #state{type=pull, remItems=Contacts} = State) ->
    {ok, get_contacts_gt(CurKey, BlkCount, Contacts), State}.

reconnect_exec(State, Fun, Args) ->
    case connect_check_src(State#state{isConnected = false}) of
        {ok, State1} ->
            erlang:apply(?MODULE, Fun, Args ++ [State1]);
        {error, Error, State1} ->
            {error, Error, State1}
    end.

-spec do_cleanup(remKeys(), remKeys(), remKeys(), boolean(), #state{}) -> {ok, #state{}}. 
do_cleanup(_Deletes, _Inserts, _Diffs, _IsFinished, #state{type = push}) ->
    {error, <<"cleanup only for pull job">>};
do_cleanup(Deletes, Inserts, Diffs, IsFinished, State) ->
    NewState = State#state{dirtyKeys=Inserts++Diffs++Deletes},
    if IsFinished ->    {ok, finish, NewState#state{isCleanupFinished=true}};
       true ->          {ok, NewState}
    end.

get_status(#state{}) -> #{}.

init_state(_) -> #state{}.

init({#dperlJob{ name=Name, srcArgs=#{apiUrl:=ApiUrl}, args=Args
               , dstArgs=#{channel:=Channel} = DstArgs}, State}) ->
    case dperl_auth_cache:get_enc_hash(Name) of
        undefined ->
            ?JError("Encryption hash is not avaialable"),
            {stop, badarg};
        {AccountId, EncHash} ->
            ?JInfo("Starting with ~p's enchash...", [AccountId]),
            imem_enc_mnesia:put_enc_hash(EncHash),
            KeyPrefix = maps:get(keyPrefix, DstArgs, get_key_prefix(Name)),
            TokenPrefix = maps:get(tokenPrefix, Args, get_auth_token_key_prefix(Name)),
            Type = maps:get(type, Args, pull),
            ChannelBin = dperl_dal:to_binary(Channel),
            dperl_dal:create_check_channel(ChannelBin),
            ContactIff = <<"fun() ->imem_index:gen_iff_binterm_list_pattern([\"contact\",'_','_']) end.">>,
            PLContact = [{':',<<"id">>, {'#', <<"values">>, {':', <<"META">>, <<"cvalue">>}}}],
            IdxContact = #ddIdxDef{ id = ?CONTACT_INDEXID
                                  , name = <<"idx_contact">>
                                  , type = iv_k
                                  , pl = PLContact
                                  , vnf = <<"fun imem_index:vnf_identity/1.">>
                                  , iff = ContactIff},
            dperl_dal:create_check_index(ChannelBin, [IdxContact]),
            case dderl_oauth:get_token_info(AccountId, TokenPrefix, ?SYNC_OFFICE365) of
                #{<<"access_token">>:=Token} ->
                    {ok, State#state{ name=Name, type=Type, channel=ChannelBin, keyPrefix=KeyPrefix
                                    , apiUrl=ApiUrl, tokenPrefix=TokenPrefix
                                    , token=Token, accountId = AccountId
                                    , template=dperl_dal:read_channel(Channel, KeyPrefix)}};
                _ ->
                    ?JError("Access token not found for ~p at ~p", [AccountId, TokenPrefix]),
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
fetch_all_contacts(Url, Token, KeyPrefix, JobName) ->
    fetch_all_contacts(Url, Token, KeyPrefix, JobName, []).

fetch_all_contacts(Url, Token, KeyPrefix, JobName, AccContacts) ->
    ?JTrace("Fetching contacts with url : ~s", [Url]),
    ?JTrace("Fetched contacts : ~p", [length(AccContacts)]),
    case exec_req(Url, Token) of
        #{<<"@odata.nextLink">> := NextUrl, <<"value">> := MoreContacts} ->
            Contacts = format_remote_values_to_kv(MoreContacts, KeyPrefix, JobName),
            fetch_all_contacts(NextUrl, Token, KeyPrefix, lists:append(Contacts, AccContacts));
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

-spec exec_req(Url::binary()|string(), Token::binary()) -> tuple().
exec_req(Url, Token) when is_binary(Url) ->
    exec_req(binary_to_list(Url), Token);
exec_req(Url, Token) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(Token)}],
    case httpc:request(get, {Url, AuthHeader}, [], []) of
        {ok, {{_, 200, "OK"}, _, Result}} ->
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok, {{_, 401, _}, _, _}} ->
            {error, unauthorized};
        Error ->
            {error, Error}
    end.

-spec exec_req(Url::binary()|string(), Token::binary(), Body::map(), Method::atom()) -> tuple().
exec_req(Url, Token, Body, Method) when is_binary(Url), is_map(Body) ->
    exec_req(binary_to_list(Url), Token, Body, Method);
exec_req(Url, Token, Body, Method) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(Token)}],
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
