-module(dpjob_office_365).

%% implements scr puller (and later also) pusher for Office356 contact data as a c/r job.
%% The KeyPrefix ["contacts","Office365"] can be altered to camouflage the nature of the data.
%% This also supports multiple Office365 synchronisations on the same table, if needed 

%% sync order: remKey() which is the binary contact Id on the remote side (cloud)
%% remKey() is injected into the value as the primary value of a META attribute with one META
%% object per sync job. Multiple cloud contact lists can be merged into one local table key range.
%% The intermediate table (could be an encrypted skvh table). It uses an index on remKeys()
%% for lookup and for cleanup scanning in remKey() order.

-include("../dperl.hrl").
-include("../dperl_strategy_scr.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr). 

-type remKey()  :: binary().     % remote key, <<"id">> in Office365 (cleanup sort order)
-type remKeys() :: [remKey()].   % remote keys
-type remVal()  :: map().        % remote value of a contact (relevant fields only)
-type remMeta() :: map().        % contact meta information with respect to this remote cloud
-type remKVP()  :: {remKey(), {remVal(), remMeta()}}.    % remote key value pair
-type remKVPs() :: [remKVP()].   % list of remote key value pairs 

-type locKey()  :: [string()].   % local key of a contact, e.g. ["contact","My","Ah2hA77a"]
-type locId()   :: string().     % last item in locKey() is called local id e.g. "Ah2hA77a"
-type locVal()  :: map().        % sync relevant part of local cvalue of a contact
-type locMeta() :: map().        % meta part of local cvalue of a contact
-type locKVP()  :: {remKey(), {locVal(), locMeta()}}.    % local key value pair
-type locKVPs() :: [locKVP()].   % local key value pairs

-type token()   :: binary().     % OAuth access token (refreshed after 'unauthorized')

-define(COMPARE_MIN_KEY, <<>>).     % cleanup traversal by external id, scan starting after this key  
-define(COMPARE_MAX_KEY, <<255>>>). % scan ending at or beyond this key  

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
            [<<"companyName">>
            ,<<"givenName">>,<<"surname">>,<<"jobTitle">>,<<"profession">>
            %,<<"emailAddresses">>,<<"businessPhones">>,<<"mobilePhone">>,<<"homePhones">>,<<"imAddresses">>
            %,<<"department">>,<<"manager">>,<<"assistantName">> 
            %,<<"businessAddress">>
            %,<<"officeLocation">>,<<"businessHomePage">>
            %,<<"displayName">>,<<"title">>,<<"middleName">>,<<"initials">>,<<"nickName">>
            %,<<"birthday">>,<<"categories">>,<<"personalNotes">>
            %,<<"spouseName">>,<<"children">>,<<"generation">>
            %,<<"yomiSurname">>,<<"yomiGivenName">>,<<"yomiCompanyName">>
            %,<<"homeAddress">>,<<"otherAddress">>,<<"fileAs">>
            ], 
            "Attributes to be synced for Office365 contact data"
            )
       ).

-define(META_ATTRIBUTES(__JOB_NAME),
            ?GET_CONFIG(metaAttributes,
            [__JOB_NAME],
            [<<"id">>
            %,<<"lastModifiedDateTime">>
            %,<<"changeKey">>
            %,<<"parentFolderId">>
            %,<<"createdDateTime">>
            ], 
            "Meta attributes used for Office365 contact change tracking"
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

%% scr processing state
-record(state,      { name                      :: jobName()
                    , type = pull               :: scrDirection()
                    , channel                   :: scrChannel()     % channel name
                    , keyPrefix                 :: locKey()         % key space prefix in channel
                    , tokenPrefix               :: locKey()         % without id #token#
                    , token                     :: token()          % access token binary
                    , apiUrl                    :: string()
                    , fetchUrl                  :: string()
                    , cycleBuffer = []          :: remKVPs() | remKVPs() % dirty buffer for one c/r cycle
                    , isConnected = true        :: boolean()        % fail unauthorized on first use
                    , isFirstSync = true        :: boolean()
                    , isCleanupFinished = true  :: boolean()
                    , auditStartTime = {0,0}    :: ddTimestamp()    % UTC timestamp {Sec,MicroSec}
                    , template  = #{}           :: locVal()         % empty contact with default values 
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

-spec get_auth_config() -> map().
get_auth_config() -> ?OAUTH2_CONFIG(<<>>).

-spec get_auth_config(jobName()) -> map().
get_auth_config(JobName) -> ?OAUTH2_CONFIG(JobName).

-spec get_auth_token_key_prefix() -> locKey().
get_auth_token_key_prefix() -> ?OAUTH2_TOKEN_KEY_PREFIX(<<>>).

-spec get_auth_token_key_prefix(jobName()) -> locKey().
get_auth_token_key_prefix(JobName) -> ?OAUTH2_TOKEN_KEY_PREFIX(JobName).

-spec get_key_prefix() -> locKey().
get_key_prefix() -> ?KEY_PREFIX(<<>>).

-spec get_key_prefix(jobName()) -> locKey().
get_key_prefix(JobName) -> ?KEY_PREFIX(JobName).

% determine the local id as the last piece of ckey (if available) 
%-spec local_id(locKey()) -> locId().
%local_id(Key) when is_list(Key) -> lists:last(Key).

% calculate a new local id as a string representing the hash of the remote key (id)
-spec new_local_id(remKey()) -> locId().
new_local_id(RemKey) when is_binary(RemKey) -> io_lib:format("~.36B",[erlang:phash2(RemKey)]).

-spec get_local_key(remKey(), jobName(), scrChannel(), locKey()) -> locKey() | ?NOT_FOUND.
get_local_key(Id, _Name, Channel, KeyPrefix) ->
    Stu = {?CONTACT_INDEXID,Id}, 
    case dperl_dal:read_channel_index_key_prefix(Channel, Stu, KeyPrefix) of 
        [] ->       ?NOT_FOUND;
        [Key] ->    Key     % ToDo: Check/filter with META key = Name
    end.

% convert list of remote values (already maps) to list of {Key,RemoteId,RemoteValue} triples
% which serves as a lookup buffer of the complete remote state, avoiding sorting issues
% -spec format_remote_values_to_kv(remVal(), locKey(), jobName()) -> remVal(). 
% format_remote_values_to_kv(Values, KeyPrefix, JobName) ->
%     format_remote_values_to_kv(Values, KeyPrefix, JobName, []).

% format_remote_values_to_kv([], _KeyPrefix, _JobName, Acc) -> Acc;
% format_remote_values_to_kv([Value|Values], KeyPrefix, JobName, Acc) -> 
%     #{<<"id">> := RemoteId} = Value,        
%     Key = KeyPrefix ++ [new_local_id(RemoteId)],
%     format_remote_values_to_kv(Values, KeyPrefix, JobName, [{Key,RemoteId,format_value(Value, JobName)}|Acc]).

% format remote value into a special KV pair with seperated contact- and meta-maps
% by projecting out syncable attributes(content / meta)
-spec remote_kvp(remVal(), jobName()) -> remKVP().
remote_kvp(#{<<"id">>:=Id} = Value, Name) when is_map(Value) -> 
    {Id, {maps:with(?CONTENT_ATTRIBUTES(Name), Value), maps:with(?META_ATTRIBUTES(Name), Value)}}.

% format local value into a local KV pair
% by projecting out syncable attributes(content / meta)
-spec local_kvp(remKey(), locVal(), jobName()) -> locKVP().
local_kvp(Id, Value, Name) when is_map(Value) -> 
    {Id, {maps:with(?CONTENT_ATTRIBUTES(Name), Value), maps:with(?META_ATTRIBUTES(Name), Value)}}.

-spec connect_check_src(#state{}) -> {ok,#state{}} | {error,any()} | {error,any(), #state{}}.
connect_check_src(#state{isConnected=true} = State) ->
    {ok, State};
connect_check_src(#state{isConnected=true, type=push} = State) ->
    {ok, State#state{isConnected=true}};
connect_check_src(#state{isConnected=false, type=pull, accountId=AccountId, tokenPrefix=TokenPrefix} = State) ->
    ?JTrace("Refreshing access token"),
    case dderl_oauth:refresh_access_token(AccountId, TokenPrefix, ?SYNC_OFFICE365) of
        {ok, Token} ->      %?Info("new access token fetched"),
            {ok, State#state{token=Token, isConnected=true}};
        {error, Error} ->
            ?JError("Unexpected response refreshing access token: ~p", [Error]),
            {error, Error, State}
    end.

-spec get_source_events(#state{}, scrBatchSize()) -> 
        {ok, remKVPs(), #state{}} | {ok, sync_complete, #state{}}. % {error,scrAnyKey()}
% get_source_events(#state{auditStartTime=LastStartTime, type=push,
%             channel=Channel, isFirstSync=IsFirstSync} = State, BulkSize) ->
%     case dperl_dal:read_audit_keys(Channel, LastStartTime, BulkSize) of
%         {LastStartTime, LastStartTime, []} ->
%             if
%                 IsFirstSync -> 
%                     ?JInfo("Audit rollup is complete"),
%                     {ok, sync_complete, State#state{isFirstSync=false}};
%                 true -> 
%                     {ok, sync_complete, State}
%             end;
%         {_StartTime, NextStartTime, []} ->
%             {ok, [], State#state{auditStartTime=NextStartTime}};
%         {_StartTime, NextStartTime, Keys} ->
%             UniqueKeys = lists:delete(undefined, lists:usort(Keys)),
%             {ok, UniqueKeys, State#state{auditStartTime=NextStartTime}}
%     end;
get_source_events(#state{cycleBuffer=[]} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{cycleBuffer=CycleBuffer} = State, _BulkSize) ->
    ?Info("get_source_events result count ~p~n~p",[length(CycleBuffer), hd(CycleBuffer)]),
    {ok, CycleBuffer, State#state{cycleBuffer=[]}}.

- spec connect_check_dst(#state{}) -> {ok, #state{}}. % | {error, term()} | {error, term(), #state{}}
connect_check_dst(State) -> {ok, State}.    % ToDo: Maybe implement for push destination?

do_refresh(_State, _BulkSize) -> {error, cleanup_only}. % using cleanup/refresh combined

-spec fetch_src(remKey(), #state{}) -> ?NOT_FOUND | {remVal(), remMeta()}.
% fetch_src(Key, #state{channel=Channel, type=push}) ->
%     dperl_dal:read_channel(Channel, Key);
fetch_src(Id, #state{name=Name, type=pull, apiUrl=ApiUrl, token=Token} = State) -> 
    ContactUrl = ApiUrl ++ binary_to_list(Id),
    ?JTrace("Fetching contact with url : ~s", [ContactUrl]),
    case exec_req(ContactUrl, Token) of
        {error, unauthorized} ->        reconnect_exec(State, fetch_src, [Id]);
        {error, Error} ->               {error, Error, State};
        #{<<"id">> := _} = RemVal ->    remote_kvp(RemVal, Name);
        _ ->                            ?NOT_FOUND
    end.

-spec fetch_dst(remKVP() | locKVP(), #state{}) -> ?NOT_FOUND | {locVal(), locMeta()} | {remVal(), remMeta()}.
% fetch_dst(Key, #state{ name=Name, remItems=RemItems, type=push
%                      , apiUrl=ApiUrl, token=Token} = State) ->
%     case lists:keyfind(Key, 1, RemItems) of
%         {Key, RemoteId, _Value} -> 
%             ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
%             case exec_req(ContactUrl, Token) of
%                 #{<<"id">> := _} = RValue ->    format_value(RValue, Name);
%                 {error, unauthorized} ->        reconnect_exec(State, fetch_dst, [Key]);
%                 {error, Error} ->               {error, Error};
%                 _ ->                            ?NOT_FOUND
%             end; 
%         false -> 
%             ?NOT_FOUND
%     end;
fetch_dst(Id, #state{name=Name, channel=Channel, keyPrefix=KeyPrefix, type=pull}) ->
    Key = get_local_key(Id, Name, Channel, KeyPrefix),
    case dperl_dal:read_channel(Channel, Key) of 
        ?NOT_FOUND ->   ?NOT_FOUND;
        Value ->        local_kvp(Id, Value, Name)
    end.

-spec insert_dst(remKey(), remKVP()|locKVP(), #state{}) -> {scrSoftError(), #state{}}.
% insert_dst(Key, Value, #state{type=push, apiUrl=ApiUrl, token=Token} = State) ->
%     case exec_req(ApiUrl, Token, Value, post) of
%         #{<<"id">> := _} = RemoteValue ->   merge_meta_to_local(Key, RemoteValue, State);
%         {error, unauthorized} ->            reconnect_exec(State, insert_dst, [Key, Value]);
%         {error, Error} ->                   {error, Error}
%     end;
insert_dst(Id, {Id, {Value,Meta}}, #state{ name=Name, channel=Channel, type=pull
                                   , keyPrefix=KeyPrefix, template=Template} = State) ->
    Key = KeyPrefix ++ [new_local_id(Id)],
    ?Info("insert_dst ~p",[Key]),
    MergedValue = maps:merge(maps:merge(Template, Value), #{<<"META">> => #{Name=>Meta}}),
    MergedBin = imem_json:encode(MergedValue), 
    case dperl_dal:write_channel(Channel, Key, MergedBin) of 
        ok ->
            {false, State};
        {error, Error} ->   
            ?Error("insert_dst ~p~n~p~nresult ~p",[Key, MergedBin, {error, Error}]),
            {true, State}
    end.

-spec update_dst(remKey(), remKVP()|locKVP(), #state{}) -> {scrSoftError(), #state{}}.
% update_dst(Key, Value, #state{name=Name, channel=Channel, type=push, 
%                 remItems=RemItems, apiUrl=ApiUrl, token=Token} = State) ->
%     case lists:keyfind(Key, 1, RemItems) of
%         {Key, RemoteId, _Value} -> 
%             ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
%             case exec_req(ContactUrl, Token, Value, patch) of
%                 #{<<"id">> := _} = RemoteValue ->
%                     FormRemote = format_value(RemoteValue, Name),
%                     OldValue = dperl_dal:read_channel(Channel, Key),
%                     MergeValue = maps:merge(OldValue, FormRemote),
%                     MergedBin = imem_json:encode(MergeValue),
%                     dperl_dal:write_channel(Channel, Key, MergedBin),
%                     {false, State};
%                 {error, unauthorized} ->
%                     reconnect_exec(State, update_dst, [Key, Value]);
%                 {error, Error} ->
%                     {error, Error}
%             end;
%         false -> 
%             {false, State}
%     end;
update_dst(Id, {Id, {Value,Meta}}, #state{ name=Name, channel=Channel, keyPrefix=KeyPrefix
                                         , type=pull, template=Template} = State) ->
    Key = get_local_key(Id, Name, Channel, KeyPrefix),
    ?Info("update_dst ~p",[Key]),
    case dperl_dal:read_channel(Channel, Key) of
        ?NOT_FOUND ->   
            ?JError("update_dst key ~p not found for remote id ~p", [Key, Id]),
            {true, State};
        #{<<"META">> := OldMeta} = OldVal ->
            NewMeta = maps:merge(OldMeta, #{Name => Meta}),
            AllVal = maps:merge(Template, OldVal),
            NewVal = maps:merge(AllVal, Value),
            case update_local(Channel, Key, OldVal, NewVal, NewMeta) of 
                ok ->
                    {false, State};
                {error, _Error} ->
                    ?JError("update_dst cannot update key ~p to ~p", [Key, NewVal]),   
                    {true, State}
            end 
    end.

%% update a local contact record to new value and new metadata.
%% create an audit log only if the value changes, not for a pure meta update.
%% this should avoid endless provisioning loops for metadata changes only. 
-spec update_local(scrChannel(), locKey(), remVal(), locVal(), remMeta()) -> 
        {scrSoftError(), #state{}}.
update_local(Channel, Key, OldVal, OldVal, NewMeta) ->
    MergedBin = imem_json:encode(OldVal#{<<"META">> => NewMeta}),
    dperl_dal:write_channel_no_audit(Channel, Key, MergedBin);
update_local(Channel, Key, _OldVal, NewVal, NewMeta) ->
    MergedBin = imem_json:encode(NewVal#{<<"META">> => NewMeta}),
    dperl_dal:write_channel(Channel, Key, MergedBin).

-spec delete_dst(remKey(), #state{}) -> {scrSoftError(), #state{}}.
% delete_dst(Key, #state{channel=Channel, type=push, remItems=RemItems, 
%                 apiUrl=ApiUrl, token=Token} = State) ->
%     case lists:keyfind(Key, 1, RemItems) of
%         {Key, RemoteId, _Value} -> 
%             ContactUrl = erlang:iolist_to_binary([ApiUrl, RemoteId]),
%             case exec_req(ContactUrl, Token, #{}, delete) of
%                 ok ->
%                     dperl_dal:remove_from_channel(Channel, Key),
%                     {false, State};
%                 {error, unauthorized} ->
%                     reconnect_exec(State, delete_dst, [Key]);
%                 Error ->
%                     Error
%             end;
%         false -> 
%             {false, State}
%     end;
delete_dst(Id, #state{ name=Name, channel=Channel, keyPrefix=KeyPrefix
                     , type=pull, template=Template} = State) ->
    Key = get_local_key(Id, Name, Channel, KeyPrefix),
    ?Info("delete_dst ~p",[Key]),
    case fetch_dst(Id, State) of 
        ?NOT_FOUND ->           
            {true, state};
        {Id, {Value, Meta}} ->
            case maps:without(Name, Meta) of 
                #{} ->      % no other syncs remaining for this key
                    dperl_dal:remove_from_channel(Channel, Key),
                    {false, State};
                NewMeta ->  % other syncs remaining for this key
                    NewVal = maps:merge(Template, Value),       
                    case update_local(Channel, Key, Value, NewVal, NewMeta) of 
                        ok ->
                            {false, State};
                        {error, _Error} ->
                            ?JError("delete_dst cannot delete key ~p", [Key]),   
                            {true, State}
                    end 
            end    
    end.

report_status(_Key, _Status, _State) -> no_op.

-spec load_dst_after_key(remKVP() | locKVP(), scrBatchSize(), #state{}) -> {ok, locKVPs(), #state{}} | {error, term(), #state{}}.
load_dst_after_key({Id,{_,_}}, BlkCount, #state{name=Name, channel=Channel, type=pull, keyPrefix=KeyPrefix}) ->
    {ok, read_local_kvps_after_id(Channel, Name, KeyPrefix, Id, BlkCount, [])};
load_dst_after_key(_Key, BlkCount, #state{name=Name, channel=Channel, type=pull, keyPrefix=KeyPrefix} = State) ->
    ?Info("load_dst_after_key for non-matching (initial) key ~p", [_Key]),
    case read_local_kvps_after_id(Channel, Name, KeyPrefix, ?COMPARE_MIN_KEY, BlkCount, []) of 
        L when is_list(L) ->    {ok, L, State};
        {error, Reason} ->      {error, Reason, State}
    end.

%% starting after Id, run through remoteId index and collect a block of locKVP() data belonging to 
%% this job name and keyPrefix 
-spec read_local_kvps_after_id(scrChannel(), jobName(), locKey(), remKey(), scrBatchSize(), locKVPs()) -> locKVPs() | {error, term()}.
read_local_kvps_after_id(_Channel, _Name, _KeyPrefix, _Id, _BlkCount, _Acc) ->
    % lists:prefix(KeyPrefix,K)
    [].

-spec load_src_after_key(remKVP()| locKVP(), scrBatchSize(), #state{}) -> 
        {ok, remKVPs(), #state{}} | {error, term(), #state{}}.
load_src_after_key(_CurKVP, _BlkCount, #state{type=pull, fetchUrl=finished} = State) ->
    {ok, [], State};
load_src_after_key(CurKVP, BlkCount, #state{type=pull, fetchUrl=undefined, apiUrl=ApiUrl} = State) ->
    UrlParams = dperl_dal:url_enc_params(#{"$top" => integer_to_list(BlkCount)}),
    ContactsUrl = lists:flatten([ApiUrl, "?", UrlParams]),
    load_src_after_key(CurKVP, BlkCount, State#state{fetchUrl=ContactsUrl});
load_src_after_key(CurKVP, BlkCount, #state{ name=Name, type=pull, token=Token
                                           , fetchUrl=FetchUrl} = State) ->
    ?JTrace("Fetching contacts with url : ~s", [FetchUrl]),
    case exec_req(FetchUrl, Token) of
        {error, unauthorized} ->    reconnect_exec(State, load_src_after_key, [CurKVP, BlkCount]);
        {error, Error} ->           {error, Error, State};
        #{<<"@odata.nextLink">> := NextUrl, <<"value">> := RemVals} ->
            KVPs = [remote_kvp(RemVal, Name) || RemVal <- RemVals],
            ?JTrace("Fetched contacts : ~p", [length(KVPs)]),
            %?Info("First fetched contact : ~p", [element(1,hd(KVPs))]),
            {ok, KVPs, State#state{fetchUrl=NextUrl}};
        #{<<"value">> := RemVals} ->        % may be an empty list
            KVPs = [remote_kvp(RemVal, Name) || RemVal <- RemVals],
            ?JTrace("Last fetched contacts : ~p", [length(KVPs)]),
            {ok, KVPs, State#state{fetchUrl=finished}}
    end.

-spec reconnect_exec(#state{}, fun(), list()) -> 
        {scrSoftError(), #state{}} | {ok, remKVPs(), #state{}} | {error, term(), #state{}}.
reconnect_exec(State, Fun, Args) ->
    case connect_check_src(State#state{isConnected=false}) of
        {ok, State1} ->             erlang:apply(?MODULE, Fun, Args ++ [State1]);
        {error, Error, State1} ->   {error, Error, State1}
    end.

% execute cleanup/refresh for found differences (Deletes, Inserts and value Diffs)
-spec do_cleanup(remKeys(), remKeys(), remKeys(), boolean(), #state{}) -> 
        {ok, #state{}} | {ok, finish, #state{}} . 
do_cleanup(Deletes, Inserts, Diffs, IsFinished, #state{type=pull} = State) ->
    NewState = State#state{cycleBuffer=Deletes++Diffs++Inserts},
    if 
        IsFinished ->
            %% deposit cleanup batch dirty results in state for sync to pick up
            %% confirm finished cleanup cycle (last Diffs to sync in cycle)
            %% re-arm cleanup fetching to restart with top rows in id order    
            {ok, finish, NewState#state{fetchUrl=undefined}};
        true ->
            %% deposit cleanup batch dirty results in state for sync to pick up
            {ok, NewState}
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
            Template = case dperl_dal:read_channel(Channel, KeyPrefix) of 
                ?NOT_FOUND ->           #{};
                T when is_map(T) ->     T 
            end,
            case dderl_oauth:get_token_info(AccountId, TokenPrefix, ?SYNC_OFFICE365) of
                #{<<"access_token">>:=Token} ->
                    {ok, State#state{ name=Name, type=Type, channel=ChannelBin, keyPrefix=KeyPrefix
                                    , apiUrl=ApiUrl, tokenPrefix=TokenPrefix
                                    , token=Token, accountId=AccountId
                                    , template=Template}};
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

%% get a json object from the cloud service and convert it to a map
%% use OAuth2 header with token
-spec exec_req(string(), binary()) -> map() | {error, term()}.
exec_req(Url, Token) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(Token)}],
    case httpc:request(get, {Url, AuthHeader}, [], []) of
        {ok, {{_, 200, "OK"}, _, Result}} ->
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok, {{_, 401, _}, _, _}} ->
            {error, unauthorized};
        {error, Reason} ->
            ?Info("exec_req get ~p returns error ~p",[Url,Reason]),
            {error, Reason}
    end.

%% emit a cloud service request and convert json result into a map.
%% The request body is cast from a map.
%% use OAuth2 header with token
-spec exec_req(string(), binary(), map(), atom()) -> ok | map() | {error, term()}.
exec_req(Url, Token, Body, Method) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(Token)}],
    case httpc:request(Method, 
                       {Url, AuthHeader, "application/json", imem_json:encode(Body)}, 
                       [], 
                       []) of
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
