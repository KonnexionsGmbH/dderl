-module(dpjob_ouraring_crawl).

%% implements scr puller callback for OuraRing cloud data as a cleanup/refresh job (c/r)
%% avoids pulling incomplete intra-day data by not pulling beyond yesterday's data
%% the default "OuraRing" identifier can be altered to camouflage the nature of the data
%% this also supports multiple OuraRing pullers into the same table, if needed 

-include("../dperl.hrl").
-include("../dperl_strategy_scr.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

-type metric()      :: string().     % "userinfo" | "sleep" | "readiness" | "activity".
-type locKey()      :: [string()].   % local key of a metric, e.g. ["healthDevice","OuraRing","sleep"]
-type locVal()      :: map().        % local cvalue of a metric, converted to a map for processing
-type locKVP()      :: {locKey(), locVal()}.    % local key value pair
-type year()        :: non_neg_integer().
-type month()       :: 1..12.
-type day()         :: 1..31.
-type date()        :: {year(), month(), day()}.
-type stringDate()  :: string().     % "YYYY-MM-DD"
-type dayDate()     :: date() | stringDate().
-type maybeDate()   :: undefined | date().
-type status()      :: #{ lastSleepDay := maybeDate()
                        , lastActivityDay := maybeDate()
                        , lastReadinessDay := maybeDate()}.     % relevant cleanup status
-type token()       :: binary().    % OAuth access token (refreshed after 'unauthorized')

-define(METRICS, ["userinfo", "readiness", "sleep", "activity"]).

-define(OAUTH2_CONFIG(__JOB_NAME),
        ?GET_CONFIG(oAuth2Config,[__JOB_NAME],
            #{auth_url =>"https://cloud.ouraring.com/oauth/authorize?response_type=code",
              client_id => "12345", redirect_uri => "https://localhost:8443/dderl/",
              client_secret => "12345", grant_type => "authorization_code",
              token_url => "https://cloud.ouraring.com/oauth/token",
              scope => "email personal daily"},
            "Oura Ring auth config")).

-define(OAUTH2_TOKEN_KEY_PREFIX(__JOB_NAME),
            ?GET_CONFIG(oAuth2KeyPrefix,
            [__JOB_NAME],
            ["dpjob","OuraRing"],
            "Default KeyPrefix for OuraRing token cache"
            )
       ).

-define(KEY_PREFIX(__JOB_NAME),
          ?GET_CONFIG(keyPrefix, [__JOB_NAME], ["healthDevice","OuraRing"],
          "Default KeyPrefix for Oura Ring data")
       ).

-define(SHIFT_DAYS(__JOB_NAME),
          ?GET_CONFIG(daysToBeShiftedAtStart, [__JOB_NAME], 100,
          "Days to be shifted backwards for starting the job")
       ).

-record(state,  { name                  :: jobName()
                , type = pull           :: scrDirection()   % constant here
                , channel               :: scrChannel()     % channel name
                , keyPrefix             :: locKey()         % key space prefix in channel
                , tokenPrefix           :: locKey()         % without id #token#
                , token                 :: token()          % token binary string 
                , apiUrl                :: string()
                , isConnected = true    :: boolean()        % provokes unauthorized in first cycle
                , lastSleepDay          :: maybeDate()
                , lastActivityDay       :: maybeDate()
                , lastReadinessDay      :: maybeDate()
                , cycleBuffer = []      :: [locKVP()]       % dirty buffer for one c/r cycle
                , accountId             :: ddEntityId()
                }).

% dperl_worker exports
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , get_status/1
        , init_state/1
        ]).

% dderl_oauth exports
-export([ get_auth_config/0
        , get_auth_config/1
        , get_key_prefix/0
        , get_key_prefix/1
        , get_auth_token_key_prefix/0
        , get_auth_token_key_prefix/1
        ]).

% dperl_strategy_scr export
-export([ connect_check_src/1
        , get_source_events/2
        , connect_check_dst/1
        , do_cleanup/2
        , do_refresh/2
        , fetch_src/2
        , fetch_dst/2
        , delete_dst/2
        , insert_dst/3
        , update_dst/3
        , report_status/3
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


-spec connect_check_src(#state{}) -> {ok, #state{}} | {error, term(), #state{}}.
connect_check_src(#state{isConnected = true} = State) ->
    {ok, State};
connect_check_src(#state{isConnected=false, accountId=AccountId, tokenPrefix=TokenPrefix} = State) ->
    ?JTrace("Refreshing access token"),
    case dderl_oauth:refresh_access_token(AccountId, TokenPrefix, ?SYNC_OURARING) of
        {ok, Token} ->
            %?Info("new access token fetched"),
            {ok, State#state{token=Token, isConnected=true}};
        {error, Error} ->
            ?JError("Unexpected response : ~p", [Error]),
            {error, Error, State}
    end.

%% get CycleBuffer (dirty kv-pairs as detected in this c/r cycle)
-spec get_source_events(#state{}, scrBatchSize()) ->
    {ok, [{locKey(),locVal()}], #state{}} | {ok, sync_complete, #state{}}.
get_source_events(#state{cycleBuffer=[]} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{cycleBuffer=CycleBuffer} = State, _BulkSize) ->
    {ok, CycleBuffer, State#state{cycleBuffer=[]}}.

connect_check_dst(State) -> {ok, State}.

do_refresh(_State, _BulkSize) -> {error, cleanup_only}.

fetch_src({_Key, Value}, _State) -> Value.

-spec fetch_dst(locKey(), #state{}) -> ?NOT_FOUND | locVal().
fetch_dst({Key, _}, #state{channel=Channel}) ->
    dperl_dal:read_channel(Channel, Key).

-spec insert_dst(locKey(), locVal(), #state{}) -> {scrSoftError(), #state{}}.
insert_dst(Key, Val, State) ->
    update_dst(Key, Val, State).

report_status(_Key, _Status, _State) -> no_op.

% execute simple cleanup for next batch of KVPs 
-spec do_cleanup(#state{}, scrBatchSize()) ->
    {ok, #state{}} | {ok, finish, #state{}} | {error, term(), #state{}}.
do_cleanup(State, _BlkCount) ->
    case fetch_metrics(?METRICS, State) of
        {ok, #state{cycleBuffer=CycleBuffer} = State1} ->
            case CycleBuffer of
                [_] ->  % only userinfo remains. we are done
                    %?Info("do_cleanup is finished with ~p", [CycleBuffer]),
                    {ok, finish, State1};
                _ ->    % other items in the list, continue
                    %?Info("do_cleanup continues with ~p", [CycleBuffer]),
                    {ok, State1}
            end;
        {error, Error} ->
            {error, Error, State#state{isConnected=false}}
    end.

-spec delete_dst(locKey(), #state{}) -> {scrSoftError(), #state{}}. 
delete_dst(Key, #state{channel = Channel} = State) ->
    ?JInfo("Deleting : ~p", [Key]),
    dperl_dal:remove_from_channel(Channel, Key),
    {false, State}.

-spec update_dst(locKey() | locKVP(), locVal(), #state{}) -> {scrSoftError(), #state{}}.
update_dst({Key, _}, Val, State) ->
    update_dst(Key, Val, State);
update_dst(Key, Val, #state{channel=Channel} = State) when is_binary(Val) ->
    dperl_dal:write_channel(Channel, Key, Val),
    {false, State};
update_dst(Key, Val, State) ->
    update_dst(Key, imem_json:encode(Val), State).

-spec get_status(#state{}) -> status().
get_status(#state{ lastSleepDay=LastSleepDay, lastActivityDay=LastActivityDay
                 , lastReadinessDay=LastReadinessDay}) ->
    #{ lastSleepDay=>LastSleepDay
     , lastActivityDay=>LastActivityDay
     , lastReadinessDay=>LastReadinessDay}.

%% (partially) initialize job state from status info in first matching 
%% jobDyn table (from all available nodes) 
-spec init_state([#dperlNodeJobDyn{}]) -> #state{}.
init_state([]) -> #state{};
init_state([#dperlNodeJobDyn{state = State} | _]) ->
    LastSleepDay = maps:get(lastSleepDay, State, undefined),
    LastActivityDay = maps:get(lastActivityDay, State, undefined),
    LastReadinessDay = maps:get(lastReadinessDay, State, undefined),
    #state{ lastSleepDay=LastSleepDay
          , lastActivityDay=LastActivityDay
          , lastReadinessDay = LastReadinessDay};
init_state([_|Others]) -> init_state(Others).

%% fully initialize a job using the job config and the partially filled
%% state record (see init_state) derived from nodeJobDyn entries
-spec init({#dperlJob{}, #state{}}) -> {ok, #state{}} | {stop, badarg}.
init({#dperlJob{ name=Name, dstArgs=#{channel:=Channel} = DstArgs, args=Args
               , srcArgs=#{apiUrl:=ApiUrl}}, State}) ->
    case dperl_auth_cache:get_enc_hash(Name) of
        undefined ->
            ?JError("Encryption hash is not avaialable"),
            {stop, badarg};
        {AccountId, EncHash} ->
            ?JInfo("Starting with ~p's enchash...", [AccountId]),
            imem_enc_mnesia:put_enc_hash(EncHash),
            KeyPrefix = maps:get(keyPrefix, DstArgs, get_key_prefix(Name)),
            TokenPrefix = maps:get(tokenPrefix, Args, get_auth_token_key_prefix(Name)),
            ChannelBin = dperl_dal:to_binary(Channel),
            dperl_dal:create_check_channel(ChannelBin),
            case dderl_oauth:get_token_info(AccountId, TokenPrefix, ?SYNC_OURARING) of
                #{<<"access_token">> := Token} ->
                    {ok, State#state{ name=Name, channel=ChannelBin, keyPrefix=KeyPrefix
                                    , apiUrl=ApiUrl, tokenPrefix=TokenPrefix, token=Token
                                    , accountId = AccountId}};
                _ ->
                    ?JError("Access token not found for KeyPrefix ~p",[KeyPrefix]),
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

%% perform one fetch round for all desired metrics
%% fetch one value per metric for its respective due date
%% skip to next metric, if metric is not available or if all values are fetched
%% aggregate kv into cycleBuffer in state to be processed/stored per round  
-spec fetch_metrics([metric()], #state{}) -> {ok, #state{}} | {error, term()}.
fetch_metrics([], State) -> {ok, State};
fetch_metrics(["userinfo"|Metrics], State) ->
    case fetch_userinfo(State) of
        {error, Error} ->   {error, Error};
        {ok, State1} ->     fetch_metrics(Metrics, State1)
    end;
fetch_metrics([Metric|Metrics], #state{cycleBuffer=CycleBuffer} = State) ->
    case get_day(Metric, State) of
        fetched ->
            fetch_metrics(Metrics, State);
        Day ->
            case fetch_metric(Metric, Day, State) of
                {error, Error} ->
                    {error, Error};
                none ->
                    fetch_metrics(Metrics, State);
                {ok, MDay, KVP} ->
                    State1 = set_metric_day(Metric, MDay, State#state{cycleBuffer=[KVP|CycleBuffer]}),
                    fetch_metrics(Metrics, State1)
            end
    end.

%% fetch metric value for Day from cloud service, if it exists
%% values may be missing for certain days in which case a 'start_day_query' is used which gives
%% the first data after the missing day 
-spec fetch_metric(metric(), date(), #state{}) -> {ok, date(), locKVP()} | none | {error, term()}.
fetch_metric(Metric, Day, #state{keyPrefix=KeyPrefix, apiUrl=ApiUrl, token=Token} = State) ->
    ?JInfo("Fetching metric for ~s on ~p", [Metric, Day]),
    NextDay = next_day(Day),
    case fetch_metric(Metric, day_query(Day), ApiUrl, Token) of
        none ->
            case fetch_metric(Metric, start_day_query(NextDay), ApiUrl, Token) of
                {ok, _} ->  fetch_metric(Metric, NextDay, State);
                _Other ->   none
            end;
        {ok, Value} ->
            Key = build_key(KeyPrefix, Metric),
            KVP = {Key, Value#{<<"_day">> => list_to_binary(edate:date_to_string(Day))}},
            case Metric of
                Metric when Metric=="sleep"; Metric=="readiness" ->
                    {ok, Day, KVP};
                "activity" ->
                    % fetching activity only if next days activity data (partially) exists
                    case fetch_metric(Metric, start_day_query(NextDay), ApiUrl, Token) of
                        {ok, _} ->          {ok, Day, KVP};
                        none ->             none;
                        {error, Error} ->   {error, Error}
                    end
            end;
        {error, Error} ->
            ?JError("Error fetching ~s for ~p : ~p", [Metric, Day, Error]),
            {error, Error}
    end.

%% fetch metric data from Oura cloud by metric and date condition (given as url parameter string)
-spec fetch_metric(metric(), string(), string(), token()) -> {ok, locVal()} | none | {error, term()}.
fetch_metric(Metric, DayQuery, ApiUrl, Token) ->
    Url = ApiUrl ++ Metric ++ DayQuery,
    MetricBin = list_to_binary(Metric),
    case exec_req(Url, Token) of
        #{MetricBin:=[]} = _R ->         
            %?Info("fetch_metric ~p ~p result none ~p",[Metric, DayQuery, _R]),
            none;
        Value when is_map(Value) ->     
            %?Info("fetch_metric ~p ~p result ok",[Metric, DayQuery]),
            {ok, Value};
        {error, Error} ->               
            %?Info("fetch_metric ~p ~p result error ~p",[Metric, DayQuery, Error]),
            {error, Error}
    end.
    
%% fetch userinfo from Oura cloud and add it to the CycleBuffer
%% userinfo comes as latest value only, no day history available
-spec fetch_userinfo(#state{}) ->{ok, #state{}} | {error, term()}.
fetch_userinfo(#state{keyPrefix=KeyPrefix, apiUrl=ApiUrl, token=Token, cycleBuffer=CycleBuffer} = State) ->
    case exec_req(ApiUrl ++ "userinfo", Token) of
        UserInfo when is_map(UserInfo) ->
            KVP = {build_key(KeyPrefix, "userinfo"), UserInfo},
            %?Info("fetch_userinfo adds ~p to ~p",[KVP,CycleBuffer]),
            {ok, State#state{cycleBuffer=[KVP|CycleBuffer]}};
        {error, Error} ->
            ?JError("Error fetching userinfo : ~p", [Error]),
            {error, Error}
    end.

%% evaluate next day to fetch or 'fetched' if done
%% increment the _day value for the current metric but not beyond yesterday
%% if current metric does not exist in avatar, it may need to be initialized
%% in the past as configured in ?SHIFT_DAYS, else use last day previously fetched
%% day for this metric according to jobDyn state and increment by one day
%% re-fetch yesterday if not there
-spec get_day(metric(), #state{}) -> date() | fetched.
get_day(Metric, #state{name=Name, keyPrefix=KeyPrefix, channel=Channel} = State) ->
    LastDay = get_last_day(Metric, State),
    Key = build_key(KeyPrefix, Metric),
    Yesterday = edate:yesterday(),
    case dperl_dal:read_channel(Channel, Key) of
        ?NOT_FOUND ->
            case LastDay of
                undefined ->
                    SDays = ?SHIFT_DAYS(Name),
                    edate:shift(-1 * SDays, days);
                Yesterday ->
                    Yesterday;
                LastDay ->
                    edate:shift(LastDay, 1, days)
            end;
        #{<<"_day">> := DayBin} ->
            DayStr = binary_to_list(DayBin),
            case {edate:string_to_date(DayStr), Yesterday} of
                {D, D} -> fetched;
                {D1, D2} when D1 < D2 -> edate:shift(D1, 1, day);
                {_, Yesterday} -> Yesterday
            end
    end.

%% request a map response from web service (local value to be stored in avatar)
-spec exec_req(string(),token()) -> locVal() | {error, term()}.
exec_req(Url, Token) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(Token)}],
    case httpc:request(get, {Url, AuthHeader}, [], []) of
        {ok, {{_, 200, "OK"}, _, Result}} ->
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok, {{_, 401, _}, _, Error}} ->
            ?JError("Unauthorized body : ~s", [Error]),
            {error, unauthorized};
        Error ->
            {error, Error}
    end.

-spec next_day(dayDate()) -> date().
next_day(Day) when is_list(Day) ->          (edate:string_to_date(Day));
next_day(Day) when is_tuple(Day) ->         edate:shift(Day, 1, day).

-spec day_query(dayDate()) -> string().
day_query(Day) when is_tuple(Day) ->        day_query(edate:date_to_string(Day));
day_query(Day) when is_list(Day) ->         "?start=" ++ Day ++ "&end=" ++ Day.

-spec start_day_query(dayDate()) -> string().
start_day_query(Day) when is_tuple(Day) ->  start_day_query(edate:date_to_string(Day));
start_day_query(Day) when is_list(Day) ->   "?start=" ++ Day.

-spec get_last_day(metric(), #state{}) -> maybeDate().
get_last_day("sleep", #state{lastSleepDay=LastSleepDay}) -> LastSleepDay;
get_last_day("activity", #state{lastActivityDay=LastActivityDay}) -> LastActivityDay;
get_last_day("readiness", #state{lastReadinessDay=LastReadinessDay}) -> LastReadinessDay.

-spec set_metric_day(metric(), maybeDate(), #state{}) -> #state{}.
set_metric_day("sleep", Day, State) -> State#state{lastSleepDay=Day};
set_metric_day("activity", Day, State) -> State#state{lastActivityDay=Day};
set_metric_day("readiness", Day, State) -> State#state{lastReadinessDay=Day}.

-spec build_key(locKey(), metric()) -> locKey().
build_key(KeyPrefix, Metric) when is_list(Metric), is_list(KeyPrefix) -> KeyPrefix ++ [Metric].

