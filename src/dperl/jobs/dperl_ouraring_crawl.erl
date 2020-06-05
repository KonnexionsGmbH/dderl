-module(dperl_ouraring_crawl).

-include_lib("../dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

-define(AUTH_CONFIG(__JOB_NAME),
        ?GET_CONFIG(oAuth2Config,[__JOB_NAME],
            #{auth_url =>"https://cloud.ouraring.com/oauth/authorize?response_type=code",
              client_id => "12345", redirect_uri => "https://localhost:8443/dderl/",
              client_secret => "12345", grant_type => "authorization_code",
              token_url => "https://cloud.ouraring.com/oauth/token",
              scope => "email personal daily"},
            "Oura Ring auth config")).

-define(KEY_PREFIX(__JOB_NAME),
          ?GET_CONFIG(keyPrefix, [__JOB_NAME], ["healthDevice","OuraRing"],
          "Default KeyPrefix for Oura Ring data")
       ).

-define(SHIFT_DAYS(__JOB_NAME),
          ?GET_CONFIG(daysToBeShiftedAtStart, [__JOB_NAME], 100,
          "Days to be shifted backwards for starting the job")
       ).

-record(state,  { name
                , channel
                , is_connected = true
                , access_token
                , api_url
                , last_sleep_day
                , last_activity_day
                , last_readiness_day
                , infos = []
                , key_prefix
                , accountId
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

get_auth_config() -> ?AUTH_CONFIG(<<>>).

get_auth_config(JobName) -> ?AUTH_CONFIG(JobName).

get_key_prefix() -> ?KEY_PREFIX(<<>>).

get_key_prefix(JobName) -> ?KEY_PREFIX(JobName).

connect_check_src(#state{is_connected = true} = State) ->
    {ok, State};
connect_check_src(#state{is_connected=false, accountId=AccountId, key_prefix=KeyPrefix} = State) ->
    ?JTrace("Refreshing access token"),
    case dderl_oauth:refresh_access_token(AccountId, KeyPrefix, ?SYNC_OURARING) of
        {ok, AccessToken} ->
            ?Info("new access token fetched"),
            {ok, State#state{access_token=AccessToken, is_connected=true}};
        {error, Error} ->
            ?JError("Unexpected response : ~p", [Error]),
            {error, Error, State}
    end.

get_source_events(#state{infos = []} = State, _BulkSize) ->
    {ok, sync_complete, State};
get_source_events(#state{infos = Infos} = State, _BulkSize) ->
    {ok, Infos, State#state{infos = []}}.

connect_check_dst(State) -> {ok, State}.

do_refresh(_State, _BulkSize) -> {error, cleanup_only}.

fetch_src({_Key, Value}, _State) -> Value.

fetch_dst({Key, _}, State) ->
    dperl_dal:read_channel(State#state.channel, Key).

insert_dst(Key, Val, State) ->
    update_dst(Key, Val, State).

report_status(_Key, _Status, _State) -> no_op.

do_cleanup(State, _BlkCount) ->
    Types = ["sleep", "activity", "readiness", "userinfo"],
    case fetch_metrics(Types, State) of
        {ok, State2} ->
            case State2#state.infos of
                [_] ->
                    {ok, finish, State2};
                _ ->
                    {ok, State2}
            end;
        {error, Error} ->
            {error, Error, State#state{is_connected = false}}
    end.

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

get_status(#state{last_sleep_day = LastSleepDay,
                  last_activity_day = LastActivityDay,
                  last_readiness_day = LastReadinessDay}) ->
    #{lastSleepDay => LastSleepDay, lastActivityDay => LastActivityDay,
      lastReadinessDay => LastReadinessDay}.

init_state([]) -> #state{};
init_state([#dperlNodeJobDyn{state = State} | _]) ->
    LastSleepDay = maps:get(lastSleepDay, State, undefined),
    LastActivityDay = maps:get(lastActivityDay, State, undefined),
    LastReadinessDay = maps:get(lastReadinessDay, State, undefined),
    #state{last_sleep_day = LastSleepDay, last_activity_day = LastActivityDay,
           last_readiness_day = LastReadinessDay};
init_state([_ | Others]) ->
    init_state(Others).

init({#dperlJob{name=Name, dstArgs = #{channel := Channel} = DstArgs,
                srcArgs = #{api_url := ApiUrl}}, State}) ->
    case dperl_auth_cache:get_enc_hash(Name) of
        undefined ->
            ?JError("Encryption hash is not avaialable"),
            {stop, badarg};
        {AccountId, EncHash} ->
            ?JInfo("Starting with ~p's enchash...", [AccountId]),
            imem_enc_mnesia:put_enc_hash(EncHash),
            KeyPrefix = maps:get(key_prefix, DstArgs, get_key_prefix(Name)),
            case dderl_oauth:get_token_info(AccountId, KeyPrefix, ?SYNC_OURARING) of
                #{<<"access_token">> := AccessToken} ->
                    ChannelBin = dperl_dal:to_binary(Channel),
                    dperl_dal:create_check_channel(ChannelBin),
                    {ok, State#state{channel=ChannelBin, api_url=ApiUrl, accountId=AccountId,
                                    key_prefix=KeyPrefix, access_token=AccessToken}};
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

fetch_metrics([], State) -> {ok, State};
fetch_metrics(["userinfo" | Types], State) ->
    case fetch_userinfo(State) of
        {error, Error} ->
            {error, Error};
        State1 ->
            fetch_metrics(Types, State1)
    end;
fetch_metrics([Type | Types], State) ->
    case get_day(Type, State) of
        fetched ->
            fetch_metrics(Types, State);
        Day ->
            case fetch_metric(Type, Day, State) of
                {error, Error} ->
                    {error, Error};
                none ->
                    fetch_metrics(Types, State);
                {ok, MDay, Metric} ->
                    State1 = set_metric_day(Type, MDay, State#state{infos = [Metric | State#state.infos]}),
                    fetch_metrics(Types, State1)
            end
    end.

fetch_metric(Type, Day, #state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    ?JInfo("Fetching metric for ~s on ~p", [Type, Day]),
    NextDay = next_day(Day),
    case fetch_metric(Type, day_query(Day), ApiUrl, AccessToken) of
        none ->
            case fetch_metric(Type, start_day_query(NextDay), ApiUrl, AccessToken) of
                {ok, _} ->
                    fetch_metric(Type, NextDay, State);
                _Other ->
                    none
            end;
        {ok, Metric} ->
            Key = build_key(Type, State#state.key_prefix),
            Info = {Key, Metric#{<<"_day">> => list_to_binary(edate:date_to_string(Day))}},
            case Type of
                Type when Type == "sleep"; Type == "readiness" ->
                    {ok, Day, Info};
                "activity" ->
                    % fetching activity only if next days data exists
                    case fetch_metric(Type, start_day_query(NextDay), ApiUrl, AccessToken) of
                        {ok, _} ->
                            {ok, Day, Info};
                        Other ->
                            Other
                    end
            end;
        {error, Error} ->
            ?JError("Error fetching ~s for ~p : ~p", [Type, Day, Error]),
            {error, Error}
    end.

fetch_metric(Type, DayQuery, ApiUrl, AccessToken) ->
    Url = ApiUrl ++ Type ++ DayQuery,
    TypeBin = list_to_binary(Type),
    case exec_req(Url, AccessToken) of
        #{TypeBin := []} ->
            none;
        Metric when is_map(Metric) ->
            {ok, Metric};
        {error, Error} ->
            {error, Error}
    end.

fetch_userinfo(#state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    case exec_req(ApiUrl ++ "userinfo", AccessToken) of
        UserInfo when is_map(UserInfo) ->
            Info = {build_key("userinfo", State#state.key_prefix), UserInfo},
            State#state{infos = [Info | State#state.infos]};
        {error, Error} ->
            ?JError("Error fetching userinfo : ~p", [Error]),
            {error, Error}
    end.

get_day(Type, State) ->
    LastDay = get_last_day(Type, State),
    Key = build_key(Type, State#state.key_prefix),
    Yesterday = edate:yesterday(),
    case dperl_dal:read_channel(State#state.channel, Key) of
        ?NOT_FOUND ->
            case LastDay of
                undefined ->
                    SDays = ?SHIFT_DAYS(State#state.name),
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

exec_req(Url, AccessToken) ->
    AuthHeader = [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}],
    case httpc:request(get, {Url, AuthHeader}, [], []) of
        {ok, {{_, 200, "OK"}, _, Result}} ->
            imem_json:decode(list_to_binary(Result), [return_maps]);
        {ok, {{_, 401, _}, _, Error}} ->
            ?JError("Unauthorized body : ~s", [Error]),
            {error, unauthorized};
        Error ->
            {error, Error}
    end.

next_day(Day) when is_list(Day) ->
    next_day(edate:string_to_date(Day));
next_day(Day) when is_tuple(Day) ->
    edate:shift(Day, 1, day).

day_query(Day) when is_tuple(Day) ->
    day_query(edate:date_to_string(Day));
day_query(Day) when is_list(Day) ->
    "?start=" ++ Day ++ "&end=" ++ Day.

start_day_query(Day) when is_tuple(Day) ->
    start_day_query(edate:date_to_string(Day));
start_day_query(Day) when is_list(Day) ->
    "?start=" ++ Day.

get_last_day("sleep", #state{last_sleep_day = LastSleepDay}) -> LastSleepDay;
get_last_day("activity", #state{last_activity_day = LastActivityDay}) -> LastActivityDay;
get_last_day("readiness", #state{last_readiness_day = LastReadinessDay}) -> LastReadinessDay.

set_metric_day("sleep", Day, State) -> State#state{last_sleep_day = Day};
set_metric_day("activity", Day, State) -> State#state{last_activity_day = Day};
set_metric_day("readiness", Day, State) -> State#state{last_readiness_day = Day}.

build_key(Type, KeyPrefix) when is_list(Type), is_list(KeyPrefix)->
    KeyPrefix ++ [Type].

