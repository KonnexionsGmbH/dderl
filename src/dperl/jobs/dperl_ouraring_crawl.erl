-module(dperl_ouraring_crawl).

-include_lib("dperl/dperl.hrl").

-behavior(dperl_worker).
-behavior(dperl_strategy_scr).

-define(SHIFT_DAYS(__JOB_NAME),
          ?GET_CONFIG(daysToBeShiftedAtStart, [__JOB_NAME], 100,
          "Days to be shifted backwards for starting the job")
       ).

% dperl_worker exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         get_status/1, init_state/1]).

-record(state, {name, channel, client_id, client_secret, password, email,
                cb_uri, is_connected = false, access_token, api_url, oauth_url,
                last_sleep_day, last_activity_day, last_readiness_day, infos = []}).

% dperl_strategy_scr export
-export([connect_check_src/1, get_source_events/2, connect_check_dst/1,
         do_cleanup/2, do_refresh/2, 
         fetch_src/2, fetch_dst/2, delete_dst/2, insert_dst/3,
         update_dst/3, report_status/3]).

connect_check_src(#state{is_connected = false, client_id = ClientId, cb_uri = CallbackUri,
                         client_secret = ClientSecret, password = Password,
                         email = Email, oauth_url = OauthUrl} = State) ->
    inets:start(httpc, [{profile, ?MODULE}]),
    ok = httpc:set_options([{cookies, enabled}], ?MODULE),
    Url = OauthUrl ++ "/oauth/authorize"
    ++ "?response_type=code"
    ++ "&client_id=" ++ ClientId
    ++ "&redirect_uri=" ++ edoc_lib:escape_uri(CallbackUri)
    ++ "&scope=email+personal+daily"
    ++ "&state=" ++ "test",
    %io:format(">>>>>>>>>> authorize: ~s~n", [Url]),
    case httpc:request(get, {Url, []}, [{autoredirect, false}], [], ?MODULE) of
        {ok, {{"HTTP/1.1",302,"Found"}, RespHeader302, []}} ->
            RedirectUri = OauthUrl ++ proplists:get_value("location", RespHeader302),
            % io:format(">>>>>>>>>> 302 Redirect: ~s~n", [RedirectUri]),
            {ok, {{"HTTP/1.1",200,"OK"}, RespHeader, _Body}} = httpc:request(get, {RedirectUri, []}, [{autoredirect, false}], [], ?MODULE),
            SetCookieHeader = proplists:get_value("set-cookie", RespHeader),
            {match, [XRefCookie]} = re:run(SetCookieHeader, ".*_xsrf=(.*);.*", [{capture, [1], list}]),
            {ok,{{"HTTP/1.1",302,"Found"}, RespHeader302_1, []}} = httpc:request(
            post, {
                RedirectUri, [], "application/x-www-form-urlencoded",
                "_xsrf="++edoc_lib:escape_uri(XRefCookie)
                ++ "&email=" ++ edoc_lib:escape_uri(Email)
                ++ "&password=" ++ edoc_lib:escape_uri(Password)
            }, [{autoredirect, false}], [], ?MODULE
            ),
            RedirectUri_1 = OauthUrl ++ proplists:get_value("location", RespHeader302_1),
            % io:format(">>>>>>>>>> 302 Redirect: ~s~n", [RedirectUri_1]),
            {ok, {{"HTTP/1.1",200,"OK"}, _, _}} = httpc:request(get, {RedirectUri_1, []}, [{autoredirect, false}], [], ?MODULE),
            {ok, {{"HTTP/1.1",302,"Found"}, RespHeader302_2, []}} = httpc:request(
            post, {
                RedirectUri_1, [], "application/x-www-form-urlencoded",
                "_xsrf="++edoc_lib:escape_uri(XRefCookie)
                ++ "&scope_email=on"
                ++ "&scope_personal=on"
                ++ "&scope_daily=on"
                ++ "&allow=Accept"
            }, [{autoredirect, false}], [], ?MODULE
            ),
            RedirectUri_2 = proplists:get_value("location", RespHeader302_2),
            % io:format(">>>>>>>>>> 302 RedirectUri: ~s~n", [RedirectUri_2]),
            #{query := QueryString} = uri_string:parse(RedirectUri_2),
            #{"code" := Code} = maps:from_list(uri_string:dissect_query(QueryString)),
            % io:format(">>>>>>>>>> Code: ~p~n", [Code]),
            {ok, {{"HTTP/1.1",200,"OK"}, _, BodyJson}} = httpc:request(
            post, {
                OauthUrl ++ "/oauth/token", [], "application/x-www-form-urlencoded",
                "grant_type=authorization_code"
                ++ "&code=" ++ Code
                ++ "&redirect_uri=" ++ edoc_lib:escape_uri(CallbackUri)
                ++ "&client_id=" ++ ClientId
                ++ "&client_secret=" ++ ClientSecret
            }, [{autoredirect, false}], [], ?MODULE
            ),
            #{<<"access_token">> := AccessToken} = Auth = jsx:decode(list_to_binary(BodyJson), [return_maps]),
            ?JInfo("Auth is : ~p", [Auth]),
            {ok, State#state{is_connected = true, access_token = AccessToken}};
        {ok, {{_, 200, _}, _, Body}} ->
            ?JInfo("code : ~p body :  ~p", [200, Body]),
            ?JInfo("!!!! cookies :~p", [httpc:which_cookies(?MODULE)]),
            {error, Body, State}
    end;
connect_check_src(State) -> {ok, State}.

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
    {ok, State1} = connect_check_src(State),
    State2 = lists:foldl(
        fun(Type, Acc) ->
            case get_day(Type, Acc) of
                fetched -> Acc;
                Day ->
                    try fetch_metric(Type, Day, Acc)
                    catch E:C:S ->
                        ?JError("E : ~p, C : ~p, S : ~p", [E, C, S]),
                        {ok, Acc1} = connect_check_src(Acc#state{is_connected = false}),
                        fetch_metric(Type, Day, Acc1)
                    end
            end
        end, State1, ["sleep", "activity", "readiness"]),
    State3 = fetch_userinfo(State2),
    case State2#state.infos of
        [] ->
            {ok, finish, State3};
        _ ->
            {ok, State3}
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

init({#dperlJob{name=Name, dstArgs = #{channel := Channel},
                srcArgs = #{client_id := ClientId, user_password := Password,
                            client_secret := ClientSecret, user_email := Email,
                            cb_uri := CallbackUri, api_url := ApiUrl,
                            oauth_url := OauthUrl}}, State}) ->
    ?JInfo("Starting ..."),
    ChannelBin = dperl_dal:to_binary(Channel),
    dperl_dal:create_check_channel(ChannelBin),
    {ok, State#state{channel = ChannelBin, client_id = ClientId,
                     client_secret = ClientSecret, password = Password,
                     email = Email, cb_uri = CallbackUri, name = Name,
                     api_url = ApiUrl, oauth_url = OauthUrl}};
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

fetch_userinfo(#state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    {ok,{{"HTTP/1.1",200,"OK"}, _, UserInfoJson}} = httpc:request(
      get, {ApiUrl ++ "/v1/userinfo", [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
      [{autoredirect, false}], [], ?MODULE
    ),
    UserInfo = imem_json:decode(list_to_binary(UserInfoJson), [return_maps]),
    Info = {["ouraring", "userinfo"], UserInfo},
    State#state{infos = [Info | State#state.infos]}.

get_day(Type, State) ->
    LastDay = get_last_day(Type, State),
    Key = ["ouraring", Type],
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

fetch_metric(Type, Day, #state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    ?JInfo("Fetching metric for ~s on ~p", [Type, Day]),
    {ok,{{"HTTP/1.1",200,"OK"}, _, MetricJson}} = httpc:request(
        get, {ApiUrl ++ "/v1/" ++ Type ++ day_query(Day), [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
        [{autoredirect, false}], [], ?MODULE),
    TypeBin = list_to_binary(Type),
    case imem_json:decode(list_to_binary(MetricJson), [return_maps]) of
        #{TypeBin := []} ->
            NextDay = next_day(Day),
            case NextDay =< edate:yesterday() of
                true ->
                    fetch_metric(Type, NextDay, State);
                false ->
                    State
            end;
        Metric ->
            Info = {["ouraring", Type], Metric#{<<"_day">> => list_to_binary(edate:date_to_string(Day))}},
            set_metric_day(Type, Day, State#state{infos = [Info | State#state.infos]})
    end.

next_day(Day) when is_list(Day) ->
    next_day(edate:string_to_date(Day));
next_day(Day) when is_tuple(Day) ->
    edate:shift(Day, 1, day).

day_query(Day) when is_tuple(Day) ->
    day_query(edate:date_to_string(Day));
day_query(Day) when is_list(Day) ->
    "?start=" ++ Day ++ "&end=" ++ Day.

get_last_day("sleep", #state{last_sleep_day = LastSleepDay}) -> LastSleepDay;
get_last_day("activity", #state{last_activity_day = LastActivityDay}) -> LastActivityDay;
get_last_day("readiness", #state{last_readiness_day = LastReadinessDay}) -> LastReadinessDay.

set_metric_day("sleep", Day, State) -> State#state{last_sleep_day = Day};
set_metric_day("activity", Day, State) -> State#state{last_activity_day = Day};
set_metric_day("readiness", Day, State) -> State#state{last_readiness_day = Day}.

% format_links(Links) ->
%     lists:map(
%         fun(#{url := Url} = Link) ->
%             NewUrl = 
%             case lists:last(Url) of
%                 $/ -> Url;
%                 _ -> Url ++ "/"
%             end,
%             Link#{url := NewUrl};
%           (Link) -> Link
%         end, Links).
