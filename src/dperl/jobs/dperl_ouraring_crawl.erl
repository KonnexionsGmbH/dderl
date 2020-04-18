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
                last_day, infos = []}).

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
    {DayQuery, State2} = get_day(State1),
    State3 = lists:foldl(
        fun(Fun, Acc) ->
            try Fun(DayQuery, Acc)
            catch E:C:S ->
                ?JError("E : ~p, C : ~p, S : ~p", [E, C, S]),
                {ok, Acc1} = connect_check_src(Acc#state{is_connected = false}),
                Fun(Acc1)
            end
        end, State2, [fun fetch_userinfo/2, fun fetch_activity/2,
                      fun fetch_sleep/2, fun fetch_readiness/2]),
    case State3#state.infos of
        [_] ->
            {ok, finish, State3#state{infos = []}};
        Infos ->
            ?Info("Infos : ~p", [Infos]),
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

get_status(#state{last_day = LastDay}) ->
    #{lastDay => LastDay}.

init_state([]) -> #state{};
init_state([#dperlNodeJobDyn{state = #{lastDay := LastDay}} | _]) ->
    #state{last_day = LastDay};
init_state([_ | Others]) ->
    init_state(Others).

init({#dperlJob{name=Name, dstArgs = #{channel := Channel},
                srcArgs = #{client_id := ClientId, user_password := Password,
                            client_secret := ClientSecret, user_email := Email,
                            cb_uri := CallbackUri, api_url := ApiUrl,
                            oauth_url := OauthUrl}}, State}) ->
    ?JInfo("Starting from : ~s", [State#state.last_day]),
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

fetch_userinfo(_, #state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    {ok,{{"HTTP/1.1",200,"OK"}, _, UserInfoJson}} = httpc:request(
      get, {ApiUrl ++ "/v1/userinfo", [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
      [{autoredirect, false}], [], ?MODULE
    ),
    UserInfo = imem_json:decode(list_to_binary(UserInfoJson), [return_maps]),
    Info = {["ouraring", "userinfo"], UserInfo},
    State#state{infos = [Info | State#state.infos]}.

get_day(State) ->
    Key = ["ouraring", "sleep"],
    Yesterday = edate:yesterday(),
    YDayStr = edate:date_to_string(Yesterday),
    Day =
    case dperl_dal:read_channel(State#state.channel, Key) of
        ?NOT_FOUND ->
            case State#state.last_day of
                undefined ->
                     SDays = ?SHIFT_DAYS(State#state.name),
                    edate:date_to_string(edate:shift(-1 * SDays, days));
                YDayStr ->
                    YDayStr;
                LastDay ->
                    edate:date_to_string(edate:shift(edate:string_to_date(LastDay), 1, days))
            end;
        #{<<"_day">> := DayBin} ->
            DayStr = binary_to_list(DayBin),
            case {edate:string_to_date(DayStr), Yesterday} of
                {D, D} -> DayStr;
                {D1, D2} when D1 < D2 -> edate:date_to_string(edate:shift(D1, 1, day));
                {_, Yesterday} -> edate:date_to_string(Yesterday)
            end
    end,
    DayQuery = "?start=" ++ Day ++ "&end=" ++ Day,
    {DayQuery, State#state{last_day = Day}}.

fetch_sleep(DayQuery, #state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    {ok,{{"HTTP/1.1",200,"OK"}, _, SleepInfoJson}} = httpc:request(
        get, {ApiUrl ++ "/v1/sleep" ++ DayQuery, [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
        [{autoredirect, false}], [], ?MODULE
    ),
    case imem_json:decode(list_to_binary(SleepInfoJson), [return_maps]) of
        #{<<"sleep">> := []} ->
            State;
        Sleep ->
            Info = {["ouraring", "sleep"], Sleep#{<<"_day">> => list_to_binary(State#state.last_day)}},
            State#state{infos = [Info | State#state.infos]}
    end.

fetch_activity(DayQuery, #state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    {ok,{{"HTTP/1.1",200,"OK"}, _, ActivityInfoJson}} = httpc:request(
        get, {ApiUrl ++ "/v1/activity" ++ DayQuery, [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
        [{autoredirect, false}], [], ?MODULE
    ),
    case imem_json:decode(list_to_binary(ActivityInfoJson), [return_maps]) of
        #{<<"activity">> := []} ->
            State;
        Activity ->
            Info = {["ouraring", "activity"], Activity#{<<"_day">> => list_to_binary(State#state.last_day)}},
            State#state{infos = [Info | State#state.infos]}
    end.

fetch_readiness(DayQuery, #state{api_url = ApiUrl, access_token = AccessToken} = State) ->
    {ok,{{"HTTP/1.1",200,"OK"}, _, ReadinessJson}} = httpc:request(
        get, {ApiUrl ++ "/v1/readiness" ++ DayQuery, [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
        [{autoredirect, false}], [], ?MODULE
    ),
    case imem_json:decode(list_to_binary(ReadinessJson), [return_maps]) of
        #{<<"readiness">> := []} ->
            State;
        Readiness ->
            Info = {["ouraring", "readiness"], Readiness#{<<"_day">> => list_to_binary(State#state.last_day)}},
            State#state{infos = [Info | State#state.infos]}
    end.

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
