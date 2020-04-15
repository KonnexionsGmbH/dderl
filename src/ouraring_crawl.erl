-module(ouraring_crawl).

-export([run/0, run/1]).

-define(OAUTH2_URL_PREFIX, "https://cloud.ouraring.com").
-define(API_URL_PREFIX, "https://api.ouraring.com").
run() ->
  {ok, _} = application:ensure_all_started(inets),
  {ok, _} = application:ensure_all_started(ssl),
  run(#{
    client_id => "REMERNOADFFDIN3O",
    client_secret => "HYJEW2WTIVIEXQNOTPDN7Y346GYSLNL3",
    cb_uri => "https://127.0.0.1:8443/callback",
    user_email => "max.ochsenbein@k2informatics.ch",
    user_password => "cFMMax--XG$k2sa",
    state => "any+value+as+state"
  }).

run(#{
  client_id := ClientId,
  client_secret := ClientSecret,
  cb_uri := CallbackUri,
  user_email := Email,
  user_password := Password,
  state := State
}) ->  
  inets:stop(httpc, ?MODULE),
  {ok, _} = inets:start(httpc, [{profile, ?MODULE}]),
  ok = httpc:set_options([{cookies, enabled}], ?MODULE),
  Url = ?OAUTH2_URL_PREFIX ++ "/oauth/authorize"
    ++ "?response_type=code"
    ++ "&client_id=" ++ ClientId
    ++ "&redirect_uri=" ++ edoc_lib:escape_uri(CallbackUri)
    ++ "&scope=email+personal+daily"
    ++ "&state=" ++ State,
  %io:format(">>>>>>>>>> authorize: ~s~n", [Url]),
  {ok, {{"HTTP/1.1",302,"Found"}, RespHeader302, []}} = httpc:request(get, {Url, []}, [{autoredirect, false}], [], ?MODULE),
  RedirectUri = ?OAUTH2_URL_PREFIX ++ proplists:get_value("location", RespHeader302),
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
  RedirectUri_1 = ?OAUTH2_URL_PREFIX ++ proplists:get_value("location", RespHeader302_1),
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
      ?OAUTH2_URL_PREFIX ++ "/oauth/token", [], "application/x-www-form-urlencoded",
      "grant_type=authorization_code"
      ++ "&code=" ++ Code
      ++ "&redirect_uri=" ++ edoc_lib:escape_uri(CallbackUri)
      ++ "&client_id=" ++ ClientId
      ++ "&client_secret=" ++ ClientSecret
    }, [{autoredirect, false}], [], ?MODULE
  ),
  #{<<"access_token">> := AccessToken} = Auth = jsx:decode(list_to_binary(BodyJson), [return_maps]),
  io:format("Auth ~p~n", [Auth]),
  io:format("-----~nUserInfo :~n~p~n-----~n", [userinfo(AccessToken)]),
  io:format("-----~nSleep :~n~p~n-----~n", [sleep(AccessToken)]),
  io:format("-----~nActivity :~n~p~n-- Activity --~n", [activity(AccessToken)]).

userinfo(AccessToken) ->
  {ok,{{"HTTP/1.1",200,"OK"}, _, UserInfoJson}} = httpc:request(
    get, {?API_URL_PREFIX ++ "/v1/userinfo", [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
    [{autoredirect, false}], [], ?MODULE
  ),
  jsx:decode(list_to_binary(UserInfoJson), [return_maps]).

sleep(AccessToken) ->
  {ok,{{"HTTP/1.1",200,"OK"}, _, SleepInfoJson}} = httpc:request(
    get, {?API_URL_PREFIX ++ "/v1/sleep", [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
    [{autoredirect, false}], [], ?MODULE
  ),
  jsx:decode(list_to_binary(SleepInfoJson), [return_maps]).

activity(AccessToken) ->
  {ok,{{"HTTP/1.1",200,"OK"}, _, ActivityInfoJson}} = httpc:request(
    get, {?API_URL_PREFIX ++ "/v1/activity", [{"Authorization", "Bearer " ++ binary_to_list(AccessToken)}]},
    [{autoredirect, false}], [], ?MODULE
  ),
  jsx:decode(list_to_binary(ActivityInfoJson), [return_maps]).
