-ifndef(_dperl_ora_).
-define(_dperl_ora_, true).

-include("dperl.hrl").

-define(MAX_COMMENT_LENGTH, 200).
-define(MAX_REQUESTOR_LENGTH, 20).

-define(RESP_BUFFER, list_to_binary(lists:duplicate(1024, 0))).

-define(SERVICE_MAXACCEPTORS(__SERVICE_NAME),
          ?GET_CONFIG(maxNumberOfAcceptors, [__SERVICE_NAME], 100,
                      "Maximum number of TCP acceptors")
       ).

-define(SERVICE_MAXCONNS(__SERVICE_NAME), 
          ?GET_CONFIG(maxNumberOfSockets, [__SERVICE_NAME], 5000, 
                      "Maximum number of simulteneous connections")
       ).
  
-define(SERVICE_PROBE_URL(__SERVICE_NAME),
          ?GET_CONFIG(probeUrl, [__SERVICE_NAME], "/probe.html",
                      "Defines the url of the probe for the load balancer")
       ).

-define(SERVICE_PROBE_RESP(__SERVICE_NAME),
          ?GET_CONFIG(probeResp, [__SERVICE_NAME],
                      {200,
                       <<"<html>"
                         "<body>Service is alive</body>"
                         "</html>">>},
                      "Response given to the load balancer when the probeUrl"
                      " is requested")
       ).

-define(SERVICE_UPDATE_PERIOD(__SERVICE_NAME),
          ?GET_CONFIG(serviceDynUpdatePeriod, [__SERVICE_NAME], 2000,
                      "Delay in millisecond between a service updates its DYN"
                      " info")
       ).

-define(SERVICE_STATUS_RESET_PERIOD(__SERVICE_NAME),
          ?GET_CONFIG(serviceDynResetPeriod, [__SERVICE_NAME], 3600 * 1000,
                      "Delay in millisecond between a service resets its DYN"
                      " info")
       ).

-define(SERVICE_ACTIVE_THRESHOLD(__SERVICE_NAME),
          ?GET_CONFIG(serviceStatusActiveThreshold, [__SERVICE_NAME], 1,
                      "Request count threshhold beyond which service is marked"
                      " as active in DYN")
       ).

-define(SERVICE_OVERLOAD_THRESHOLD(__SERVICE_NAME),
          ?GET_CONFIG(serviceStatusOverloadThreshold, [__SERVICE_NAME], 100,
                      "Request count threshhold beyond which service is marked"
                      " as overloaded in DYN")
       ).

-define(SWAGGER_WHITELIST(__SERVICE_NAME),
          ?GET_CONFIG(swaggerWhitelist, [__SERVICE_NAME], #{},
                      "Whitelist for SwaggerClient Access")).

%% sqls

%% common sqls and binds
-define(PROFILE_DELETE_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_profile_msisdn_delete(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    Requestor=>:SQLT_CHR_REQUESTOR,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).

%% topstopper sqls and binds
-define(TOPSTOPPER_GET_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_ts_msisdn_get(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN);
END;
">>).

-define(TOPSTOPPER_POST_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_ts_msisdn_post(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    Requestor=>:SQLT_CHR_REQUESTOR,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).

-define(PROFILE_DELETE_TOPSTOPPER_POST_BINDS,
        [{<<":SQLT_CHR_OUT_RESULT">>, out,    'SQLT_CHR'},
         {<<":SQLT_CHR_APP">>,        in,     'SQLT_CHR'},
         {<<":SQLT_CHR_MSISDN">>,     in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REQUESTOR">>,  in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REMARK">>,     in,     'SQLT_CHR'}]).

-define(TOPSTOPPER_PUT_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_ts_msisdn_put(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    BarringType=>:SQLT_CHR_BARRINGTYPE,
    TopStopLimit=>:SQLT_VNU_TOPSTOPLIMIT,
    Requestor=>:SQLT_CHR_REQUESTOR,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).

-define(TOPSTOPPER_PUT_BINDS,
        [{<<":SQLT_CHR_OUT_RESULT">>,   out,    'SQLT_CHR'},
         {<<":SQLT_CHR_APP">>,          in,     'SQLT_CHR'},
         {<<":SQLT_CHR_MSISDN">>,       in,     'SQLT_CHR'},
         {<<":SQLT_CHR_BARRINGTYPE">>,  in,     'SQLT_CHR'},
         {<<":SQLT_VNU_TOPSTOPLIMIT">>, in,     'SQLT_VNU'},
         {<<":SQLT_CHR_REQUESTOR">>,    in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REMARK">>,       in,     'SQLT_CHR'}]).

-define(TOPSTOPPER_RESET_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_ts_msisdn_reset(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    BarringType=>:SQLT_CHR_BARRINGTYPE,
    Requestor=>:SQLT_CHR_REQUESTOR,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).


-define(TOPSTOPPER_REVERT_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_ts_msisdn_revert(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    BarringType=>:SQLT_CHR_BARRINGTYPE,
    Requestor=>:SQLT_CHR_REQUESTOR,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).

-define(TOPSTOPPER_RESET_REVERT_BINDS,
        [{<<":SQLT_CHR_OUT_RESULT">>,  out,    'SQLT_CHR'},
         {<<":SQLT_CHR_APP">>,         in,     'SQLT_CHR'},
         {<<":SQLT_CHR_MSISDN">>,      in,     'SQLT_CHR'},
         {<<":SQLT_CHR_BARRINGTYPE">>, in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REQUESTOR">>,   in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REMARK">>,      in,     'SQLT_CHR'}]).

%% barring sqls and binds
-define(BARRING_GET_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_barring_msisdn_get(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN
  );
END;
">>).

-define(TOPSTOPPER_BARRING_GET_BINDS,
        [{<<":SQLT_CHR_OUT_RESULT">>, out,    'SQLT_CHR'},
         {<<":SQLT_CHR_APP">>,        in,     'SQLT_CHR'},
         {<<":SQLT_CHR_MSISDN">>,     in,     'SQLT_CHR'}]).

-define(BARRING_POST_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_barring_msisdn_post(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    Requestor=>:SQLT_CHR_REQUESTOR,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).

-define(BARRING_POST_BINDS,
        [{<<":SQLT_CHR_OUT_RESULT">>, out,    'SQLT_CHR'},
         {<<":SQLT_CHR_APP">>,        in,     'SQLT_CHR'},
         {<<":SQLT_CHR_MSISDN">>,     in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REMARK">>,     in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REQUESTOR">>,  in,     'SQLT_CHR'}]).

-define(BARRING_PUT_SQL, <<"
BEGIN
  :SQLT_CHR_OUT_RESULT := pkg_ora_dperl.aaa_barring_msisdn_put(
    ProvisioningTenant=>:SQLT_CHR_APP,
    Msisdn=>:SQLT_CHR_MSISDN,
    BarringType=>:SQLT_CHR_BARRINGTYPE,
    Requestor=>:SQLT_CHR_REQUESTOR,
    BarringLevel=>:SQLT_INT_LEVEL,
    Remark=>:SQLT_CHR_REMARK
  );
END;
">>).

-define(BARRING_PUT_BINDS,
        [{<<":SQLT_CHR_OUT_RESULT">>,  out,    'SQLT_CHR'},
         {<<":SQLT_CHR_APP">>,         in,     'SQLT_CHR'},
         {<<":SQLT_CHR_MSISDN">>,      in,     'SQLT_CHR'},
         {<<":SQLT_CHR_BARRINGTYPE">>, in,     'SQLT_CHR'},
         {<<":SQLT_INT_LEVEL">>,       in,     'SQLT_INT'},
         {<<":SQLT_CHR_REMARK">>,      in,     'SQLT_CHR'},
         {<<":SQLT_CHR_REQUESTOR">>,   in,     'SQLT_CHR'}]).

-endif. %_dperl_ora_
