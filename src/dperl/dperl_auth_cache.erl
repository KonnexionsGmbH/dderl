-module(dperl_auth_cache).

-behaviour(gen_server).

-include("dperl.hrl").

-export([start_link/0,
         set_enc_hash/3,
         get_enc_hash/1,
         set_enc_hash_locally/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).

-safe([set_enc_hash/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #{}}.

handle_call({setEncHash, #dperlJob{name = JobName}, User, EncHash}, _From, State) ->
    {reply, ok, State#{JobName => {User, EncHash}}};
handle_call({setEncHash, #dperlService{name = ServiceName}, User, EncHash}, _From, State) ->
    {reply, ok, State#{ServiceName => {User, EncHash}}};
handle_call({getEncHash, JobOrServiceName}, _From, State) ->
    case State of
        #{JobOrServiceName := {User, EncHash}} ->
            {reply, {User, EncHash}, State};
        _ ->
            {reply, undefined, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

set_enc_hash(JobOrServiceName, User, EncHash) ->
    DataNodes = [N || {_, N} <- imem_meta:data_nodes()],
    rpc:multicall(DataNodes, ?MODULE, set_enc_hash_locally, [JobOrServiceName, User, EncHash]).

set_enc_hash_locally(JobOrServiceName, User, EncHash) ->
    gen_server:call(?MODULE, {setEncHash, JobOrServiceName, User, EncHash}).

get_enc_hash(JobOrServiceName) ->
    gen_server:call(?MODULE, {getEncHash, JobOrServiceName}).
