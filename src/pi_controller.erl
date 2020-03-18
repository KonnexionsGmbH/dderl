-module(pi_controller).

-export([get_pi_net/0, sys_info/0, temp/0, temp/1, build_pi_net/1]).
-export([get_pi_net/1, sys_info/1, build_pi_net/2]). % API mocks for simulation

build_pi_net([]) -> [];
build_pi_net([Host|Hosts]) ->
    Node = binary_to_atom(list_to_binary(io_lib:format("pi@~s", [Host])), utf8),
    [{Node, net_adm:ping(Node)} | build_pi_net(Hosts)].

get_pi_net() ->
    [N || N <- nodes(), re:run(atom_to_binary(N, utf8), "pi@.*") /= nomatch].

temp() -> temp(get_pi_net()).
sys_info() -> sys_info(get_pi_net(), []).

% tail recursion
sys_info([], Acc) -> Acc;
sys_info([Node|Nodes], Acc) ->
    CPUTopo = rpc:call(Node, erlang, system_info, [cpu_topology]),
    sys_info(Nodes, [{Node, CPUTopo} | Acc]).

% inline recursion
temp(sim) -> temp_sim(); % simulation only
temp([]) -> [];
temp([Node|Nodes]) ->
    [{Node, temp(Node)} | temp(Nodes)];
% implicit match by type (no guards necessary)
temp(Node) ->
    TempCmd = "sudo cat /sys/class/thermal/thermal_zone0/temp",
    TempStr = rpc:call(Node, os, cmd, [TempCmd]),
    list_to_integer(string:trim(TempStr)).

%-------------------------------------------------------------------------------
% Simulates pi nodes
%-------------------------------------------------------------------------------
get_pi_net(sim) -> get_pi_net_sim().
sys_info(sim) -> sys_info_sim().
build_pi_net(sim, Hosts) -> build_pi_net_sim(Hosts).


build_pi_net_sim([]) -> [];
build_pi_net_sim([Host|Hosts]) ->
    Node = binary_to_atom(list_to_binary(io_lib:format("pi@~s", [Host])), utf8),
    [{Node, pong} | build_pi_net_sim(Hosts)].

% simulates 10 nodes
get_pi_net_sim() ->
    [binary_to_atom(list_to_binary(io_lib:format("pi@192.168.1.~p", [H])), utf8)
     || H <- lists:seq(2,11)].

temp_sim() -> temp_sim(get_pi_net_sim()).
sys_info_sim() -> sys_info_sim(get_pi_net_sim(), []).

sys_info_sim([], Acc) -> Acc;
sys_info_sim([Node|Nodes], Acc) ->
    CPUTopo = [
        {processor, [
            {core, {logical,0}},
            {core, {logical,1}},
            {core, {logical,2}},
            {core, {logical,3}}
        ]}
    ],
    sys_info_sim(Nodes, [{Node, CPUTopo} | Acc]).

temp_sim([]) -> [];
temp_sim([Node|Nodes]) ->
    [{Node, temp_sim(Node)} | temp_sim(Nodes)];
temp_sim(_Node) -> 40000 + 5000 - rand:uniform(10000).
