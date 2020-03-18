#!/bin/bash

host=192.168.1.159
port=1236
dderlport=8443
pi_nodes="'pi@192.168.1.168'"
pi_nodes=$pi_nodes",'pi1@192.168.1.168'"

node=controller@$host
imemtyp=disc
ck=secure_pi
unamestr=`uname`
if [[ "$unamestr" == 'Linux' || "$unamestr" == 'Darwin' ]]; then
     exename=erl
else
    exename='start //MAX werl.exe'
    #exename='erl.exe'
fi

# Node name
node_name="-name $node"

# Cookie
cookie="-setcookie $ck"

# PATHS
paths="-pa"
paths=$paths" _build/default/lib/*/ebin"
paths=$paths" _checkouts/*/ebin"

# Proto dist module
dist_opts="-proto_dist"
dist_opts=$dist_opts" imem_inet_tcp"

# Kernel Opts
kernel_opts="-kernel"
kernel_opts=$kernel_opts" inet_dist_listen_min 7000"
kernel_opts=$kernel_opts" inet_dist_listen_max 8000"

# Imem Opts
imem_opts="-imem"
imem_opts=$imem_opts" mnesia_node_type $imemtyp"
imem_opts=$imem_opts" erl_cluster_mgrs [$pi_nodes]"
imem_opts=$imem_opts" mnesia_schema_name dderl"
imem_opts=$imem_opts" tcp_port $port"
imem_opts=$imem_opts" cold_start_recover false"

# dderl opts
dderl_opts="-dderl"
dderl_opts=$dderl_opts" port $dderlport"

lager_config="dderl.config"

# sasl opts
sasl_opts="-sasl"
sasl_opts=$sasl_opts"  sasl_error_logger false"

start_opts="$paths $cookie $node_name $dist_opts $kernel_opts $imem_opts $dderl_opts $sasl_opts -config $lager_config"

# DDERL start options
echo "------------------------------------------"
echo "Starting DDERL (Opts)"
echo "------------------------------------------"
echo "Node Name : $node_name"
echo "Cookie    : $cookie"
echo "EBIN Path : $paths"
echo "Dist      : $dist_opts"
echo "Kernel    : $kernel_opts"
echo "IMEM      : $imem_opts"
echo "DDERL     : $dderl_opts"
echo "SASL      : $sasl_opts"
echo "------------------------------------------"

# Starting dderl
$exename $start_opts -s dderl
