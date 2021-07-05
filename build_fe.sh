#!/bin/bash
. $(dirname $0)/common.sh

log green "-------------------------------------------------------------------------"
cd priv/dev
log green "building front end (priv/dev) : yarn"
yarn
log green "building front end (priv/dev) : yarn build"
yarn build
cd ../..
log green "-------------------------------------------------------------------------"
