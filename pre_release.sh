. $(dirname $0)/common.sh
app=${1:-dderl}
rel=${2}

log green "-------------------------------------------------------------------------"
log green "post_release $rel/$app @ $(pwd)"

if [[ "$OSTYPE" == *darwin* ]]
then
    READLINK_CMD='greadlink'
else
    READLINK_CMD='readlink'
fi

if [ -z "$rel" ]
then
    dderlPriv=$($READLINK_CMD -f priv/)
else
    dderlPriv=$($READLINK_CMD -f _build/$rel/rel/$app/lib/dderl-*/priv/)
fi

if [ -z "$dderlPriv" ]
then
    log red "dderlPriv dir not found"
    exit 1
fi

log lightgrey "building dderl @ $dderlPriv"

cd $dderlPriv/dev
log green "yarn install-build-prod @ $(pwd)"
yarn install-build-prod

# cleanup
cd $dderlPriv
rm -rf $dderlPriv/dev
log green "dir $dderlPriv/dev deleted"

log green "------------------------------------------------------------ post_release"
