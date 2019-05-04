#!/bin/bash -ex
set -x
set -e
CODE_FOLDER=$1
if [[ -z "$CODE_FOLDER" ]]; then
    echo "Please provide the code folder"
    exit 1
fi
if [[ ! -d "$CODE_FOLDER/lib" ]]; then
    echo "Please provide the code folder"
    exit 1
fi
shift
unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     machine=Linux;;
    Darwin*)    machine=Mac;;
    CYGWIN*)    machine=Cygwin;;
    MINGW*)     machine=MinGw;;
    *)          machine="UNKNOWN:${unameOut}"
esac
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [[ "$machine" == "Mac" ]]; then
    SED_COMMAND="sed -i .bak"
else
    SED_COMMAND="sed -i"
fi
ORIGINAL_ETC=$SCRIPT_DIR/etc
RUNNING_ETC=$SCRIPT_DIR/new_etc
rm -rf $RUNNING_ETC || true
cp -R "$ORIGINAL_ETC" "$RUNNING_ETC"
$SED_COMMAND "s/%HOSTNAME%/$HOSTNAME/g" $RUNNING_ETC/node.properties
$SED_COMMAND "s/%HOSTNAME%/$HOSTNAME/g" $RUNNING_ETC/config.properties
CLUSTER_IDENTIFIER=''
HTTP_PORT_TO_USE=8080
if [[ -n "$UBER_PORT_HTTP" ]]; then
    HTTP_PORT_TO_USE=$UBER_PORT_HTTP
    $SED_COMMAND "s/%HTTP_REQUEST_LOG_ENABLED%/false/g" $RUNNING_ETC/config.properties
else
    $SED_COMMAND "s/%HTTP_REQUEST_LOG_ENABLED%/true/g" $RUNNING_ETC/config.properties
fi
$SED_COMMAND "s/%HTTPPORT%/$HTTP_PORT_TO_USE/g" $RUNNING_ETC/config.properties
$SED_COMMAND "s/%USERNAME%/$USER/g" $RUNNING_ETC/node.properties
ENVIRONMENT_FILE=/etc/uber/environment
DATACENTER_FILE=/etc/uber/datacenter
if [[ -f "$ENVIRONMENT_FILE" ]]; then
    if [[ -f "$DATACENTER_FILE" ]]; then
        CLUSTER_IDENTIFIER="$(cat $DATACENTER_FILE)_$(cat $ENVIRONMENT_FILE)"
    fi
fi
$SED_COMMAND "s/%EVENT_LISTENER_CLUSTER%/$CLUSTER_IDENTIFIER/g" $RUNNING_ETC/event-listener.properties
NODE_ID="$HOSTNAME"
if [[ -n "$UDEPLOY_EXECUTION_ID" ]]; then
    NODE_ID="$UDEPLOY_EXECUTION_ID"
fi
NODE_ID=$(echo $NODE_ID|sed 's/-/_/g'|tr '[:upper:]' '[:lower:]')
$SED_COMMAND "s/%UNIQUE_NODE_ID%/$NODE_ID/g" $RUNNING_ETC/node.properties

NEUTRINO_CLUSTER_NAME=''
if [[ -n "$UBER_DATACENTER" ]]; then
    if [[ -n "$UDEPLOY_DEPLOYMENT_NAME" ]]; then
        if [[ -n "$UDEPLOY_APP_ID" ]]; then
            NEUTRINO_CLUSTER_NAME="${UBER_DATACENTER}_${UDEPLOY_APP_ID}_${UDEPLOY_DEPLOYMENT_NAME}"
        fi
    fi
fi

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-amd64}
mkdir -p $SCRIPT_DIR/log
# non daemon run
export PATH=$JAVA_HOME/bin:$PATH
# Override the default airlift one
cp -f $SCRIPT_DIR/launcher.py $CODE_FOLDER/bin/launcher.py
$CODE_FOLDER/bin/launcher run --verbose --etc-dir $RUNNING_ETC --data-dir $SCRIPT_DIR/data --launcher-log-file $SCRIPT_DIR/log/launcher.log --server-log-file $SCRIPT_DIR/log/server.log --enable-console $* &
launcher_pid=$!
checker_pid=0
if [[ -n "$UBER_PORT_HTTP" ]]; then
    if [[ -n "$NEUTRINO_CLUSTER_NAME" ]]; then
        $SCRIPT_DIR/neutrino_checker.py --port $UBER_PORT_HTTP --service "presto.$NEUTRINO_CLUSTER_NAME" --debug &
        checker_pid=$!
    fi
fi
trap 'kill $launcher_pid; kill $checker_pid; exit 1' SIGINT SIGTERM
sleep 5

while [ 1 ]; do
    kill -0 $launcher_pid
    if [[ $checker_pid -ne 0 ]]; then
        kill -0 $checker_pid
    fi
    set +x
    sleep 10
done
