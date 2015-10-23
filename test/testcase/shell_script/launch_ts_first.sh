#!/bin/bash
CURRENT_DIR=`dirname $0`
source ${CURRENT_DIR}/config

# make sure tera is killed
./kill_tera.sh

FAKE_ZK_PATH_PREFIX="${CURRENT_DIR}/../fakezk"
TIME=`date +%Y-%m-%d-%H:%M:%S`

# init all fake zk node
rm -rf ${FAKE_ZK_PATH_PREFIX}
mkdir -p ${FAKE_ZK_PATH_PREFIX}/master-lock
mkdir -p ${FAKE_ZK_PATH_PREFIX}/ts
mkdir -p ${FAKE_ZK_PATH_PREFIX}/kick

# backup tabletnode log & launch tera tabletnodes
if [ ! -x ${CURRENT_DIR}/../log ];then
  mkdir ${CURRENT_DIR}/../log
fi

# backup master log & launch tera master
echo "launching master..."
MASTER_LOG_FILE=${CURRENT_DIR}/../log/master.stderr
if [ -f ${MASTER_LOG_FILE} ];then
    mv ${MASTER_LOG_FILE} ${MASTER_LOG_FILE}.${TIME}
fi
${CURRENT_DIR}/tera_main \
    --flagfile=${CURRENT_DIR}/../conf/tera.flag \
    --tera_role=master \
    --tera_master_port=${PORT} \
    --tera_fake_zk_path_prefix=${FAKE_ZK_PATH_PREFIX} \
    --tera_log_prefix=master &> ${MASTER_LOG_FILE} </dev/null &

## wait one second for master startup
for ((i=1; i<=$TABLETNODE_NUM; i++)); do
    echo "launching tabletnode $i..."
    TABLETNODE_LOG_FILE=${CURRENT_DIR}/../log/tabletnode.$i.stderr
    if [ -f ${TABLETNODE_LOG_FILE} ];then
        mv ${TABLETNODE_LOG_FILE} ${TABLETNODE_LOG_FILE}.${TIME}
    fi
    LEVELDB_LOG_FILE=${CURRENT_DIR}/../log/leveldb.$i.log
    if [ -f ${LEVELDB_LOG_FILE} ];then
        mv ${LEVELDB_LOG_FILE} ${LEVELDB_LOG_FILE}.${TIME}
    fi
    CACHE_PATH=${CURRENT_DIR}/../cache/tabletnode.$i
    if [ ! -x $CACHE_PATH ];then
        mkdir -p $CACHE_PATH
    fi
    ${CURRENT_DIR}/tera_main \
        --flagfile=${CURRENT_DIR}/../conf/tera.flag \
        --tera_role=tabletnode \
        --tera_tabletnode_port=$((PORT+i)) \
        --tera_leveldb_log_path=${LEVELDB_LOG_FILE} \
        --tera_tabletnode_cache_paths=${CACHE_PATH} \
        --tera_log_prefix=tabletnode.$i \
        --tera_fake_zk_path_prefix=${FAKE_ZK_PATH_PREFIX} \
        &> ${TABLETNODE_LOG_FILE} </dev/null &
done

sleep 10

