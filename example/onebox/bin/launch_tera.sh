#!/bin/bash
source config

CURRENT_DIR=`dirname $0`
TIME=`date +%Y-%m-%d-%H:%M:%S`
# backup master log & launch tera master
MASTER_LOG_FILE=../log/master.stderr
if [ -f ${CURRENT_DIR}/${MASTER_LOG_FILE} ];then
    mv ${CURRENT_DIR}/${MASTER_LOG_FILE} \
        ${CURRENT_DIR}/${MASTER_LOG_FILE}.${TIME}
fi
${CURRENT_DIR}/tera_main \
    --flagfile=${CURRENT_DIR}/../conf/tera.flag \
    --tera_role=master \
    --tera_master_port=${PORT} \
    --tera_log_prefix=master &> ${CURRENT_DIR}/../log/master.stderr </dev/null &

# backup tabletnode log & launch tera tabletnodes
for ((i=1; i<=$TABLETNODE_NUM; i++)); do
    LEVELDB_LOG_FILE=../log/leveldb.$i.log
    TABLETNODE_LOG_FILE=../log/tabletnode.$i.stderr
    if [ -f ${CURRENT_DIR}/${TABLETNODE_LOG_FILE} ];then
        mv ${CURRENT_DIR}/${TABLETNODE_LOG_FILE} \
            ${CURRENT_DIR}/${TABLETNODE_LOG_FILE}.${TIME}
    fi
    if [ -f ${CURRENT_DIR}/${LEVELDB_LOG_FILE} ];then
        mv ${CURRENT_DIR}/${LEVELDB_LOG_FILE} \
            ${CURRENT_DIR}/${LEVELDB_LOG_FILE}.${TIME}
    fi
    ${CURRENT_DIR}/tera_main \
        --flagfile=${CURRENT_DIR}/../conf/tera.flag \
        --tera_role=tabletnode \
        --tera_tabletnode_port=$((PORT+i)) \
        --tera_leveldb_log_path=${LEVELDB_LOG_FILE} \
        --tera_tabletnode_cache_paths=../cache/tabletnode.$i \
        --tera_log_prefix=tabletnode.$i \
        &> ${CURRENT_DIR}/${TABLETNODE_LOG_FILE} </dev/null &
done
