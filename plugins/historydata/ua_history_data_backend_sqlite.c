/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *    Copyright 2022 (c) rjapeer <open62541@peerpoint.nl> (Author: Ronald Peer)
 */

#include <open62541/plugin/historydata/history_data_backend_sqlite.h>
#include <open62541/plugin/historydata/sqlite/sqlite3.h>


static const char *maxUUL = "18446744073709551615";

typedef struct {
    UA_HistoryDataBackend parent;
    sqlite3 *sqldb;
    size_t pruneInterval;
    size_t pruneCounter;
    size_t maxValuesPerNode;
} UA_SqliteStoreContext;

static void
UA_SqliteStoreContext_clear(UA_SqliteStoreContext *ctx) {
    if(ctx->sqldb)
        sqlite3_close(ctx->sqldb);
    memset(ctx, 0, sizeof(UA_SqliteStoreContext));
}

static UA_StatusCode
serverSetHistoryData_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId,
                                        void *sessionContext, const UA_NodeId *nodeId,
                                        UA_Boolean historizing, const UA_DataValue *value)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->serverSetHistoryData(server, parent->context, sessionId, sessionContext, nodeId, historizing, value);
}

static char *
uaStringToCString(const UA_ByteString *uaString)
{
    char *cString = (char *)UA_malloc(uaString->length + 1);
    if(cString) {
        strncpy(cString, (char *)uaString->data, uaString->length);
        cString[uaString->length] = '\0';
    }
    return cString;
}
static const char*
CheckNull(const char *cString)
{
    if(cString)
        return cString;
    else
        return "NULL";
}

static bool
sqliteBackend_is_prune_enabled(UA_SqliteStoreContext *ctx)
{
    if(!ctx->pruneInterval)
        return false;
    if(!ctx->maxValuesPerNode)
        return false;

    return true;
}

static void
sqliteBackend_db_prune_circular(UA_SqliteStoreContext *ctx)
{
    if(!ctx)
        return;
    if(!sqliteBackend_is_prune_enabled(ctx))
        return;

    ctx->pruneCounter = 0;

    const char *sqlFmt =
        "DELETE FROM HISTORY"
        " WHERE EXISTS ("
        "   SELECT * "
        "   FROM ("
        "       SELECT TIMESTAMP, SESSIONID, NODEID, DATAVALUE, row_number()"
        "         OVER ( PARTITION BY NODEID ORDER BY TIMESTAMP DESC) AS timestamp_sequence"
        "         FROM HISTORY) AS prune"
        "     WHERE timestamp_sequence > %ld"
        "       AND HISTORY.TIMESTAMP = prune.TIMESTAMP"
        "       AND HISTORY.NODEID    = prune.NODEID"
        "       AND HISTORY.SESSIONID = prune.SESSIONID"
        ")";
    size_t len = strlen(sqlFmt) * 2;

    char *sqlCmd = (char *)UA_malloc(len);
    if(sqlCmd) {
        snprintf(sqlCmd, len, sqlFmt, ctx->maxValuesPerNode);
        int sqlRet = sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
        UA_free(sqlCmd);
    }
}

static void
sqliteBackend_db_prune_if_needed(UA_SqliteStoreContext *ctx)
{
    if(!ctx)
        return;
    if(!sqliteBackend_is_prune_enabled(ctx))
        return;

    ctx->pruneCounter++;
    if(ctx->pruneCounter < ctx->pruneInterval)
        return;

    if(ctx->maxValuesPerNode > 0)
        sqliteBackend_db_prune_circular(ctx);
}

static char *
uaNodeIdAsJsonCStr(const UA_NodeId *nodeId)
{
    UA_ByteString nodeIdAsJson = UA_BYTESTRING_NULL;

    UA_encodeJson(nodeId, &UA_TYPES[UA_TYPES_NODEID], &nodeIdAsJson, NULL);
    char *nodeIdAsJsonString = uaStringToCString(&nodeIdAsJson);
    UA_ByteString_clear(&nodeIdAsJson);

    return nodeIdAsJsonString;
}

static char *
uaDataValueAsJsonCStr(const UA_DataValue *value)
{
    UA_ByteString valueAsJson = UA_BYTESTRING_NULL;

    UA_encodeJson(value, &UA_TYPES[UA_TYPES_DATAVALUE], &valueAsJson, NULL);
    char *valueAsJsonString = uaStringToCString(&valueAsJson);
    UA_ByteString_clear(&valueAsJson);

    return valueAsJsonString;
}

static UA_DateTime
uaValueTimeStamp(const UA_DataValue* value)
{
    if(value->hasSourceTimestamp) {
        return value->sourceTimestamp;
    } else if(value->hasServerTimestamp) {
        return value->serverTimestamp;
    } else {
        return 0;
    }
}

static UA_StatusCode
sqliteBackend_db_storeHistoryEntry(UA_SqliteStoreContext *ctx,
                                   const UA_NodeId *sessionId,
                                   const UA_NodeId *nodeId,
                                   const UA_DataValue *value)
{
    UA_DateTime keyTimestamp = uaValueTimeStamp(value);
    if(!keyTimestamp) {
        keyTimestamp = UA_DateTime_now();
    }

    char *sessionIdCStr = uaNodeIdAsJsonCStr(sessionId);
    char *nodeIdCStr = uaNodeIdAsJsonCStr(nodeId);
    char *valueAsCStr = uaDataValueAsJsonCStr(value);  // TODO: NEEDS ESCAPING !

    const char *sqlFmt = "INSERT INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE ) "
                         "VALUES('%lld','%s', '%s', '%s');";
    size_t bufferLen = strlen(sqlFmt) + strlen(maxUUL);
    bufferLen += strlen(sessionIdCStr) + strlen(nodeIdCStr) + strlen(valueAsCStr) + 1;

    char *sqlCmd = (char *)UA_malloc(bufferLen);
    if(sqlCmd) {
        snprintf(sqlCmd, bufferLen, sqlFmt,
            keyTimestamp,
            CheckNull(sessionIdCStr),
            CheckNull(nodeIdCStr),
            CheckNull(valueAsCStr));

        char *zErrMsg = 0;
        int sqlRes = sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, &zErrMsg);
        if(sqlRes != SQLITE_OK) {
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            zErrMsg = 0;
        }
        UA_free(sqlCmd);
    }
    UA_free(sessionIdCStr);
    UA_free(nodeIdCStr);
    UA_free(valueAsCStr);

    return UA_STATUSCODE_GOOD;
}

static UA_StatusCode
serverSetHistoryData_backend_sqlite_Circular(UA_Server *server, void *context, 
                                             const UA_NodeId *sessionId, void *sessionContext,
                                             const UA_NodeId *nodeId, UA_Boolean historizing,
                                             const UA_DataValue *value)
{

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;

    if(historizing) {
        sqliteBackend_db_storeHistoryEntry(ctx, sessionId, nodeId, value);
        sqliteBackend_db_prune_if_needed(ctx);
    }

    return parent->serverSetHistoryData(server, parent->context, sessionId,
                                        sessionContext, nodeId, historizing, value);
}

static size_t
resultSize_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId,
                              void *sessionContext, const UA_NodeId *nodeId,
                              size_t startIndex, size_t endIndex)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->resultSize(server, parent->context, sessionId, sessionContext, nodeId,
                              startIndex, endIndex);
}

static size_t
getEnd_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId,
                      void *sessionContext, const UA_NodeId *nodeId)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->getEnd(server, parent->context, sessionId, sessionContext, nodeId);
}

static size_t
lastIndex_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId,
                         void *sessionContext, const UA_NodeId *nodeId)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->lastIndex(server, parent->context, sessionId, sessionContext, nodeId);
}

static size_t
firstIndex_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId,
                              void *sessionContext, const UA_NodeId *nodeId)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->firstIndex(server, parent->context, sessionId, sessionContext, nodeId);
}

static size_t
getDateTimeMatch_backend_sqlite(UA_Server *server, void *context,
                                    const UA_NodeId *sessionId, void *sessionContext,
                                    const UA_NodeId *nodeId, const UA_DateTime timestamp,
                                    const MatchStrategy strategy)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->getDateTimeMatch(server, parent->context, sessionId, sessionContext, nodeId, timestamp, strategy);
}

static UA_StatusCode
copyDataValues_backend_sqlite(UA_Server *server, void *context,
                              const UA_NodeId *sessionId, void *sessionContext,
                              const UA_NodeId *nodeId, size_t startIndex, size_t endIndex,
                              UA_Boolean reverse, size_t maxValues, UA_NumericRange range,
                              UA_Boolean releaseContinuationPoints,
                              const UA_ByteString *continuationPoint,
                              UA_ByteString *outContinuationPoint, size_t *providedValues,
                              UA_DataValue *values)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->copyDataValues(server, parent->context, sessionId, sessionContext, nodeId,
                                  startIndex, endIndex, reverse, maxValues, range,
                                  releaseContinuationPoints, continuationPoint,
                                  outContinuationPoint, providedValues, values);
}

static const UA_DataValue*
getDataValue_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId, 
                                void *sessionContext, const UA_NodeId *nodeId, size_t index)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->getDataValue(server, parent->context, sessionId, sessionContext,
                                nodeId, index);
}

static UA_Boolean
boundSupported_backend_sqlite(UA_Server *server, void *context,
                                  const UA_NodeId *sessionId, void *sessionContext,
                                  const UA_NodeId *nodeId) {
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->boundSupported(server, parent->context, sessionId, sessionContext, nodeId);
}

static UA_Boolean
timestampsToReturnSupported_backend_sqlite(UA_Server *server, void *context, 
                                           const UA_NodeId *sessionId, void *sessionContext,
                                           const UA_NodeId *nodeId,
                                           const UA_TimestampsToReturn timestampsToReturn)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
    return parent->timestampsToReturnSupported(
        server, parent->context, sessionId, sessionContext, nodeId, timestampsToReturn);
}

static UA_StatusCode
insertDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                   const UA_NodeId *sessionId, void *sessionContext,
                                   const UA_NodeId *nodeId, const UA_DataValue *value)
{
    const UA_DateTime timestamp = uaValueTimeStamp(value);
    if(!timestamp) {
        return UA_STATUSCODE_BADINVALIDTIMESTAMP;
    }

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;
    UA_HistoryDataBackend *parent = &ctx->parent;
    UA_StatusCode res = parent->insertDataValue(server, parent->context, sessionId, sessionContext,
                                                nodeId, value);

    if(UA_StatusCode_isGood(res)) {
        char *sessionIdCStr = uaNodeIdAsJsonCStr(sessionId);
        char *nodeIdCStr = uaNodeIdAsJsonCStr(nodeId);
        char *valueCStr = uaDataValueAsJsonCStr(value);  // TODO: NEEDS ESCAPING !

        if(sessionIdCStr && nodeIdCStr && valueCStr) {
            char *sqlFmt = "INSERT INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE) "
                           "VALUES(%lld, '%s', '%s', '%s')";
            size_t bufferLen = strlen(sqlFmt) + strlen(maxUUL) + strlen(sessionIdCStr) +
                               strlen(nodeIdCStr) + strlen(valueCStr) + 1;
            char *sqlCmd = (char *)UA_malloc(bufferLen);
            snprintf(sqlCmd, bufferLen, sqlFmt, timestamp, 
                CheckNull(sessionIdCStr),
                CheckNull(nodeIdCStr), 
                CheckNull(valueCStr));

            int sqlRes = sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
            UA_free(sqlCmd);
        }

    }
    return res;
}

static UA_StatusCode
updateDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                   const UA_NodeId *sessionId, void *sessionContext,
                                   const UA_NodeId *nodeId, const UA_DataValue *value)
{
    const UA_DateTime timestamp = uaValueTimeStamp(value);
    if(!timestamp) {
        return UA_STATUSCODE_BADINVALIDTIMESTAMP;
    }

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;
    UA_HistoryDataBackend *parent = &ctx->parent;
    UA_StatusCode res = parent->updateDataValue(server, parent->context, sessionId, sessionContext,
                                            nodeId, value);
    if(UA_StatusCode_isGood(res)) {
        char *sessionIdCStr = uaNodeIdAsJsonCStr(sessionId);
        char *nodeIdCStr = uaNodeIdAsJsonCStr(nodeId);
        char *valueCStr = uaDataValueAsJsonCStr(value);  // TODO: NEEDS ESCAPING !

        if(sessionIdCStr && nodeIdCStr && valueCStr) {
            char *sqlFmt =
                "UPDATE HISTORY "
                "   SET SESSIONID = '%', NODEID = '%s', DATAVALUE = '%s' "
                " WHERE TIMESTAMP = %lld";
            size_t bufferLen = strlen(sqlFmt) + strlen(maxUUL) + strlen(sessionIdCStr) +
                               strlen(nodeIdCStr) + strlen(valueCStr) + 1;
            char *sqlCmd = (char *)UA_malloc(bufferLen);
            snprintf(sqlCmd, bufferLen, sqlFmt, timestamp, sessionIdCStr, nodeIdCStr,
                     valueCStr);

            int sqlRes = sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
            UA_free(sqlCmd);
        }
    }
    return res;
}

static UA_StatusCode
replaceDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                    const UA_NodeId *sessionId, void *sessionContext,
                                    const UA_NodeId *nodeId, const UA_DataValue *value)
{
    const UA_DateTime timestamp = uaValueTimeStamp(value);
    if(!timestamp) {
        return UA_STATUSCODE_BADINVALIDTIMESTAMP;
    }

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;
    UA_HistoryDataBackend *parent = &ctx->parent;
    UA_StatusCode res = parent->replaceDataValue(server, parent->context, sessionId,
                                                 sessionContext, nodeId, value);
    if(UA_StatusCode_isGood(res)) {
        char *sessionIdCStr = uaNodeIdAsJsonCStr(sessionId);
        char *nodeIdCStr = uaNodeIdAsJsonCStr(nodeId);
        char *valueCStr = uaDataValueAsJsonCStr(value);  // TODO: NEEDS ESCAPING !

        if(sessionIdCStr && nodeIdCStr && valueCStr) {
            char *sqlFmt = "INSERT OR REPLACE"
                           "  INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE) "
                           "  VALUES(%lld, '%s', '%s', '%s')";
            size_t bufferLen = strlen(sqlFmt) + strlen(maxUUL) + strlen(sessionIdCStr) +
                               strlen(nodeIdCStr) + strlen(valueCStr) + 1;
            char *sqlCmd = (char *)UA_malloc(bufferLen);
            snprintf(sqlCmd, bufferLen, sqlFmt, timestamp, sessionIdCStr, nodeIdCStr,
                     valueCStr);

            int sqlRes = sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
            UA_free(sqlCmd);
        }
    }
    return res;
}

static UA_StatusCode
removeDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                   const UA_NodeId *sessionId, void *sessionContext,
                                   const UA_NodeId *nodeId, UA_DateTime startTimestamp,
                                   UA_DateTime endTimestamp)
{
    if(startTimestamp > endTimestamp) {
        return UA_STATUSCODE_BADTIMESTAMPNOTSUPPORTED;
    }
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;
    UA_HistoryDataBackend *parent = &ctx->parent;
    UA_StatusCode res = 
        parent->removeDataValue(server, parent->context, sessionId,
                                sessionContext, nodeId, startTimestamp, endTimestamp);

    if(UA_StatusCode_isGood(res)) {
        char *sessionIdCStr = uaNodeIdAsJsonCStr(sessionId);
        char *nodeIdCStr = uaNodeIdAsJsonCStr(nodeId);

        if(sessionIdCStr && nodeIdCStr) {
            char *sqlFmt = "DELETE FROM HISTORY "
                           " WHERE TIMESTAMP >= %lld "
                           "   AND TIMESTAMP <= %lld "
                           "   AND SESSIONID = '%s'"
                           "   AND NODEID = '%s'";
            size_t bufferLen = strlen(sqlFmt) + strlen(maxUUL) + strlen(sessionIdCStr) +
                               strlen(nodeIdCStr) + 1;
            char *sqlCmd = (char *)UA_malloc(bufferLen);
            snprintf(sqlCmd, bufferLen, sqlFmt, startTimestamp, endTimestamp, sessionIdCStr, nodeIdCStr);

            int sqlRes = sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
            UA_free(sqlCmd);
        }
    }
    return res;
}

static void
deleteMembers_backend_sqlite(UA_HistoryDataBackend *backend)
{
    if(backend == NULL || backend->context == NULL)
        return;
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)backend->context)->parent;
    if (parent)
        parent->deleteMembers(parent);

    UA_SqliteStoreContext_clear((UA_SqliteStoreContext *)backend->context);
    UA_free(backend->context);
}

static UA_StatusCode
getHistoryData_service_sqlite_Circular(
    UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
    const UA_HistoryDataBackend *backend, const UA_DateTime start, const UA_DateTime end,
    const UA_NodeId *nodeId, size_t maxSize, UA_UInt32 numValuesPerNode,
    UA_Boolean returnBounds, UA_TimestampsToReturn timestampsToReturn,
    UA_NumericRange range, UA_Boolean releaseContinuationPoints,
    const UA_ByteString *continuationPoint, UA_ByteString *outContinuationPoint,
    UA_HistoryData *historyData)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)backend->context)->parent;
    return parent->getHistoryData(server, sessionId, sessionContext, parent, start, end, nodeId,
                                  maxSize, numValuesPerNode, returnBounds,
                                  timestampsToReturn, range, releaseContinuationPoints,
                                  continuationPoint, outContinuationPoint, historyData);
}

static void
sqliteBackend_db_initalize(UA_SqliteStoreContext *context, const char *dbFilePath) {
    char *zErrMsg = 0;
    int sqlRet = sqlite3_open(dbFilePath, &context->sqldb);

    if(sqlRet) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(context->sqldb));
    } else {
        char *sql = "CREATE TABLE IF NOT EXISTS HISTORY ("
                    "    TIMESTAMP INT PRIMARY KEY NOT NULL,"
                    "    SESSIONID TEXT,"
                    "    NODEID    TEXT, "
                    "    DATAVALUE TEXT);";
        sqlRet = sqlite3_exec(context->sqldb, sql, NULL, NULL, &zErrMsg);
    }

    if(sqlRet != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        zErrMsg = 0;
    }
}

static void
restoreHistoryEntry(
    UA_SqliteStoreContext *context,
    const char *sessionIdAsJson,
    const char *nodeIdAsJson,
    const char *valueAsJson)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;

    UA_ByteString uaSessionIdAsJson;
    uaSessionIdAsJson.length = strlen(sessionIdAsJson);
    uaSessionIdAsJson.data = (UA_Byte *)sessionIdAsJson;

    UA_ByteString uaNodeIdAsJson;
    uaNodeIdAsJson.length = strlen(nodeIdAsJson);
    uaNodeIdAsJson.data = (UA_Byte *)nodeIdAsJson;

    UA_ByteString uaValueAsJson;
    uaValueAsJson.length = strlen(valueAsJson);
    uaValueAsJson.data = (UA_Byte *)valueAsJson;

    UA_NodeId sessionId = UA_NODEID_NULL;
    UA_NodeId nodeId = UA_NODEID_NULL;
    UA_DataValue dataValue;
    UA_DataValue_init(&dataValue);

    UA_StatusCode scSid = UA_decodeJson(&uaSessionIdAsJson, &sessionId, &UA_TYPES[UA_TYPES_NODEID], NULL);
    UA_StatusCode scNid = UA_decodeJson(&uaNodeIdAsJson, &nodeId, &UA_TYPES[UA_TYPES_NODEID], NULL);
    UA_StatusCode scDV =  UA_decodeJson(&uaValueAsJson, &dataValue, &UA_TYPES[UA_TYPES_DATAVALUE], NULL);

    if(UA_StatusCode_isGood(scSid) && 
       UA_StatusCode_isGood(scNid) &&
       UA_StatusCode_isGood(scDV)) {
        parent->serverSetHistoryData(NULL, parent->context, &sessionId, NULL, &nodeId, true, &dataValue);
    }

    UA_NodeId_clear(&sessionId);
    UA_NodeId_clear(&nodeId);
    UA_DataValue_clear(&dataValue);
}

static int
callback_db_restore_entry(void *context, int argc, char **argv, char **azColName) {
    int i;
    char *nodeId = NULL;
    char *sessionId = NULL;
    char *dataValue = NULL;

    for(i = 0; i < argc; i++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
        char *col = azColName[i];
        char *val = argv[i];
        if(0 == strcmp(col, "SESSIONID"))
            sessionId = val;
        else if(0 == strcmp(col, "NODEID"))
            nodeId = val;
        else if(0 == strcmp(col, "DATAVALUE"))
            dataValue = val;
    }
    if(sessionId && nodeId && dataValue)
        restoreHistoryEntry((UA_SqliteStoreContext *)context, sessionId, nodeId, dataValue);
    else
        printf("Invalid entry");
    printf("\n");
    return SQLITE_OK;
}

static void
sqliteBackend_db_restore(UA_SqliteStoreContext *context)
{
    char *sql = "SELECT TIMESTAMP, SESSIONID, NODEID, DATAVALUE FROM HISTORY";
    sqlite3_exec(context->sqldb, sql, callback_db_restore_entry, context, NULL);
}

static UA_SqliteStoreContext*
sqliteBackend_createDefaultStoreContext(UA_HistoryDataBackend parent)
{
    UA_SqliteStoreContext *ctx =
        (UA_SqliteStoreContext *)UA_calloc(1, sizeof(UA_SqliteStoreContext));
    if(!ctx)
        return ctx;

    memset(ctx, 0, sizeof(UA_SqliteStoreContext));
    ctx->parent = parent;
    ctx->pruneInterval = 0;
    ctx->pruneCounter = 0;
    ctx->maxValuesPerNode = 0;
    return ctx;
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite(UA_HistoryDataBackend parent, const char* dbFilePath) 
{
    UA_HistoryDataBackend newBackend;
    memset(&newBackend, 0, sizeof(UA_HistoryDataBackend));

    UA_SqliteStoreContext *ctx = sqliteBackend_createDefaultStoreContext(parent);
    if (!ctx)
        return newBackend;

    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite;
    newBackend.resultSize = &resultSize_backend_sqlite;
    newBackend.getEnd = &getEnd_backend_sqlite;
    newBackend.lastIndex = &lastIndex_backend_sqlite;
    newBackend.firstIndex = &firstIndex_backend_sqlite;
    newBackend.getDateTimeMatch = &getDateTimeMatch_backend_sqlite;
    newBackend.copyDataValues = &copyDataValues_backend_sqlite;
    newBackend.getDataValue = &getDataValue_backend_sqlite;
    newBackend.boundSupported = &boundSupported_backend_sqlite;
    newBackend.timestampsToReturnSupported = &timestampsToReturnSupported_backend_sqlite;
    newBackend.insertDataValue = &insertDataValue_backend_sqlite;
    newBackend.updateDataValue = &updateDataValue_backend_sqlite;
    newBackend.replaceDataValue = &replaceDataValue_backend_sqlite;
    newBackend.removeDataValue = &removeDataValue_backend_sqlite;
    newBackend.deleteMembers = &deleteMembers_backend_sqlite;
    newBackend.getHistoryData = NULL;

    sqliteBackend_db_initalize(ctx, dbFilePath);
    sqliteBackend_db_restore(ctx);
    newBackend.context = ctx;

    return newBackend;
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite_Circular(UA_HistoryDataBackend parent,
                                      const char *dbFilePath,
                                      size_t pruneInterval,
                                      size_t maxValuesPerNode)
{
    UA_HistoryDataBackend newBackend = UA_HistoryDataBackend_SQLite(parent, dbFilePath);
    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_Circular;
    newBackend.getHistoryData = &getHistoryData_service_sqlite_Circular;

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)newBackend.context;
    if(ctx) {
        ctx->maxValuesPerNode = maxValuesPerNode;
        ctx->pruneInterval = pruneInterval;
        ctx->pruneCounter = 0;
        sqliteBackend_db_prune_circular(ctx);
    }

    return newBackend;
}
