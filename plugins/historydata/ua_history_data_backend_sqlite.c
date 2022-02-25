/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *    Copyright 2022 (c) rjapeer <open62541@peerpoint.nl> (Author: Ronald Peer)
 */

#include <open62541/plugin/historydata/history_data_backend_sqlite.h>
#include <open62541/plugin/historydata/sqlite/sqlite3.h>

typedef struct {
    UA_HistoryDataBackend parent;
    sqlite3 *sqldb;
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

static UA_StatusCode
serverSetHistoryData_backend_sqlite_Circular(UA_Server *server, void *context, 
                                                 const UA_NodeId *sessionId, void *sessionContext,
                                                 const UA_NodeId *nodeId, UA_Boolean historizing,
                                                 const UA_DataValue *value)
{
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;

    /* Encode the sessionId */
    UA_ByteString sessionIdAsJson = UA_BYTESTRING_NULL;
    UA_ByteString nodeIdAsJson = UA_BYTESTRING_NULL;
    UA_ByteString valueAsJson = UA_STRING_NULL;

    UA_encodeJson(sessionId, &UA_TYPES[UA_TYPES_NODEID], &sessionIdAsJson, NULL);
    UA_encodeJson(nodeId, &UA_TYPES[UA_TYPES_NODEID], &nodeIdAsJson, NULL);
    UA_encodeJson(value, &UA_TYPES[UA_TYPES_DATAVALUE], &valueAsJson, NULL);

    /* Decode the nodeId */
    UA_NodeId newSessionId = UA_NODEID_NULL;
    UA_NodeId newNodeId = UA_NODEID_NULL;
    UA_DataValue newJsonValue;

    UA_decodeJson(&sessionIdAsJson, &newSessionId, &UA_TYPES[UA_TYPES_NODEID], NULL);
    UA_decodeJson(&nodeIdAsJson, &newNodeId, &UA_TYPES[UA_TYPES_NODEID], NULL);
    UA_decodeJson(&valueAsJson, &newJsonValue, &UA_TYPES[UA_TYPES_DATAVALUE], NULL);

    UA_String outSid = UA_STRING_NULL;
    UA_NodeId_print(sessionId, &outSid);
    UA_String outNid = UA_STRING_NULL;
    UA_NodeId_print(nodeId, &outNid);
    UA_String outTid = UA_STRING_NULL;
    UA_NodeId_print(&value->value.type->typeId, &outTid);
    UA_String outDV = UA_STRING_NULL;
    UA_print(value, &UA_TYPES[UA_TYPES_DATAVALUE], &outDV);
    UA_String outDVJson = UA_STRING_NULL;
    UA_print(&newJsonValue, &UA_TYPES[UA_TYPES_DATAVALUE], &outDVJson);

    UA_DateTime keyTimestamp = 0;
    if(value->hasSourceTimestamp) {
        keyTimestamp = value->sourceTimestamp;
    } else if(value->hasServerTimestamp) {
        keyTimestamp = value->serverTimestamp;
    } else {
        keyTimestamp = UA_DateTime_now();
    }

    char *sessionIdCStr = uaStringToCString(&sessionIdAsJson);
    char *nodeIdCStr = uaStringToCString(&nodeIdAsJson);
    char *valueAsCStr = uaStringToCString(&valueAsJson);  // TODO: NEEDS ESCAPING !

    char *sqlFmt = "INSERT INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE) "
                   "VALUES('%lld','%s', '%s', '%s');";
    size_t len = (strlen(sqlFmt) * 2) + 1;
    len += sessionIdAsJson.length + nodeIdAsJson.length + valueAsJson.length;

    char *sqlCmd = (char *)UA_malloc(len);
    if(sqlCmd) {
        snprintf(sqlCmd, len, sqlFmt, keyTimestamp, CheckNull(sessionIdCStr), CheckNull(nodeIdCStr), CheckNull(valueAsCStr));

        char *zErrMsg = 0;
        int sqlRes = sqlite3_exec(ctx->sqldb, sqlCmd, 0, 0, &zErrMsg);
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
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)hdbContext)->parent;
    return parent->insertDataValue(server, parent->context, sessionId, sessionContext,
                                   nodeId, value);
}

static UA_StatusCode
updateDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                   const UA_NodeId *sessionId, void *sessionContext,
                                   const UA_NodeId *nodeId, const UA_DataValue *value)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)hdbContext)->parent;
    return parent->updateDataValue(server, parent->context, sessionId, sessionContext,
                                   nodeId, value);
}

static UA_StatusCode
replaceDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                    const UA_NodeId *sessionId, void *sessionContext,
                                    const UA_NodeId *nodeId, const UA_DataValue *value)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)hdbContext)->parent;
    return parent->replaceDataValue(server, parent->context, sessionId, sessionContext,
                                   nodeId, value);
}

static UA_StatusCode
removeDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                                   const UA_NodeId *sessionId, void *sessionContext,
                                   const UA_NodeId *nodeId, UA_DateTime startTimestamp,
                                   UA_DateTime endTimestamp)
{
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)hdbContext)->parent;
    return parent->removeDataValue(server, parent->context, sessionId, sessionContext,
                                   nodeId, startTimestamp, endTimestamp);
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
                    "    NODEID TEXT, "
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
restoreHistoryEntry(UA_SqliteStoreContext *context, 
    const char *sessionIdAsJson, const char *nodeIdAsJson, const char *valueAsJson)
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

    /* Decode the nodeId */
    UA_NodeId sessionId = UA_NODEID_NULL;
    UA_NodeId nodeId = UA_NODEID_NULL;
    UA_DataValue dataValue;

    UA_decodeJson(&uaSessionIdAsJson, &sessionId, &UA_TYPES[UA_TYPES_NODEID], NULL);
    UA_decodeJson(&uaNodeIdAsJson, &nodeId, &UA_TYPES[UA_TYPES_NODEID], NULL);
    UA_decodeJson(&uaValueAsJson, &dataValue, &UA_TYPES[UA_TYPES_DATAVALUE], NULL);

    parent->serverSetHistoryData(NULL, parent->context, &sessionId, NULL, &nodeId, true,
                                 &dataValue);
}

static int
db_restore_callback(void *context, int argc, char **argv, char **azColName)
{
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
    sqlite3_exec(context->sqldb, sql, db_restore_callback, context, NULL);
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite(UA_HistoryDataBackend parent, const char* dbFilePath) 
{
    UA_HistoryDataBackend result;
    memset(&result, 0, sizeof(UA_HistoryDataBackend));
    UA_SqliteStoreContext *ctx =
        (UA_SqliteStoreContext *)UA_calloc(1, sizeof(UA_SqliteStoreContext));
    if (!ctx)
        return result;

    sqliteBackend_db_initalize(ctx, dbFilePath);

    ctx->parent = parent;
    result.serverSetHistoryData = &serverSetHistoryData_backend_sqlite;
    result.resultSize = &resultSize_backend_sqlite;
    result.getEnd = &getEnd_backend_sqlite;
    result.lastIndex = &lastIndex_backend_sqlite;
    result.firstIndex = &firstIndex_backend_sqlite;
    result.getDateTimeMatch = &getDateTimeMatch_backend_sqlite;
    result.copyDataValues = &copyDataValues_backend_sqlite;
    result.getDataValue = &getDataValue_backend_sqlite;
    result.boundSupported = &boundSupported_backend_sqlite;
    result.timestampsToReturnSupported = &timestampsToReturnSupported_backend_sqlite;
    result.insertDataValue = &insertDataValue_backend_sqlite;
    result.updateDataValue = &updateDataValue_backend_sqlite;
    result.replaceDataValue = &replaceDataValue_backend_sqlite;
    result.removeDataValue = &removeDataValue_backend_sqlite;
    result.deleteMembers = &deleteMembers_backend_sqlite;
    result.getHistoryData = NULL;
    result.context = ctx;

    sqliteBackend_db_restore(ctx);

    return result;
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite_Circular(UA_HistoryDataBackend parent, const char *dbFilePath)
{
    UA_HistoryDataBackend result = UA_HistoryDataBackend_SQLite(parent, dbFilePath);
    result.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_Circular;
    result.getHistoryData = &getHistoryData_service_sqlite_Circular;
    return result;
}
