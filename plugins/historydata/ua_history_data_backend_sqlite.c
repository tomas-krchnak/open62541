/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *    Copyright 2022 (c) rjapeer <open62541@peerpoint.nl> (Author: Ronald Peer)
 */

#include <open62541/plugin/historydata/history_data_backend_sqlite.h>
#include <open62541/plugin/historydata/sqlite/sqlite3.h>

typedef char*              CharBuffer;
typedef char*              SQLCharBuffer;
typedef const char *       ConstCharBuffer;
typedef const char * const FixedCharBuffer;

static const FixedCharBuffer COLUMN_ROWID = "ROWID";
static const FixedCharBuffer COLUMN_VERSION = "VERSION";
static const FixedCharBuffer COLUMN_TIMESTAMP = "TIMESTAMP";
static const FixedCharBuffer COLUMN_SESSIONID = "SESSION";
static const FixedCharBuffer COLUMN_NODEID = "NODEID";
static const FixedCharBuffer COLUMN_DATAVALUE = "DATAVALUE";


typedef struct UA_SqliteStoreContext {
    UA_HistoryDataBackend parent;
    sqlite3 *sqldb;
    long dbSchemeVersion;
    void (*pruneExecuteFunc)(struct UA_SqliteStoreContext *ctx);
    UA_Boolean (*pruneNeededFunc)(struct UA_SqliteStoreContext *ctx);
    UA_DateTime pruneRetainTimeSec;
    UA_DateTime pruneRequestTimestamp;
    size_t pruneTriggerInterval;
    size_t pruneCheckCount;
    size_t maxValuesPerNode;
} UA_SqliteStoreContext;

typedef struct SqliteGetValueContext {
    FixedCharBuffer columnName;
    size_t value;
    size_t nrFound;
} SqliteGetValueContext;

static void
UA_SqliteStoreContext_clear(UA_SqliteStoreContext *ctx) {
    if(ctx->sqldb)
        sqlite3_close(ctx->sqldb);
    memset(ctx, 0, sizeof(UA_SqliteStoreContext));
}

static CharBuffer
AllocCharBuffer(size_t len)
{
    return (CharBuffer)UA_malloc(len);
}

static void
DeleteCharBuffer(CharBuffer* buffer)
{
    if (*buffer) UA_free(*buffer);
    *buffer = NULL;
}

static void
DeleteSQLCharBuffer(SQLCharBuffer* buffer)
{
    if(*buffer) sqlite3_free(*buffer);
    *buffer = NULL;
}

static bool
IsSQLColumnName(FixedCharBuffer colName, FixedCharBuffer label)
{
    return 0 == sqlite3_strnicmp(colName, label, (int)strlen(label));
}

static CharBuffer
AllocUaStringAsCString(const UA_ByteString *uaString)
{
    CharBuffer cString = AllocCharBuffer(uaString->length + 1);
    if(cString) {
        strncpy(cString, (char *)uaString->data, uaString->length);
        cString[uaString->length] = '\0';
    }
    return cString;
}

static UA_Boolean
sqliteBackend_db_prune_needed_never(UA_SqliteStoreContext *ctx)
{
    return false;
}

static UA_Boolean
sqliteBackend_db_prune_needed_default(UA_SqliteStoreContext *ctx)
{
    if(!ctx)
        return false;
    return (ctx->pruneCheckCount >= ctx->pruneTriggerInterval);
}

static void
sqliteBackend_db_prune_execute_never(UA_SqliteStoreContext *ctx)
{
    /* Nothing to do */
}

static void
sqliteBackend_db_prune_execute_circular(UA_SqliteStoreContext *ctx)
{
    if(!ctx)
        return;

    FixedCharBuffer sqlFmt =
        "DELETE FROM HISTORY"
        " WHERE EXISTS ("
        "   SELECT * "
        "   FROM ("
        "       SELECT TIMESTAMP, SESSIONID, NODEID, DATAVALUE, row_number()"
        "         OVER ( PARTITION BY NODEID ORDER BY TIMESTAMP DESC) AS timestamp_sequence"
        "         FROM HISTORY"
        "   ) AS prune"
        "   WHERE timestamp_sequence > %uld"
        "     AND HISTORY.TIMESTAMP = prune.TIMESTAMP"
        "     AND HISTORY.NODEID    = prune.NODEID"
        "     AND HISTORY.SESSIONID = prune.SESSIONID"
        ")";

    SQLCharBuffer sqlCmd = sqlite3_mprintf(sqlFmt, ctx->maxValuesPerNode);
    if(sqlCmd) {
        sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
        DeleteSQLCharBuffer(&sqlCmd);
    }
}

static void
sqliteBackend_db_prune_execute_timed(UA_SqliteStoreContext *ctx) {
    if(!ctx)
        return;
    UA_DateTime now = UA_DateTime_now();
    UA_DateTime pruneBeforeTimeStamp = now - (ctx->pruneRetainTimeSec * UA_DATETIME_SEC);
    ctx->pruneRequestTimestamp = pruneBeforeTimeStamp;

    FixedCharBuffer sqlFmt = "DELETE FROM HISTORY WHERE TIMESTAMP < %lld";

    SQLCharBuffer sqlCmd = sqlite3_mprintf(sqlFmt, pruneBeforeTimeStamp);
    if(sqlCmd) {
        sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
        DeleteSQLCharBuffer(&sqlCmd);
    }
}

static void
sqliteBackend_db_prune_if_needed(UA_SqliteStoreContext *ctx)
{
    if(!ctx)
        return;

    if (ctx->pruneNeededFunc(ctx)) {
        ctx->pruneCheckCount = 0;
        ctx->pruneExecuteFunc(ctx);
    } else {
        ctx->pruneCheckCount++;
    }
}

static CharBuffer
AllocUaNodeIdAsJsonCStr(const UA_NodeId *nodeId)
{
    UA_ByteString nodeIdAsJson = UA_BYTESTRING_NULL;

    UA_encodeJson(nodeId, &UA_TYPES[UA_TYPES_NODEID], &nodeIdAsJson, NULL);
    CharBuffer nodeIdAsJsonString = AllocUaStringAsCString(&nodeIdAsJson);
    UA_ByteString_clear(&nodeIdAsJson);

    return nodeIdAsJsonString;
}

static CharBuffer
AllocUaDataValueAsJsonCStr(const UA_DataValue *value)
{
    UA_ByteString valueAsJson = UA_BYTESTRING_NULL;

    UA_encodeJson(value, &UA_TYPES[UA_TYPES_DATAVALUE], &valueAsJson, NULL);
    CharBuffer valueAsJsonString = AllocUaStringAsCString(&valueAsJson);
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

static UA_Boolean
JsonDecode_NodeId(FixedCharBuffer json, UA_NodeId *nodeId) {
    UA_ByteString uaJson;
    uaJson.length = strlen(json);
    uaJson.data = (UA_Byte *)json;
    return UA_StatusCode_isGood(
        UA_decodeJson(&uaJson, nodeId, &UA_TYPES[UA_TYPES_NODEID], NULL));
}

static UA_Boolean
JsonDecode_DataValue(FixedCharBuffer json, UA_DataValue *dataValue) {
    UA_ByteString uaJson;
    uaJson.length = strlen(json);
    uaJson.data = (UA_Byte *)json;
    return UA_StatusCode_isGood(
        UA_decodeJson(&uaJson, dataValue, &UA_TYPES[UA_TYPES_DATAVALUE], NULL));
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

    CharBuffer sessionIdCStr = AllocUaNodeIdAsJsonCStr(sessionId);
    CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);
    CharBuffer valueAsCStr = AllocUaDataValueAsJsonCStr(value);

    FixedCharBuffer sqlFmt =
        "INSERT INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE ) "
        "VALUES(%lld, %Q, %Q, %Q);";

    SQLCharBuffer sqlCmd =
        sqlite3_mprintf(sqlFmt, keyTimestamp, sessionIdCStr, nodeIdCStr, valueAsCStr);
    if(sqlCmd) {
        sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
        DeleteSQLCharBuffer(&sqlCmd);
    }
    DeleteCharBuffer(&sessionIdCStr);
    DeleteCharBuffer(&nodeIdCStr);
    DeleteCharBuffer(&valueAsCStr);

    return UA_STATUSCODE_GOOD;
}

static UA_StatusCode
serverSetHistoryData_backend_sqlite_Default(UA_Server *server, void *context,
                                            const UA_NodeId *sessionId, void *sessionContext,
                                            const UA_NodeId *nodeId, UA_Boolean historizing,
                                            const UA_DataValue *value) {
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;

    if(historizing) {
        sqliteBackend_db_storeHistoryEntry(ctx, sessionId, nodeId, value);
    }

    return parent->serverSetHistoryData(server, parent->context, sessionId,
                                        sessionContext, nodeId, historizing, value);
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

static UA_StatusCode
serverSetHistoryData_backend_sqlite_MaxRetainingTime(
    UA_Server *server, void *context, const UA_NodeId *sessionId, void *sessionContext,
    const UA_NodeId *nodeId, UA_Boolean historizing, const UA_DataValue *value) {

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;

    if(historizing) {
        sqliteBackend_db_storeHistoryEntry(ctx, sessionId, nodeId, value);
        sqliteBackend_db_prune_if_needed(ctx);
        parent->removeDataValue(server, parent->context, sessionId, sessionContext,
                                nodeId, 0,
                                ctx->pruneRequestTimestamp);
    }

    return parent->serverSetHistoryData(server, parent->context, sessionId,
                                        sessionContext, nodeId, historizing, value);
}

static int
callback_db_getValue(void *context, int argc, char **argv, char **azColName)
{
    SqliteGetValueContext *getValueCtx = (SqliteGetValueContext *)context;
    getValueCtx->nrFound++;
    for(int i = 0; i < argc; i++) {
        FixedCharBuffer columnName = azColName[i];
        if(IsSQLColumnName(columnName, getValueCtx->columnName)) {
            const int BASE10 = 10;
            char *endPtr = NULL;
            const long valueFound = strtol(argv[i], &endPtr, BASE10);
            if(endPtr > argv[i]) {
                getValueCtx->value = valueFound;
            }
        }
    }
    return SQLITE_OK;
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
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;
    UA_HistoryDataBackend *parent = &(ctx)->parent;

    CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);

    FixedCharBuffer sqlFmt =
        "SELECT ROWID FROM HISTORY WHERE NODEID = %Q ORDER BY ROWID ASC LIMIT 1";

    SQLCharBuffer sqlCmd = sqlite3_mprintf(sqlFmt, nodeIdCStr);
    SqliteGetValueContext rowidCxt = {COLUMN_ROWID, 0u, 0u};
    if(sqlCmd) {
        sqlite3_exec(ctx->sqldb, sqlCmd, callback_db_getValue, &rowidCxt, NULL);
        DeleteSQLCharBuffer(&sqlCmd);
    }
    DeleteCharBuffer(&nodeIdCStr);
    return parent->lastIndex(server, parent->context, sessionId, sessionContext, nodeId);
}

static size_t
firstIndex_backend_sqlite(UA_Server *server, void *context, const UA_NodeId *sessionId,
                              void *sessionContext, const UA_NodeId *nodeId)
{
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;
    UA_HistoryDataBackend *parent = &(ctx)->parent;

    CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);

    FixedCharBuffer sqlFmt =
        "SELECT ROWID FROM HISTORY WHERE NODEID = %Q ORDER BY ROWID DESC LIMIT 1";

    SQLCharBuffer sqlCmd = sqlite3_mprintf(sqlFmt, nodeIdCStr);
    SqliteGetValueContext rowidCtx = { COLUMN_ROWID, 0u, 0u};
    if(sqlCmd) {
        sqlite3_exec(ctx->sqldb, sqlCmd, callback_db_getValue, &rowidCtx, NULL);
        DeleteSQLCharBuffer(&sqlCmd);
    }
    DeleteCharBuffer(&nodeIdCStr);
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
        CharBuffer sessionIdCStr = AllocUaNodeIdAsJsonCStr(sessionId);
        CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);
        CharBuffer valueCStr = AllocUaDataValueAsJsonCStr(value);

        if(sessionIdCStr && nodeIdCStr && valueCStr) {
            FixedCharBuffer sqlFmt =
                "INSERT INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE) "
                "VALUES(%lld, %Q, %Q, %Q)";
            SQLCharBuffer sqlCmd = 
                sqlite3_mprintf(sqlFmt, timestamp, sessionIdCStr, nodeIdCStr, valueCStr);
            if (sqlCmd) {
                sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
                DeleteSQLCharBuffer(&sqlCmd);
            }
        }
        DeleteCharBuffer(&valueCStr);
        DeleteCharBuffer(&nodeIdCStr);
        DeleteCharBuffer(&sessionIdCStr);
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
        CharBuffer sessionIdCStr = AllocUaNodeIdAsJsonCStr(sessionId);
        CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);
        CharBuffer valueCStr = AllocUaDataValueAsJsonCStr(value);

        if(sessionIdCStr && nodeIdCStr && valueCStr) {
            FixedCharBuffer sqlFmt =
                "UPDATE HISTORY "
                "   SET SESSIONID = %Q, NODEID = %Q, DATAVALUE = %Q "
                " WHERE TIMESTAMP = %lld";
            SQLCharBuffer sqlCmd = sqlite3_mprintf(sqlFmt, timestamp, sessionIdCStr, nodeIdCStr, valueCStr);
            if(sqlCmd) {
                sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
                DeleteSQLCharBuffer(&sqlCmd);
            }
        }
        DeleteCharBuffer(&valueCStr);
        DeleteCharBuffer(&nodeIdCStr);
        DeleteCharBuffer(&sessionIdCStr);
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
        CharBuffer sessionIdCStr = AllocUaNodeIdAsJsonCStr(sessionId);
        CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);
        CharBuffer valueCStr = AllocUaDataValueAsJsonCStr(value);

        if(sessionIdCStr && nodeIdCStr && valueCStr) {
            FixedCharBuffer sqlFmt =
                "INSERT OR REPLACE"
                "  INTO HISTORY (TIMESTAMP, SESSIONID, NODEID, DATAVALUE) "
                "  VALUES(%lld, %Q, %Q, %Q)";
            SQLCharBuffer sqlCmd =
                sqlite3_mprintf(sqlFmt, timestamp, sessionIdCStr, nodeIdCStr, valueCStr);

            if(sqlCmd) {
                sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
                DeleteSQLCharBuffer(&sqlCmd);
            }
        }
        DeleteCharBuffer(&valueCStr);
        DeleteCharBuffer(&nodeIdCStr);
        DeleteCharBuffer(&sessionIdCStr);
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
        CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);

        if(nodeIdCStr) {
            FixedCharBuffer sqlFmt =
                "DELETE FROM HISTORY "
                " WHERE TIMESTAMP >= %lld "
                "   AND TIMESTAMP <= %lld "
                "   AND NODEID = %Q";
            SQLCharBuffer sqlCmd = 
                sqlite3_mprintf(sqlFmt, startTimestamp, endTimestamp, nodeIdCStr);
            if(sqlCmd) {
                sqlite3_exec(ctx->sqldb, sqlCmd, NULL, NULL, NULL);
                DeleteSQLCharBuffer(&sqlCmd);
            }
        }
        DeleteCharBuffer(&nodeIdCStr);
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
    UA_HistoryData *historyData
) {
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)backend->context)->parent;
    return parent->getHistoryData(server, sessionId, sessionContext, parent, start, end, nodeId,
                                  maxSize, numValuesPerNode, returnBounds,
                                  timestampsToReturn, range, releaseContinuationPoints,
                                  continuationPoint, outContinuationPoint, historyData);
}

static UA_StatusCode
getHistoryData_service_sqlite_MaxRetainingTime(
    UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
    const UA_HistoryDataBackend *backend, const UA_DateTime start, const UA_DateTime end,
    const UA_NodeId *nodeId, size_t maxSizePerResponse, UA_UInt32 numValuesPerNode,
    UA_Boolean returnBounds, UA_TimestampsToReturn timestampsToReturn,
    UA_NumericRange range, UA_Boolean releaseContinuationPoints,
    const UA_ByteString *continuationPoint, UA_ByteString *outContinuationPoint,
    UA_HistoryData *historyData
) {
    UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)backend->context)->parent;
    return parent->getHistoryData(server, sessionId, sessionContext, parent, start, end,
                                  nodeId, maxSizePerResponse, numValuesPerNode, returnBounds,
                                  timestampsToReturn, range, releaseContinuationPoints,
                                  continuationPoint, outContinuationPoint, historyData);
}

static int
callback_db_dbversion(void *context, int argc, char **argv, char **azColName) {
    UA_SqliteStoreContext *dbContext = (UA_SqliteStoreContext *)context;
    dbContext->dbSchemeVersion = -1;
    for(int i = 0; i < argc; i++) {
        FixedCharBuffer columnName = azColName[i];
        if(IsSQLColumnName(columnName, COLUMN_VERSION)) {
            const int BASE10 = 10;
            char *endPtr = NULL;
            const long versionFound = strtol(argv[i], &endPtr, BASE10);
            if(endPtr > argv[i]) {
                dbContext->dbSchemeVersion = versionFound;
            }
        }
    }
    return SQLITE_OK;
}

static void
sqliteBackend_db_createDbScheme_version_1(UA_SqliteStoreContext *context) {
    FixedCharBuffer sqlCmd = 
        "CREATE TABLE IF NOT EXISTS HISTORY ("
        "    TIMESTAMP INT PRIMARY KEY NOT NULL,"
        "    SESSIONID TEXT,"
        "    NODEID    TEXT, "
        "    DATAVALUE TEXT);"
        "CREATE TABLE IF NOT EXISTS DBVERSION (VERSION INT NOT NULL);";
    sqlite3_exec(context->sqldb, sqlCmd, NULL, NULL, NULL);
}

static void
sqliteBackend_db_upgradeScheme_0_to_1(UA_SqliteStoreContext *context) {
    sqliteBackend_db_createDbScheme_version_1(context);
    FixedCharBuffer sqlCmd =
        "INSERT OR REPLACE INTO DBVERSION (VERSION) VALUES(1)";
    sqlite3_exec(context->sqldb, sqlCmd, NULL, NULL, NULL);
}

static void
sqliteBackend_db_upgrade(UA_SqliteStoreContext *context) {
    FixedCharBuffer sqlCmd =
        "SELECT VERSION FROM DBVERSION ORDER BY VERSION DESC LIMIT 1";
    sqlite3_exec(context->sqldb, sqlCmd, callback_db_dbversion, context, NULL);
    if(context->dbSchemeVersion <= 0) {
        sqliteBackend_db_upgradeScheme_0_to_1(context);
    }
}

static void
sqliteBackend_db_open(UA_SqliteStoreContext *context, FixedCharBuffer dbFilePath) {
    int sqlRet = sqlite3_open(dbFilePath, &context->sqldb);

    if(sqlRet) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(context->sqldb));
    }
}

static void
restoreHistoryEntry(
    UA_SqliteStoreContext *context,
    FixedCharBuffer sessionIdAsJson,
    FixedCharBuffer nodeIdAsJson,
    FixedCharBuffer valueAsJson)
{
    UA_NodeId sessionId = UA_NODEID_NULL;
    UA_NodeId nodeId = UA_NODEID_NULL;
    UA_DataValue dataValue;
    UA_DataValue_init(&dataValue);

    const bool sidOk = JsonDecode_NodeId(sessionIdAsJson, &sessionId);
    const bool nidOk = JsonDecode_NodeId(nodeIdAsJson, &nodeId);
    const bool dvOk = JsonDecode_DataValue(valueAsJson, &dataValue);

    if(sidOk && nidOk && dvOk) {
        UA_HistoryDataBackend *parent = &((UA_SqliteStoreContext *)context)->parent;
        parent->serverSetHistoryData(NULL, parent->context, &sessionId, NULL, &nodeId, true, &dataValue);
    }

    UA_NodeId_clear(&sessionId);
    UA_NodeId_clear(&nodeId);
    UA_DataValue_clear(&dataValue);
}

static int
callback_db_restore_entry(void *context, int argc, char **argv, char **azColName) {
    int i;
    ConstCharBuffer nodeId = NULL;
    ConstCharBuffer sessionId = NULL;
    ConstCharBuffer dataValue = NULL;
    for(i = 0; i < argc; i++) {
        FixedCharBuffer columnName = azColName[i];
        FixedCharBuffer rowValue = argv[i];
        if(IsSQLColumnName(columnName, COLUMN_SESSIONID))
            sessionId = rowValue;
        else if(IsSQLColumnName(columnName, COLUMN_NODEID))
            nodeId = rowValue;
        else if(IsSQLColumnName(columnName, COLUMN_DATAVALUE))
            dataValue = rowValue;
    }
    if(sessionId && nodeId && dataValue)
        restoreHistoryEntry((UA_SqliteStoreContext *)context, sessionId, nodeId, dataValue);
    return SQLITE_OK;
}

static void
sqliteBackend_db_restore(UA_SqliteStoreContext *context)
{
    FixedCharBuffer sqlCmd =
        "SELECT TIMESTAMP, SESSIONID, NODEID, DATAVALUE FROM HISTORY";
    sqlite3_exec(context->sqldb, sqlCmd, callback_db_restore_entry, context, NULL);
}

static UA_SqliteStoreContext*
sqliteBackend_createDefaultStoreContext(UA_HistoryDataBackend parent)
{
    UA_SqliteStoreContext *ctx =
        (UA_SqliteStoreContext *)UA_calloc(1, sizeof(UA_SqliteStoreContext));
    if(!ctx)
        return ctx;

    ctx->parent = parent;
    ctx->pruneExecuteFunc = sqliteBackend_db_prune_execute_never;
    ctx->pruneNeededFunc = sqliteBackend_db_prune_needed_never;
    ctx->pruneRetainTimeSec = 0;
    ctx->pruneTriggerInterval = 0;
    ctx->pruneCheckCount = 0;
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

    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_Default;
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

    sqliteBackend_db_open(ctx, dbFilePath);
    sqliteBackend_db_upgrade(ctx);
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
        ctx->pruneNeededFunc = sqliteBackend_db_prune_needed_default;
        ctx->pruneExecuteFunc = sqliteBackend_db_prune_execute_circular;
        ctx->pruneTriggerInterval = pruneInterval;
        ctx->pruneCheckCount = 0;
        sqliteBackend_db_prune_if_needed(ctx);
    }

    return newBackend;
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite_TimeBuffered(UA_HistoryDataBackend parent,
                                          const char *dbFilePath, 
                                          UA_DateTime pruneRetainTimeSec,
                                          size_t maxValuesPerNode
) {
    UA_HistoryDataBackend newBackend = UA_HistoryDataBackend_SQLite(parent, dbFilePath);
    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_MaxRetainingTime;
    newBackend.getHistoryData = &getHistoryData_service_sqlite_MaxRetainingTime;

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)newBackend.context;
    if(ctx) {
        ctx->maxValuesPerNode = maxValuesPerNode;
        ctx->pruneNeededFunc = sqliteBackend_db_prune_needed_default;
        ctx->pruneExecuteFunc = sqliteBackend_db_prune_execute_timed;
        ctx->pruneRetainTimeSec = pruneRetainTimeSec;
        ctx->pruneTriggerInterval = 10;
        ctx->pruneCheckCount = 0;
        sqliteBackend_db_prune_if_needed(ctx);
    }

    return newBackend;
}
