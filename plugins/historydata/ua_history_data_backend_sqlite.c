/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 *    Copyright 2022 (c) rjapeer <open62541@peerpoint.nl> (Author: Ronald Peer)
 */

#include <open62541/plugin/historydata/history_data_backend_sqlite.h>
#include <open62541/plugin/historydata/sqlite/sqlite3.h>
#include <open62541/plugin/log_stdout.h>

typedef char*              CharBuffer;
typedef char*              SQLCharBuffer;
typedef const char *       ConstCharBuffer;
typedef const char * const FixedCharBuffer;

static FixedCharBuffer COLUMN_VERSION = "VERSION";
static FixedCharBuffer COLUMN_DATAVALUE = "DATAVALUE";

#define LOWLEVELITF_NOTIMPL (NULL)

typedef struct UA_SqliteStoreContext {
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

typedef struct SqliteGetHistoryValuesContext {
    size_t nrValuesFound;
    size_t maxNrDataValues;
    UA_TimestampsToReturn timestampsToReturn;
    UA_DataValue *dataValues;
    UA_NumericRange requestedRange;
} SqliteGetHistoryValuesContext;

static void
UA_SqliteStoreContext_clear(UA_SqliteStoreContext *ctx) {
    if(ctx->sqldb)
        sqlite3_close(ctx->sqldb);
    memset(ctx, 0, sizeof(UA_SqliteStoreContext));
}

static void
UA_SqliteStoreContext_delete(UA_SqliteStoreContext *ctx)
{
    UA_SqliteStoreContext_clear(ctx);
    UA_free(ctx);
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
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;

    if(historizing) {
        res = sqliteBackend_db_storeHistoryEntry(ctx, sessionId, nodeId, value);
    }
    return res;
}

static UA_StatusCode
serverSetHistoryData_backend_sqlite_Circular(UA_Server *server, void *context, 
                                             const UA_NodeId *sessionId, void *sessionContext,
                                             const UA_NodeId *nodeId, UA_Boolean historizing,
                                             const UA_DataValue *value)
{
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;

    if(historizing) {
        res = sqliteBackend_db_storeHistoryEntry(ctx, sessionId, nodeId, value);
        sqliteBackend_db_prune_if_needed(ctx);
    }
    return res;
}

static UA_StatusCode
serverSetHistoryData_backend_sqlite_MaxRetainingTime(
    UA_Server *server, void *context, const UA_NodeId *sessionId, void *sessionContext,
    const UA_NodeId *nodeId, UA_Boolean historizing, const UA_DataValue *value
) {
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)context;

    if(historizing) {
        res = sqliteBackend_db_storeHistoryEntry(ctx, sessionId, nodeId, value);
        sqliteBackend_db_prune_if_needed(ctx);
    }
    return res;
}

static UA_Boolean
boundSupported_backend_sqlite(UA_Server *server, void *context,
                                  const UA_NodeId *sessionId, void *sessionContext,
                                  const UA_NodeId *nodeId)
{
    return true;
}

static UA_Boolean
timestampsToReturnSupported_backend_sqlite(UA_Server *server, void *context, 
                                           const UA_NodeId *sessionId, void *sessionContext,
                                           const UA_NodeId *nodeId,
                                           const UA_TimestampsToReturn timestampsToReturn)
{
    UA_Boolean supported = true;

    switch(timestampsToReturn) {
        case UA_TIMESTAMPSTORETURN_SOURCE:
        break;
        case UA_TIMESTAMPSTORETURN_SERVER:
        break;
        case UA_TIMESTAMPSTORETURN_BOTH:
            break;
        case UA_TIMESTAMPSTORETURN_NEITHER:
        case UA_TIMESTAMPSTORETURN_INVALID:
        default:
            supported = false;
            break;
    }
    return supported;
}

static UA_StatusCode
insertDataValue_backend_sqlite(UA_Server *server, void *hdbContext,
                               const UA_NodeId *sessionId, void *sessionContext,
                               const UA_NodeId *nodeId, const UA_DataValue *value)
{
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;

    const UA_DateTime timestamp = uaValueTimeStamp(value);
    if(!timestamp) {
        return UA_STATUSCODE_BADINVALIDTIMESTAMP;
    }

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
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;

    const UA_DateTime timestamp = uaValueTimeStamp(value);
    if(!timestamp) {
        return UA_STATUSCODE_BADINVALIDTIMESTAMP;
    }

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
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;

    const UA_DateTime timestamp = uaValueTimeStamp(value);
    if(!timestamp) {
        return UA_STATUSCODE_BADINVALIDTIMESTAMP;
    }

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
    UA_StatusCode res = UA_STATUSCODE_GOOD;
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)hdbContext;

    if(startTimestamp > endTimestamp) {
        return UA_STATUSCODE_BADTIMESTAMPNOTSUPPORTED;
    }

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

    UA_SqliteStoreContext_clear((UA_SqliteStoreContext *)backend->context);
    UA_free(backend->context);
}

static int
callback_db_getHistoryEntries(void *context, int argc, char **argv, char **azColName) {
    SqliteGetHistoryValuesContext *ctx = (SqliteGetHistoryValuesContext *)context;
    UA_DataValue* currentValue = &ctx->dataValues[ctx->nrValuesFound];

    if(ctx->nrValuesFound >= ctx->maxNrDataValues)
        return SQLITE_OK;

    for(int i = 0; i < argc; i++) {
        FixedCharBuffer columnName = azColName[i];
        FixedCharBuffer columnValue = argv[i];
        if(IsSQLColumnName(columnName, COLUMN_DATAVALUE)) {
            UA_DataValue dataValue;
            UA_DataValue_init(&dataValue);
            const bool dvOk = JsonDecode_DataValue(columnValue, &dataValue);
            if(dvOk) {
                UA_TimestampsToReturn timestampMode = ctx->timestampsToReturn;
                const bool removeSourceTimeStamp = timestampMode == UA_TIMESTAMPSTORETURN_SERVER;
                const bool removeServerTimeStamp = timestampMode == UA_TIMESTAMPSTORETURN_SOURCE;
                if(removeServerTimeStamp) {
                    dataValue.hasServerTimestamp = FALSE;
                    dataValue.hasServerPicoseconds = FALSE;
                    dataValue.serverPicoseconds = 0u;
                    dataValue.serverTimestamp = 0u;
                }
                if(removeSourceTimeStamp) {
                    dataValue.hasSourceTimestamp = FALSE;
                    dataValue.hasSourcePicoseconds = FALSE;
                    dataValue.sourceTimestamp = 0u;
                    dataValue.sourcePicoseconds = 0u;
                }
                UA_NumericRange range = ctx->requestedRange;
                if(range.dimensionsSize > 0) {
                    memcpy(currentValue, &dataValue, sizeof(UA_DataValue));
                    if(dataValue.hasValue)
                        UA_Variant_copyRange(&dataValue.value, &currentValue->value, range);
                } else {
                    UA_DataValue_copy(&dataValue, currentValue);
                }
                ctx->nrValuesFound++;
            } else {
                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Bad history entry found (%s)", columnValue);
            }
        }
    }
    return SQLITE_OK;
}

/* This function is the high level interface for the ReadRaw operation. Used since
 * we don't us the low level interface. All low level api functions are set to NULL.
 *
 * server               server the node lives in (not used)
 * sessionId            identifies the session that wants to read historical data (not used)
 * sessionContext       context of the session that wants to read historical data (not used)
 * backend              HistoryDataBackend whose storage is to be queried.
 * start                start time of the HistoryRead request.
 * end                  end time of the HistoryRead request.
 * nodeId               node id of the node for which historical data is requested.
 * maxSizePerResponse   maximum number of items per response the server can provide.
 *                      if more are requested, continuation points are used.
 * numValuesPerNode     maximum number of items per response the client wants to receive.
 * returnBounds         determines if the client wants to receive bounding values.
 * timestampsToReturn   contains the time stamps the client is interested in.
 * range                numeric range the client wants to read.
 * releaseContinuationPoints determines if the continuation points shall be released.
 * continuationPoint    continuation point the client wants to release or start from.
 * outContinuationPoint continuation point that gets passed to the client by the HistoryRead service.
 * historyData          history data that gets passed back to the client.
 *
 * returns UA_STATUSCODE_GOOD on success
 */
static UA_StatusCode
getHistoryData_service_sqlite(
    UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
    const UA_HistoryDataBackend *backend, const UA_DateTime start, const UA_DateTime end,
    const UA_NodeId *nodeId, size_t maxSizePerResponse, UA_UInt32 numValuesPerNode,
    UA_Boolean returnBounds, UA_TimestampsToReturn timestampsToReturn,
    UA_NumericRange range, UA_Boolean releaseContinuationPoints,
    const UA_ByteString *continuationPoint, UA_ByteString *outContinuationPoint,
    UA_HistoryData *historyData
) {
    UA_StatusCode res = UA_STATUSCODE_GOOD;

    const UA_SqliteStoreContext *ctx = (const UA_SqliteStoreContext *)(backend->context);

    size_t nrEntriesToSkip = 0;
    if(continuationPoint->length > 0) {
        if(continuationPoint->length < sizeof(size_t))
            return UA_STATUSCODE_BADCONTINUATIONPOINTINVALID;
        nrEntriesToSkip = *((size_t *)(continuationPoint->data));
    }

    SqliteGetHistoryValuesContext getValuesContext;
    getValuesContext.maxNrDataValues = maxSizePerResponse + 1;
    getValuesContext.nrValuesFound = 0;
    getValuesContext.timestampsToReturn = timestampsToReturn;
    getValuesContext.dataValues = (UA_DataValue *)UA_Array_new(
        getValuesContext.maxNrDataValues, &UA_TYPES[UA_TYPES_DATAVALUE]);
    getValuesContext.requestedRange = range;

    const size_t maxNrEntriesToSelect =
        (maxSizePerResponse < numValuesPerNode || numValuesPerNode == 0)
            ? (maxSizePerResponse + 1) // Peek one ahead to check for continuation
            : numValuesPerNode;
    const bool startAtOldest = (end == 0 || start < end);
    FixedCharBuffer orderBySorting = (startAtOldest ? "ASC" : "DESC");
    const UA_DateTime oldestToGet = (startAtOldest ? start : end);
    const UA_DateTime latestToGet = (startAtOldest ? end : start);

    CharBuffer nodeIdCStr = AllocUaNodeIdAsJsonCStr(nodeId);

    FixedCharBuffer sqlBaseFmt    = "SELECT * FROM HISTORY WHERE NODEID = %Q";
    FixedCharBuffer sqlStartFmt   = "   AND TIMESTAMP >%s %lld ";
    FixedCharBuffer sqlEndFmt     = "   AND TIMESTAMP <%s %lld ";
    FixedCharBuffer sqlOrderFmt   = " ORDER BY TIMESTAMP %s ";
    FixedCharBuffer sqlLimitFmt   = " LIMIT %u";
    FixedCharBuffer sqlOffsetFmt  = " OFFSET %u";

    sqlite3_str* sqlCmdStr = sqlite3_str_new(ctx->sqldb);
    sqlite3_str_appendf(sqlCmdStr, sqlBaseFmt, nodeIdCStr);
    if(start != 0)
        sqlite3_str_appendf(sqlCmdStr, sqlStartFmt, returnBounds ? "=" : "", oldestToGet);
    if(end != 0)
        sqlite3_str_appendf(sqlCmdStr, sqlEndFmt, returnBounds ? "=" : "", latestToGet);
    sqlite3_str_appendf(sqlCmdStr, sqlOrderFmt, orderBySorting);
    if(maxNrEntriesToSelect > 0)
        sqlite3_str_appendf(sqlCmdStr, sqlLimitFmt, maxNrEntriesToSelect);
    if(nrEntriesToSkip > 0)
        sqlite3_str_appendf(sqlCmdStr, sqlOffsetFmt, nrEntriesToSkip);
    SQLCharBuffer sqlCmd = sqlite3_str_finish(sqlCmdStr);

    if(sqlCmd) {
        sqlite3_exec(ctx->sqldb, sqlCmd, callback_db_getHistoryEntries, &getValuesContext, NULL);
        DeleteSQLCharBuffer(&sqlCmd);
    }

    DeleteCharBuffer(&nodeIdCStr);

    // Don't return the continuation peek ahead entry if needed
    const bool continuationNeeded = getValuesContext.nrValuesFound > maxSizePerResponse;
    const size_t nrValuesToReturn = (continuationNeeded ? maxSizePerResponse : getValuesContext.nrValuesFound);
    if(nrValuesToReturn > 0) {
        // There is data to return
        historyData->dataValuesSize = nrValuesToReturn;
        historyData->dataValues = (UA_DataValue *)UA_Array_new(nrValuesToReturn, &UA_TYPES[UA_TYPES_DATAVALUE]);
        res = UA_Array_copy(getValuesContext.dataValues, nrValuesToReturn,
                            (void **)&historyData->dataValues, &UA_TYPES[UA_TYPES_DATAVALUE]);

        if (UA_StatusCode_isGood(res) && continuationNeeded) {
            UA_ByteString newContinuationPoint;
            UA_ByteString_init(&newContinuationPoint);
            newContinuationPoint.length = sizeof(size_t);
            newContinuationPoint.data = (UA_Byte*)UA_malloc(newContinuationPoint.length);
            if(newContinuationPoint.data) {
                *((size_t *)(newContinuationPoint.data)) = nrEntriesToSkip + maxSizePerResponse;
                UA_ByteString_copy(&newContinuationPoint, outContinuationPoint);
                UA_ByteString_clear(&newContinuationPoint);
            } else {
                res = UA_STATUSCODE_BADOUTOFMEMORY;
            }
        }
    }

    UA_Array_delete(getValuesContext.dataValues, getValuesContext.maxNrDataValues, &UA_TYPES[UA_TYPES_DATAVALUE]);
    getValuesContext.dataValues = NULL;

    return res;
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
sqliteBackend_db_reset(UA_SqliteStoreContext *context)
{
    FixedCharBuffer sqlCmd = "DELETE FROM HISTORY WHERE NODEID LIKE '%'";
    sqlite3_exec(context->sqldb, sqlCmd, NULL, NULL, NULL);
}

static void
sqliteBackend_db_open(UA_SqliteStoreContext *context, FixedCharBuffer dbFilePath) {
    int sqlRet = sqlite3_open(dbFilePath, &context->sqldb);

    if(sqlRet) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(context->sqldb));
    }
}

static UA_SqliteStoreContext*
sqliteBackend_createDefaultStoreContext(void)
{
    UA_SqliteStoreContext *ctx =
        (UA_SqliteStoreContext *)UA_calloc(1, sizeof(UA_SqliteStoreContext));
    if(!ctx)
        return ctx;

    ctx->pruneExecuteFunc = sqliteBackend_db_prune_execute_never;
    ctx->pruneNeededFunc = sqliteBackend_db_prune_needed_never;
    ctx->pruneRetainTimeSec = 0;
    ctx->pruneTriggerInterval = 0;
    ctx->pruneCheckCount = 0;
    ctx->maxValuesPerNode = 0;
    return ctx;
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite(const char* dbFilePath, UA_Boolean resetHistory) 
{
    UA_HistoryDataBackend newBackend;
    memset(&newBackend, 0, sizeof(UA_HistoryDataBackend));

    UA_SqliteStoreContext *ctx = sqliteBackend_createDefaultStoreContext();
    if (!ctx)
        return newBackend;

    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_Default;
    newBackend.resultSize = LOWLEVELITF_NOTIMPL; // &resultSize_backend_sqlite;
    newBackend.getEnd = LOWLEVELITF_NOTIMPL; // &getEnd_backend_sqlite;
    newBackend.lastIndex = LOWLEVELITF_NOTIMPL; // &lastIndex_backend_sqlite;
    newBackend.firstIndex = LOWLEVELITF_NOTIMPL; // &firstIndex_backend_sqlite;
    newBackend.getDateTimeMatch = LOWLEVELITF_NOTIMPL; // &getDateTimeMatch_backend_sqlite;
    newBackend.copyDataValues = LOWLEVELITF_NOTIMPL; // &copyDataValues_backend_sqlite;
    newBackend.getDataValue = LOWLEVELITF_NOTIMPL; // &getDataValue_backend_sqlite;
    newBackend.boundSupported = &boundSupported_backend_sqlite;
    newBackend.timestampsToReturnSupported = &timestampsToReturnSupported_backend_sqlite;
    newBackend.insertDataValue = &insertDataValue_backend_sqlite;
    newBackend.updateDataValue = &updateDataValue_backend_sqlite;
    newBackend.replaceDataValue = &replaceDataValue_backend_sqlite;
    newBackend.removeDataValue = &removeDataValue_backend_sqlite;
    newBackend.deleteMembers = &deleteMembers_backend_sqlite;
    newBackend.getHistoryData = getHistoryData_service_sqlite;

    sqliteBackend_db_open(ctx, dbFilePath);
    sqliteBackend_db_upgrade(ctx);
    if(resetHistory)
        sqliteBackend_db_reset(ctx);
    newBackend.context = ctx;

    return newBackend;
}

void
UA_HistoryDataBackend_SQLite_clear(UA_HistoryDataBackend *backend) {
    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)backend->context;
    UA_SqliteStoreContext_delete(ctx);
    memset(backend, 0, sizeof(UA_HistoryDataBackend));
}

UA_HistoryDataBackend
UA_HistoryDataBackend_SQLite_Circular(const char *dbFilePath,
                                      size_t pruneInterval,
                                      size_t maxValuesPerNode,
                                      UA_Boolean resetHistory
) {
    UA_HistoryDataBackend newBackend = UA_HistoryDataBackend_SQLite(dbFilePath, resetHistory);
    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_Circular;

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
UA_HistoryDataBackend_SQLite_TimeBuffered(const char *dbFilePath,
                                          size_t pruneInterval,
                                          UA_DateTime pruneRetainTimeSec,
                                          UA_Boolean resetHistory
) {
    UA_HistoryDataBackend newBackend = UA_HistoryDataBackend_SQLite(dbFilePath, resetHistory);
    newBackend.serverSetHistoryData = &serverSetHistoryData_backend_sqlite_MaxRetainingTime;

    UA_SqliteStoreContext *ctx = (UA_SqliteStoreContext *)newBackend.context;
    if(ctx) {
        ctx->pruneNeededFunc = sqliteBackend_db_prune_needed_default;
        ctx->pruneExecuteFunc = sqliteBackend_db_prune_execute_timed;
        ctx->pruneRetainTimeSec = pruneRetainTimeSec;
        ctx->pruneTriggerInterval = pruneInterval;
        ctx->pruneCheckCount = 0;
        sqliteBackend_db_prune_if_needed(ctx);
    }

    return newBackend;
}
