/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.data.presto.eventlistener;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryEventInfo
{
    private static final String createEvent = "create";
    private static final String completeEvent = "complete";

    private final String queryId;
    private final String state;
    private final String eventType;
    private final String janusId;
    private final String transactionId;
    private final String translationId;
    private final String benchmarkId;
    private final String user;
    private final String engine;  // eg. presto or jarvis
    private final String cluster; // eg. sjc1_secure or dca1_nonsec
    private final String source;
    private final String catalog;
    private final String resourceGroup;
    private final String schema;
    private final String query;
    private final long createTime;
    private final String remoteClientAddress;
    private final String userAgent;
    private final String sessionProperties;
    private int totalTasks;
    private int totalStages;
    private long endTime;
    private long elapsedTime;
    private long queuedTime;
    private double memory;
    private long cpuTime; // aggregate across all tasks
    private long wallTime; // aggregate across all tasks
    private long blockedTime; // aggregate across all tasks
    private long analysisTime;
    private long distributedPlanningTime;
    private long totalDrivers;          // aka completedSplits
    private long peakMemoryReservation; // aka peakMemoryBytes
    private long rawInputPositions;     // aka totalRows
    private long rawInputDataSize;      // aka totalBytes
    private long errorCode;
    private String errorType;
    private String errorName;
    private String failureJson;
    private List<Map<String, Object>> columnAccess;
    private List<String> predicates;
    private List<String> operatorSummaries;

    private class ColumnAccessEntry
    {
        private final String database;
        private final String table;
        private final List<String> columns;

        ColumnAccessEntry(String database, String table, List<String> columns)
        {
            this.database = database;
            this.table = table;
            this.columns = columns;
        }

        public Map<String, Object> toMap()
        {
            List<String> columnNames = new ArrayList<>();
            for (String column : columns) {
                // Change "Column{driver_flow, varchar}" to "driver_flow"
                int start = column.indexOf("{");
                int end = column.indexOf(",");
                if (end == -1) {
                    end = column.length();
                }
                String columnName = column.substring(start + 1, end);

                columnNames.add(columnName);
            }

            Map<String, Object> map = new HashMap<>();
            map.put("database", this.database);
            map.put("table", this.table);
            map.put("columns", columnNames);
            return map;
        }
    }

    List<Map<String, Object>> getColumnAccess(List<QueryInputMetadata> queryInputMetadataList)
    {
        List<Map<String, Object>> columnAccessList = new ArrayList<>();
        for (QueryInputMetadata inputMetadata : queryInputMetadataList) {
            ColumnAccessEntry columnAccessEntry =
                    new ColumnAccessEntry(inputMetadata.getSchema(), inputMetadata.getTable(), inputMetadata.getColumns());
            columnAccessList.add(columnAccessEntry.toMap());
        }

        return columnAccessList;
    }

    QueryEventInfo(QueryCreatedEvent queryCreatedEvent, String engine, String cluster)
    {
        QueryContext queryContext = queryCreatedEvent.getContext();
        QueryMetadata queryMetadata = queryCreatedEvent.getMetadata();

        this.cluster = cluster;
        this.engine = engine;
        this.eventType = createEvent;

        // Query Medatada
        this.queryId = queryMetadata.getQueryId();
        this.state = queryMetadata.getQueryState();
        this.transactionId = queryMetadata.getTransactionId().orElse("");
        this.query = queryMetadata.getQuery();

        // Query Context
        this.user = queryContext.getUser();
        this.source = queryContext.getSource().orElse("");
        this.catalog = queryContext.getCatalog().orElse("");
        this.resourceGroup = queryContext.getResourceGroupName().orElse("");
        this.schema = queryContext.getSchema().orElse("");
        this.janusId = getJanusId(this.query);
        this.translationId = getTranslationId(this.query);
        this.benchmarkId = getBenchmarkId(this.query);
        this.remoteClientAddress = queryContext.getRemoteClientAddress().orElse("");
        this.userAgent = queryContext.getUserAgent().orElse("");
        this.sessionProperties = getSessionPropertiesString(queryContext.getSessionProperties());

        // Time related
        this.createTime = queryCreatedEvent.getCreateTime().getEpochSecond();
    }

    QueryEventInfo(QueryCompletedEvent queryCompletedEvent, String engine, String cluster)
    {
        QueryMetadata queryMetadata = queryCompletedEvent.getMetadata();
        QueryContext queryContext = queryCompletedEvent.getContext();
        QueryIOMetadata queryIOMetadata = queryCompletedEvent.getIoMetadata();
        QueryStatistics queryStatistics = queryCompletedEvent.getStatistics();

        this.engine = engine;
        this.cluster = cluster;
        this.eventType = completeEvent;

        // Query Metadata
        this.queryId = queryMetadata.getQueryId();
        this.state = queryMetadata.getQueryState();
        this.transactionId = queryMetadata.getTransactionId().orElse("");
        this.query = queryMetadata.getQuery();

        // Query Context
        this.user = queryContext.getUser();
        this.source = queryContext.getSource().orElse("");
        this.catalog = queryContext.getCatalog().orElse("");
        this.resourceGroup = queryContext.getResourceGroupName().orElse("");
        this.schema = queryContext.getSchema().orElse("");
        this.janusId = getJanusId(this.query);
        this.translationId = getTranslationId(this.query);
        this.benchmarkId = getBenchmarkId(this.query);
        this.remoteClientAddress = queryContext.getRemoteClientAddress().orElse("");
        this.userAgent = queryContext.getUserAgent().orElse("");
        this.sessionProperties = getSessionPropertiesString(queryContext.getSessionProperties());

        // Time related
        this.createTime = queryCompletedEvent.getCreateTime().getEpochSecond();
        this.endTime = queryCompletedEvent.getEndTime().getEpochSecond();
        this.elapsedTime = endTime - createTime;

        // Query Statistics
        this.totalTasks = queryStatistics.getTotalTasks();
        this.totalStages = queryStatistics.getStageGcStatistics().size();
        this.queuedTime = queryStatistics.getQueuedTime().toMillis();
        this.memory = queryStatistics.getCumulativeMemory();
        this.cpuTime = queryStatistics.getCpuTime().getSeconds();
        this.wallTime = queryStatistics.getWallTime().getSeconds();
        this.blockedTime = queryStatistics.getBlockedTime().getSeconds();
        this.analysisTime = queryStatistics.getAnalysisTime().orElse(Duration.ZERO).toMillis();
        this.distributedPlanningTime = queryStatistics.getDistributedPlanningTime().orElse(Duration.ZERO).toMillis();
        this.totalDrivers = queryStatistics.getCompletedSplits();
        this.peakMemoryReservation = queryStatistics.getPeakTotalNonRevocableMemoryBytes();
        this.rawInputPositions = queryStatistics.getTotalRows();
        this.rawInputDataSize = queryStatistics.getTotalBytes();

        // Query IOMetadata
        this.columnAccess = getColumnAccess(queryIOMetadata.getInputs());

        // Column Predicates
        this.predicates = queryIOMetadata.getColumnDomains();

        this.operatorSummaries = queryStatistics.getOperatorSummaries();

        // Failure related
        if (queryCompletedEvent.getFailureInfo().isPresent()) {
            QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();

            this.errorCode = queryFailureInfo.getErrorCode().getCode();
            this.errorType = queryFailureInfo.getErrorCode().getType().toString();
            this.errorName = queryFailureInfo.getErrorCode().getName();
            this.failureJson = queryFailureInfo.getFailuresJson();
        }
        else {
            this.errorCode = 0;
            this.errorType = "";
            this.errorName = "";
            this.failureJson = "";
        }
    }

    String getQueryId()
    {
        return this.queryId;
    }

    String getState()
    {
        return this.state;
    }

    // text is like `"JanusId": "ec84b317-1a53-4cfb-bd3f-3e77df79bd90"`
    String getJanusId(String query)
    {
        Pattern p = Pattern.compile("\"JanusId\": \"(.+?)\"");
        Matcher matcher = p.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    // text is like `"TranslationId": "ec84b317-1a53-4cfb-bd3f-3e77df79bd90"`
    String getTranslationId(String query)
    {
        Pattern p = Pattern.compile("\"TranslationId\": \"(.+?)\"");
        Matcher matcher = p.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    // text is like `"Benchmark": "00000001-0000-0000-0000-000000000003"`
    String getBenchmarkId(String query)
    {
        Pattern p = Pattern.compile("\"Benchmark\": \"(.+?)\"");
        Matcher matcher = p.matcher(query);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    String getSessionPropertiesString(Map<String, String> sessionProperties)
    {
        String strSessionProperties = "";
        for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
            strSessionProperties += ("\"" + entry.getKey() + "\":\"" + entry.getValue() + "\",");
        }
        return strSessionProperties;
    }

    public Map<String, Object> toMap()
    {
        Map<String, Object> map = new HashMap<>();
        map.put("queryId", this.queryId);
        map.put("state", this.state);
        map.put("eventType", this.eventType);
        map.put("transactionId", this.transactionId);
        map.put("janusId", this.janusId);
        map.put("translationId", this.translationId);
        map.put("benchmarkId", this.benchmarkId);
        map.put("user", this.user);
        map.put("engine", this.engine);
        map.put("cluster", this.cluster);
        map.put("source", this.source);
        map.put("catalog", this.catalog);
        map.put("resourceGroup", this.resourceGroup);
        map.put("schema", this.schema);
        map.put("query", this.query);
        map.put("createTime", this.createTime);
        map.put("endTime", this.endTime);
        map.put("elapsedTime", this.elapsedTime);
        map.put("queuedTime", this.queuedTime);
        map.put("memory", this.memory);
        map.put("cpuTime", this.cpuTime);
        map.put("wallTime", this.wallTime);
        map.put("blockedTime", this.blockedTime);
        map.put("analysisTime", this.analysisTime);
        map.put("distributedPlanningTime", this.distributedPlanningTime);
        map.put("totalDrivers", this.totalDrivers);
        map.put("peakMemoryReservation", this.peakMemoryReservation);
        map.put("rawInputPositions", this.rawInputPositions);
        map.put("rawInputDataSize", this.rawInputDataSize);
        map.put("columnAccess", this.columnAccess);
        map.put("remoteClientAddress", this.remoteClientAddress);
        map.put("userAgent", this.userAgent);
        map.put("sessionProperties", this.sessionProperties);
        map.put("errorCode", this.errorCode);
        map.put("errorType", this.errorType);
        map.put("errorName", this.errorName);
        map.put("failureJson", this.failureJson);
        map.put("totalTasks", this.totalTasks);
        map.put("totalStages", this.totalStages);
        map.put("predicates", this.predicates);
        map.put("operatorSummaries", this.operatorSummaries);
        return map;
    }
}
