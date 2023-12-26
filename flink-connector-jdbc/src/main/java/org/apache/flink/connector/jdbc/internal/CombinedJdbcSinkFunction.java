/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Map;

/** A generic SinkFunction for JDBC. */
@Internal
public class CombinedJdbcSinkFunction<T extends Tuple2<String, RowData>> extends RichSinkFunction<T>
        implements CheckpointedFunction, InputTypeConfigurable {
    private final Map<String, JdbcOutputFormat<RowData, ?, ?>> outputFormats;
    private final Map<String, TypeInformation<?>> rowTypeHashMap;

    public CombinedJdbcSinkFunction(
            @Nonnull Map<String, JdbcOutputFormat<RowData, ?, ?>> outputFormats,
            @Nonnull Map<String, TypeInformation<?>> rowTypeHashMap) {
        this.outputFormats = Preconditions.checkNotNull(outputFormats);
        this.rowTypeHashMap = Preconditions.checkNotNull(rowTypeHashMap);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormats.values().forEach(f -> f.setRuntimeContext(ctx));
        for (JdbcOutputFormat<RowData, ?, ?> f : outputFormats.values()) {
            f.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        }
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        outputFormats.get(value.f0).writeRecord(value.f1);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (JdbcOutputFormat<RowData, ?, ?> rowDataJdbcOutputFormat : outputFormats.values()) {
            rowDataJdbcOutputFormat.flush();
        }
    }

    @Override
    public void close() {
        outputFormats.values().forEach(JdbcOutputFormat::close);
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        for (Map.Entry<String, JdbcOutputFormat<RowData, ?, ?>> entry : outputFormats.entrySet()) {
            entry.getValue().setInputType(rowTypeHashMap.get(entry.getKey()), executionConfig);
        }
    }
}
