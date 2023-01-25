/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.jdbc;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JdbcRecordTransformer {
  //private static final String HEADER_DB = "__source_db";
  private static final String HEADER_SCHEMA = "__source_schema";
  private static final String HEADER_TABLE = "__source_table";

  private final DataSource dataSource;
  private final Map<JdbcTableInfo, Set<String>> includedFieldsLowerMap;
  private final HashCache hashCache;
  private final Function<Schema, String> schemaNameFn;

  public JdbcRecordTransformer(
      DataSource dataSource,
      Map<JdbcTableInfo, Set<String>> includedFieldsLowerMap,
      HashCache hashCache,
      Function<Schema, String> schemaNameFn
  ) {
    this.dataSource = dataSource;
    this.includedFieldsLowerMap = includedFieldsLowerMap;
    this.hashCache = hashCache;
    this.schemaNameFn = schemaNameFn;
  }

  /**
   * NOTE: Not threadsafe, as several components update things like basic Collections
   */
  public SinkRecord transformRecord(SqlMetadataCache sqlMetadataCache,
                                    SinkRecord oldRecord) throws SQLException {
    JdbcTableInfo tableInfo = toTable(oldRecord.headers());

    // Get a list of Fields requested/configured to be in the new Schema.
    // Fields: Columns, but also non-columns, which can include Kafka/Connect properties like:
    //         kafka_ts, db_ts, etc...

    Set<String> includedFieldsLower =
        Optional
            .ofNullable(includedFieldsLowerMap.get(tableInfo))
            .orElseGet(Collections::emptySet);

    // No columns/fields to Query? No need to write anything at all to HDFS

    if (includedFieldsLower.isEmpty()) {
      return null;
    }

    // Calculate the list of Columns to query

    Schema oldValueSchema = oldRecord.valueSchema();

    Map<String, Field> oldFieldsMap = toFieldsMap(oldValueSchema);

    Set<String> columnNamesLowerToQuery =
        calculateColumnNamesLowerToQuery(
            includedFieldsLower,
            oldFieldsMap
        );

    // No actual columns to Query? No need to write anything at all to HDFS

    if (columnNamesLowerToQuery.isEmpty()) {
      return null;
    }

    // Gather Column Metadata from the DB

    List<JdbcColumnInfo> primaryKeyColumns =
        sqlMetadataCache.fetchPrimaryKeyColumns(tableInfo);

    if (primaryKeyColumns.isEmpty()) {
      throw new DataException("Table has no Primary Key(s): " + tableInfo);
    }

    List<JdbcColumnInfo> columnsToQuery = calculateColumnsToQuery(sqlMetadataCache,
                                                                  tableInfo,
                                                                  columnNamesLowerToQuery,
                                                                  primaryKeyColumns);

    // Create the mew Schema and new value Struct

    Schema newValueSchema =
        JdbcSchema.createSchema(
            schemaNameFn.apply(oldValueSchema),
            oldValueSchema,
            includedFieldsLower,
            primaryKeyColumns,
            columnsToQuery
        );

    Struct newValueStruct = new Struct(newValueSchema);

    // Populate the newValueStruct with existing values from oldValueStruct

    Struct oldValueStruct = (Struct) oldRecord.value();

    newValueSchema
        .fields()
        .forEach(newField -> Optional
            .ofNullable(oldFieldsMap.get(newField.name()))
            .map(oldValueStruct::get)
            .ifPresent(oldValue -> newValueStruct.put(newField, oldValue))
        );

    // Execute the query

    String primaryKeyStr = Optional
        .ofNullable(oldRecord.key())
        .map(Object::toString)
        .flatMap(JdbcUtil::trimToNone)
        .orElseThrow(() -> new DataException("Missing Primary Key(s) for Kafka Record"));

    FilteredColumnToStructVisitor columnVisitor =
        new FilteredColumnToStructVisitor(
            hashCache,
            tableInfo,
            primaryKeyStr,
            newValueStruct
        );

    JdbcQueryUtil.executeSingletonQuery(
        dataSource,
        tableInfo,
        primaryKeyColumns,
        columnsToQuery,
        new StructToJdbcValueMapper(oldValueStruct),
        columnVisitor,
        primaryKeyStr
    );

    // Only write a record if there are changes in the columns (usually LOBs),
    // based on whether the cached Hash of each column has changed or not.
    // TODO: Make this optimization configurable, so it can be disabled from the config

    if (!columnVisitor.hasChangedColumns()) {
      return null;
    }

    // Make sure the newValueStruct is fully populated
    newValueStruct.validate();

    // Create the newly transformed SourceRecord
    return oldRecord.newRecord(
        oldRecord.topic(),
        oldRecord.kafkaPartition(),
        oldRecord.keySchema(),
        oldRecord.key(),
        newValueSchema,
        newValueStruct,
        oldRecord.timestamp()
    );
  }

  private Map<String, Field> toFieldsMap(Schema schema) {
    return schema
        .fields()
        .stream()
        .collect(Collectors.toMap(
            field -> JdbcUtil
                .trimToNone(field.name())
                // NOTE: Should be impossible to reach here!
                .orElseThrow(() -> new DataException(
                    "Field ["
                    + field.name()
                    + "] is null or empty for Schema ["
                    + schema.name()
                    + "]"
                )),
            Function.identity()
        ));
  }

  private JdbcTableInfo toTable(Headers headers) {
    //String db = Optional
    //    .ofNullable(headers.lastWithName(HEADER_DB))
    //    .map(Header::value)
    //    .map(String.class::cast)
    //    .flatMap(JdbcUtil::trimToNone)
    //    .map(String::toLowerCase)
    //    .orElseThrow(() -> new ConfigException(
    //        "Kafka Record is missing required Header ["
    //        + HEADER_SCHEMA
    //        + "]"
    //    ));

    String schema = Optional
        .ofNullable(headers.lastWithName(HEADER_SCHEMA))
        .map(Header::value)
        .map(String.class::cast)
        .flatMap(JdbcUtil::trimToNone)
        .map(String::toLowerCase)
        .orElseThrow(() -> new ConfigException(
            "Kafka Record is missing required Header ["
            + HEADER_SCHEMA
            + "]"
        ));

    String table = Optional
        .ofNullable(headers.lastWithName(HEADER_TABLE))
        .map(Header::value)
        .map(String.class::cast)
        .flatMap(JdbcUtil::trimToNone)
        .map(String::toLowerCase)
        .orElseThrow(() -> new ConfigException(
            "Kafka Record is missing required Header ["
            + HEADER_TABLE
            + "]"
        ));

    return new JdbcTableInfo(schema, table);
  }

  private Set<String> calculateColumnNamesLowerToQuery(Set<String> includedFieldsLower,
                                                       Map<String, Field> oldFieldsMap) {
    Set<String> oldFieldNamesLower =
        oldFieldsMap
            .keySet()
            .stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    return includedFieldsLower
        .stream()
        .filter(((Predicate<String>) oldFieldNamesLower::contains).negate())
        .collect(Collectors.toSet());
  }

  private List<JdbcColumnInfo> calculateColumnsToQuery(
      SqlMetadataCache sqlMetadataCache,
      JdbcTableInfo tableInfo,
      Set<String> columnNamesLowerToQuery,
      List<JdbcColumnInfo> primaryKeyColumns
  ) throws SQLException {
    Set<String> primaryKeyColumnNamesLower =
        primaryKeyColumns
            .stream()
            .map(JdbcColumnInfo::getName)
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    Map<String, JdbcColumnInfo> allColumnsLowerMap =
        sqlMetadataCache
            .fetchAllColumns(tableInfo)
            .stream()
            .collect(Collectors.toMap(
                column -> column.getName().toLowerCase(),
                Function.identity()
            ));

    return columnNamesLowerToQuery
        .stream()
        .filter(((Predicate<String>) primaryKeyColumnNamesLower::contains).negate())
        .map(columnNameLower -> Optional
            .ofNullable(allColumnsLowerMap.get(columnNameLower))
            .orElseThrow(() -> new DataException(
                "Configured Column ["
                + columnNameLower
                + "] does not exist in Table ["
                + tableInfo
                + "]"
            ))
        )
        .sorted(JdbcColumnInfo.byOrdinal)
        .collect(Collectors.toList());
  }
}
