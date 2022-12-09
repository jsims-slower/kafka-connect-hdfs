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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class SqlCache {
  private final Map<JdbcTableInfo, List<JdbcColumn>> allColumnsMap = new HashMap<>();
  private final Map<JdbcTableInfo, List<JdbcColumn>> primaryKeyColumnsMap = new HashMap<>();
  private final JdbcConnection jdbcConnection;
  private final RetrySpec retrySpec;

  public SqlCache(JdbcConnection jdbcConnection,
                  RetrySpec retrySpec) {

    this.jdbcConnection = jdbcConnection;
    this.retrySpec = retrySpec;
  }

  public synchronized List<JdbcColumn> fetchAllColumns(JdbcTableInfo tableInfo) {
    List<JdbcColumn> allColumns = allColumnsMap.get(tableInfo);

    if (allColumns == null) {
      allColumns = jdbcConnection.fetchAllColumns(retrySpec, tableInfo);
      allColumnsMap.put(tableInfo, allColumns);
    }
    return allColumns;
  }

  public synchronized List<JdbcColumn> fetchPrimaryKeyColumns(JdbcTableInfo tableInfo) {
    List<JdbcColumn> primaryKeyColumns = primaryKeyColumnsMap.get(tableInfo);

    if (primaryKeyColumns == null) {
      Collection<String> primaryKeyNames =
          jdbcConnection.fetchPrimaryKeyNames(retrySpec, tableInfo);

      // TODO: Do we need to check and make sure the PK exists in the list of columns?
      primaryKeyColumns =
          fetchAllColumns(tableInfo)
              .stream()
              .filter(column -> primaryKeyNames.contains(column.getName()))
              .collect(Collectors.toList());

      primaryKeyColumnsMap.put(tableInfo, primaryKeyColumns);
    }
    return primaryKeyColumns;
  }
}