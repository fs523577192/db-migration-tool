/*
 * Copyright 2023
 *
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

package tech.firas.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

public interface DbMetaReader extends DbMetaUtil {

    Set<Schema> read(Connection connection) throws SQLException;

    Set<Table> readTables(Connection connection, Schema schema) throws SQLException;

    Map<String, Column> readColumns(Connection connection, Table table) throws SQLException;
    Map<String, Index> readIndexes(Connection connection, Table table) throws SQLException;

    String selectAllSqlFor(Table table);
    String selectByPrimaryKeySqlFor(Table table);
}
