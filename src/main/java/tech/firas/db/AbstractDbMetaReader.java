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

import tech.firas.db.vo.Table;

public abstract class AbstractDbMetaReader extends AbstractDbMetaUtil implements DbMetaReader {

    @Override
    public String selectAllSqlFor(final Table table) {
        final String tableName = this.tableName(table);
        return "SELECT " + String.join(", ", table.getColumnMap().keySet()) +
                " FROM " + tableName;
    }

    @Override
    public String selectByPrimaryKeySqlFor(final Table table) {
        return this.selectAllSqlFor(table) + ' ' + this.whereSqlForPrimaryKey(table);
    }
}
