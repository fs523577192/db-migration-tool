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

import java.util.Objects;
import java.util.stream.Collectors;

import tech.firas.db.vo.Table;

public abstract class AbstractDbMetaUtil implements DbMetaUtil {

    @Override
    public String quote(final String identifier) {
        return identifier;
    }

    @Override
    public String tableName(final Table table) {
        Objects.requireNonNull(table, "table must not be null");
        if (table.getSchema() == null) {
            return table.getName();
        }
        return this.quote(table.getSchema().getName()) + '.' + this.quote(table.getName());
    }

    @Override
    public String whereSqlForPrimaryKey(final Table table) {
        Objects.requireNonNull(table, "table must not be null");
        return "WHERE " + table.getPrimaryKeyColumns().stream()
                .map(column -> this.quote(column.getName()) + " = ?")
                .collect(Collectors.joining(" AND "));
    }
}
