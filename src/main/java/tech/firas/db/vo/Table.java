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

package tech.firas.db.vo;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import tech.firas.db.Identifier;
import tech.firas.db.vo.Index.IndexType;

@NoArgsConstructor
public class Table implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter @Setter private Schema schema;

    @Getter private String name;

    @Getter private Map<String, Column> columnMap;

    @Getter private Map<String, Index> indexMap;

    @Getter @Setter private String comment;

    public Table(final Schema schema, final String name) {
        this.setSchema(schema);
        this.setName(name);
    }

    public Table(final String name) {
        this(null, name);
    }

    public void setName(final String name) {
        if (!Identifier.PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + name);
        }
        this.name = name;
    }

    public void setColumnMap(final Map<String, Column> columnMap) {
        if (Objects.requireNonNull(columnMap, "columnMap must not be null").isEmpty()) {
            throw new IllegalArgumentException("columnMap must not be empty");
        }
        for (final Map.Entry<String, Column> entry : columnMap.entrySet()) {
            final String columnName = entry.getKey();
            if (columnName == null || columnName.isEmpty()) {
                throw new IllegalArgumentException("There is at least one invalid columnName");
            }
            final Column column = entry.getValue();
            if (column == null) {
                throw new IllegalArgumentException("There is at least one null column");
            }
            if (!columnName.equals(column.getName())) {
                throw new IllegalArgumentException("The key of 'columnMap' must be the name of the column");
            }
        }
        this.columnMap = columnMap;
    }

    public void setIndexMap(final Map<String, Index> indexMap) {
        int primaryKeyCount = 0;
        Objects.requireNonNull(indexMap, "indexMap must not be null");
        for (final Map.Entry<String, Index> entry : indexMap.entrySet()) {
            final Index index = entry.getValue();
            if (index == null) {
                throw new IllegalArgumentException("THere is at least one null index");
            }
            if (!Objects.equals(entry.getKey(), index.getName())) {
                throw new IllegalArgumentException("The key of 'indexMap' must be the name of the index");
            }
            if (index.getIndexType() == IndexType.PRIMARY_KEY) {
                if ((++primaryKeyCount) > 1) {
                    throw new IllegalArgumentException("There must be at most one primary key for one table");
                }
            }
        }
        this.indexMap = indexMap;
    }

    public Collection<Column> getPrimaryKeyColumns() {
        return this.getIndexMap().values().stream()
                .filter(index -> index.getIndexType() == IndexType.PRIMARY_KEY)
                .findFirst()
                .map(index -> (Collection<Column>) index.getColumns())
                .orElse(this.getColumnMap().values());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Table table = (Table) o;
        return Objects.equals(this.schema, table.schema) &&
                Objects.equals(this.name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.schema, this.name);
    }

    @Override
    public String toString() {
        return "Table{" +
                "schema=" + this.schema +
                ", name='" + name + '\'' +
                '}';
    }
}
