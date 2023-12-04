package tech.firas.db.vo;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import tech.firas.db.Identifier;

@NoArgsConstructor
public class Schema implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private String name;

    @Getter @Setter private Set<Table> tables;

    @Getter @Setter private String comment;

    public Schema(final String name) {
        this.setName(name);
    }

    public void setName(final String name) {
        if (!Identifier.PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid schema name: " + name);
        }
        this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Schema schema = (Schema) o;
        return Objects.equals(name, schema.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Schema{" +
                "name='" + name + '\'' +
                '}';
    }
}
