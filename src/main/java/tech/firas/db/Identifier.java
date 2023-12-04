package tech.firas.db;

import java.util.regex.Pattern;

public final class Identifier {

    public static final Pattern PATTERN = Pattern.compile("[A-Za-z_]\\w*");

    private Identifier() throws IllegalAccessException {
        throw new IllegalAccessException();
    }
}
