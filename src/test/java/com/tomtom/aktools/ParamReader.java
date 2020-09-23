package com.tomtom.aktools;

import org.apache.commons.lang.BooleanUtils;

import java.util.Objects;
import java.util.Optional;

final class ParamReader {
    public static Integer getTestParameter(String paramName, int defaultValue) {
        return Optional.ofNullable(System.getenv(paramName)).filter(Objects::nonNull).map(Integer::parseInt).orElse(defaultValue);
    }

    public static String getTestParameter(String paramName, String defaultValue) {
        return Optional.ofNullable(System.getenv(paramName)).filter(Objects::nonNull).map(Object::toString).orElse(defaultValue);
    }

    public static boolean getTestParameter(String paramName, boolean defaultValue) {
        return Optional.ofNullable(System.getenv(paramName)).filter(Objects::nonNull).map(BooleanUtils::toBoolean).orElse(defaultValue);
    }
}
