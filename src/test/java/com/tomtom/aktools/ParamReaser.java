package com.tomtom.aktools;

import java.util.Objects;
import java.util.Optional;

final class ParamReaser {
    private static Integer getTestParameter(String paramName, int defaultValue) {
        return Optional.ofNullable(System.getenv(paramName)).filter(Objects::nonNull).map(Integer::parseInt).orElse(defaultValue);
    }

    private static String getTestParameter(String paramName, String defaultValue) {
        return Optional.ofNullable(System.getenv(paramName)).filter(Objects::nonNull).map(Object::toString).orElse(defaultValue);
    }
}
