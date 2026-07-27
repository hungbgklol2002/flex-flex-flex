package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util;

import java.util.Objects;

public final class StringUtils {

    private StringUtils() {
    }

    public static String trim(Object value) {
        return Objects.toString(value, "").trim();
    }
    public static String defaultString(Object value) {
        return value == null ? "" : value.toString();
    }
}