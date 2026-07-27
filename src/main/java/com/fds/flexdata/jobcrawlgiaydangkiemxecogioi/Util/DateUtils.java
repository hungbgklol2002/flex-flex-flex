package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public final class DateUtils {

    private static final String DEFAULT_PATTERN = "yyyy-MM-dd";

    private DateUtils() {
    }

    public static String toDateString(Object value) {
        return toDateString(value, DEFAULT_PATTERN);
    }

    public static String toDateString(Object value, String pattern) {

        if (value == null) {
            return null;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);

        if (value instanceof Timestamp ts) {
            return ts.toLocalDateTime()
                    .toLocalDate()
                    .format(formatter);
        }

        if (value instanceof LocalDateTime ldt) {
            return ldt.toLocalDate()
                    .format(formatter);
        }

        if (value instanceof LocalDate ld) {
            return ld.format(formatter);
        }

        if (value instanceof Date date) {
            return date.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate()
                    .format(formatter);
        }

        if (value instanceof String str && !str.isBlank()) {
            try {
                return LocalDateTime.parse(str, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                        .toLocalDate()
                        .format(formatter);
            } catch (Exception e1) {
                try {
                    return LocalDate.parse(str).format(formatter);
                } catch (Exception e2) {
                    return null;
                }
            }
        }

        return null;
    }
}