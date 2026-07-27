package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Component
public class RecordUtil {

    private static final String BASE_PATH = "logs";
    private static final String SYNC_FILE_NAME = "sync-state.txt";

    private static final Logger log = LoggerFactory.getLogger(RecordUtil.class);

    public synchronized void logFailed(Object soGiay, Object id, String reason) {

        createDirectoryIfNotExists();

        String fileName = BASE_PATH + "/failed_records_" + LocalDate.now() + ".txt";

        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(fileName, true))) {

            writer.write(String.format(
                    "%s | SoGiay: %s | Id: %s | Reason: %s",
                    LocalDateTime.now(),
                    soGiay,
                    id,
                    reason
            ));
            writer.newLine();

        } catch (IOException e) {
            log.error("Cannot write failed record to file", e);
        }
    }

    public synchronized void saveLastProcessedId(Long id) {
        createDirectoryIfNotExists();

        Path path = Paths.get(BASE_PATH, SYNC_FILE_NAME);

        try {
            Files.writeString(path,
                    String.valueOf(id),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.error("Không thể ghi sync-state", e);
        }
    }

    public synchronized Long getLastProcessedId() {

        Path path = Paths.get(BASE_PATH, SYNC_FILE_NAME);

        if (!Files.exists(path)) {
            return 0L;
        }

        try {
            String content = Files.readString(path).trim();
            return content.isEmpty() ? 0L : Long.parseLong(content);
        } catch (Exception e) {
            log.error("Không thể đọc sync-state", e);
            return 0L;
        }
    }

    private void createDirectoryIfNotExists() {
        try {
            Files.createDirectories(Paths.get(BASE_PATH));
        } catch (IOException e) {
            log.error("Không thể tạo thư mục logs", e);
        }
    }
}