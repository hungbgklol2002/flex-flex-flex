package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.job;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemXeCoGioiSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GiayDangKiemXeCoGioiSyncJob {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemXeCoGioiSyncJob.class);

    private final GiayDangKiemXeCoGioiSyncService syncService;

    public GiayDangKiemXeCoGioiSyncJob(GiayDangKiemXeCoGioiSyncService syncService) {
        this.syncService = syncService;
    }

//    @Scheduled(cron = "${sync.job.cron}", zone = "${sync.job.zone}")
    @Scheduled(fixedDelayString = "${sync.job.xcg.cron}", zone = "${sync.job.zone}")
    public void sync() {

        log.info("Starting daily sync job for T_GiayDangKiemXeCoGioi");
        syncService.sync();
        log.info("Completed daily sync job for T_GiayDangKiemXeCoGioi");
    }
}
