package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.job;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.AppState;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemPhuongTienDuongSatSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GiayDangKiemPhuongTienDuongSatSyncJob {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemPhuongTienDuongSatSyncJob.class);

    private final AppState appState;
    private final GiayDangKiemPhuongTienDuongSatSyncService syncService;

    public GiayDangKiemPhuongTienDuongSatSyncJob(
            AppState appState,
            GiayDangKiemPhuongTienDuongSatSyncService syncService
    ) {
        this.appState = appState;
        this.syncService = syncService;
    }

    @Scheduled(fixedDelayString = "${sync.job.ds.cron}", zone = "${sync.job.zone}")
    public void sync() {
        if (!appState.isRedisLoaded()) {
            return;
        }
        log.info("Starting sync job for T_GiayDangKiemPhuongTienDuongSat");
        syncService.sync();
        log.info("Completed sync job for T_GiayDangKiemPhuongTienDuongSat");
    }
}
