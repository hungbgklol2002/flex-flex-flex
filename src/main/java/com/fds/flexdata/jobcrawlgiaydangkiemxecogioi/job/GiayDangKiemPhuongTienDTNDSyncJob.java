package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.job;



import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.AppState;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemPhuongTienTauBienSyncService;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKyPhuongTienDTNDSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GiayDangKiemPhuongTienDTNDSyncJob {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemPhuongTienDTNDSyncJob.class);
    private final AppState appState;
    private final GiayDangKyPhuongTienDTNDSyncService syncService;

    public GiayDangKiemPhuongTienDTNDSyncJob(AppState appState, GiayDangKyPhuongTienDTNDSyncService syncService) {
        this.appState = appState;
        this.syncService = syncService;
    }

//    @Scheduled(cron = "${sync.job.cron}", zone = "${sync.job.zone}")
    @Scheduled(fixedDelayString = "${sync.job.dt.cron}", zone = "${sync.job.zone}")
    public void sync() {
        if (!appState.isRedisLoaded()) {
            return;
        }
        log.info("Starting daily sync job for T_GiayDangKiemTauBienDTND");
        syncService.sync();
        log.info("Completed daily sync job for T_GiayDangKiemTauBienDTND");
    }
}
