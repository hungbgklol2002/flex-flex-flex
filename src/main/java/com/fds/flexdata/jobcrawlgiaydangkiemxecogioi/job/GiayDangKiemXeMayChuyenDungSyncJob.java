//package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.job;
//
//import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.AppState;
//import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemXeMayChuyenDungSyncService;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//@Component
//public class GiayDangKiemXeMayChuyenDungSyncJob {
//
//    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemXeMayChuyenDungSyncJob.class);
//
//    private final GiayDangKiemXeMayChuyenDungSyncService syncService;
//    private final AppState appState;
//
//    public GiayDangKiemXeMayChuyenDungSyncJob(
//            GiayDangKiemXeMayChuyenDungSyncService syncService,
//            AppState appState
//    ) {
//        this.syncService = syncService;
//        this.appState = appState;
//    }
//
//    @Scheduled(fixedDelayString = "${sync.job.xmcd.cron}", zone = "${sync.job.zone}")
//    public void sync() {
//        if (!appState.isRedisLoaded()) {
//            log.info("Skip XMCD sync because danh muc Redis is not loaded yet");
//            return;
//        }
//
//        log.info("Starting sync job for T_GiayDangKiemXeMayChuyenDung");
//        syncService.sync();
//        log.info("Completed sync job for T_GiayDangKiemXeMayChuyenDung");
//    }
//}
