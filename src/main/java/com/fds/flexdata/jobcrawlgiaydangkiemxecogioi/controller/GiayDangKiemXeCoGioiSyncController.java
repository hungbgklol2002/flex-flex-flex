package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.controller;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemXeCoGioiSyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sync/chung-chi-hanh-nghe")
public class GiayDangKiemXeCoGioiSyncController {

    private final GiayDangKiemXeCoGioiSyncService syncService;

    public GiayDangKiemXeCoGioiSyncController(GiayDangKiemXeCoGioiSyncService syncService) {
        this.syncService = syncService;
    }

    @PostMapping("")
    public ResponseEntity<String> syncLatest() {

        syncService.sync();

        return ResponseEntity.ok("Sync job executed successfully.");
    }
}