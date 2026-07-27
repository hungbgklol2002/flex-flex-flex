package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.controller;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemXeCoGioiSyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sync/giay-dang-kiem-xe-co-gioi")
public class GiayDangKiemXeCoGioiManualSyncController {

    private final GiayDangKiemXeCoGioiSyncService syncService;

    public GiayDangKiemXeCoGioiManualSyncController(GiayDangKiemXeCoGioiSyncService syncService) {
        this.syncService = syncService;
    }

    @PostMapping
    public ResponseEntity<String> sync() {
        syncService.sync();
        return ResponseEntity.ok("Giay dang kiem xe co gioi sync executed successfully.");
    }
}
