package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.controller;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemPhuongTienTauBienSyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/sync/giay-dang-kiem-phuong-tien-tau-bien")
public class GiayDangKiemPhuongTienTauBienManualSyncController {

    private final GiayDangKiemPhuongTienTauBienSyncService syncService;

    public GiayDangKiemPhuongTienTauBienManualSyncController(
            GiayDangKiemPhuongTienTauBienSyncService syncService
    ) {
        this.syncService = syncService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> sync() {
        return ResponseEntity.ok(syncService.sync());
    }
}
