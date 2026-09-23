package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.controller;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKiemPhuongTienDuongSatSyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sync/giay-dang-kiem-phuong-tien-duong-sat")
public class GiayDangKiemPhuongTienDuongSatManualSyncController {

    private final GiayDangKiemPhuongTienDuongSatSyncService syncService;

    public GiayDangKiemPhuongTienDuongSatManualSyncController(
            GiayDangKiemPhuongTienDuongSatSyncService syncService
    ) {
        this.syncService = syncService;
    }

    @PostMapping
    public ResponseEntity<String> sync() {
        syncService.sync();
        return ResponseEntity.ok("Giay dang kiem phuong tien duong sat sync executed successfully.");
    }
}
