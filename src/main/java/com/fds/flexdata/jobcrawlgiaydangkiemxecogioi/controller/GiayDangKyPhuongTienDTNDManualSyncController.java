package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.controller;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service.GiayDangKyPhuongTienDTNDSyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sync/giay-dang-ky-phuong-tien-dtnd")
public class GiayDangKyPhuongTienDTNDManualSyncController {

    private final GiayDangKyPhuongTienDTNDSyncService syncService;

    public GiayDangKyPhuongTienDTNDManualSyncController(
            GiayDangKyPhuongTienDTNDSyncService syncService
    ) {
        this.syncService = syncService;
    }

    @PostMapping
    public ResponseEntity<String> sync() {
        syncService.sync();
        return ResponseEntity.ok("Giay dang ky phuong tien DTND sync executed successfully.");
    }
}
