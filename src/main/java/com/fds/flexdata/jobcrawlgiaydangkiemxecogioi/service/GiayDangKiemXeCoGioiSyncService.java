package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.DateUtils;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.RecordUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.StringUtils;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.TokenUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.SyncJobProperties;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository.GiayDangKiemXeCoGioiRepository;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class GiayDangKiemXeCoGioiSyncService {

    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DanhMucCacheService danhMucCacheService;

    @Autowired
    private RestClient restClient;
    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemXeCoGioiSyncService.class);

    @Autowired
    private RecordUtil recordUtil;
    @Autowired
    private TokenUtil tokenUtil;
    @Autowired
    private SyncJobProperties properties;
    private final RedissonClient redissonClient;

    private static final String LAST_ID_KEY = "jobcrawl:giaydangkiem:last-id";
    private final GiayDangKiemXeCoGioiRepository repository;

    public GiayDangKiemXeCoGioiSyncService(
            RedissonClient redissonClient, GiayDangKiemXeCoGioiRepository repository
    ) {
        this.redissonClient = redissonClient;
        this.repository = repository;
    }

    public void sync() {
//        String token = tokenUtil.getAccessToken();
        String token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIydGVXODhTNkduazEyTVZaWjdVc250cWVQT3VYU1BVRzFMWVg5a3ZEU1VBIn0.eyJleHAiOjE3ODUzODA5ODUsImlhdCI6MTc4NTM3NzM4NSwiYXV0aF90aW1lIjoxNzg1Mzc3Mzg0LCJqdGkiOiJvbnJ0YWM6MDk3YWZkZDAtNWQ3MC00YjUyLWFlNzUtNWM5NDZmMzQ1OGQ2IiwiaXNzIjoiaHR0cHM6Ly9pZHMtc3RhZ2luZy5mZHMudm4vYXV0aC9yZWFsbXMvYm94YXlkdW5nLWNzZGwtbmVudGFuZyIsImF1ZCI6WyJjc2RsLWRvYW5obmdoaWVwIiwiY3NkbC1kdWxpZXVkdW5nY2h1bmciLCJjc2RsLWtob2R1bGlldXRvbmdob3AiLCJjc2RsLW5ndW9pZGlldWtoaWVucGh1b25ndGllbiIsImNzZGwtcGh1b25ndGllbiIsImFjY291bnQiLCJjc2RsLWtldGNhdWhhdGFuZyJdLCJzdWIiOiI2NmQzNDZiNy1hNjQ1LTQzZDMtYWVkMy0yYTk2NmE3ZDk0YTQiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJjc2RsLXF1YW50cmloZXRob25nIiwic2lkIjoiNGIxYmNlOWMtNWFjNC00YTczLTgxNGEtNjRhYzRhNGU2MzA4IiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL2NzZGwtcXVhbnRyaWhldGhvbmcuZ292LnZuIiwiaHR0cDovL2xvY2FsaG9zdDo1MTczIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsImRlZmF1bHQtcm9sZXMtYm94YXlkdW5nLWNzZGwtbmVudGFuZyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiY3NkbC1xdWFudHJpaGV0aG9uZyI6eyJyb2xlcyI6WyJyZXN0cmljdGVkLWFjY2VzcyJdfSwiY3NkbC1kb2FuaG5naGllcCI6eyJyb2xlcyI6WyJyZXN0cmljdGVkLWFjY2VzcyIsInRlc3RSb2xlIl19LCJjc2RsLWR1bGlldWR1bmdjaHVuZyI6eyJyb2xlcyI6WyJyZXN0cmljdGVkLWFjY2VzcyJdfSwiY3NkbC1raG9kdWxpZXV0b25naG9wIjp7InJvbGVzIjpbInJlc3RyaWN0ZWQtYWNjZXNzIl19LCJjc2RsLW5ndW9pZGlldWtoaWVucGh1b25ndGllbiI6eyJyb2xlcyI6WyJyZXN0cmljdGVkLWFjY2VzcyJdfSwiY3NkbC1waHVvbmd0aWVuIjp7InJvbGVzIjpbInJlc3RyaWN0ZWQtYWNjZXNzIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX0sImNzZGwta2V0Y2F1aGF0YW5nIjp7InJvbGVzIjpbInJlc3RyaWN0ZWQtYWNjZXNzIl19fSwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicHJlZmVycmVkX3VzZXJuYW1lIjoiYnhkLWFkbWluIn0.kxn1_RXmI9Mtb_qJlwdQCAdZ1m7llLJXvjcJbJ7MGMe9DeNLqrcBK0tKISy2QrZ1HtHZFYJwg92nyCFNr5nIKpr-foYN8gw_urZ0w9FnyRIpLPgWI9DW-qJHGLlXngYKVxKE_LHiYNt5_9AhOYzH-dvtHffb1oyy1uuM5QUMTrvXQ3duavnoY8jYvNrx2NIFP73TmTgqwYzoFxS-3aCBvMYQA8YTi3STl-qgUdIgUd3wqokgUhALGKHrDAe6Dfw3rPfDQ15nojCBwH9FC1MOyZfu5qj39WdPrAOPOFSdog0-CtSzdKjOaOqDrjd53V0hzZl6vgSGKR20XiEKP7Ecgg";
        int batchSize = 1000;

        RBucket<String> checkpointBucket = redissonClient.getBucket(LAST_ID_KEY);

        String checkpoint = checkpointBucket.get();

        LocalDateTime lastTime = LocalDateTime.of(1970, 1, 1, 0, 0);
        String lastId = "";

        if (checkpoint != null && checkpoint.contains("|")) {
            String[] parts = checkpoint.split("\\|", 2);
            lastTime = LocalDateTime.parse(parts[0]);
            lastId = parts[1];
        }

        while (true) {
            List<Map<String, Object>> records =
                    repository.findDatas(lastTime, lastId, batchSize);

            records.forEach(this::trimRecord);

            if (records.isEmpty()) {
                log.info("No records found. lastTime={}, lastId={}", lastTime, lastId);
                return;
            }

            Map<String, Object> body = new HashMap<>();

            Map<String, Object> dacTaBanTin = new HashMap<>();
            String maBanTin = "DKLH-" + LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            dacTaBanTin.put("MaBanTin", maBanTin);
            dacTaBanTin.put("NoiTaoBanTin", "co-quan-x");

            List<Map<String, Object>> duLieuBanTin = new ArrayList<>();

            for (Map<String, Object> record : records) {
                duLieuBanTin.add(buildBanTinDuLieu(record));
            }

            body.put("DacTaBanTin", dacTaBanTin);
            body.put("DuLieuBanTin", duLieuBanTin);
            body.put("ChuKySo", buildChuKySo());
//            try {
//                String bodyJson = objectMapper.writerWithDefaultPrettyPrinter()
//                        .writeValueAsString(body);
//
//                log.info("Request body:\n{}", bodyJson);
//
//            } catch (Exception e) {
//                log.error("Cannot serialize request body", e);
//            }
            try {
                ResponseEntity<Void> response = restClient.post()
                        .uri(properties.getApiUrl().getXcg())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("AuthorizationDC", "Bearer " + token)
                        .header("ApiKey", properties.getApikey())
                        .body(body)
                        .retrieve()
                        .onStatus(
                                status -> status.isError(),
                                (request, result) -> {
                                    String errorBody = new String(
                                            result.getBody().readAllBytes(),
                                            StandardCharsets.UTF_8
                                    );

                                    throw new RuntimeException(
                                            "Sync API error. Status: "
                                                    + result.getStatusCode()
                                                    + ", Body: "
                                                    + errorBody
                                    );
                                }
                        )
                        .toBodilessEntity();

                Map<String, Object> lastRecord = records.get(records.size() - 1);

                Object syncTimeObj = lastRecord.get("SyncTime");

                LocalDateTime newLastTime;

                if (syncTimeObj instanceof java.sql.Timestamp timestamp) {
                    newLastTime = timestamp.toLocalDateTime();
                } else if (syncTimeObj instanceof LocalDateTime localDateTime) {
                    newLastTime = localDateTime;
                } else {
                    newLastTime = LocalDateTime.parse(syncTimeObj.toString().replace(" ", "T"));
                }

                String newLastId = String.valueOf(lastRecord.get("Id"));

                String newCheckpoint = newLastTime + "|" + newLastId;

                checkpointBucket.set(newCheckpoint);

                lastTime = newLastTime;
                lastId = newLastId;

                log.info("Sync batch success - size: {}, status: {}, checkpoint={}, MaBanTin:{} ",
                        records.size(),
                        response.getStatusCode(),
                        newCheckpoint,maBanTin
                );

            } catch (Exception ex) {
                String errorMessage = ex.getMessage();

                log.error("Sync batch failed - lastTime: {}, lastId: {}, size: {}, error: {}",
                        lastTime,
                        lastId,
                        records.size(),
                        errorMessage,
                        ex
                );

                for (Map<String, Object> record : records) {
                    recordUtil.logFailed(
                            record.get("SoGiay"),
                            record.get("Id"),
                            errorMessage
                    );
                }

                return;
            }
        }
    }

    private Map<String, Object> buildChuKySo() {
        return Map.of(
                "payload", "VGhpcyBpcyB0aGUgc2lnbmVkIGRhdGEu",
                "signatures", List.of(
                        Map.of(
                                "protected", "eyJhbGciOiJSUzI1NiJ9",
                                "signature", "Q1h5c2lnbmF0dXJlYmFzZTY0...",
                                "header", Map.of(
                                        "jades", Map.of(
                                                "signingTime", "2025-09-24T10:00:00Z",
                                                "signingCertificate", Map.of(
                                                        "digest", Map.of(
                                                                "alg", "sha256",
                                                                "value", "f2ca1bb6c7e907d06dafe4687e579fce..."
                                                        )
                                                )
                                        )
                                )
                        )
                )
        );
    }

    private Map<String, Object> buildBanTinDuLieu(Map<String, Object> record) {

        Map<String, Object> banTin = new HashMap<>();

        banTin.put("MaDinhDanh", record.get("MaDinhDanh"));
        banTin.put("SoGiay", record.get("MaDinhDanh"));
        banTin.put("NgayCap", DateUtils.toDateString(record.get("NgayCap")));
        banTin.put("NgayHetHan", DateUtils.toDateString(record.get("NgayHetHan")));
        banTin.put("SoPhieuKiemDinh", record.get("SoPhieuKiemDinh"));
        banTin.put("QR_URL", record.get("QR_URL"));

        banTin.put("MucPhatThai",toInteger(record.get("MucPhatThai")) );

//        DanhMucItem nc = danhMucCacheService.get(
//                "co-quan-don-vi",
//                record.get("NoiCap.MaDinhDanh")
//        );

//        if (nc != null) {
//            Map<String, Object> noiCap = new HashMap<>();
//            noiCap.put("MaDinhDanh", nc.getMaDinhDanh());
//            noiCap.put("TenToChuc", nc.getTenToChuc());
//
//            banTin.put("NoiCap", noiCap);
//        }

        Map<String, Object> noiCap = new HashMap<>();
        noiCap.put("MaDinhDanh", record.get("NoiCap.MaDinhDanh"));
        noiCap.put("TenToChuc", record.get("NoiCap.TenDinhDanh"));
        banTin.put("NoiCap", noiCap);


        DanhMucItem tthlgt = danhMucCacheService.get(
                "tinh-trang-hieu-luc-giay-to",
                record.get("TinhTrangHieuLucGiayTo.MaMuc")
        );

        Map<String, Object> tthlgtObject = new HashMap<>();

        if (tthlgt != null) {
            tthlgtObject.put("MaMuc", "0" + tthlgt.getMaMuc());
            tthlgtObject.put("TenMuc", tthlgt.getTenMuc());
        } else {
            tthlgtObject.put("MaMuc", "01");
            tthlgtObject.put("TenMuc", "Hiệu lực");
        }

        banTin.put("TinhTrangHieuLucGiayTo", tthlgtObject);

        banTin.put("PhuongTien", buildPhuongTien(record));
        banTin.put("NgayDangKiem", DateUtils.toDateString(record.get("NgayDangKiem")));

        return banTin;
    }

    private Map<String, Object> buildPhuongTien(Map<String, Object> record) {

        Map<String, Object> phuongTien = new HashMap<>();

        String soKhung = Objects.toString(record.get("PhuongTien.MaDinhDanh"), "");

        phuongTien.put("TinhTrangPhuongTien", null);
        phuongTien.put("MaDinhDanh", soKhung);
        phuongTien.put("SoKhung", soKhung);
        phuongTien.put("SoMay", record.get("PhuongTien.SoMay"));
        phuongTien.put("BienSoXe", record.get("PhuongTien.BienSoXe"));
        phuongTien.put("NhanHieu", Objects.toString(record.get("PhuongTien.NhanHieu"), "").trim());
        Object nienHan = record.get("PhuongTien.NienHanSuDung");

        if (nienHan != null) {
            phuongTien.put("NienHanSuDung", nienHan.toString());
        }
        phuongTien.put("SoLoai", StringUtils.trim(StringUtils.defaultString((String) record.get("PhuongTien.TenThuongMai")) + " " + StringUtils.defaultString((String) record.get("PhuongTien.SoLoai"))));
        //        phuongTien.put("SoLoai", record.get("PhuongTien.SoLoai"));
        phuongTien.put("TenThuongMai", record.get("PhuongTien.TenThuongMai"));
        phuongTien.put("NamSanXuat", record.get("PhuongTien.NamSanXuat"));

        phuongTien.put("SoChoNgoi", record.get("PhuongTien.SoChoNgoi"));
        phuongTien.put("SoChoNam", record.get("PhuongTien.SoChoNam"));
        phuongTien.put("SoChoDung", record.get("PhuongTien.SoChoDung"));

        phuongTien.put("CongSuat", record.get("PhuongTien.CongSuat"));
        boolean xeDaCaiTao = "1".equals(String.valueOf(record.get("PhuongTien.XeDaCaiTao")));
        phuongTien.put("XeDaCaiTao", xeDaCaiTao);

        boolean kinhDoanhVanTai = "1".equals(String.valueOf(record.get("PhuongTien.KinhDoanhVanTai")));

        phuongTien.put("KinhDoanhVanTai", kinhDoanhVanTai);

        boolean gsha = "1".equals(String.valueOf(record.get("PhuongTien.ThietBiGiamSatHinhAnh")));

        phuongTien.put("ThietBiGiamSatHinhAnh", gsha);

        boolean hsht = "1".equals(String.valueOf(record.get("PhuongTien.ThietBiGiamSatHanhTrinh")));

        phuongTien.put("ThietBiGiamSatHanhTrinh", hsht);

        phuongTien.put("KhoiLuongbanThan", record.get("PhuongTien.KhoiLuongbanThan"));
        phuongTien.put("KhoiLuongHangTK", record.get("PhuongTien.KhoiLuongHangTK"));
        phuongTien.put("KhoiLuongHangCP", record.get("PhuongTien.KhoiLuongHangCP"));
        phuongTien.put("KhoiLuongToanBoTK", record.get("PhuongTien.KhoiLuongToanBoTK"));
        phuongTien.put("KhoiLuongToanBoCP", record.get("PhuongTien.KhoiLuongToanBoCP"));
        phuongTien.put("KhoiLuongKeoTheoCP", record.get("PhuongTien.KhoiLuongKeoTheoCP"));
        phuongTien.put("KichThuocLongThung", record.get("PhuongTien.KichThuocLongThung"));
        phuongTien.put("KichThuocBao", record.get("PhuongTien.KichThuocBao"));

        phuongTien.put("DungTich",toInteger(record.get("PhuongTien.DungTich")));
        phuongTien.put("Hybrid", record.get("PhuongTien.Hybrid") == null
                ? false
                : record.get("PhuongTien.Hybrid"));



        phuongTien.put("SoQuanLy", record.get("PhuongTien.SoQuanLy"));
        Map<String, Object> nguonGoc = new HashMap<>();

        boolean sanXuatTrongNuoc = "VN".equals(String.valueOf(record.get("PhuongTien.NuocSanXuat.MaMuc")));

        String nguonGocTen = sanXuatTrongNuoc ? "Sản xuất lắp ráp" : "Nhập khẩu";

        String nguonGocMa = sanXuatTrongNuoc ? "0" : "1";
        nguonGoc.put("TenMuc", nguonGocTen);
        nguonGoc.put("MaMuc", nguonGocMa);
        phuongTien.put("NguonGoc", nguonGoc);

        DanhMucItem ldc = danhMucCacheService.get(
                "loai-dong-co",
                    record.get("PhuongTien.LoaiDongCo.MaMuc")
        );

        if (ldc != null) {
            Map<String, Object> ldcObject = new HashMap<>();
            ldcObject.put("MaMuc", ldc.getMaMuc());
            ldcObject.put("TenMuc", ldc.getTenMuc());

            phuongTien.put("LoaiDongCo", ldcObject);
        }

        DanhMucItem lnl = danhMucCacheService.get(
                "loai-nhien-lieu",
                record.get("PhuongTien.LoaiNhienLieu.MaMuc")
        );

        if (lnl != null) {
            Map<String, Object> lnlObject = new HashMap<>();
            lnlObject.put("MaMuc", lnl.getMaMuc());
            lnlObject.put("TenMuc", lnl.getTenMuc());

            phuongTien.put("LoaiNhienLieu", lnlObject);
        }

        DanhMucItem phanLoaiXe = danhMucCacheService.get(
                "phan-loai-xe",
                record.get("PhuongTien.PhanLoaiXeCoGioi.MaMuc")
        );

        if (phanLoaiXe != null) {
            Map<String, Object> phanLoai = new HashMap<>();
            phanLoai.put("MaMuc", phanLoaiXe.getMaMuc());
            phanLoai.put("TenMuc", phanLoaiXe.getTenMuc());

            phuongTien.put("PhanLoaiXeCoGioi", phanLoai);
        }

        DanhMucItem nuocSanXuat = danhMucCacheService.get(
                "quoc-gia",
                record.get("PhuongTien.NuocSanXuat.MaMuc")
        );

        List<Map<String, Object>> dsNuocSX = new ArrayList<>();

        Map<String, Object> nuocSX = new HashMap<>();

        if (nuocSanXuat != null) {
            nuocSX.put("MaMuc", nuocSanXuat.getMaMuc());
            nuocSX.put("TenMuc", nuocSanXuat.getTenMuc());
        } else {
            nuocSX.put("MaMuc", null);
            nuocSX.put("TenMuc", null);
        }

        dsNuocSX.add(nuocSX);

        phuongTien.put("NuocSanXuat", dsNuocSX);


        phuongTien.put("@type", "T_XeCoGioi");

        return phuongTien;
    }

    private void trimRecord(Map<String, Object> record) {
        record.replaceAll((k, v) -> {
            if (!(v instanceof String str)) {
                return v;
            }

            String value = str.trim();

            if ("PhuongTien.MaDinhDanh".equals(k)
                    || "PhuongTien.SoMay".equals(k)) {
                value = value.replaceAll("\\s+", "");
            }

            return value;
        });
    }

    public static Integer toInteger(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Integer integer) {
            return integer;
        }

        if (value instanceof Number number) {
            return number.intValue();
        }

        String str = value.toString().trim();

        if (str.isEmpty()) {
            return null;
        }

        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
