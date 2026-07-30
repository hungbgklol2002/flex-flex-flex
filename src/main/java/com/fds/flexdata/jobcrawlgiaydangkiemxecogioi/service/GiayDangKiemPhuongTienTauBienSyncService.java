package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.RecordUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.TokenUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.SyncJobProperties;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository.GiayDangKiemPhuongTienTauBienRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class GiayDangKiemPhuongTienTauBienSyncService {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemPhuongTienTauBienSyncService.class);

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DanhMucCacheService  danhMucCacheService;

    @Autowired
    private RestClient restClient;

    @Autowired
    private RecordUtil recordUtil;
    @Autowired
    private TokenUtil tokenUtil;
    @Autowired
    private SyncJobProperties properties;


    private static final String LAST_NGAY_CAP_KEY =
            "jobcrawl:giaydangkiemtaubien:last-ngay-cap";

    private static final String LAST_SO_GIAY_KEY =
            "jobcrawl:giaydangkiemtaubien:last-so-giay";

    private static final int BATCH_SIZE = 1000;
    private final GiayDangKiemPhuongTienTauBienRepository repository;

    public GiayDangKiemPhuongTienTauBienSyncService(
            GiayDangKiemPhuongTienTauBienRepository repository
    ) {
        this.repository = repository;
    }

    public Map<String, Object> sync() {

        String token = tokenUtil.getAccessToken();
        Timestamp lastNgayCap = loadLastNgayCap();
        String lastSoGiay = loadLastSoGiay();
        log.info(
                "Loaded checkpoint: ngayCap={}, soGiay={}",
                lastNgayCap,
                lastSoGiay
        );
        while (true) {

            List<Map<String, Object>> records =
                    repository.findDatas(
                            lastNgayCap,
                            lastSoGiay,
                            BATCH_SIZE
                    );

            if (records.isEmpty()) {

                log.info(
                        "Sync completed. checkpoint={} - {}",
                        lastNgayCap,
                        lastSoGiay
                );

                return Map.of(
                        "status", "NO_DATA",
                        "maBanTin", "",
                        "records", 0,
                        "checkpointNgayCap", lastNgayCap.toString(),
                        "checkpointSoGiay", lastSoGiay
                );
            }

            records.forEach(this::trimRecord);

            Map<String, Object> last =
                    records.get(records.size() - 1);

            Object checkpointSoGiay =
                    last.get("__CHECKPOINT_SO_GIAY");

            Object checkpointNgayCap =
                    last.get("NgayCap");

            records.forEach(r ->
                    r.remove("__CHECKPOINT_SO_GIAY")
            );

            try {

                String maBanTin = syncBatch(records, token);
                log.info(
                        "checkpointNgayCap={}, class={}",
                        checkpointNgayCap,
                        checkpointNgayCap == null ? null : checkpointNgayCap.getClass().getName()
                );
                if (checkpointNgayCap instanceof Timestamp ts) {
                    lastNgayCap = ts;
                } else if (checkpointNgayCap instanceof String str) {

                    if (str.length() == 10) {
                        lastNgayCap = Timestamp.valueOf(
                                str + " 00:00:00"
                        );
                    } else {
                        lastNgayCap = Timestamp.valueOf(str);
                    }

                } else {
                    throw new IllegalStateException(
                            "Unsupported type: " +
                                    checkpointNgayCap.getClass()
                    );
                }

                lastSoGiay =
                        checkpointSoGiay == null
                                ? ""
                                : checkpointSoGiay.toString();

                saveCheckpoint(
                        lastNgayCap,
                        lastSoGiay
                );

                log.info(
                        "Checkpoint saved. ngayCap={}, soGiay={}",
                        lastNgayCap,
                        lastSoGiay
                );

                log.info(
                        "Batch synced successfully. size={}",
                        records.size()
                );

                return Map.of(
                        "status", "SUCCESS",
                        "maBanTin", maBanTin,
                        "records", records.size(),
                        "checkpointNgayCap", lastNgayCap.toString(),
                        "checkpointSoGiay", lastSoGiay
                );

            } catch (Exception ex) {

                log.error(
                        "Sync failed",
                        ex
                );

                return Map.of(
                        "status", "FAILED",
                        "maBanTin", "",
                        "records", records.size(),
                        "error", ex.getMessage() == null ? "" : ex.getMessage()
                );
            }
        }
    }

    private String syncBatch(
            List<Map<String, Object>> records,
            String token
    ) throws Exception {

        String maBanTin =
                "MBT-" +
                        LocalDateTime.now()
                                .format(
                                        DateTimeFormatter.ofPattern(
                                                "yyyyMMddHHmmss"
                                        )
                                );

        Map<String, Object> body =
                new LinkedHashMap<>();

        body.put(
                "DacTaBanTin",
                Map.of(
                        "MaBanTin", maBanTin,
                        "NoiTaoBanTin", "G17.46"
                )
        );

        List<Map<String, Object>> duLieuBanTin =
                records.stream()
                        .map(this::buildBanTinDuLieu)
                        .toList();

        body.put(
                "DuLieuBanTin",
                duLieuBanTin
        );

        body.put(
                "ChuKySo",
                buildChuKySo()
        );
        String requestBody = objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(body);

        restClient.post()
                .uri(properties.getApiUrl().getTb())
                .contentType(MediaType.APPLICATION_JSON)
                .header(
                        "AuthorizationDC",
                        "Bearer " + token
                )
                .header(
                        "ApiKey",
                        properties.getApikey()
                )
                .body(body)
                .retrieve()
                .onStatus(
                        status -> status.isError(),
                        (request, result) -> {

                            String errorBody =
                                    new String(
                                            result.getBody().readAllBytes(),
                                            StandardCharsets.UTF_8
                                    );

                            throw new RuntimeException(errorBody);
                        }
                )
                .toBodilessEntity();

        log.info(
                "Push success. MaBanTin={}, records={}",
                maBanTin,
                records.size()
        );
        return maBanTin;
    }
    private Timestamp loadLastNgayCap() {

        String value =
                redisTemplate.opsForValue()
                        .get(LAST_NGAY_CAP_KEY);

        if (value == null) {
            return Timestamp.valueOf(
                    "1900-01-01 00:00:00"
            );
        }

        return Timestamp.valueOf(value);
    }

    private String loadLastSoGiay() {

        return Optional.ofNullable(
                redisTemplate.opsForValue()
                        .get(LAST_SO_GIAY_KEY)
        ).orElse("");
    }

    private void saveCheckpoint(
            Timestamp ngayCap,
            String soGiay
    ) {

        redisTemplate.opsForValue().set(
                LAST_NGAY_CAP_KEY,
                ngayCap.toString()
        );

        redisTemplate.opsForValue().set(
                LAST_SO_GIAY_KEY,
                soGiay
        );
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
    private Map<String, Object> buildBanTinDuLieu(
            Map<String, Object> record
    ) {

        resolveTauBienMaDinhDanh(record);

        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : record.entrySet()) {

            Object value = entry.getValue();

            if (shouldSkip(value)) {
                continue;
            }

            buildNestedField(
                    result,
                    entry.getKey(),
                    value
            );
        }
        enrich(result);
        removeEmptyObjects(result);

        return result;
    }

    private void resolveTauBienMaDinhDanh(Map<String, Object> record) {
        try {
            Object maDinhDanh = record.get("PhuongTien.MaDinhDanh");

            if (maDinhDanh != null && !maDinhDanh.toString().isBlank()) {
                return;
            }

            DanhMucItem item = danhMucCacheService.getBySoPhanCap(
                    "tau-bien-khac-pattern",
                    record.get("__TAU_BIEN_SO_PHAN_CAP")
            );

            if (item == null) {
                item = danhMucCacheService.getBySoDangKyCucHH(
                        "tau-bien-khac-pattern",
                        record.get("__TAU_BIEN_SO_DANG_KY_CUC_HH")
                );
            }

            if (item == null) {
                item = danhMucCacheService.getBySoIMO(
                        "tau-bien-khac-pattern",
                        record.get("PhuongTien.SoIMO")
                );
            }

            if (item != null
                    && item.getMaDinhDanh() != null
                    && !item.getMaDinhDanh().isBlank()) {
                record.put("PhuongTien.MaDinhDanh", item.getMaDinhDanh());
            }
        } finally {
            record.remove("__TAU_BIEN_SO_PHAN_CAP");
            record.remove("__TAU_BIEN_SO_DANG_KY_CUC_HH");
        }
    }
    @SuppressWarnings("unchecked")
    private void buildNestedField(
            Map<String, Object> root,
            String path,
            Object value
    ) {

        String[] keys = path.split("\\.");

        Map<String, Object> current = root;

        for (int i = 0; i < keys.length - 1; i++) {

            String key = keys[i];

            current = (Map<String, Object>)
                    current.computeIfAbsent(
                            key,
                            k -> new LinkedHashMap<>()
                    );
        }

        current.put(keys[keys.length - 1], value);
    }

    @SuppressWarnings("unchecked")
    private boolean removeEmptyObjects(Object obj) {

        if (!(obj instanceof Map<?, ?> map)) {
            return false;
        }

        Iterator<? extends Map.Entry<?, ?>> iterator =
                map.entrySet().iterator();

        while (iterator.hasNext()) {

            Map.Entry<?, ?> entry = iterator.next();

            Object value = entry.getValue();

            if (value instanceof Map<?, ?> nested) {

                boolean empty =
                        removeEmptyObjects(nested);

                if (empty) {
                    iterator.remove();
                }
            }
        }

        return map.isEmpty();
    }

    @SuppressWarnings("unchecked")
    private void enrich(Map<String, Object> result) {

        Object maDinhDanh = result.get("MaDinhDanh");

        result.put(
                "SoGiay",
                maDinhDanh == null ? "" : maDinhDanh.toString()
        );

        // ================= NOI CAP =================

        Map<String, Object> noiCap =
                (Map<String, Object>) result.computeIfAbsent(
                        "NoiCap",
                        k -> new LinkedHashMap<>()
                );


    noiCap.put(
            "@type","T_CoQuanDonVi");


//        Map<String, Object> trangThai =
//                (Map<String, Object>) result.computeIfAbsent(
//                        "TrangThaiGiayDangKiem",
//                        k -> new LinkedHashMap<>()
//                );
//
//        trangThai.putIfAbsent("MaMuc", "01");
//
//        trangThai.put(
//                "TenMuc",
//                "Hiệu lực"
//        );





        // ================= LOAI GIAY =================

        Map<String, Object> loaiGiay =
                (Map<String, Object>) result.computeIfAbsent(
                        "LoaiGiayDangKiemTauBien",
                        k -> new LinkedHashMap<>()
                );

        loaiGiay.putIfAbsent("MaMuc", "");

        loaiGiay.put(
                "TenMuc",
                Optional.ofNullable(
                        danhMucCacheService.getTenMuc(
                                "loai-giay-dang-kiem-tau-bien",
                                loaiGiay.get("MaMuc")
                        )
                ).orElse("")

        );


        // ================= TINH TRANG =================

        Map<String, Object> tinhTrang =
                (Map<String, Object>) result.computeIfAbsent(
                        "TinhTrangHieuLucGiayTo",
                        k -> new LinkedHashMap<>()
                );

        tinhTrang.putIfAbsent("MaMuc", "");

        tinhTrang.put(
                "TenMuc",
                Optional.ofNullable(
                        danhMucCacheService.getTenMuc(
                                "tinh-trang-hieu-luc-giay-to-tau-bien",
                                tinhTrang.get("MaMuc")
                        )
                ).orElse("")
        );

        // ================= PHUONG TIEN =================

        Map<String, Object> phuongTien =
                (Map<String, Object>) result.computeIfAbsent(
                        "PhuongTien",
                        k -> new LinkedHashMap<>()
                );

        // Chưa có cache => chỉ tạo object cho đúng schema
        Map<String, Object> nhomPhuongTien =
                (Map<String, Object>) phuongTien.computeIfAbsent(
                        "NhomPhuongTienTauBien",
                        k -> new LinkedHashMap<>()
                );

        nhomPhuongTien.putIfAbsent("MaMuc", "");

        nhomPhuongTien.put(
                "TenMuc",
                Optional.ofNullable(
                        danhMucCacheService.getTenMuc(
                                "nhom-phuong-tien-tau-bien",
                                nhomPhuongTien.get("MaMuc")
                        )
                ).orElse("")
        );
        Map<String, Object> tuyenKhaiThac =
                (Map<String, Object>) phuongTien.computeIfAbsent(
                        "TuyenKhaiThacTauBien",
                        k -> new LinkedHashMap<>()
                );

        tuyenKhaiThac.putIfAbsent("MaMuc", "");

        tuyenKhaiThac.put(
                "TenMuc",
                Optional.ofNullable(
                        danhMucCacheService.getTenMuc(
                                "tuyen-khai-thac-tau-bien",
                                tuyenKhaiThac.get("MaMuc")
                        )
                ).orElse("")
        );
        Map<String, Object> vungHoatDong =
                (Map<String, Object>) phuongTien.computeIfAbsent(
                        "VungHoatDong",
                        k -> new LinkedHashMap<>()
                );

        vungHoatDong.putIfAbsent("MaMuc", "");

        vungHoatDong.put(
                "TenMuc",
                Optional.ofNullable(
                        danhMucCacheService.getTenMuc(
                                "vung-hoat-dong-tau-bien",
                                vungHoatDong.get("MaMuc")
                        )
                ).orElse("")
        );
        Map<String, Object> nuocSanXuat =
                (Map<String, Object>) phuongTien.computeIfAbsent(
                        "NuocSanXuat",
                        k -> new LinkedHashMap<>()
                );

        nuocSanXuat.putIfAbsent("MaMuc", "");

        nuocSanXuat.put(
                "TenMuc",
                Optional.ofNullable(
                        danhMucCacheService.getTenMuc(
                                "quoc-gia",
                                nuocSanXuat.get("MaMuc")
                        )
                ).orElse("")
        );

        // Schema yêu cầu String
        convertToString(phuongTien, "SoIMO");
        convertToString(phuongTien, "DungTichCoIch");
        convertToString(phuongTien, "TongDungTich");
        convertToString(phuongTien, "SoLuongMayChinh");
        convertToString(phuongTien, "SucChoKhach");
    }

    @SuppressWarnings("unchecked")
    private void ensureDanhMuc(
            Map<String, Object> parent,
            String fieldName
    ) {
        Map<String, Object> obj =
                (Map<String, Object>) parent.computeIfAbsent(
                        fieldName,
                        k -> new LinkedHashMap<>()
                );

        obj.putIfAbsent("MaMuc", "");
        obj.putIfAbsent("TenMuc", "");
    }

    private void convertToString(
            Map<String, Object> map,
            String field
    ) {
        Object value = map.get(field);

        if (value != null && !(value instanceof String)) {
            map.put(field, String.valueOf(value));
        }
    }
    private boolean shouldSkip(Object value) {

        if (value == null) {
            return true;
        }

        if (value instanceof String str) {
            return str.trim().isEmpty();
        }

        return false;
    }
    private Map<String, Object> buildPhuongTien(Map<String, Object> record) {

        Map<String, Object> phuongTien = new HashMap<>();



        return phuongTien;
    }

    private void trimRecord(Map<String, Object> record) {

        record.replaceAll((k, v) -> {

            if (!(v instanceof String str)) {
                return v;
            }

            return str.trim();
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
