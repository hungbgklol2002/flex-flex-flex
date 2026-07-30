package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.DateUtils;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.TokenUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.SyncJobProperties;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository.GiayDangKyPhuongTienDTNDRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class GiayDangKyPhuongTienDTNDSyncService {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKyPhuongTienDTNDSyncService.class);
    private static final String LAST_NGAY_CAP_KEY = "jobcrawl:giaydangkiemDTND:last-ngay-cap";
    private static final String LAST_SO_GIAY_KEY = "jobcrawl:giaydangkiemDTND:last-so-giay";
    private static final String CHECKPOINT_SO_GIAY_FIELD = "__CHECKPOINT_SO_GIAY";
    private static final String THONG_SO_VUNG_HOAT_DONG_FIELD = "__THONG_SO_VUNG_HOAT_DONG";
    private static final String DEFAULT_NGAY_CAP = "1900-01-01 00:00:00";
    private static final String NOI_TAO_BAN_TIN = "G17.46";
    private static final String TINH_TRANG_HIEU_LUC_MA = "01";
    private static final String TINH_TRANG_HIEU_LUC_TEN = "Hiệu lực";
    private static final DateTimeFormatter MA_BAN_TIN_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final int BATCH_SIZE = 1000;

    private final GiayDangKyPhuongTienDTNDRepository repository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final DanhMucCacheService danhMucCacheService;
    private final RestClient restClient;
    private final TokenUtil tokenUtil;
    private final SyncJobProperties properties;

    public GiayDangKyPhuongTienDTNDSyncService(
            GiayDangKyPhuongTienDTNDRepository repository,
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            DanhMucCacheService danhMucCacheService,
            RestClient restClient,
            TokenUtil tokenUtil,
            SyncJobProperties properties
    ) {
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.danhMucCacheService = danhMucCacheService;
        this.restClient = restClient;
        this.tokenUtil = tokenUtil;
        this.properties = properties;
    }

    public void sync() {
        String token = tokenUtil.getAccessToken();
        Timestamp lastNgayCap = loadLastNgayCap();
        String lastSoGiay = loadLastSoGiay();
        log.info("Loaded checkpoint: ngayCap={}, soGiay={}", lastNgayCap, lastSoGiay);

        while (true) {
            List<Map<String, Object>> records = repository.findDatas(lastNgayCap, lastSoGiay, BATCH_SIZE);
            if (records.isEmpty()) {
                log.info("Sync completed. checkpoint={} - {}", lastNgayCap, lastSoGiay);
                return;
            }

            records.forEach(this::trimRecord);
            BatchCheckpoint nextCheckpoint = extractCheckpoint(records);
            removeInternalFields(records);

            try {
                syncBatch(records, token);
                lastNgayCap = toCheckpointTimestamp(nextCheckpoint.ngayCap());
                lastSoGiay = stringValue(nextCheckpoint.soGiay());
                saveCheckpoint(lastNgayCap, lastSoGiay);

                log.info("Checkpoint saved. ngayCap={}, soGiay={}", lastNgayCap, lastSoGiay);
                log.info("Batch synced successfully. size={}", records.size());
            } catch (Exception ex) {
                log.error(
                        "Sync failed. Current checkpoint: ngayCap={}, soGiay={}, batchSize={}",
                        lastNgayCap,
                        lastSoGiay,
                        records.size(),
                        ex
                );
                return;
            }
        }
    }

    private void removeInternalFields(List<Map<String, Object>> records) {
        records.forEach(record -> record.remove(CHECKPOINT_SO_GIAY_FIELD));
    }

    private BatchCheckpoint extractCheckpoint(List<Map<String, Object>> records) {
        Map<String, Object> lastRecord = records.get(records.size() - 1);
        return new BatchCheckpoint(
                lastRecord.get("NgayCap"),
                lastRecord.get(CHECKPOINT_SO_GIAY_FIELD)
        );
    }

    private Timestamp toCheckpointTimestamp(Object ngayCap) {
        log.debug(
                "Converting checkpoint NgayCap. value={}, type={}",
                ngayCap,
                ngayCap == null ? null : ngayCap.getClass().getName()
        );

        if (ngayCap instanceof Timestamp timestamp) {
            return timestamp;
        }
        if (ngayCap instanceof String value) {
            return Timestamp.valueOf(value.length() == 10 ? value + " 00:00:00" : value);
        }

        throw new IllegalStateException(
                "Unsupported checkpoint NgayCap type: " + (ngayCap == null ? "null" : ngayCap.getClass().getName())
        );
    }

    private void syncBatch(
            List<Map<String, Object>> records,
            String token
    ) throws Exception {
        String maBanTin = createMaBanTin();
        Map<String, Object> requestBody = buildRequestBody(records, maBanTin);

        // Serialize trước khi gửi để lỗi dữ liệu được ghi nhận tại đúng batch.
        String serializedRequest = objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(requestBody);


        restClient.post()
                .uri(properties.getApiUrl().getDt())
                .contentType(MediaType.APPLICATION_JSON)
                .header("AuthorizationDC", "Bearer " + token)
                .header("ApiKey", properties.getApikey())
                .body(requestBody)
                .retrieve()
                .onStatus(
                        status -> status.isError(),
                        (request, response) -> handleErrorResponse(response.getBody().readAllBytes())
                )
                .toBodilessEntity();

        log.info("Push success. MaBanTin={}, records={}", maBanTin, records.size());
    }

    private String createMaBanTin() {
        return "MBT-" + LocalDateTime.now().format(MA_BAN_TIN_FORMATTER);
    }

    private Map<String, Object> buildRequestBody(
            List<Map<String, Object>> records,
            String maBanTin
    ) {
        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("DacTaBanTin", Map.of(
                "MaBanTin", maBanTin,
                "NoiTaoBanTin", NOI_TAO_BAN_TIN
        ));
        requestBody.put("DuLieuBanTin", records.stream().map(this::buildBanTinDuLieu).toList());
        requestBody.put("ChuKySo", buildChuKySo());
        return requestBody;
    }

    private void handleErrorResponse(byte[] responseBody) {
        throw new RuntimeException(new String(responseBody, StandardCharsets.UTF_8));
    }

    private Timestamp loadLastNgayCap() {
        String value = redisTemplate.opsForValue().get(LAST_NGAY_CAP_KEY);
        return Timestamp.valueOf(value == null ? DEFAULT_NGAY_CAP : value);
    }

    private String loadLastSoGiay() {
        return Optional.ofNullable(redisTemplate.opsForValue().get(LAST_SO_GIAY_KEY)).orElse("");
    }

    private void saveCheckpoint(
            Timestamp ngayCap,
            String soGiay
    ) {
        redisTemplate.opsForValue().set(LAST_NGAY_CAP_KEY, ngayCap.toString());
        redisTemplate.opsForValue().set(LAST_SO_GIAY_KEY, soGiay);
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
        Map<String, Object> result = new LinkedHashMap<>();

        result.put("MaDinhDanh", stringValue(record.get("MaDinhDanh")));
        result.put("SoGiay", stringValue(record.get("SoGiay")));
        result.put("NgayCap", dateValue(record.get("NgayCap")));

        result.put("NoiCap", buildNoiCap(record));
        result.put("NgayHetHan", dateValue(record.get("NgayHetHan")));
        result.put("SoPhieuKiemDinh", stringValue(record.get("SoPhieuKiemDinh")));
        result.put("NoiDangKiem", stringValue(record.get("NoiDangKiem")));
        result.put("TinhTrangHieuLucGiayTo", buildTinhTrangHieuLuc());
        result.put("PhuongTien", buildPhuongTienBody(record));

        return result;
    }

    private Map<String, Object> buildNoiCap(Map<String, Object> record) {
        Map<String, Object> noiCap = new LinkedHashMap<>();
        noiCap.put("MaDinhDanh", stringValue(record.get("NoiCap.MaDinhDanh")));
        noiCap.put("TenToChuc", stringValue(record.get("NoiCap.TenToChuc")));
        return noiCap;
    }

    private Map<String, Object> buildTinhTrangHieuLuc() {
        // Nguồn DTND luôn đồng bộ giấy tờ đang hiệu lực theo yêu cầu nghiệp vụ.
        Map<String, Object> tinhTrang = new LinkedHashMap<>();
        tinhTrang.put("MaMuc", TINH_TRANG_HIEU_LUC_MA);
        tinhTrang.put("TenMuc", TINH_TRANG_HIEU_LUC_TEN);
        return tinhTrang;
    }

    private Map<String, Object> buildPhuongTienBody(Map<String, Object> record) {
        Map<String, Object> phuongTien = new LinkedHashMap<>();

        phuongTien.put("MaDinhDanh", stringValue(record.get("PhuongTien.MaDinhDanh")));
        phuongTien.put("SoKiemSoat", stringValue(record.get("PhuongTien.SoKiemSoat")));
        phuongTien.put("SoDangKiem", stringValue(record.get("PhuongTien.SoDangKiem")));
//        phuongTien.put("HanCheVungHD", stringValue(record.get("PhuongTien.HanCheVungHD")));
        phuongTien.put("TenTau", stringValue(record.get("PhuongTien.TenTau")));
        phuongTien.put("NhomPhuongTienThuyNoiDia", buildDanhMucByMa(
                "nhom-phuong-tien-thuy-noi-dia",
                record.get("PhuongTien.NhomPhuongTienThuyNoiDia.MaMuc")
        ));
        phuongTien.put(
                "CapPhuongTienThuyNoiDia",
                buildCapPhuongTienThuyNoiDia(record.get("PhuongTien.CapPhuongTienThuyNoiDia.MaMuc"))
        );
        phuongTien.put("ThongSoVungHoatDong", buildThongSoVungHoatDong(record));
        phuongTien.put("ChieuCaoMan", numberValue(record.get("PhuongTien.ChieuCaoMan")));
        phuongTien.put("NienHanSuDung", numberValue(record.get("PhuongTien.NienHanSuDung")));
        phuongTien.put("NuocSanXuat", null);
        phuongTien.put("NoiDong", stringValue(record.get("PhuongTien.NoiDong")));
        phuongTien.put("NamDong", numberValue(record.get("PhuongTien.NamDong")));
        phuongTien.put("CongDungPhuongTien", stringValue(record.get("PhuongTien.CongDungPhuongTien")));
        phuongTien.put("ChieuDaiThietKe", numberValue(record.get("PhuongTien.ChieuDaiThietKe")));
        phuongTien.put("ChieuRongThietKe", numberValue(record.get("PhuongTien.ChieuRongThietKe")));
        phuongTien.put("ChieuDaiLonNhat", numberValue(record.get("PhuongTien.ChieuDaiLonNhat")));
        phuongTien.put("ChieuRongLonNhat", numberValue(record.get("PhuongTien.ChieuRongLonNhat")));
        phuongTien.put("SoLuongMC", numberValue(record.get("PhuongTien.SoLuongMC")));
        phuongTien.put("CongSuatMayChinh", numberValue(record.get("PhuongTien.CongSuatMayChinh")));
        phuongTien.put("VatLieuThanTau", stringValue(record.get("PhuongTien.VatLieuThanTau")));
        phuongTien.put("SoNguoiDuocCho", null);
        phuongTien.put("SucChoHang", null);
        phuongTien.put("TongDungTich", stringValue(record.get("PhuongTien.TongDungTich")));
        phuongTien.put("SoDangKyHanhChinh", stringValue(record.get("PhuongTien.SoDangKyHanhChinh")));
        return phuongTien;
    }

    private List<Map<String, Object>> buildCapPhuongTienThuyNoiDia(Object rawTenMuc) {
        String tenMuc = stringValue(rawTenMuc);
        if (tenMuc.isBlank()) {
            // Giữ nguyên output cũ khi nguồn không có cấp phương tiện.
            return List.of(buildDanhMucByMa("cap-phuong-tien", tenMuc));
        }

        List<Map<String, Object>> capPhuongTien = new ArrayList<>();
        for (String item : tenMuc.split(";")) {
            String normalizedTenMuc = item.trim();
            if (!normalizedTenMuc.isEmpty()) {
                capPhuongTien.add(buildDanhMucByMa("cap-phuong-tien", normalizedTenMuc));
            }
        }
        return capPhuongTien;
    }

    private List<Map<String, Object>> buildThongSoVungHoatDong(Map<String, Object> record) {
        Object rawValue = record.get(THONG_SO_VUNG_HOAT_DONG_FIELD);
        if (rawValue == null) {
            return List.of();
        }

        try {
            List<Map<String, Object>> rawItems = parseThongSoVungHoatDong(rawValue);

            List<Map<String, Object>> items = new ArrayList<>();
            for (Map<String, Object> rawItem : rawItems) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("VungHoatDong", buildDanhMucByMa(
                        "vung-hoat-dong-phuong-tien-thuy-noi-dia",
                        rawItem.get("MaMuc")
                ));
                item.put("ChieuChim", numberValue(rawItem.get("ChieuChim")));
                item.put("ManKho", numberValue(rawItem.get("ManKho")));
                item.put("TrongTaiToanPhan", numberValue(rawItem.get("TrongTaiToanPhan")));
                item.put("SoNguoiDuocCho", numberValue(rawItem.get("SoNguoiDuocCho")));
                item.put("SucChoHang", numberValue(rawItem.get("LuongHang")));
                items.add(item);
            }
            return items;
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot parse ThongSoVungHoatDong", ex);
        }
    }

    private List<Map<String, Object>> parseThongSoVungHoatDong(Object rawValue) throws Exception {
        if (rawValue instanceof List<?> list) {
            return objectMapper.convertValue(list, new TypeReference<>() {});
        }
        if (rawValue.toString().isBlank()) {
            return List.of();
        }
        return objectMapper.readValue(rawValue.toString(), new TypeReference<>() {});
    }

    private Map<String, Object> buildDanhMucByMa(String cacheName, Object maMuc) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("MaMuc", stringValue(maMuc));
        value.put("TenMuc", cacheName == null
                ? ""
                : Optional.ofNullable(danhMucCacheService.getTenMuc(cacheName, maMuc)).orElse(""));
        return value;
    }

    private Map<String, Object> buildDanhMucByTen(String cacheName, Object tenMuc) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("TenMuc", stringValue(tenMuc));
        value.put("MaMuc", cacheName == null
                ? ""
                : Optional.ofNullable(danhMucCacheService.getMaMuc(cacheName, tenMuc)).orElse(""));
        return value;
    }

    private String stringValue(Object value) {
        return value == null ? "" : value.toString();
    }

    private String dateValue(Object value) {
        return Optional.ofNullable(DateUtils.toDateString(value)).orElse("");
    }

    private Object numberValue(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof BigDecimal decimal) {
            return normalizeDecimal(decimal);
        }
        if (value instanceof Float || value instanceof Double) {
            return normalizeDecimal(BigDecimal.valueOf(((Number) value).doubleValue()));
        }
        return value;
    }

    private Object normalizeDecimal(BigDecimal decimal) {
        BigDecimal normalized = decimal.stripTrailingZeros();
        if (normalized.scale() > 0) {
            return normalized;
        }

        BigInteger integer = normalized.toBigIntegerExact();
        if (integer.bitLength() < Integer.SIZE) {
            return integer.intValue();
        }
        if (integer.bitLength() < Long.SIZE) {
            return integer.longValue();
        }
        return integer;
    }

    private void trimRecord(Map<String, Object> record) {
        record.replaceAll((key, value) -> value instanceof String text ? text.trim() : value);
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

    private record BatchCheckpoint(Object ngayCap, Object soGiay) {
    }
}
