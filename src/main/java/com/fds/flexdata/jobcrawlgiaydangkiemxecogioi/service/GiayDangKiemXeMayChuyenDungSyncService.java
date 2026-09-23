package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.DateUtils;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.RecordUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.TokenUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.SyncJobProperties;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository.GiayDangKiemXeMayChuyenDungRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
public class GiayDangKiemXeMayChuyenDungSyncService {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemXeMayChuyenDungSyncService.class);
    private static final String LAST_ID_KEY = "jobcrawl:giaydangkiem-xmcd:last-id";
    private static final String CHECKPOINT_ID_FIELD = "__CHECKPOINT_ID";
    private static final String THONG_SO_KY_THUAT_FIELD = "__THONG_SO_KY_THUAT_DAC_TRUNG";
    private static final String NOI_TAO_BAN_TIN = "G17.46";
    private static final int BATCH_SIZE = 1000;

    private final GiayDangKiemXeMayChuyenDungRepository repository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final DanhMucCacheService danhMucCacheService;
    private final RestClient restClient;
    private final TokenUtil tokenUtil;
    private final SyncJobProperties properties;
    private final RecordUtil recordUtil;

    public GiayDangKiemXeMayChuyenDungSyncService(
            GiayDangKiemXeMayChuyenDungRepository repository,
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            DanhMucCacheService danhMucCacheService,
            RestClient restClient,
            TokenUtil tokenUtil,
            SyncJobProperties properties,
            RecordUtil recordUtil
    ) {
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.danhMucCacheService = danhMucCacheService;
        this.restClient = restClient;
        this.tokenUtil = tokenUtil;
        this.properties = properties;
        this.recordUtil = recordUtil;
    }

    public void sync() {
        String token = tokenUtil.getAccessToken();
        String lastId = loadLastId();
        log.info("Loaded XMCD checkpoint: id={}", lastId);

        while (true) {
            List<Map<String, Object>> records = repository.findDatas(lastId, BATCH_SIZE);
            if (records.isEmpty()) {
                log.info("XMCD sync completed. checkpoint={}", lastId);
                return;
            }

            records.forEach(this::trimRecord);
            String nextLastId = Objects.toString(records.get(records.size() - 1).get(CHECKPOINT_ID_FIELD), "");
            records.forEach(record -> record.remove(CHECKPOINT_ID_FIELD));

            try {
                syncBatch(records, token);
                lastId = nextLastId;
                saveLastId(lastId);
                log.info("XMCD checkpoint saved. id={}, records={}", lastId, records.size());
            } catch (Exception ex) {
                log.error("XMCD sync failed. checkpoint={}, batchSize={}", lastId, records.size(), ex);
                for (Map<String, Object> record : records) {
                    recordUtil.logFailed(record.get("SoGiay"), nextLastId, ex.getMessage());
                }
                return;
            }
        }
    }

    private void syncBatch(
            List<Map<String, Object>> records,
            String token
    ) throws Exception {
        String maBanTin = "MBT-" + LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        Map<String, Object> requestBody = buildRequestBody(records, maBanTin);

        String requestBodyJson = objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(requestBody);
//        log.info("XMCD request body:\n{}", requestBodyJson);

        restClient.post()
                .uri(properties.getApiUrl().getXmcd())
                .contentType(MediaType.APPLICATION_JSON)
                .header("AuthorizationDC", "Bearer " + token)
                .header("ApiKey", properties.getApikey())
                .body(requestBody)
                .retrieve()
                .onStatus(
                        status -> status.isError(),
                        (request, response) -> {
                            String errorBody = new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8);
                            throw new RuntimeException(errorBody);
                        }
                )
                .toBodilessEntity();

        log.info("XMCD push success. MaBanTin={}, records={}", maBanTin, records.size());
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
        requestBody.put("DuLieuBanTin", records.stream()
                .map(this::buildBanTinDuLieu)
                .toList());
        requestBody.put("ChuKySo", buildChuKySo());
        return requestBody;
    }

    private String loadLastId() {
        return Optional.ofNullable(redisTemplate.opsForValue().get(LAST_ID_KEY)).orElse("");
    }

    private void saveLastId(String lastId) {
        redisTemplate.opsForValue().set(LAST_ID_KEY, lastId);
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
        Map<String, Object> banTin = new LinkedHashMap<>();
        banTin.put("MaDinhDanh", stringValue(record.get("MaDinhDanh")));
        banTin.put("SoGiay", stringValue(record.get("SoGiay")));
        banTin.put("NgayCap", dateValue(record.get("NgayCap")));
        banTin.put("NoiCap", buildNoiCap(record));
        banTin.put("NgayHetHan", dateValue(record.get("NgayHetHan")));
        banTin.put("SoBienBanKiemTra", stringValue(record.get("SoBienBanKiemTra")));
        banTin.put("ThoiGianKiemTra", dateValue(record.get("ThoiGianKiemTra")));
        banTin.put("DiaChiKiemTra", stringValue(record.get("DiaChiKiemTra")));
        banTin.put("SoDangKyKiemTra", stringValue(record.get("SoDangKyKiemTra")));
        banTin.put("PhuongTien", buildPhuongTien(record));
        banTin.put("TinhTrangHieuLucGiayTo", buildTinhTrangHieuLuc(record));
        List<Map<String, Object>> thongSoKyThuat = buildThongSoKyThuatDacTrung(record);
        if (!thongSoKyThuat.isEmpty()) {
            banTin.put("ThongSoKyThuatDacTrung", thongSoKyThuat);
        }
        return banTin;
    }

    private Map<String, Object> buildNoiCap(Map<String, Object> record) {
        Map<String, Object> noiCap = new LinkedHashMap<>();
        noiCap.put("MaDinhDanh", stringValue(record.get("NoiCap.MaDinhDanh")));
        noiCap.put("TenToChuc", stringValue(record.get("NoiCap.TenDinhDanh")));
        return noiCap;
    }

    private Map<String, Object> buildTinhTrangHieuLuc(Map<String, Object> record) {
        Object maMuc = record.get("TinhTrangHieuLucGiayTo.MaMuc");
        DanhMucItem item = danhMucCacheService.get("tinh-trang-hieu-luc-giay-to", maMuc);
        Map<String, Object> tinhTrang = new LinkedHashMap<>();
        tinhTrang.put("MaMuc", String.format("%02d", Integer.parseInt(stringValue(maMuc))));
        tinhTrang.put("TenMuc", item == null ? "" : stringValue(item.getTenMuc()));
        return tinhTrang;
    }

    private Map<String, Object> buildPhuongTien(Map<String, Object> record) {
        Map<String, Object> phuongTien = new LinkedHashMap<>();
        phuongTien.put("MaDinhDanh", stringValue(record.get("PhuongTien.MaDinhDanh")));
        phuongTien.put("SoQuanLy", stringValue(record.get("PhuongTien.SoQuanLy")));
        phuongTien.put("BienSoXe", stringValue(record.get("PhuongTien.BienSoXe")));
        phuongTien.put("SoKhung", stringValue(record.get("PhuongTien.SoKhung")));
        phuongTien.put(
                "NhomPhuongTienXeMayChuyenDung",
                buildDanhMuc(
                        "nhom-phuong-tien-xe-may-chuyen-dung",
                        record.get("PhuongTien.NhomPhuongTienXeMayChuyenDung.MaMuc")
                )
        );
        phuongTien.put(
                "LoaiPhuongTienXeMayChuyenDung",
                buildDanhMuc(
                        "loai-phuong-tien-xe-may-chuyen-dung",
                        record.get("PhuongTien.LoaiPhuongTienXeMayChuyenDung.MaMuc")
                )
        );
        phuongTien.put("MaKieuLoai", stringValue(record.get("PhuongTien.MaKieuLoai")));
        phuongTien.put("TenThuongMai", stringValue(record.get("PhuongTien.TenThuongMai")));
        phuongTien.put("NhanHieu", stringValue(record.get("PhuongTien.NhanHieu")));
        phuongTien.put("SoDongCo", stringValue(record.get("PhuongTien.SoDongCo")));
        phuongTien.put("DaCaiTao", numberValue(record.get("PhuongTien.DaCaiTao")));
        phuongTien.put("NuocSanXuat", List.of(buildNuocSanXuat(record)));
        phuongTien.put("NamSanXuat", numberValue(record.get("PhuongTien.NamSanXuat")));
        phuongTien.put("KhoiLuongBanThan", stringValue(record.get("PhuongTien.KhoiLuongBanThan")));
        phuongTien.put("ChieuDai", stringValue(record.get("PhuongTien.ChieuDai")));
        phuongTien.put("ChieuRong", stringValue(record.get("PhuongTien.ChieuRong")));
        phuongTien.put("ChieuCao", stringValue(record.get("PhuongTien.ChieuCao")));
        phuongTien.put("KyHieuDongCo", stringValue(record.get("PhuongTien.KyHieuDongCo")));
        phuongTien.put("LoaiDongCo", buildDanhMuc("loai-dong-co",record.get("PhuongTien.LoaiDongCo")));
        phuongTien.put("LoaiNhienLieu", buildDanhMuc("loai-nhien-lieu", record.get("PhuongTien.LoaiNhienLieu.MaMuc")));
        phuongTien.put("CongSuat", stringValue(record.get("PhuongTien.CongSuat")));
        phuongTien.put("TocDoQuay", stringValue(record.get("PhuongTien.TocDoQuay")));
        phuongTien.put("VanTocDiChuyen", stringValue(record.get("PhuongTien.VanTocDiChuyen")));
        phuongTien.put("@type", "T_XeMayChuyenDung");
        return phuongTien;
    }

    private Map<String, Object> buildNuocSanXuat(Map<String, Object> record) {
        Object maMuc = record.get("PhuongTien.NuocSanXuat.MaMuc");
        DanhMucItem item = danhMucCacheService.get("quoc-gia", maMuc);
        Map<String, Object> nuocSanXuat = new LinkedHashMap<>();
        nuocSanXuat.put("MaMuc", stringValue(maMuc));
        nuocSanXuat.put("TenMuc", item == null
                ? stringValue(record.get("PhuongTien.NuocSanXuat.TenMuc"))
                : stringValue(item.getTenMuc()));
        return nuocSanXuat;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> buildThongSoKyThuatDacTrung(Map<String, Object> record) {
        Object rawValue = record.get(THONG_SO_KY_THUAT_FIELD);
        if (rawValue instanceof List<?> items) {
            return items.stream()
                    .filter(Map.class::isInstance)
                    .map(item -> (Map<String, Object>) item)
                    .map(this::buildThongSoKyThuatItem)
                    .toList();
        }
        return List.of();
    }

    private Map<String, Object> buildThongSoKyThuatItem(Map<String, Object> row) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("MaThongSo", stringValue(row.get("MaThongSo")));
        item.put("TenThongSo", stringValue(row.get("TenThongSo")));
        item.put("GiaTriThongSo", stringValue(row.get("GiaTriThongSo")));
        item.put("DonViDo", stringValue(row.get("DonViDo")));
        return item;
    }

    private Map<String, Object> buildDanhMuc(
            String cacheName,
            Object maMuc
    ) {
        DanhMucItem item = cacheName == null ? null : danhMucCacheService.get(cacheName, maMuc);
        Map<String, Object> danhMuc = new LinkedHashMap<>();
        danhMuc.put("MaMuc", stringValue(maMuc));
        danhMuc.put("TenMuc", item == null ? "" : stringValue(item.getTenMuc()));
        return danhMuc;
    }

    private void trimRecord(Map<String, Object> record) {
        record.replaceAll((key, value) -> value instanceof String text ? text.trim() : value);
    }

    private String stringValue(Object value) {
        return value == null ? "" : value.toString();
    }

    private String dateValue(Object value) {
        return Optional.ofNullable(DateUtils.toDateString(value)).orElse("");
    }

    private Object numberValue(Object value) {
        return value == null ? 0 : value;
    }

    private boolean booleanValue(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() == 1;
        }
        return "1".equals(String.valueOf(value))
                || "true".equalsIgnoreCase(String.valueOf(value));
    }
}
