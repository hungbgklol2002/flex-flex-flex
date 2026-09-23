package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.DateUtils;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.RecordUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util.TokenUtil;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.SyncJobProperties;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository.GiayDangKiemPhuongTienDuongSatRepository;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
public class GiayDangKiemPhuongTienDuongSatSyncService {

    private static final Logger log = LoggerFactory.getLogger(GiayDangKiemPhuongTienDuongSatSyncService.class);
    private static final String LAST_ID_KEY = "jobcrawl:giaydangkiem-duongsat:last-id";
    private static final String CHECKPOINT_ID_FIELD = "__CHECKPOINT_ID";
    private static final String THONG_SO_KY_THUAT_FIELD = "__THONG_SO_KY_THUAT";
    // The /upsert endpoint accepts one BanTinDuLieu object, not a batch array.
    private static final int BATCH_SIZE = 10;
    private static final String NOI_TAO_BAN_TIN = "G17.46";

    private final GiayDangKiemPhuongTienDuongSatRepository repository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final RestClient restClient;
    private final TokenUtil tokenUtil;
    private final SyncJobProperties properties;
    private final RecordUtil recordUtil;
    private final DanhMucCacheService danhMucCacheService;

    public GiayDangKiemPhuongTienDuongSatSyncService(
            GiayDangKiemPhuongTienDuongSatRepository repository,
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            RestClient restClient,
            TokenUtil tokenUtil,
            SyncJobProperties properties,
            RecordUtil recordUtil,
            DanhMucCacheService danhMucCacheService
    ) {
        this.repository = repository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.restClient = restClient;
        this.tokenUtil = tokenUtil;
        this.properties = properties;
        this.recordUtil = recordUtil;
        this.danhMucCacheService = danhMucCacheService;
    }

    public void sync() {
        String token = tokenUtil.getAccessToken();
        String lastId = loadLastId();
        log.info("Loaded railway certificate checkpoint: id={}", lastId);

        while (true) {
            List<Map<String, Object>> records = repository.findDatas(lastId, BATCH_SIZE);
            if (records.isEmpty()) {
                log.info("Railway certificate sync completed. checkpoint={}", lastId);
                return;
            }

            records.forEach(this::trimRecord);
            String nextLastId = Objects.toString(records.get(records.size() - 1).get(CHECKPOINT_ID_FIELD), "");
            if (nextLastId.isBlank()) {
                throw new IllegalStateException("SQL query must return " + CHECKPOINT_ID_FIELD);
            }
            records.forEach(record -> record.remove(CHECKPOINT_ID_FIELD));

            try {
                String maBanTin = syncBatch(records, token);
                lastId = nextLastId;
                saveLastId(lastId);
                log.info("Railway certificate checkpoint saved. id={}, maBanTin={}, records={}", lastId, maBanTin, records.size());
            } catch (Exception ex) {
                log.error("Railway certificate sync failed. checkpoint={}, batchSize={}", lastId, records.size(), ex);
                for (Map<String, Object> record : records) {
                    recordUtil.logFailed(record.get("SoGiay"), nextLastId, ex.getMessage());
                }
                return;
            }
        }
    }

    private String syncBatch(List<Map<String, Object>> records, String token) {
        String maBanTin = "MBT-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("DacTaBanTin", Map.of("MaBanTin", maBanTin, "NoiTaoBanTin", NOI_TAO_BAN_TIN));
        // MOC bupsert gateway uses the same batch envelope as Tau Bien/Xe Co Gioi.
        body.put("DuLieuBanTin", records.stream()
                .map(this::buildBanTinDuLieu)
                .toList());
        body.put("ChuKySo", buildChuKySo());

//        try {
//            log.info(
//                    "Railway certificate request body. MaBanTin={}:\n{}",
//                    maBanTin,
//                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(body)
//            );
//        } catch (Exception ex) {
//            log.warn("Cannot serialize railway certificate request body. MaBanTin={}", maBanTin, ex);
//        }

        restClient.post()
                .uri(properties.getApiUrl().getDs())
                .contentType(MediaType.APPLICATION_JSON)
                .header("AuthorizationDC", "Bearer " + token)
                .header("ApiKey", properties.getApikey())
                .body(body)
                .retrieve()
                .onStatus(status -> status.isError(), (request, response) -> {
                    String errorBody = new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8);
                    throw new RuntimeException(errorBody);
                })
                .toBodilessEntity();

        log.info("Railway certificate push success. MaBanTin={}, records={}", maBanTin, records.size());
        return maBanTin;
    }

    private Map<String, Object> buildBanTinDuLieu(Map<String, Object> record) {
        Map<String, Object> banTin = new LinkedHashMap<>();
        banTin.put("MaDinhDanh", stringValue(record.get("MaDinhDanh")));
        banTin.put("SoGiay", stringValue(record.get("SoGiay")));
        banTin.put("SoTemKiemDinh", nullableString(record.get("SoTemKiemDinh")));
        banTin.put("SoDangKiem", stringValue(record.get("SoDangKiem")));
        banTin.put("NgayCap", dateValue(record.get("NgayCap")));
        banTin.put("NgayHetHan", dateValue(record.get("NgayHetHan")));
        banTin.put("NgayDangKiem", dateValue(record.get("NgayDangKiem")));
        banTin.put("BaoCaoKiemTra", dateValue(record.get("BaoCaoKiemTra")));
        banTin.put("NgayBaoCao", dateValue(record.get("NgayBaoCao")));
        banTin.put("LoaiKiemTra", null);
        banTin.put("CoSoSuaChua", null);
        banTin.put("DiaChiSuaChua", null);
        banTin.put("TinhTrangHieuLucGiayTo", tinhTrangHieuLuc(record));
        banTin.put("NoiCap", buildNoiCap(record));
        banTin.put("PhuongTien", buildPhuongTien(record));
        return banTin;
    }

    private Map<String, Object> buildNoiCap(Map<String, Object> record) {
        Map<String, Object> noiCap = new LinkedHashMap<>();
        noiCap.put("MaDinhDanh", NOI_TAO_BAN_TIN);
        noiCap.put("TenToChuc", "Cục Đăng kiểm Việt Nam");
        return noiCap;
    }

    private Map<String, Object> buildPhuongTien(Map<String, Object> record) {
        Map<String, Object> phuongTien = new LinkedHashMap<>();
        phuongTien.put("MaDinhDanh", upperCaseWithoutSpacesValue(record.get("PhuongTien.MaDinhDanh")));
        phuongTien.put("LoaiPhuongTienDuongSat", danhMuc(record, "PhuongTien.LoaiPhuongTienDuongSat", "loai-phuong-tien-duong-sat"));
        phuongTien.put("SoHieu", stringValue(record.get("PhuongTien.SoHieu")));
        phuongTien.put(
                "GiayDangKyPhuongTienDuongSat",
                giayDangKyPhuongTienDuongSat(record)
        );
        phuongTien.put("NuocSanXuat", danhMuc(record, "PhuongTien.NuocSanXuat", "quoc-gia"));
        phuongTien.put("NamSanXuat", numberValue(record.get("PhuongTien.NamSanXuat")));
        phuongTien.put("PhamViHoatDong", null);
        phuongTien.put("KhoDuongSat", khoDuongSat(record.get("PhuongTien.KhoDuongSat.MaMuc")));
        phuongTien.put("LoaiDauMay", null);
        phuongTien.put("LoaiToaXe", stringValue(record.get("PhuongTien.LoaiToaXe")));
        phuongTien.put("PhuongTienChuyenDung", stringValue(record.get("PhuongTien.PhuongTienChuyenDung")));
        phuongTien.put("KyHieuDongCo", stringValue(record.get("PhuongTien.KyHieuDongCo")));
        phuongTien.put("SoDongCo", stringValue(record.get("PhuongTien.SoDongCo")));
        phuongTien.put("CongThucTruc", stringValue(record.get("PhuongTien.CongThucTruc")));
        phuongTien.put("CongSuat", stringValue(record.get("PhuongTien.CongSuat")));
        phuongTien.put("DonViCongSuat", stringValue(record.get("PhuongTien.DonViCongSuat")));
        phuongTien.put("SoChoNgoi", numberValue(record.get("PhuongTien.SoChoNgoi")));
        phuongTien.put("SoChoNam", numberValue(record.get("PhuongTien.SoChoNam")));
        phuongTien.put("TrongTai", stringValue(record.get("PhuongTien.TrongTai")));
        phuongTien.put("KhoiLuongToaXe", stringValue(record.get("PhuongTien.KhoiLuongToaXe")));
        phuongTien.put("KieuTruyenDong", nullableString(record.get("PhuongTien.KieuTruyenDong")));
        phuongTien.put("GiaChuyenHuong", danhMucOrNull(record, "PhuongTien.GiaChuyenHuong", "gia-chuyen-huong"));
        phuongTien.put("LoaiVanHam", stringValue(record.get("PhuongTien.LoaiVanHam")));
        phuongTien.put("LoaiMocNoi", stringValue(record.get("PhuongTien.LoaiMocNoi")));
        phuongTien.put("LoaiDieuHoa", nullableString(record.get("PhuongTien.LoaiDieuHoa")));
        phuongTien.put("BanKinhCong", nullableString(record.get("PhuongTien.BanKinhCong")));
        phuongTien.put("LoaiMayPhatDien", nullableString(record.get("PhuongTien.LoaiMayPhatDien")));
        phuongTien.put("TocDoCauTao", stringValue(record.get("PhuongTien.TocDoCauTao")));
        phuongTien.put("TaiTrongTruc", nullableString(record.get("PhuongTien.TaiTrongTruc")));
        phuongTien.put("CongSuatNhiet", nullableString(record.get("PhuongTien.CongSuatNhiet")));
        phuongTien.put("CongSuatDinhMuc", nullableString(record.get("PhuongTien.CongSuatDinhMuc")));
        phuongTien.put("TuTrong", nullableString(record.get("PhuongTien.TuTrong")));
        phuongTien.put("TheTichThung", nullableString(record.get("PhuongTien.TheTichThung")));
        phuongTien.put("ChieuDai", stringValue(record.get("PhuongTien.ChieuDai")));
        phuongTien.put("ChieuRong", stringValue(record.get("PhuongTien.ChieuRong")));
        phuongTien.put("ChieuCao", stringValue(record.get("PhuongTien.ChieuCao")));
        phuongTien.put("ThongSoKyThuat", technicalParameters(record));
//        phuongTien.put("GiayDangKiemPhuongTienDuongSat", null);
//        phuongTien.put("@type", stringValue(record.get("PhuongTien.@type")));
        return phuongTien;
    }

    private Map<String, Object> tinhTrangHieuLuc(Map<String, Object> record) {
        Object maMuc = record.get("TinhTrangHieuLucGiayTo.MaMuc");
        DanhMucItem item = danhMucCacheService.get("tinh-trang-hieu-luc-giay-to-tau-bien", maMuc);
        Map<String, Object> tinhTrang = new LinkedHashMap<>();
        tinhTrang.put("MaMuc", formatTwoDigits(maMuc));
        tinhTrang.put("TenMuc", item == null ? "" : stringValue(item.getTenMuc()));
        return tinhTrang;
    }

    private Map<String, Object> khoDuongSat(Object maMuc) {
        Map<String, Object> khoDuong = new LinkedHashMap<>();
        String ma = formatTwoDigits(maMuc);
        khoDuong.put("MaMuc", ma);
        khoDuong.put("TenMuc", switch (ma) {
            case "01" -> "Khổ 1000 mm";
            case "02" -> "Khổ 1435 mm";
            case "03" -> "Khổ lồng";
            default -> "";
        });
        return khoDuong;
    }

    private Map<String, Object> danhMuc(Map<String, Object> record, String prefix, String cacheName) {
        Object maMuc = record.get(prefix + ".MaMuc");
        DanhMucItem item = danhMucCacheService.get(cacheName, maMuc);
        Map<String, Object> danhMuc = new LinkedHashMap<>();
        danhMuc.put("MaMuc", stringValue(maMuc));
        danhMuc.put("TenMuc", item == null
                ? stringValue(record.get(prefix + ".TenMuc"))
                : stringValue(item.getTenMuc()));
        return danhMuc;
    }

    private Map<String, Object> danhMucOrNull(Map<String, Object> record, String prefix, String cacheName) {
        Object maMuc = record.get(prefix + ".MaMuc");
        return maMuc == null || maMuc.toString().isBlank()
                ? null
                : danhMuc(record, prefix, cacheName);
    }

    private List<Map<String, Object>> technicalParameters(Map<String, Object> record) {
        Object value = record.get(THONG_SO_KY_THUAT_FIELD);
        if (!(value instanceof List<?> parameters)) {
            return List.of();
        }
        return parameters.stream()
                .filter(Map.class::isInstance)
                .map(Map.class::cast)
                .map(this::technicalParameter)
                .toList();
    }

    private Map<String, Object> technicalParameter(Map<?, ?> parameter) {
        Map<String, Object> key = new LinkedHashMap<>();
        key.put("MaMuc", stringValue(parameter.get("Key.MaMuc")));
        key.put("TenMuc", stringValue(parameter.get("Key.TenMuc")));
        key.put("KieuDuLieu", Map.of(
                "MaMuc", stringValue(parameter.get("Key.KieuDuLieu.MaMuc")),
                "TenMuc", stringValue(parameter.get("Key.KieuDuLieu.TenMuc"))
        ));

        Map<String, Object> item = new LinkedHashMap<>();
        item.put("Key", key);
        item.put("Value", stringValue(parameter.get("Value")));
        return item;
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
                "signatures", List.of(Map.of(
                        "protected", "eyJhbGciOiJSUzI1NiJ9",
                        "signature", "Q1h5c2lnbmF0dXJlYmFzZTY0...",
                        "header", Map.of("jades", Map.of(
                                "signingTime", "2025-09-24T10:00:00Z",
                                "signingCertificate", Map.of("digest", Map.of(
                                        "alg", "sha256",
                                        "value", "f2ca1bb6c7e907d06dafe4687e579fce..."
                                ))
                        ))
                ))
        );
    }

    private void trimRecord(Map<String, Object> record) {
        record.replaceAll((key, value) -> value instanceof String text ? text.trim() : value);
    }

    private String stringValue(Object value) {
        return value == null ? "" : value.toString();
    }

    private String upperCaseWithoutSpacesValue(Object value) {
        return stringValue(value)
                .replace(" ", "")
                .toUpperCase(Locale.ROOT);
    }

    private String nullableString(Object value) {
        return value == null ? null : value.toString();
    }

    private Map<String, Object> giayDangKyPhuongTienDuongSat(Map<String, Object> record) {
        String soDangKy = nullableString(record.get("PhuongTien.GiayDangKyPhuongTienDuongSat"));
        if (soDangKy == null || soDangKy.isBlank()) {
            return null;
        }

        Map<String, Object> giayDangKy = new LinkedHashMap<>();
        giayDangKy.put("SoDangKy", soDangKy);
        giayDangKy.put("NgayCap", null);
        giayDangKy.put("NoiCap", null);
        return giayDangKy;
    }

    private String formatTwoDigits(Object value) {
        if (value == null || value.toString().isBlank()) {
            return "";
        }
        try {
            return String.format("%02d", Integer.parseInt(value.toString()));
        } catch (NumberFormatException ex) {
            return value.toString();
        }
    }

    private String dateValue(Object value) {
        return Optional.ofNullable(DateUtils.toDateString(value)).orElse("");
    }

    private Object numberValue(Object value) {
        return value == null ? 0 : value;
    }
}
