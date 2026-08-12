package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.runner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.AppState;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class DanhMucRedisLoader implements ApplicationRunner {

    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final AppState appState;
    private static final String REDIS_PREFIX = "jobcrawl:dm:";
    private static final String REDIS_BY_TEN_PREFIX = "jobcrawl:dm-by-ten:";
    private static final String REDIS_BY_SO_PHAN_CAP_PREFIX = "jobcrawl:dm-by-so-phan-cap:";
    private static final String REDIS_BY_SO_DANG_KY_CUC_HH_PREFIX = "jobcrawl:dm-by-so-dang-ky-cuc-hh:";
    private static final String REDIS_BY_SO_IMO_PREFIX = "jobcrawl:dm-by-so-imo:";

    @Override
    public void run(ApplicationArguments args) throws Exception {
        load("loai-dong-co", "danhmuc/CSDL_PhuongTien.C_LoaiDongCo.json");
        load("nguon-goc", "danhmuc/CSDL_PhuongTien.C_NguonGocXCG.json");
        load("phan-loai-xe", "danhmuc/CSDL_PhuongTien.C_PhanLoaiXeCoGioi.json");
        load("quoc-gia", "danhmuc/CSDL_DungChung.C_QuocGia.json");
        load("loai-nhien-lieu", "danhmuc/CSDL_PhuongTien.C_LoaiNhienLieu.json");
        load("tinh-trang-hieu-luc-giay-to", "danhmuc/CSDL_DungChung.C_TinhTrangHieuLucGiayTo.json");
        load("tinh-trang-hieu-luc-giay-to-tau-bien", "danhmuc/CSDL_DungChung.C_TinhTrangHieuLucGiayToTauBien.json");
        load("loai-giay-dang-kiem-tau-bien", "danhmuc/CSDL_PhuongTien.C_LoaiGiayDangKiemTauBien.json");
        load("tuyen-khai-thac-tau-bien", "danhmuc/CSDL_PhuongTien.C_TuyenKhaiThacTauBien.json");
        load("vung-hoat-dong-tau-bien", "danhmuc/CSDL_PhuongTien.C_VungHoatDongTauBien.json");
        load("nhom-phuong-tien-tau-bien", "danhmuc/CSDL_PhuongTien.C_NhomPhuongTienTauBien.json");
        load("nhom-phuong-tien-thuy-noi-dia", "danhmuc/CSDL_PhuongTien.C_NhomPhuongTienThuyNoiDia.json");
        load("co-quan-don-vi", "danhmuc/CSDL_DungChung.T_CoQuanDonVi.json");
        load("cap-phuong-tien", "danhmuc/CSDL_PhuongTien.C_CapPhuongTienThuyNoiDia.json");
        load("vung-hoat-dong-phuong-tien-thuy-noi-dia", "danhmuc/CSDL_PhuongTien.C_VungHoatDongPhuongTienThuyNoiDia.json");
        load("loai-phuong-tien-xe-may-chuyen-dung", "danhmuc/CSDL_PhuongTien.C_LoaiPhuongTienXeMayChuyenDung.json");
        load("nhom-phuong-tien-xe-may-chuyen-dung", "danhmuc/CSDL_PhuongTien.C_NhomPhuongTienXeMayChuyenDung.json");
        loadTauBienKhacPattern("tau-bien-khac-pattern", "danhmuc/Tau_bien_khac_pattern_1_2so.json");
        appState.setRedisLoaded(true);
    }

    private void load(String name, String path) throws IOException {
        Resource resource = new ClassPathResource(path);

        List<DanhMucItem> items = objectMapper.readValue(
                resource.getInputStream(),
                new TypeReference<List<DanhMucItem>>() {}
        );

        RMap<String, DanhMucItem> map =
                redissonClient.getMap(REDIS_PREFIX + name);
        RMap<String, DanhMucItem> mapByTen =
                redissonClient.getMap(REDIS_BY_TEN_PREFIX + name);

        map.clear();
        mapByTen.clear();

        for (DanhMucItem item : items) {
            String key = item.getRedisKey();

            if (key == null || key.isBlank()) {
                log.warn("Skip item because key is null. item={}", item);
                continue;
            }

            map.put(key, item);

            if (item.getTenMuc() != null && !item.getTenMuc().isBlank()) {
                mapByTen.put(item.getTenMuc(), item);
            }
        }

        log.info("Loaded danh muc {} to Redis, size={}", name, map.size());
    }

    private void loadTauBienKhacPattern(String name, String path) throws IOException {
        Resource resource = new ClassPathResource(path);

        List<DanhMucItem> items = objectMapper.readValue(
                resource.getInputStream(),
                new TypeReference<List<DanhMucItem>>() {}
        );

        RMap<String, DanhMucItem> map =
                redissonClient.getMap(REDIS_PREFIX + name);
        RMap<String, List<DanhMucItem>> mapBySoPhanCap =
                redissonClient.getMap(REDIS_BY_SO_PHAN_CAP_PREFIX + name);
        RMap<String, DanhMucItem> mapBySoDangKyCucHH =
                redissonClient.getMap(REDIS_BY_SO_DANG_KY_CUC_HH_PREFIX + name);
        RMap<String, DanhMucItem> mapBySoIMO =
                redissonClient.getMap(REDIS_BY_SO_IMO_PREFIX + name);

        map.clear();
        mapBySoPhanCap.clear();
        mapBySoDangKyCucHH.clear();
        mapBySoIMO.clear();

        Map<String, List<DanhMucItem>> itemsBySoPhanCap = new HashMap<>();

        for (DanhMucItem item : items) {
            String soPhanCap = normalizeKey(item.getSoPhanCap());

            if (soPhanCap != null) {
                itemsBySoPhanCap
                        .computeIfAbsent(soPhanCap, key -> new ArrayList<>())
                        .add(item);
            }
        }

        mapBySoPhanCap.putAll(itemsBySoPhanCap);

        for (DanhMucItem item : items) {
            putIfPresent(map, item.getMaDinhDanh(), item);
            putIfPresent(mapBySoDangKyCucHH, item.getSoDangKyCucHH(), item);
            putIfPresent(mapBySoIMO, item.getSoIMO(), item);
        }

        long duplicateSoPhanCapCount = itemsBySoPhanCap.values()
                .stream()
                .filter(phanCapItems -> phanCapItems.size() > 1)
                .count();

        log.info(
                "Loaded danh muc {} to Redis, size={}, bySoPhanCap={}, duplicateSoPhanCap={}, bySoDangKyCucHH={}, bySoIMO={}",
                name,
                map.size(),
                mapBySoPhanCap.size(),
                duplicateSoPhanCapCount,
                mapBySoDangKyCucHH.size(),
                mapBySoIMO.size()
        );
    }

    private void putIfPresent(
            RMap<String, DanhMucItem> map,
            String key,
            DanhMucItem item
    ) {
        String normalizedKey = normalizeKey(key);

        if (normalizedKey == null) {
            return;
        }

        map.put(normalizedKey, item);
    }

    private String normalizeKey(String key) {
        if (key == null || key.isBlank() || "-".equals(key.trim())) {
            return null;
        }

        return key.trim();
    }
}
