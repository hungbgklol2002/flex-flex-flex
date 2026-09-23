package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class DanhMucCacheService {

    private static final String REDIS_PREFIX = "jobcrawl:dm:";
    private static final String REDIS_BY_TEN_PREFIX = "jobcrawl:dm-by-ten:";
    private static final String REDIS_BY_SO_PHAN_CAP_PREFIX = "jobcrawl:dm-by-so-phan-cap:";
    private static final String REDIS_BY_SO_DANG_KY_CUC_HH_PREFIX = "jobcrawl:dm-by-so-dang-ky-cuc-hh:";
    private static final String REDIS_BY_SO_IMO_PREFIX = "jobcrawl:dm-by-so-imo:";

    private final RedissonClient redissonClient;

    public DanhMucItem get(String danhMucName, Object maMuc) {
        if (maMuc == null) {
            return null;
        }

        RMap<String, DanhMucItem> map =
                redissonClient.getMap(REDIS_PREFIX + danhMucName);

        return map.get(maMuc.toString());
    }

    public String getTenMuc(String danhMucName, Object maMuc) {
        DanhMucItem item = get(danhMucName, maMuc);
        return item == null ? null : item.getTenMuc();
    }

    public String getMaMuc(String danhMucName, Object tenMuc) {
        if (tenMuc == null) {
            return null;
        }

        RMap<String, DanhMucItem> map =
                redissonClient.getMap(REDIS_BY_TEN_PREFIX + danhMucName);

        DanhMucItem item = map.get(tenMuc.toString());
        return item == null ? null : item.getMaMuc();
    }

    public DanhMucItem getBySoPhanCap(String danhMucName, Object soPhanCap) {
        if (soPhanCap == null) {
            return null;
        }

        RMap<String, List<DanhMucItem>> map =
                redissonClient.getMap(REDIS_BY_SO_PHAN_CAP_PREFIX + danhMucName);

        List<DanhMucItem> items = map.get(soPhanCap.toString());

        if (items == null || items.size() != 1) {
            return null;
        }

        return items.get(0);
    }

    public DanhMucItem getBySoDangKyCucHH(String danhMucName, Object soDangKyCucHH) {
        if (soDangKyCucHH == null) {
            return null;
        }

        RMap<String, DanhMucItem> map =
                redissonClient.getMap(REDIS_BY_SO_DANG_KY_CUC_HH_PREFIX + danhMucName);

        return map.get(soDangKyCucHH.toString());
    }

    public DanhMucItem getBySoIMO(String danhMucName, Object soIMO) {
        if (soIMO == null) {
            return null;
        }

        RMap<String, DanhMucItem> map =
                redissonClient.getMap(REDIS_BY_SO_IMO_PREFIX + danhMucName);

        return map.get(soIMO.toString());
    }
}
