package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.service;

import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto.DanhMucItem;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DanhMucCacheService {

    private final RedissonClient redissonClient;

    private static final String REDIS_PREFIX = "jobcrawl:dm:";
    private static final String REDIS_BY_TEN_PREFIX = "jobcrawl:dm-by-ten:";

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
}
