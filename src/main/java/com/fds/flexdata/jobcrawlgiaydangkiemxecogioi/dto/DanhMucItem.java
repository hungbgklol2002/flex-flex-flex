package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DanhMucItem {

    @JsonProperty("MaMuc")
    private String maMuc;

    @JsonProperty("TenMuc")
    private String tenMuc;

    @JsonProperty("MaDinhDanh")
    private String maDinhDanh;

    @JsonProperty("TenToChuc")
    private String tenToChuc;

    public String getRedisKey() {
        if (maMuc != null && !maMuc.isBlank()) {
            return maMuc;
        }

        if (maDinhDanh != null && !maDinhDanh.isBlank()) {
            return maDinhDanh;
        }

        return null;
    }
}