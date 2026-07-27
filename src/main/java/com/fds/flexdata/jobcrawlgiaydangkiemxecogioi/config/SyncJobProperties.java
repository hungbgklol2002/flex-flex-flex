package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties(prefix = "sync.job")
@Getter
@Setter
@Configuration
public class SyncJobProperties {

    private String cron;
    private String zone;
    private ApiUrl apiUrl;
    private String apiUrlToken;
    private String clientId;
    private String clientSecret;
    private String username;
    private String password;
    private String apikey;
    private int batchSize;
    private String orderByColumn;

    @Getter
    @Setter
    public static class ApiUrl {
        private String xcg;
        private String tb;
        private String dt;
    }

}
