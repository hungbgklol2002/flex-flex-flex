package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config;

import org.springframework.stereotype.Component;

@Component
public class AppState {

    private volatile boolean redisLoaded = false;

    public boolean isRedisLoaded() {
        return redisLoaded;
    }

    public void setRedisLoaded(boolean redisLoaded) {
        this.redisLoaded = redisLoaded;
    }
}