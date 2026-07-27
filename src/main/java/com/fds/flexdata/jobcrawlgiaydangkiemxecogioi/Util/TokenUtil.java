package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.Util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.config.SyncJobProperties;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

@Component
public class TokenUtil {

    private final RestClient restClient;
    private final SyncJobProperties syncJobProperties;

    public TokenUtil(RestClient.Builder builder,
                     SyncJobProperties syncJobProperties) {
        this.restClient = builder.build();
        this.syncJobProperties = syncJobProperties;
    }

    public String getAccessToken() {

        MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
        formData.add("username", syncJobProperties.getUsername());
        formData.add("password", syncJobProperties.getPassword());
        formData.add("client_id", syncJobProperties.getClientId());
        formData.add("grant_type", "password");

        JsonNode response = restClient.post()
                .uri(syncJobProperties.getApiUrlToken())
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .header("ApiKey",syncJobProperties.getApikey())
                .accept(MediaType.APPLICATION_JSON)
                .body(formData)
                .retrieve()
                .body(JsonNode.class);

        if (response == null || response.get("access_token") == null) {
            throw new RuntimeException("Không lấy được access_token từ Keycloak");
        }

        return response.get("access_token").asText();
    }
}