package com.enterprise.salesforce.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages Salesforce OAuth 2.0 authentication using the
 * Username-Password flow (suitable for server-to-server / daemon processes).
 *
 * <p>For production systems, prefer Connected App JWT Bearer flow to avoid
 * storing user passwords. This class is structured so that the token-fetch
 * logic can be swapped without changing callers.
 *
 * <p>Thread-safety: token refresh is guarded by a ReentrantLock so that
 * concurrent worker threads do not trigger duplicate token requests.
 */
public class SalesforceAuthManager {

    private static final Logger log = LoggerFactory.getLogger(SalesforceAuthManager.class);

    // Salesforce token endpoint – use login.salesforce.com for production,
    // test.salesforce.com for sandboxes.
    private static final String TOKEN_ENDPOINT_TEMPLATE = "%s/services/oauth2/token";

    // Proactively refresh the token this many seconds before it expires.
    private static final long REFRESH_BUFFER_SECONDS = 120;

    private final String loginUrl;
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password; // password + security token concatenated
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ReentrantLock lock = new ReentrantLock();

    // Mutable auth state – guarded by `lock`
    private volatile BearerToken currentToken;

    public SalesforceAuthManager(String loginUrl,
                                  String clientId,
                                  String clientSecret,
                                  String username,
                                  String passwordWithToken) {
        this.loginUrl = loginUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = passwordWithToken;
    }

    /**
     * Returns a valid bearer token, refreshing if necessary.
     * Multiple threads calling this concurrently will block on the lock;
     * only the first thread triggers an actual HTTP call.
     */
    public BearerToken getValidToken() throws IOException {
        // Fast path: token still valid
        if (currentToken != null && !currentToken.isExpiringSoon(REFRESH_BUFFER_SECONDS)) {
            return currentToken;
        }

        lock.lock();
        try {
            // Double-checked locking: re-validate after acquiring lock
            if (currentToken != null && !currentToken.isExpiringSoon(REFRESH_BUFFER_SECONDS)) {
                return currentToken;
            }
            log.info("Obtaining/refreshing Salesforce bearer token...");
            currentToken = fetchToken();
            log.info("Token obtained. Instance URL: {}", currentToken.instanceUrl());
            return currentToken;
        } finally {
            lock.unlock();
        }
    }

    /** Force-refreshes the token regardless of expiry (call after 401 responses). */
    public BearerToken forceRefresh() throws IOException {
        lock.lock();
        try {
            currentToken = fetchToken();
            return currentToken;
        } finally {
            lock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private BearerToken fetchToken() throws IOException {
        String tokenUrl = String.format(TOKEN_ENDPOINT_TEMPLATE, loginUrl);

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("grant_type", "client_credentials"));
        params.add(new BasicNameValuePair("client_id", clientId));
        params.add(new BasicNameValuePair("client_secret", clientSecret));
        params.add(new BasicNameValuePair("username", username));
        params.add(new BasicNameValuePair("password", password));

        HttpPost post = new HttpPost(tokenUrl);
        post.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
        post.setHeader("Accept", "application/json");

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            return httpClient.execute(post, response -> {
                int status = response.getCode();
                byte[] body = response.getEntity().getContent().readAllBytes();

                if (status != 200) {
                    String errorBody = new String(body, StandardCharsets.UTF_8);
                    throw new IOException(
                        "Salesforce token request failed. HTTP " + status + ": " + errorBody);
                }

                JsonNode json = objectMapper.readTree(body);
                String accessToken = json.get("access_token").asText();
                String instanceUrl = json.get("instance_url").asText();

                // Salesforce sessions are valid for a configurable session timeout (default 2 hours).
                // The token endpoint does NOT return expires_in, so we set a conservative TTL.
                Instant expiresAt = Instant.now().plusSeconds(7200);

                return new BearerToken(accessToken, instanceUrl, expiresAt);
            });
        }
    }

    // -----------------------------------------------------------------------
    // Value object
    // -----------------------------------------------------------------------

    /**
     * Immutable holder for a Salesforce bearer token and its associated
     * instance URL (needed to build the CometD endpoint).
     */
    public record BearerToken(String accessToken, String instanceUrl, Instant expiresAt) {

        /** Returns true if the token expires within {@code bufferSeconds}. */
        public boolean isExpiringSoon(long bufferSeconds) {
            return Instant.now().isAfter(expiresAt.minusSeconds(bufferSeconds));
        }

        /** Full CometD streaming endpoint for this org. */
        public String cometDUrl() {
            return instanceUrl + "/cometd/57.0"; // API version 57.0 (Winter '23+)
        }
    }
}
