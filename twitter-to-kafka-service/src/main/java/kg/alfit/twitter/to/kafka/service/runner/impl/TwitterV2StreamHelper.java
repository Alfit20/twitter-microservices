package kg.alfit.twitter.to.kafka.service.runner.impl;

import kg.alfit.config.TwitterToKafkaServiceConfig;
import kg.alfit.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
@Slf4j
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
@RequiredArgsConstructor
public class TwitterV2StreamHelper {
    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final String TWEET_AS_ROW_JSON = "{" +
            "\"created_at\":\"{0}\" ," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"" +
            "}";


    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";


    void connectStream(String bearerToken) throws URISyntaxException, IOException {
        CloseableHttpClient httpClient = createHttpClient();
        HttpGet httpGetRequest = createHttpGetRequest(bearerToken);
        CloseableHttpResponse response = httpClient.execute(httpGetRequest);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
            String line = reader.readLine();
            while (line != null) {
                line = reader.readLine();
                if (!line.isEmpty()) {
                    String tweet = getFormattedTweet(line);
                    Status status = null;
                    try {
                        status = TwitterObjectFactory.createStatus(tweet);
                    } catch (TwitterException e) {
                        log.error("Could not create status for text: {}", tweet, e);
                    }
                    if (status != null) {
                        twitterKafkaStatusListener.onStatus(status);
                    }
                }
            }
        }
    }


    void setupRules(String bearerToken, Map<String, String> rules) throws URISyntaxException, IOException {
        List<String> existingRules = getRules(bearerToken);
        if (existingRules.size() > 0) {
            deleteRules(bearerToken, existingRules);
        }
        createRules(bearerToken, rules);
        log.info("Created rules for twitter stream {}", rules.keySet().toArray());
    }

    private void createRules(String bearerToken, Map<String, String> rules) throws URISyntaxException, IOException {
        CloseableHttpClient httpClient = createHttpClient();
        HttpPost httpPostRequest = createHttpPostRequest(bearerToken);
        httpPostRequest.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
        httpPostRequest.setEntity(body);
        CloseableHttpResponse response = httpClient.execute(httpPostRequest);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            log.info(EntityUtils.toString(entity, "UTF-8"));
        }

    }

    private String getFormattedString(String string, List<String> ids) {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1) {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } else {
            for (String id : ids) {
                sb.append("\"" + id + "\"" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    private String getFormattedString(String string, Map<String, String> rules) {
        StringBuilder sb = new StringBuilder();
        if (rules.size() == 1) {
            String key = rules.keySet().iterator().next();
            return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
        } else {
            for (Map.Entry<String, String> entry : rules.entrySet()) {
                String value = entry.getKey();
                String tag = entry.getValue();
                sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }


    private void deleteRules(String bearerToken, List<String> existingRules) throws URISyntaxException, IOException {
        CloseableHttpClient httpClient = createHttpClient();
        HttpPost httpPostRequest = createHttpPostRequest(bearerToken);
        httpPostRequest.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}",
                existingRules));
        httpPostRequest.setEntity(body);
        CloseableHttpResponse response = httpClient.execute(httpPostRequest);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            log.info(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    private List<String> getRules(String bearerToken) throws URISyntaxException, IOException {
        List<String> rules = new ArrayList<>();
        CloseableHttpClient httpClient = createHttpClient();
        HttpGet httpGetRequest = createHttpGetRequest(bearerToken);
        httpGetRequest.setHeader("content-type", "application/json");
        CloseableHttpResponse execute = httpClient.execute(httpGetRequest);
        HttpEntity entity = execute.getEntity();
        if (entity != null) {
            JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1 && json.has("data")) {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++) {
                    JSONObject jsonObject = (JSONObject) array.get(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }

    private String getFormattedTweet(String data) {
        JSONObject jsonData = (JSONObject) new JSONObject(data).get("data");
        String[] params = new String[]{
                ZonedDateTime.parse(jsonData.get("created_at").toString()).
                        withZoneSameInstant(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                jsonData.get("id").toString(),
                jsonData.get("text").toString().replace("\"", "\\\\\""),
                jsonData.get("author_id").toString()
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = TWEET_AS_ROW_JSON;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private CloseableHttpClient createHttpClient() {
        return HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();
    }

    private HttpGet createHttpGetRequest(String bearerToken) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(twitterToKafkaServiceConfig.getTwitterV2BaseUrl());
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        return httpGet;
    }

    private HttpPost createHttpPostRequest(String bearerToken) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(twitterToKafkaServiceConfig.getTwitterV2BaseUrl());
        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        return httpPost;
    }
}
