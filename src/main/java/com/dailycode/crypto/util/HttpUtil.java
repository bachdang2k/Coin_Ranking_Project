package com.dailycode.crypto.util;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.Collections;

public class HttpUtil {

    private static String apiHost = "coinranking1.p.rapidapi.com";
    private static String apiKey = "593bd04611mshea48609379ab17cp15942bjsnc20765939753";

    public static HttpEntity<String> getHttpEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.set("X-RapidAPI-Host", apiHost);
        headers.set("X-RapidAPI-Key", apiKey);
        return new HttpEntity<>(null, headers);
    }
}
