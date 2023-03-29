package com.dailycode.crypto.controller;

import com.dailycode.crypto.model.CoinInfo;
import com.dailycode.crypto.model.HistoryData;
import com.dailycode.crypto.service.CoinsDataService;
import com.dailycode.crypto.util.Utility;
import io.github.dengliming.redismodule.redistimeseries.Sample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@CrossOrigin(value = "http://localhost:3000")
@RestController
@RequestMapping("/api/v1/coins")
@RequiredArgsConstructor
@Slf4j
public class CoinsRankingController {

    private final CoinsDataService coinsDataService;

    @GetMapping
    public ResponseEntity<List<CoinInfo>> fetchAllCoins() {
        return ResponseEntity.ok()
                .body(coinsDataService.fetchAllCoinsFromRedisJSON());
    }

    @GetMapping("/{symbol}/{timePeriod}")
    public List<HistoryData> fetchCoinHistoryPerTimePeriod(@PathVariable String symbol, @PathVariable String timePeriod) {

        List<Sample.Value> coinsTSData
                = coinsDataService.fetchCoinHistoryPerTimePeriodFromRedisTS(symbol, timePeriod);

        List<HistoryData> coinHistory = coinsTSData.stream().map(value -> new HistoryData(
                Utility.convertUnixTimeToDate(value.getTimestamp()),
                Utility.round(value.getValue(), 2)
        )).collect(Collectors.toList());

        return coinHistory;
    }
}
