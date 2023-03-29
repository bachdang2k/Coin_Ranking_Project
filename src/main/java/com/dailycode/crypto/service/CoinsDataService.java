package com.dailycode.crypto.service;

import com.dailycode.crypto.model.*;
import com.dailycode.crypto.util.HttpUtil;
import com.google.gson.Gson;
import io.github.dengliming.redismodule.redisjson.RedisJSON;
import io.github.dengliming.redismodule.redisjson.args.GetArgs;
import io.github.dengliming.redismodule.redisjson.args.SetArgs;
import io.github.dengliming.redismodule.redisjson.utils.GsonUtils;
import io.github.dengliming.redismodule.redistimeseries.DuplicatePolicy;
import io.github.dengliming.redismodule.redistimeseries.RedisTimeSeries;
import io.github.dengliming.redismodule.redistimeseries.Sample;
import io.github.dengliming.redismodule.redistimeseries.TimeSeriesOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class CoinsDataService {
    public final static String GET_COINS_API = "https://coinranking1.p.rapidapi.com/coins?referenceCurrencyUuid=yhjMzLPhuIDl&timePeriod=24h&tiers%5B0%5D=1&orderBy=marketCap&orderDirection=desc&limit=50&offset=0";
    public final static String REDIS_KEY_COINS = "coins";
    public final static String GET_COIN_HISTORY_API = "https://coinranking1.p.rapidapi.com/coin/";
    public final static String COIN_HISTORY_TIME_PERIOD_PARAM = "/history?timePeriods=";
    public final static List<String> timePeriods = List.of("24h", "7d", "30d", "3m", "1y", "3y", "5y");

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private RedisJSON redisJSON;

    @Autowired
    private RedisTimeSeries redisTimeSeries;

    public void fetchCoins() {
        log.info("Inside fetchCoins()");
        ResponseEntity<Coins> coinsResponseEntity =
                restTemplate.exchange(GET_COINS_API, HttpMethod.GET, HttpUtil.getHttpEntity(), Coins.class);

        storeCoinsToRedisJSON(coinsResponseEntity.getBody());
    }

    public void fetchCoinHistory() {
        log.info("Inside fetchCoinHistory()");
        List<CoinInfo> allCoins = getAllCoinsFromRedisJSON();
        allCoins.forEach(coinInfo -> {
            timePeriods.forEach(t -> {
                fetchCoinHistoryForTimePeriod(coinInfo, t);
            });
        });
    }

    private void fetchCoinHistoryForTimePeriod(CoinInfo coinInfo, String timePeriod) {
        log.info("Fetching Coin History of {} for time Period {}", coinInfo.getName(), timePeriod);
        String url = GET_COIN_HISTORY_API + coinInfo.getUuid() + COIN_HISTORY_TIME_PERIOD_PARAM + timePeriod;

        ResponseEntity<CoinPriceHistory> coinPriceHistoryResponseEntity =
                restTemplate.exchange(url,
                HttpMethod.GET,
                HttpUtil.getHttpEntity(),
                CoinPriceHistory.class);

        log.info("Data Fetched From API for Coin History of {} for Time Period {}", coinInfo.getName(), timePeriod);

        storeCoinHistoryToRedisTS(coinPriceHistoryResponseEntity.getBody(),
                coinInfo.getSymbol(),
                timePeriod);
    }

    private void storeCoinHistoryToRedisTS(CoinPriceHistory coinPriceHistory, String symbol, String timePeriod) {
        log.info("Storing Coin History of {} for Time Period {} into Redis TS", symbol, timePeriod);
        List<CoinPriceHistoryExchangeRate> coinExchangeRate =
                coinPriceHistory.getData().getHistory();
        //Symbol: timePeriod
        //BTC:24h, BTC:1y, ETH:3y
        coinExchangeRate.stream()
                .filter(ch -> ch.getPrice() != null && ch.getTimestamp() != null)
                .forEach(ch -> {
                    redisTimeSeries.add(new Sample(symbol + ":" + timePeriod,
                            Sample.Value.of(Long.parseLong(ch.getTimestamp()), Double.parseDouble(ch.getPrice()))),
                            new TimeSeriesOptions()
                                    .unCompressed()
                                    .duplicatePolicy(DuplicatePolicy.LAST)
                            );
                });
    }

    private List<CoinInfo> getAllCoinsFromRedisJSON() {
        CoinData coinData =
                redisJSON.get(REDIS_KEY_COINS,
                CoinData.class,
                new GetArgs().path(".data").indent("\t").newLine("\n").space(" "));
        return coinData.getCoins();
    }

    private void storeCoinsToRedisJSON(Coins coins) {
        redisJSON.set(REDIS_KEY_COINS,
                SetArgs.Builder.create(".", GsonUtils.toJson(coins)));
    }

    public List<CoinInfo> fetchAllCoinsFromRedisJSON() {
        return getAllCoinsFromRedisJSON();
    }

    public List<Sample.Value> fetchCoinHistoryPerTimePeriodFromRedisTS(String symbol, String timePeriod) {
        Map<String, Object> tsInfo = fetchTSInforForSymbol(symbol, timePeriod);
        Long firstTimeStamp = Long.valueOf(tsInfo.get("firstTimestamp").toString());
        Long lastTimeStamp = Long.valueOf(tsInfo.get("lastTimestamp").toString());

        List<Sample.Value> coinsTSData =
                fetchTSDataForCoin(symbol, timePeriod, firstTimeStamp, lastTimeStamp);
        return coinsTSData;
    }

    private List<Sample.Value> fetchTSDataForCoin(String symbol, String timePeriod, Long firstTimeStamp, Long lastTimeStamp) {
        String key = symbol + ":" + timePeriod;
        return redisTimeSeries.range(key, firstTimeStamp, lastTimeStamp);
    }

    private Map<String, Object> fetchTSInforForSymbol(String symbol, String timePeriod) {
        return redisTimeSeries.info(symbol + ":" + timePeriod);
    }
}
