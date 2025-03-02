package com.flink.tutorials.java.utils.stock;

/**
 * POJO StockPrice
 * symbol      股票代号
 * ts          时间戳
 * price       价格
 * volume      交易量
 * mediaStatus 媒体对该股票的评价状态
 */

public class StockPrice {
    // 文件 "stock/stock-tick-20200108.csv" 一行数据的每个字段
    public static final int IDX_SYMBOL = 0;
    public static final int IDX_YYYYMMDD = 1;
    public static final int IDX_HHMMSS = 2;
    public static final int IDX_PRICE = 3;
    public static final int IDX_VOLUME = 4;

    /**
     * 股票代号
     */
    public String symbol;

    /**
     * 价格
     */
    public double price;

    /**
     * 时间戳
     */
    public long ts;

    /**
     * 交易量
     */
    public int volume;

    /**
     * 媒体对该股票的评价状态
     */
    public String mediaStatus;

    public StockPrice() {
    }

    public StockPrice(String symbol, double price, long ts, int volume, String mediaStatus) {
        this.symbol = symbol;
        this.price = price;
        this.ts = ts;
        this.volume = volume;
        this.mediaStatus = mediaStatus;
    }

    public static StockPrice of(String symbol, double price, long ts, int volume) {
        return new StockPrice(symbol, price, ts, volume, "");
    }

    @Override
    public String toString() {
        return "(" + this.symbol + "," +
                this.price + "," + this.ts +
                "," + this.volume + "," +
                this.mediaStatus + ")";
    }
}
