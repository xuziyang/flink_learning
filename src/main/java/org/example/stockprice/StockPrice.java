package org.example.stockprice;

import lombok.Data;

@Data
public class StockPrice {
    private String symbol;
    private double price;
    private long ts;
    private int volume;
    
    public static StockPrice of(String symbol, double price, long ts, int volume) {
        StockPrice stockPrice = new StockPrice();
        stockPrice.setSymbol(symbol);
        stockPrice.setPrice(price);
        stockPrice.setTs(ts);
        stockPrice.setVolume(volume);
        return stockPrice;
    }
}
