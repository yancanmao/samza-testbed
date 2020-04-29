package samzatask.stock;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.*;

import org.apache.samza.config.Config;

/**
 * This is a simple task that writes each message to a state store and prints them all out on reload.
 *
 * It is useful for command line testing with the kafka console producer and consumer and text messages.
 */
public class StockExchangeTask implements StreamTask, InitableTask, Serializable {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 3;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";
    private Map<String, Map<Float, List<Order>>> pool = new HashMap<>();
    private Map<String, List<Float>> poolPrice = new HashMap<>();

    private final Config config;
    private static final int DefaultDelay = 5;

    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "stock_cj");
    private KeyValueStore<String, String> stockExchangeMapSell;
    private KeyValueStore<String, String> stockExchangeMapBuy;
    private RandomDataGenerator randomGen = new RandomDataGenerator();

    // pool is a architecture used to do stock transaction, we can use collction.sort to sort orders by price.
    // then we need to sort order by timestamp, im not sure how to do this now...
    private Map<String, HashMap<Integer, ArrayList<Order>>> poolS = new HashMap<>();
    private Map<String, HashMap<Integer, ArrayList<Order>>> poolB = new HashMap<>();

    private Map<Integer, ArrayList<Order>> specPoolS = new HashMap<>();
    private Map<Integer, ArrayList<Order>> specPoolB = new HashMap<>();

    private int continuousAuction = 92500;

    public StockExchangeTask(Config config) {
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    public void init(Context context) {
        this.stockExchangeMapSell = (KeyValueStore<String, String>) context.getTaskContext().getStore("stock-exchange-sell");
        this.stockExchangeMapBuy = (KeyValueStore<String, String>) context.getTaskContext().getStore("stock-exchange-buy");
        // load the pool
        loadPool();
        System.out.println("+++++Store loaded successfully!");
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String stockOrder = (String) envelope.getMessage();
        String[] orderArr = stockOrder.split("\\|");
        Map<String, String> matchedResult = new HashMap<>();

//        delay(config.getInt("job.delay.time.ms", DefaultDelay));

        if (stockOrder.equals("CALLAUCTIONEND")) {
            // start to do call auction
            callAuction();
        }

        //filter
        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
            return;
        }

        int curTime = Integer.parseInt(orderArr[Last_Upd_Time].replace(":", ""));

        if (curTime < continuousAuction) {
            // store all orders at maps
            if (orderArr[Tran_Maint_Code].equals("D")) {
                if (orderArr[Trade_Dir].equals("S")) {
                    stockExchangeMapSell.delete(orderArr[Order_No]);
                } else if (orderArr[Trade_Dir].equals("B")) {
                    stockExchangeMapBuy.delete(orderArr[Order_No]);
                }
            } else {

                if (orderArr[Trade_Dir].equals("S")) {
                    stockExchangeMapSell.put(orderArr[Order_No], stockOrder);
                } else if (orderArr[Trade_Dir].equals("B")) {
                    stockExchangeMapBuy.put(orderArr[Order_No], stockOrder);
                } else {
                    System.out.println("++++ error direction");
                }
            }
        } else {
            matchedResult = continuousStockExchange(orderArr, orderArr[Trade_Dir]);
        }

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, orderArr[Sec_Code], stockOrder));
    }

    private Map<String, String> doStockExchange(String[] orderArr, String direction) {
        Map<String, String> matchedResult = new HashMap<>();
        if (direction.equals("")) {
            System.out.println("bad tuple received!");
            return matchedResult;
        }
        if (direction.equals("S")) {
            stockExchangeMapSell.put(orderArr[Sec_Code], String.join("|", orderArr));
            matchedResult = tradeSell(orderArr, stockExchangeMapBuy);
        } else {
            stockExchangeMapBuy.put(orderArr[Sec_Code], String.join("|", orderArr));
            matchedResult = tradeBuy(orderArr, stockExchangeMapSell);
        }
        return matchedResult;
    }

    private Map<String, String> tradeSell(String[] sellerOrder, KeyValueStore<String, String> stockExchangeMap) {
        Map<String, String> matchedBuy = new HashMap<>();
        Map<String, String> matchedSell = new HashMap<>();
        Map<String, String> pendingBuy = new HashMap<>();
        Map<String, String> pendingSell = new HashMap<>();
        KeyValueIterator<String, String> iter = stockExchangeMap.all();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String orderNo = entry.getKey();
            String[] curBuyerOrder = entry.getValue().split("\\|");

            if (curBuyerOrder[Sec_Code].equals(sellerOrder[Sec_Code])) {
                String left = match(curBuyerOrder, sellerOrder);
                if (!left.equals("unmatched")) {
                    if (left.equals("S")) {
                        pendingSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                        matchedBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                    } else if (left.equals("B")) {
                        pendingBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                        matchedSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                    } else {
                        matchedSell.put(sellerOrder[Sec_Code], String.join("\\|", sellerOrder));
                        matchedBuy.put(curBuyerOrder[Sec_Code], String.join("\\|", curBuyerOrder));
                    }
                }
            }
        }
        iter.close();

        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

        return matchedSell;
    }

    private Map<String, String> tradeBuy(String[] buyerOrder, KeyValueStore<String, String> stockExchangeMap) {
        Map<String, String> matchedBuy = new HashMap<>();
        Map<String, String> matchedSell = new HashMap<>();
        Map<String, String> pendingBuy = new HashMap<>();
        Map<String, String> pendingSell = new HashMap<>();
        KeyValueIterator<String, String> iter = stockExchangeMap.all();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            String orderNo = entry.getKey();
            String[] curSellerOrder = entry.getValue().split("\\|");

            if (curSellerOrder[Sec_Code].equals(buyerOrder[Sec_Code])) {
                String left = match(buyerOrder, curSellerOrder);
                if (!left.equals("unmatched")) {
                    if (left.equals("S")) {
                        pendingSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                        matchedBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                    } else if (left.equals("B")) {
                        pendingBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                        matchedSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                    } else {
                        matchedSell.put(curSellerOrder[Sec_Code], String.join("\\|", curSellerOrder));
                        matchedBuy.put(buyerOrder[Sec_Code], String.join("\\|", buyerOrder));
                    }
                }
            }
        }
        iter.close();

        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);

        return matchedBuy;
    }

    private String match(String[] buyerOrder, String[] sellerOrder) {
        float buyPrice = Float.valueOf(buyerOrder[Order_Price]);
        float sellPrice = Float.valueOf(sellerOrder[Order_Price]);
        if (buyPrice < sellPrice) {
            return "unmatched";
        }
        float buyVol = Float.valueOf(buyerOrder[Order_Vol]);
        float sellVol = Float.valueOf(sellerOrder[Order_Vol]);
        if (buyVol > sellVol) {
            buyerOrder[Order_Vol] = String.valueOf(buyVol - sellVol);
            return "B";
        } else if (buyVol < sellVol) {
            sellerOrder[Order_Vol] = String.valueOf(sellVol - buyVol);
            return "S";
        } else {
            return "";
        }
    }

    private void updateStore(
                             Map<String, String> pendingBuy,
                             Map<String, String> pendingSell,
                             Map<String, String> matchedBuy,
                             Map<String, String> matchedSell) {
        for (Map.Entry<String, String> order : pendingBuy.entrySet()) {
            stockExchangeMapBuy.put(order.getKey(), order.getValue());
        }
        for (Map.Entry<String, String> order : pendingSell.entrySet()) {
            stockExchangeMapSell.put(order.getKey(), order.getValue());
        }
        for (Map.Entry<String, String> order : matchedBuy.entrySet()) {
            stockExchangeMapBuy.delete(order.getKey());
        }
        for (Map.Entry<String, String> order : matchedSell.entrySet()) {
            stockExchangeMapSell.delete(order.getKey());
        }
    }

    private void delay(int interval) {
        Double ranN = randomGen.nextGaussian(interval, 1);
        ranN = ranN*1000000;
        long delay = ranN.intValue();
        if (delay < 0) delay = 6000000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {}
    }

    public void callAuction() {
        // do call auction
        // 1. sort buy order and sell order by price and timestamp
        System.out.println("Start call auction");

        loadPool();

        // 2. do stock exchange on every stock id
        for (Map.Entry poolBentry : poolB.entrySet()) {
            String curStockId = (String) poolBentry.getKey();
            // filter for debug
            if (!curStockId.equals("600882")) return;

            HashMap<Integer, ArrayList<Order>> curBuyPool = (HashMap<Integer, ArrayList<Order>>) poolBentry.getValue();
            HashMap<Integer, ArrayList<Order>> sortedCurBuyPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyDesc(curBuyPool);
            // for sorted prices, do stock exchange
            for (Map.Entry curBuyOrdersEntry : sortedCurBuyPool.entrySet()) {
                int curBuyPrice = (int) curBuyOrdersEntry.getKey();
                ArrayList<Order> curBuyOrders = (ArrayList<Order>) curBuyOrdersEntry.getValue();

                // get the sell orders from sell pool
                HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(curStockId, new HashMap<>());
                HashMap<Integer, ArrayList<Order>> sortedCurSellPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyDesc(curSellPool);

                // match orders
                for (Map.Entry curSellOrdersEntry : sortedCurSellPool.entrySet()) {
                    int curSellPrice = (int) curSellOrdersEntry.getKey();
                    // when matched, do transaction
                    if (curBuyPrice >= curSellPrice) {
                        ArrayList<Order> curSellOrders = (ArrayList<Order>) curSellOrdersEntry.getValue();
                        stockExchange(curBuyOrders, curSellOrders);
                    }
                }
            }
        }

        poolS.clear();
        poolB.clear();
    }

    public Map<String, String> continuousStockExchange(String[] orderArr, String direction) {
        long start = System.currentTimeMillis();

//        System.out.println("stockExchangeMapSell: " + stockExchangeMapSell.size() + " stockExchangeMapBuy: " + stockExchangeMapBuy.size()
//                + " total: " + (stockExchangeMapSell.size() + stockExchangeMapBuy.size()));

        Map<String, String> matchedResult = new HashMap<>();

        // delete stock order
        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1)) {
            if (direction.equals("")) {
                System.out.println("no order to delete!");
            }
            if (direction.equals("S")) {
                stockExchangeMapSell.delete(orderArr[Order_No]);
            }
            if (direction.equals("B")) {
                stockExchangeMapBuy.delete(orderArr[Order_No]);
            }

            return matchedResult;
        }

        Order curOrder = new Order(orderArr);
        loadPool(curOrder.getSecCode(), direction);

        System.out.println("loading time: " + (System.currentTimeMillis() - start));

        if (direction.equals("")) {
            System.out.println("bad tuple received!");
            return matchedResult;
        }
        if (direction.equals("B")) {
            stockExchangeMapBuy.put(curOrder.getOrderNo(), curOrder.toString());
            int curBuyPrice = curOrder.getOrderPrice();
            ArrayList<Order> curBuyOrders = new ArrayList<>();
            curBuyOrders.add(curOrder);

            // do partial transaction
            HashMap<Integer, ArrayList<Order>> sortedCurSellPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyDesc(specPoolB);
            // match orders
            for (Map.Entry curSellOrdersEntry : sortedCurSellPool.entrySet()) {
                int curSellPrice = (int) curSellOrdersEntry.getKey();
                // when matched, do transaction
                if (curBuyPrice >= curSellPrice) {
                    ArrayList<Order> curSellOrders = (ArrayList<Order>) curSellOrdersEntry.getValue();
                    stockExchange(curBuyOrders, curSellOrders);
                }
            }
            // can check whether next time it still useful
            specPoolB.clear();
        } else {
            stockExchangeMapSell.put(curOrder.getOrderNo(), curOrder.toString());
            int curSellPrice = curOrder.getOrderPrice();
            ArrayList<Order> curSellOrders = new ArrayList<>();
            curSellOrders.add(curOrder);

            // do partial transaction
            HashMap<Integer, ArrayList<Order>> sortedCurBuyPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyDesc(specPoolS);
            // match orders
            for (Map.Entry curBuyOrdersEntry : sortedCurBuyPool.entrySet()) {
                int curBuyPrice = (int) curBuyOrdersEntry.getKey();
                // when matched, do transaction
                if (curBuyPrice >= curSellPrice) {
                    ArrayList<Order> curBuyOrders = (ArrayList<Order>) curBuyOrdersEntry.getValue();
                    stockExchange(curBuyOrders, curSellOrders);
                }
            }
            specPoolS.clear();
        }

        System.out.println("processing time: " + (System.currentTimeMillis() - start));
        return matchedResult;
    }

    public void stockExchange(ArrayList<Order> curBuyOrders, ArrayList<Order> curSellOrders) {
        ArrayList<Order> tradedBuyOrders = new ArrayList<>();
        ArrayList<Order> tradedSellOrders = new ArrayList<>();

        // match orders one by one, until all orders are matched
        for (Order curBuyOrder : curBuyOrders) {
            for (Order curSellOrder : curSellOrders) {
                int buyVol = curBuyOrder.getOrderVol();
                int sellVol = curSellOrder.getOrderVol();
                if (buyVol == 0 || sellVol == 0) continue;
                if (buyVol > sellVol) {
                    curBuyOrder.updateOrder(sellVol);
                    curSellOrder.updateOrder(sellVol);
                    tradedSellOrders.add(curSellOrder);
                    System.out.println("Traded Sell: " + sellVol +  " - " + curSellOrder.toString());
                    System.out.println("Half-Traded Buy: " + sellVol +  " - " + curBuyOrder.toString());
                } else {
                    curBuyOrder.updateOrder(buyVol);
                    curSellOrder.updateOrder(buyVol);
                    tradedBuyOrders.add(curBuyOrder);
                    System.out.println("Traded Buy: " + buyVol + " - " + curBuyOrder.toString());
                    System.out.println("Half-Traded Sell: " + buyVol +  " - " + curSellOrder.toString());
                }
            }
        }
        // remove traded orders, and update half-traded orders
        for (Order tradedSellOrder : tradedSellOrders) {
//            System.out.println(stockExchangeMapSell.containsKey(tradedSellOrder.getOrderNo()) + " "
//                    + tradedSellOrder.toString());
            curSellOrders.remove(tradedSellOrder);
            stockExchangeMapSell.delete(tradedSellOrder.getOrderNo());
        }

        for (Order tradedBuyOrder : tradedBuyOrders) {
//            System.out.println(stockExchangeMapBuy.containsKey(tradedBuyOrder.getOrderNo()) + " "
//                    + tradedBuyOrder.toString());
            curBuyOrders.remove(tradedBuyOrder);
            stockExchangeMapBuy.delete(tradedBuyOrder.getOrderNo());
        }

        // update orders half traded.
        for (Order halfTradedSellOrder : curSellOrders) {
            stockExchangeMapSell.put(halfTradedSellOrder.getOrderNo(), halfTradedSellOrder.toString());
        }

        for (Order halfTradedBuyOrder : curBuyOrders) {
            stockExchangeMapBuy.put(halfTradedBuyOrder.getOrderNo(), halfTradedBuyOrder.toString());
        }
    }

    public void loadPool() {
        // load pool from state backend, then do matchmaking by use old logic
        KeyValueIterator<String, String> buyIter = stockExchangeMapBuy.all();
        KeyValueIterator<String, String> sellIter = stockExchangeMapSell.all();

        while (buyIter.hasNext()) {
            Entry<String, String> entry = buyIter.next();
            String orderNo = entry.getKey();
            String[] curBuyerOrder = entry.getValue().split("\\|");
            Order curOrder = new Order(curBuyerOrder);
            String curSecCode = curOrder.getSecCode();
            int curOrderPrice = curOrder.getOrderPrice();

            HashMap<Integer, ArrayList<Order>> curPool = poolB.getOrDefault(curSecCode, new HashMap<>());
            ArrayList<Order> curOrderList = curPool.getOrDefault(curOrderPrice, new ArrayList<>());
            // need to keep pool price be sorted, so insert it into pool price
            curOrderList.add(curOrder);
            curPool.put(curOrderPrice, curOrderList);
            poolB.put(curOrder.getSecCode(), curPool);
        }

        while (sellIter.hasNext()) {
            Entry<String, String> entry = sellIter.next();
            String orderNo = entry.getKey();
            String[] curBuyerOrder = entry.getValue().split("\\|");
            Order curOrder = new Order(curBuyerOrder);
            String curSecCode = curOrder.getSecCode();
            int curOrderPrice = curOrder.getOrderPrice();

            HashMap<Integer, ArrayList<Order>> curPool = poolS.getOrDefault(curSecCode, new HashMap<>());
            ArrayList<Order> curOrderList = curPool.getOrDefault(curOrderPrice, new ArrayList<>());
            // need to keep pool price be sorted, so insert it into pool price
            curOrderList.add(curOrder);
            curPool.put(curOrderPrice, curOrderList);
            poolS.put(curOrder.getSecCode(), curPool);
        }
    }

    public void loadPool(String stockId, String direction) {
        // load pool from state backend, then do matchmaking by use old logic
        if (direction.equals("S")) {
            KeyValueIterator<String, String> buyIter = stockExchangeMapBuy.all();
            while (buyIter.hasNext()) {
                Entry<String, String> entry = buyIter.next();
                String orderNo = entry.getKey();
                String[] curBuyerOrder = entry.getValue().split("\\|");
                Order curOrder = new Order(curBuyerOrder);
                String curSecCode = curOrder.getSecCode();
                int curOrderPrice=0;
                try {
                    curOrderPrice = curOrder.getOrderPrice();
                } catch (Exception e) {
                    System.out.println(entry.getValue());
                }

                ArrayList<Order> curOrderList = specPoolS.getOrDefault(curOrderPrice, new ArrayList<>());
                // need to keep pool price be sorted, so insert it into pool price
                curOrderList.add(curOrder);
                specPoolS.put(curOrderPrice, curOrderList);
            }
        } else {
            KeyValueIterator<String, String> sellIter = stockExchangeMapSell.all();
            while (sellIter.hasNext()) {
                Entry<String, String> entry = sellIter.next();
                String orderNo = entry.getKey();
                String[] curBuyerOrder = entry.getValue().split("\\|");
                Order curOrder = new Order(curBuyerOrder);
                String curSecCode = curOrder.getSecCode();
                if (curSecCode.equals(stockId)) {
                    int curOrderPrice = curOrder.getOrderPrice();

                    ArrayList<Order> curOrderList = specPoolB.getOrDefault(curOrderPrice, new ArrayList<>());
                    // need to keep pool price be sorted, so insert it into pool price
                    curOrderList.add(curOrder);
                    specPoolB.put(curOrderPrice, curOrderList);
                }
            }
        }
    }


    public static Map<Integer, ArrayList<Order>> sortMapBykeyDesc(Map<Integer, ArrayList<Order>> oriMap) {
        Map<Integer, ArrayList<Order>> sortedMap = new LinkedHashMap<>();
        try {
            if (oriMap != null && !oriMap.isEmpty()) {
                List<Map.Entry<Integer, ArrayList<Order>>> entryList = new ArrayList<>(oriMap.entrySet());
                Collections.sort(entryList,
                        (o1, o2) -> {
                            int value1 = 0, value2 = 0;
                            try {
                                value1 = o1.getKey();
                                value2 = o2.getKey();
                            } catch (NumberFormatException e) {
                                value1 = 0;
                                value2 = 0;
                            }
                            return value2 - value1;
                        });
                Iterator<Map.Entry<Integer, ArrayList<Order>>> iter = entryList.iterator();
                Map.Entry<Integer, ArrayList<Order>> tmpEntry;
                while (iter.hasNext()) {
                    tmpEntry = iter.next();
                    sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
                }
            }
        } catch (Exception e) {
        }
        return sortedMap;
    }
}