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

import static samzatask.stock.utils.sortMapBykeyAsc;
import static samzatask.stock.utils.sortMapBykeyDesc;

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

    private int continuousAuction = 92500;
    private boolean callAuctionAllowed = true;

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

        if (stockOrder.equals("CALLAUCTIONEND") && callAuctionAllowed) {
            // start to do call auction
            callAuction();
            callAuctionAllowed = false;
            return;
        }

        //filter
        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
            return;
        }

        int curTime = Integer.parseInt(orderArr[Last_Upd_Time].replace(":", ""));

        Order curOrder = new Order(orderArr);

        if (curTime < continuousAuction) {
            // store all orders at maps
            if (orderArr[Tran_Maint_Code].equals("D")) {
//                if (orderArr[Trade_Dir].equals("S")) {
//                    stockExchangeMapSell.delete(orderArr[Order_No]);
//                } else if (orderArr[Trade_Dir].equals("B")) {
//                    stockExchangeMapBuy.delete(orderArr[Order_No]);
//                }
                deleteOrder(curOrder, orderArr[Trade_Dir]);
            } else {
//                if (orderArr[Trade_Dir].equals("S")) {
//                    stockExchangeMapSell.put(orderArr[Order_No], stockOrder);
//                } else if (orderArr[Trade_Dir].equals("B")) {
//                    stockExchangeMapBuy.put(orderArr[Order_No], stockOrder);
//                } else {
//                    System.out.println("++++ error direction");
//                }
                insertPool(curOrder);
            }
        } else {
            delay(config.getInt("job.delay.time.ms", DefaultDelay));
            matchedResult = continuousStockExchange(orderArr, orderArr[Trade_Dir]);
        }

        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, orderArr[Sec_Code], stockOrder));
    }

    public void loadPool() {
        // load pool from state backend, then do matchmaking by use old logic
        KeyValueIterator<String, String> buyIter = stockExchangeMapBuy.all();
        KeyValueIterator<String, String> sellIter = stockExchangeMapSell.all();

        while (buyIter.hasNext()) {
            Entry<String, String> entry = buyIter.next();
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
            String[] curSellerOrder = entry.getValue().split("\\|");
            Order curOrder = new Order(curSellerOrder);
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


    public void callAuction() {
        // do call auction
        // 1. sort buy order and sell order by price and timestamp
        System.out.println("Start call auction");
//        loadPool();

        // 2. do stock exchange on every stock id
        for (Map.Entry poolBentry : poolB.entrySet()) {
            String curStockId = (String) poolBentry.getKey();
            // filter for debug
            HashMap<Integer, ArrayList<Order>> curBuyPool = (HashMap<Integer, ArrayList<Order>>) poolBentry.getValue();
            HashMap<Integer, ArrayList<Order>> sortedCurBuyPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyDesc(curBuyPool);
            // for sorted prices, do stock exchange
            for (Map.Entry curBuyOrdersEntry : sortedCurBuyPool.entrySet()) {
                int curBuyPrice = (int) curBuyOrdersEntry.getKey();
                ArrayList<Order> curBuyOrders = (ArrayList<Order>) curBuyOrdersEntry.getValue();

                // get the sell orders from sell pool
                HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(curStockId, new HashMap<>());
                // buyer list should descending, seller should be ascending
                HashMap<Integer, ArrayList<Order>> sortedCurSellPool = (HashMap<Integer, ArrayList<Order>>) sortMapBykeyAsc(curSellPool);

                // match orders
                for (Map.Entry curSellOrdersEntry : sortedCurSellPool.entrySet()) {
                    int curSellPrice = (int) curSellOrdersEntry.getKey();
                    // when matched, do transaction
                    if (curBuyPrice >= curSellPrice) {
                        ArrayList<Order> curSellOrders = (ArrayList<Order>) curSellOrdersEntry.getValue();
                        stockExchange(curBuyOrders, curSellOrders);

                        // add pending orders into pool again for future usage
                        // TODO: either use sorted or unsorted, I think latter is better for isolation
                        updatePool(curSellPool, curSellOrders, curSellPrice);
                    }
                }
                // put updated sell pool into original pool
                poolS.replace(curStockId, curSellPool);

                updatePool(curBuyPool, curBuyOrders, curBuyPrice);
            }
            // TODO: sanity check, whether this is equal to map size
            // put updated buy pool into original pool
            poolB.replace(curStockId, curBuyPool);

            metricsDump();
        }

        auctionFlush();
    }

    public Map<String, String> continuousStockExchange(String[] orderArr, String direction) {
        long start = System.currentTimeMillis();
        Map<String, String> matchedResult = new HashMap<>();

//        metricsDump();

        Order curOrder = new Order(orderArr);
        // delete stock orderm, index still needs to be deleted

        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1)) {
            deleteOrder(curOrder, direction);
            return matchedResult;
        }

        if (direction.equals("")) {
            System.out.println("bad tuple received!");
            return matchedResult;
        }
        if (direction.equals("B")) {
            // put into state and index
            stockExchangeMapBuy.put(curOrder.getOrderNo(), curOrder.toString());
            HashMap<Integer, ArrayList<Order>> curBuyPool = poolB.getOrDefault(curOrder.getSecCode(), new HashMap<>());
            ArrayList<Order> curBuyOrders = curBuyPool.getOrDefault(curOrder.getOrderPrice(), new ArrayList<>());
            curBuyOrders.add(curOrder);
            curBuyPool.put(curOrder.getOrderPrice(), curBuyOrders);
            poolB.put(curOrder.getSecCode(), curBuyPool);

            int curBuyPrice = curOrder.getOrderPrice();

            // do partial transaction
            HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(curOrder.getSecCode(), new HashMap<>());
            HashMap<Integer, ArrayList<Order>> sortedCurSellPool = (HashMap<Integer, ArrayList<Order>>)
                    sortMapBykeyAsc(curSellPool);

            // match orders
            for (Map.Entry curSellOrdersEntry : sortedCurSellPool.entrySet()) {
                int curSellPrice = (int) curSellOrdersEntry.getKey();
                // when matched, do transaction
                if (curBuyPrice >= curSellPrice) {
                    ArrayList<Order> curSellOrders = (ArrayList<Order>) curSellOrdersEntry.getValue();
                    stockExchange(curBuyOrders, curSellOrders);
                    // add pending orders into pool again for future usage
                    // TODO: either use sorted or unsorted, I think latter is better for isolation
                    updatePool(curSellPool, curSellOrders, curSellPrice);
                }
            }
            poolS.replace(curOrder.getSecCode(), curSellPool);
            updatePool(curBuyPool, curBuyOrders, curBuyPrice);
            poolB.replace(curOrder.getSecCode(), curBuyPool);
        } else {
            stockExchangeMapSell.put(curOrder.getOrderNo(), curOrder.toString());
            HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(curOrder.getSecCode(), new HashMap<>());
            ArrayList<Order> curSellOrders = curSellPool.getOrDefault(curOrder.getOrderPrice(), new ArrayList<>());
            curSellOrders.add(curOrder);
            curSellPool.put(curOrder.getOrderPrice(), curSellOrders);
            poolS.put(curOrder.getSecCode(), curSellPool);

            int curSellPrice = curOrder.getOrderPrice();

            // do partial transaction
            HashMap<Integer, ArrayList<Order>> curBuyPool = poolB.getOrDefault(curOrder.getSecCode(), new HashMap<>());
            HashMap<Integer, ArrayList<Order>> sortedCurBuyPool = (HashMap<Integer, ArrayList<Order>>)
                    sortMapBykeyDesc(curBuyPool);
            // match orders
            for (Map.Entry curBuyOrdersEntry : sortedCurBuyPool.entrySet()) {
                int curBuyPrice = (int) curBuyOrdersEntry.getKey();
                // when matched, do transaction
                if (curBuyPrice >= curSellPrice) {
                    ArrayList<Order> curBuyOrders = (ArrayList<Order>) curBuyOrdersEntry.getValue();
                    stockExchange(curBuyOrders, curSellOrders);
                    // add pending orders into pool again for future usage
                    // TODO: either use sorted or unsorted, I think latter is better for isolation
                    updatePool(curBuyPool, curBuyOrders, curBuyPrice);
                }
            }

            poolB.replace(curOrder.getSecCode(), curBuyPool);

            updatePool(curSellPool, curSellOrders, curSellPrice);
            poolS.replace(curOrder.getSecCode(), curSellPool);
        }
        System.out.println("processing time: " + (System.currentTimeMillis() - start));
        return matchedResult;
    }

    public void deleteOrder(Order curOrder, String direction) {
        if (direction.equals("")) {
            System.out.println("no order to delete!");
        }

        String orderNo = curOrder.getOrderNo();
        String stockId = curOrder.getSecCode();
        int orderPrice = curOrder.getOrderPrice();

        Order targetOrder = null;

        if (direction.equals("S")) {
            stockExchangeMapSell.delete(orderNo);
            HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(stockId, new HashMap<>());
            ArrayList<Order> curSellOrders = curSellPool.getOrDefault(orderPrice, new ArrayList<>());

            for (Order order : curSellOrders) {
                if (order.getOrderNo().equals(orderNo)) {
                    targetOrder = order;
                    break;
                }
            }
            curSellOrders.remove(targetOrder);
            updatePool(curSellPool, curSellOrders, curOrder.getOrderPrice());
            poolS.replace(curOrder.getSecCode(),curSellPool);
        }
        if (direction.equals("B")) {
            stockExchangeMapBuy.delete(orderNo);
            HashMap<Integer, ArrayList<Order>> curBuyPool = poolB.getOrDefault(stockId, new HashMap<>());
            ArrayList<Order> curBuyOrders = curBuyPool.getOrDefault(orderPrice, new ArrayList<>());
            for (Order order : curBuyOrders) {
                if (order.getOrderNo().equals(curOrder.getOrderNo())) {
                    targetOrder = order;
                    break;
                }
            }
            curBuyOrders.remove(targetOrder);
            updatePool(curBuyPool, curBuyOrders, curOrder.getOrderPrice());
            poolB.replace(curOrder.getSecCode(),curBuyPool);
        }
    }

    public void updatePool(HashMap<Integer, ArrayList<Order>> curPool, ArrayList<Order> orderList, int key) {
        if (orderList.isEmpty()) {
            curPool.remove(key);
        } else {
            curPool.replace(key, orderList);
        }
    }

    public void insertPool(Order curOrder) {
        String curSecCode = curOrder.getSecCode();
        int curOrderPrice = curOrder.getOrderPrice();
        String direction = curOrder.getTradeDir();

        if (direction.equals("B")) {
            HashMap<Integer, ArrayList<Order>> curPool = poolB.getOrDefault(curSecCode, new HashMap<>());
            ArrayList<Order> curOrderList = curPool.getOrDefault(curOrderPrice, new ArrayList<>());
            // need to keep pool price be sorted, so insert it into pool price
            curOrderList.add(curOrder);
            curPool.put(curOrderPrice, curOrderList);
            poolB.put(curOrder.getSecCode(), curPool);
        } else {
            HashMap<Integer, ArrayList<Order>> curPool = poolS.getOrDefault(curSecCode, new HashMap<>());
            ArrayList<Order> curOrderList = curPool.getOrDefault(curOrderPrice, new ArrayList<>());
            // need to keep pool price be sorted, so insert it into pool price
            curOrderList.add(curOrder);
            curPool.put(curOrderPrice, curOrderList);
            poolS.put(curOrder.getSecCode(), curPool);
        }
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
//                    System.out.println("Traded Sell: " + sellVol +  " - " + curSellOrder.toString());
//                    System.out.println("Half-Traded Buy: " + sellVol +  " - " + curBuyOrder.toString());
                } else {
                    curBuyOrder.updateOrder(buyVol);
                    curSellOrder.updateOrder(buyVol);
                    tradedBuyOrders.add(curBuyOrder);
//                    System.out.println("Traded Buy: " + buyVol + " - " + curBuyOrder.toString());
//                    System.out.println("Half-Traded Sell: " + buyVol +  " - " + curSellOrder.toString());
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

    public void auctionFlush() {
        for (Map.Entry entry : poolS.entrySet()) {
            HashMap<Integer, ArrayList<Order>> curPool = (HashMap<Integer, ArrayList<Order>>) entry.getValue();
            for (Map.Entry entry1 : curPool.entrySet()) {
                ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
                for (Order order : orderList) {
                    stockExchangeMapSell.put(order.getOrderNo(), order.toString());
                }
            }
        }

        for (Map.Entry entry : poolB.entrySet()) {
            HashMap<Integer, ArrayList<Order>> curPool = (HashMap<Integer, ArrayList<Order>>) entry.getValue();
            for (Map.Entry entry1 : curPool.entrySet()) {
                ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
                for (Order order : orderList) {
                    stockExchangeMapBuy.put(order.getOrderNo(), order.toString());
                }
            }
        }
    }

    public void metricsDump() {
        int totalSellIndex = 0;
        for (Map.Entry entry : poolS.entrySet()) {
            HashMap<Integer, ArrayList<Order>> curPool = (HashMap<Integer, ArrayList<Order>>) entry.getValue();
            for (Map.Entry entry1 : curPool.entrySet()) {
                ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
                totalSellIndex += orderList.size();
            }
        }

        int totalBuyIndex = 0;
        for (Map.Entry entry : poolB.entrySet()) {
            HashMap<Integer, ArrayList<Order>> curPool = (HashMap<Integer, ArrayList<Order>>) entry.getValue();
            for (Map.Entry entry1 : curPool.entrySet()) {
                ArrayList<Order> orderList = (ArrayList<Order>) entry1.getValue();
                totalBuyIndex += orderList.size();
            }
        }

        System.out.println("sell size: " + totalSellIndex + " buy size: "
                + totalBuyIndex + " total size: " + (totalBuyIndex+totalSellIndex));
    }

    private void delay(int interval) {
        Double ranN = randomGen.nextGaussian(interval, 1);
        ranN = ranN*1000000;
        long delay = ranN.intValue();
        if (delay < 0) delay = 6000000;
        Long start = System.nanoTime();
        while (System.nanoTime() - start < delay) {}
    }
}