package samzatask.stock;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.io.*;
import java.net.URI;
import java.util.*;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest.TOPIC;
import static samzatask.stock.utils.sortMapBykeyAsc;
import static samzatask.stock.utils.sortMapBykeyDesc;

public class StockExchangeSimu {
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

    public Map<String, String> stockExchangeMapSell;
    public Map<String, String> stockExchangeMapBuy;

    // pool is a architecture used to do stock transaction, we can use collction.sort to sort orders by price.
    // then we need to sort order by timestamp, im not sure how to do this now...
    private Map<String, HashMap<Integer, ArrayList<Order>>> poolS = new HashMap<>();
    private Map<String, HashMap<Integer, ArrayList<Order>>> poolB = new HashMap<>();

//    private Map<Integer, ArrayList<Order>> specPoolS = new HashMap<>();
//    private Map<Integer, ArrayList<Order>> specPoolB = new HashMap<>();
//    private Map<String, List<Float>> poolPrice = new HashMap<>();

    public StockExchangeSimu() {
        this.stockExchangeMapSell = new HashMap<>();
        this.stockExchangeMapBuy = new HashMap<>();

//        File dirFile = new File("/home/myc/Dropbox/SSE/opening");
//        String[] fileList = dirFile.list();
//        for (int i = 0; i < fileList.length; i++) {
//            String fileName = fileList[i];
//            File file = new File(dirFile.getPath(),fileName);
//            Pool poolS = this.loadPool(file.getPath() +"/S.txt");
//            Pool poolB = this.loadPool(file.getPath() +"/B.txt");
//            pool.put(file.getName()+"S", poolS.getPool());
//            pool.put(file.getName()+"B", poolB.getPool());
//            poolPrice.put(file.getName()+"S", poolS.getPricePool());
//            poolPrice.put(file.getName()+"B", poolB.getPricePool());
//        }
    }

//    /**
//     * load file into buffer
//     * @param
//     * @return List<Order>
//     */
//    public void loadPool() {
//
//        System.out.println("loading the pool...");
//
//        Configuration conf = new Configuration();
//        String hdfsuri = "hdfs://camel:9000/";
//        conf.set("fs.defaultFS", hdfsuri);
//
//        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
//        System.setProperty("hadoop.home.dir", "/");
//
//        // pool initialization
//        BufferedReader br = null;
//        Map<Float, List<Order>> poolI = new HashMap<Float, List<Order>>();
//        List<Order> orderList;
//        List<Float> pricePoolI = new ArrayList<>();
//        String sCurrentLine;
//
//        // load all opening price from HDFS
//        try {
//            FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
//            FileStatus[] stockRoot = fs.listStatus(new Path(hdfsuri+"opening"));
//            for(FileStatus status : stockRoot){
//                FileStatus[] stockDir = fs.listStatus(status.getPath());
//                for (FileStatus realFile : stockDir) {
//                    FSDataInputStream inputStream = fs.open(realFile.getPath());
//                    br = new BufferedReader(new InputStreamReader(inputStream));
//                    while ((sCurrentLine = br.readLine()) != null) {
//                        Order order = new Order(sCurrentLine.split("\\|"));
//                        orderList = poolI.get(order.getOrderPrice());
//                        if (orderList == null) {
//                            pricePoolI.add(order.getOrderPrice());
//                            orderList = new ArrayList<>();
//                        }
//                        orderList.add(order);
//                        poolI.put(order.getOrderPrice(), orderList);
//                    }
//                    String key = status.getPath().getName() + realFile.getPath().getName().charAt(0);
//                    pool.put(key, poolI);
//                    poolPrice.put(key, pricePoolI);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("loading complete");
//    }

//    /**
//     * load file into buffer
//     * @param file
//     * @return List<Order>
//     */
//    public Pool loadPool(String file) {
//        FileReader stream = null;
//        BufferedReader br = null;
//        String sCurrentLine;
//        Map<Float, List<Order>> poolI = new HashMap<Float, List<Order>>();
//        List<Order> orderList = new ArrayList<>();
//        List<Float> pricePoolI = new ArrayList<>();
//        File textFile = new File(file);
//        // if file exists
//        if (!textFile.exists()) {
//            return new Pool(poolI, pricePoolI);
//        }
//
//        try{
//            stream = new FileReader(file);
//
//            br = new BufferedReader(stream);
//            while ((sCurrentLine = br.readLine()) != null) {
//                Order order = new Order(sCurrentLine.split("\\|"));
//                orderList = poolI.get(order.getOrderPrice());
//                if (orderList == null) {
//                    pricePoolI.add(order.getOrderPrice());
//                    orderList = new ArrayList<>();
//                }
//                orderList.add(order);
//                poolI.put(order.getOrderPrice(), orderList);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (stream != null) stream.close();
//                if (br != null) br.close();
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
//        Pool poolThis = new Pool(poolI, pricePoolI);
//        return poolThis;
//    }
//
//    /**
//     * deal continous transaction
//     * @param poolB,poolS,pool,order
//     * @return output string
//     */
//    private List<String> transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
//                                     List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
//                                     Map<String, Map<Float, List<Order>>> pool, Order order) {
//        // hava a transaction
//        int top = 0;
//        int i = 0;
//        int j = 0;
//        int otherOrderVol;
//        int totalVol = 0;
//        float tradePrice = 0;
//        List<String> tradeResult = new ArrayList<>();
//
//        // compute total state
//        int stateSize = 0;
//        for (Map.Entry orderList : poolB.entrySet()) {
//            stateSize += ((List<Order>) orderList.getValue()).size();
//        }
//
//        for (Map.Entry orderList : poolS.entrySet()) {
//            stateSize += ((List<Order>) orderList.getValue()).size();
//        }
//
////        System.out.println("before match: " + stateSize);
//
//        int count = 0;
//        while (poolPriceS.get(top) <= poolPriceB.get(top)) {
//            tradePrice = poolPriceS.get(top);
//            if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
//                // B remains B_top-S_top
//                otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
//                // totalVol sum
//                totalVol += otherOrderVol;
//                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
//                // S complete
//                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
//                // remove top of poolS
//                poolS.get(poolPriceS.get(top)).remove(top);
//                // no order in poolS, transaction over
//                if (poolS.get(poolPriceS.get(top)).isEmpty()) {
//                    // find next price
//                    poolS.remove(poolPriceS.get(top));
//                    poolPriceS.remove(top);
//                    if (poolPriceS.isEmpty()) {
//                        break;
//                    }
//                }
//                // TODO: output poolB poolS price etc
//            } else {
//                otherOrderVol = poolB.get(poolPriceB.get(top)).get(top).getOrderVol();
//                // totalVol sum
//                totalVol += otherOrderVol;
//                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
//                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
//                poolB.get(poolPriceB.get(top)).remove(top);
//                // no order in poolB, transaction over
//                if (poolB.get(poolPriceB.get(top)).isEmpty()) {
//                    poolB.remove(poolPriceB.get(top));
//                    poolPriceB.remove(top);
//                    if (poolPriceB.isEmpty()) {
//                        break;
//                    }
//                }
//                // TODO: output poolB poolS price etc
//            }
//            count ++;
//        }
//
//        System.out.println("iteration: " + count);
//
//        // compute total state
//        stateSize = 0;
//        for (Map.Entry orderList : poolB.entrySet()) {
//            stateSize += ((List<Order>) orderList.getValue()).size();
//        }
//
//        for (Map.Entry orderList : poolS.entrySet()) {
//            stateSize += ((List<Order>) orderList.getValue()).size();
//        }
//
////        System.out.println("after match: " + stateSize);
//
//        pool.put(order.getSecCode()+"S", poolS);
//        pool.put(order.getSecCode()+"B", poolB);
//        poolPrice.put(order.getSecCode()+"B", poolPriceB);
//        poolPrice.put(order.getSecCode()+"S", poolPriceS);
//        tradeResult.add(order.getOrderNo());
//        tradeResult.add(order.getSecCode());
//        tradeResult.add(String.valueOf(totalVol));
//        tradeResult.add(String.valueOf(tradePrice));
//        // return tradeResult;
//        return tradeResult;
//    }
//
//    /**
//     * mapFunction
////     * @param pool, order
//     * @return String
//     */
////    private List<String> stockExchange(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
//    private List<String> stockExchange(Order order) {
//        // String complete = new String();
//        List<String> tradeResult = new ArrayList<>();
//        // load poolS poolB
//        Map<Float, List<Order>> poolS = pool.get(order.getSecCode()+"S");
//        Map<Float, List<Order>> poolB = pool.get(order.getSecCode()+"B");
//        List<Float> poolPriceB = poolPrice.get(order.getSecCode()+"B");
//        List<Float> poolPriceS = poolPrice.get(order.getSecCode()+"S");
//
//        if (poolB == null) {
//            poolB = new HashMap<>();
//        }
//        if (poolS == null) {
//            poolS = new HashMap<>();
//        }
//        if (poolPriceB == null) {
//            poolPriceB = new ArrayList<>();
//        }
//        if (poolPriceS == null) {
//            poolPriceS = new ArrayList<>();
//        }
//
//        // compute total state
//        int stateSize = 0;
//        for (Map.Entry secPool: pool.entrySet()){
//            for (Map.Entry orderList : ((Map<Float, List<Order>>) secPool.getValue()).entrySet()) {
//                stateSize += ((List<Order>) orderList.getValue()).size();
//            }
//        }
//
////        System.out.println("state size is: " + stateSize);
//
//        if (order.getTradeDir().equals("B")) {
//            float orderPrice = order.getOrderPrice();
//            List<Order> BorderList = poolB.get(orderPrice);
//            // if order tran_maint_code is "D", delete from pool
//            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
//                // if exist in order, remove from pool
//                String orderNo = order.getOrderNo();
//                if (BorderList == null) {
//                    // return "{\"process_no\":\"11\", \"result\":\"no such B order to delete:" + orderNo+"\"}";
//                    return tradeResult;
//                }
//                for (int i=0; i < BorderList.size(); i++) {
//                    if (orderNo.equals(BorderList.get(i).getOrderNo())) {
//                        BorderList.remove(i);
//                        // if no other price delete poolPrice
//                        if (BorderList.isEmpty()) {
//                            for (int j=0; j < poolPriceB.size(); j++) {
//                                if (poolPriceB.get(j) == orderPrice) {
//                                    poolPriceB.remove(j);
//                                    break;
//                                }
//                            }
//                            poolB.remove(orderPrice);
//                        } else {
//                            poolB.put(orderPrice, BorderList);
//                        }
//                        poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                        pool.put(order.getSecCode()+"B", poolB);
//                        return tradeResult;
//                    }
//                }
//                // else output no delete order exist
//                return tradeResult;
//            }
//
//            // put into buy poolB
//            if (BorderList == null) {
//                BorderList = new ArrayList<>();
//                // price add a value
//                if (poolB.isEmpty()) {
//                    poolPriceB.add(orderPrice);
//                } else {
//                    // need to keep pool price be sorted, so insert it into pool price
//                    for (int i = 0; i < poolPriceB.size(); i++) {
//                        if (poolPriceB.get(i) < orderPrice) {
//                            poolPriceB.add(i, orderPrice);
//                            break;
//                        }
//                        if (i == poolPriceB.size()-1) {
//                            poolPriceB.add(orderPrice);
//                            break;
//                        }
//                    }
//                }
//            }
//            BorderList.add(order);
//            poolB.put(orderPrice, BorderList);
//
//            // if no elements in poolS, no transaction, add poolB
//            if (poolPriceS.isEmpty()) {
//                pool.put(order.getSecCode()+"B", poolB);
//                poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                // return complete;
//                return tradeResult;
//            }
//
//            // no satisfied price
//            if (poolPriceS.get(0) > poolPriceB.get(0)) {
//                // this.savepool();
//                pool.put(order.getSecCode()+"S", poolS);
//                pool.put(order.getSecCode()+"B", poolB);
//                poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                return tradeResult;
//            } else {
//                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
//            }
//        } else if (order.getTradeDir().equals("S")) {
//            float orderPrice = order.getOrderPrice();
//            List<Order> SorderList = poolS.get(orderPrice);
//            // if order tran_maint_code is "D", delete from pool
//            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
//                // if exist in order, remove from pool
//                String orderNo = order.getOrderNo();
//                if (SorderList == null) {
//                    return tradeResult;
//                }
//                for (int i=0; i < SorderList.size(); i++) {
//                    if (orderNo.equals(SorderList.get(i).getOrderNo())) {
//                        SorderList.remove(i);
//                        // if no other price delete poolPrice
//                        if (SorderList.isEmpty()) {
//                            for (int j=0; j < poolPriceS.size(); j++) {
//                                if (poolPriceS.get(j) == orderPrice) {
//                                    poolPriceS.remove(j);
//                                    break;
//                                }
//                            }
//                            poolS.remove(orderPrice);
//                        } else {
//                            poolS.put(orderPrice, SorderList);
//                        }
//                        poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                        pool.put(order.getSecCode()+"S", poolS);
//                        return tradeResult;
//                    }
//                }
//                // else output no delete order exist
//                return tradeResult;
//            }
//
//            // put into buy poolS
//            if (SorderList == null) {
//                SorderList = new ArrayList<>();
//                // price add a value
//                if (poolS.isEmpty()) {
//                    poolPriceS.add(orderPrice);
//                } else {
//                    for (int i = 0; i < poolPriceS.size(); i++) {
//                        if (poolPriceS.get(i) > orderPrice) {
//                            poolPriceS.add(i, orderPrice);
//                            break;
//                        }
//                        if (i == poolPriceS.size()-1) {
//                            poolPriceS.add(orderPrice);
//                            break;
//                        }
//                    }
//                }
//            }
//            SorderList.add(order);
//            poolS.put(orderPrice, SorderList);
//            // if no elements in poolB, no transaction, add poolS
//            if (poolPriceB.isEmpty()) {
//                pool.put(order.getSecCode()+"S", poolS);
//                poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                return tradeResult;
//            }
//
//            // no satisfied price
//            if (poolPriceS.get(0) > poolPriceB.get(0)) {
//                // this.savepool();
//                pool.put(order.getSecCode()+"S", poolS);
//                pool.put(order.getSecCode()+"B", poolB);
//                poolPrice.put(order.getSecCode()+"S", poolPriceS);
//                poolPrice.put(order.getSecCode()+"B", poolPriceB);
//                return tradeResult;
//            } else {
//                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
//            }
//        }
//        return tradeResult;
//    }


//    public Map<String, String> doStockExchange(String[] orderArr, String direction) {
//        Map<String, String> matchedResult = new HashMap<>();
//
//        // delete stock order
//        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1)) {
//            if (direction.equals("")) {
//                System.out.println("no order to delete!");
//            }
//            if (direction.equals("S")) {
//                stockExchangeMapSell.remove(orderArr[Order_No]);
//            }
//            if (direction.equals("B")) {
//                stockExchangeMapBuy.remove(orderArr[Order_No]);
//            }
//        }
//
//        if (direction.equals("")) {
//            System.out.println("bad tuple received!");
//            return matchedResult;
//        }
//        if (direction.equals("S")) {
//            stockExchangeMapSell.put(orderArr[Order_No], String.join("|", orderArr));
//            matchedResult = tradeSell(orderArr, stockExchangeMapBuy);
//        } else {
//            stockExchangeMapBuy.put(orderArr[Order_No], String.join("|", orderArr));
//            matchedResult = tradeBuy(orderArr, stockExchangeMapSell);
//        }
//        return matchedResult;
//    }

//    private Map<String, String> tradeSell(String[] sellerOrder, Map<String, String> stockExchangeMap) {
//        Map<String, String> matchedBuy = new HashMap<>();
//        Map<String, String> matchedSell = new HashMap<>();
//        Map<String, String> pendingBuy = new HashMap<>();
//        Map<String, String> pendingSell = new HashMap<>();
//        Iterator iter = stockExchangeMap.entrySet().iterator();
//        while (iter.hasNext()) {
//            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
//            String orderNo = entry.getKey();
//            String[] curBuyerOrder = entry.getValue().split("\\|");
//
//            if (curBuyerOrder[Sec_Code].equals(sellerOrder[Sec_Code])) {
//                String left = match(curBuyerOrder, sellerOrder);
//                if (!left.equals("unmatched")) {
//                    if (left.equals("S")) {
//                        pendingSell.put(sellerOrder[Sec_Code], String.join("|", sellerOrder));
//                        matchedBuy.put(curBuyerOrder[Sec_Code], String.join("|", curBuyerOrder));
//                    } else if (left.equals("B")) {
//                        pendingBuy.put(curBuyerOrder[Sec_Code], String.join("|", curBuyerOrder));
//                        matchedSell.put(sellerOrder[Sec_Code], String.join("|", sellerOrder));
//                    } else {
//                        matchedSell.put(sellerOrder[Sec_Code], String.join("|", sellerOrder));
//                        matchedBuy.put(curBuyerOrder[Sec_Code], String.join("|", curBuyerOrder));
//                    }
//                }
//            }
//        }
//
//        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);
//
//        return matchedSell;
//    }

//    private Map<String, String> tradeBuy(String[] buyerOrder, Map<String, String> stockExchangeMap) {
//        Map<String, String> matchedBuy = new HashMap<>();
//        Map<String, String> matchedSell = new HashMap<>();
//        Map<String, String> pendingBuy = new HashMap<>();
//        Map<String, String> pendingSell = new HashMap<>();
//        Iterator iter = stockExchangeMap.entrySet().iterator();
//        while (iter.hasNext()) {
//            Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
//            String orderNo = entry.getKey();
//            String[] curSellerOrder = entry.getValue().split("\\|");
//
//            if (curSellerOrder[Sec_Code].equals(buyerOrder[Sec_Code])) {
//                String left = match(buyerOrder, curSellerOrder);
//                if (!left.equals("unmatched")) {
//                    if (left.equals("S")) {
//                        pendingSell.put(curSellerOrder[Sec_Code], String.join("|", curSellerOrder));
//                        matchedBuy.put(buyerOrder[Sec_Code], String.join("|", buyerOrder));
//                    } else if (left.equals("B")) {
//                        pendingBuy.put(buyerOrder[Sec_Code], String.join("|", buyerOrder));
//                        matchedSell.put(curSellerOrder[Sec_Code], String.join("|", curSellerOrder));
//                    } else {
//                        matchedSell.put(curSellerOrder[Sec_Code], String.join("|", curSellerOrder));
//                        matchedBuy.put(buyerOrder[Sec_Code], String.join("|", buyerOrder));
//                    }
//                }
//            }
//        }
//
//        updateStore(pendingBuy, pendingSell, matchedBuy, matchedSell);
//
//        return matchedSell;
//    }
//
//    private String match(String[] buyerOrder, String[] sellerOrder) {
//        float buyPrice = Float.valueOf(buyerOrder[Order_Price]);
//        float sellPrice = Float.valueOf(sellerOrder[Order_Price]);
//        if (buyPrice < sellPrice) {
//            return "unmatched";
//        }
//        float buyVol = Float.valueOf(buyerOrder[Order_Vol]);
//        float sellVol = Float.valueOf(sellerOrder[Order_Vol]);
//        if (buyVol > sellVol) {
//            buyerOrder[Order_Vol] = String.valueOf(buyVol - sellVol);
//            return "B";
//        } else if (buyVol < sellVol) {
//            sellerOrder[Order_Vol] = String.valueOf(sellVol - buyVol);
//            return "S";
//        } else {
//            return "";
//        }
//    }

//    private void updateStore(
//            Map<String, String> pendingBuy,
//            Map<String, String> pendingSell,
//            Map<String, String> matchedBuy,
//            Map<String, String> matchedSell) {
//
//        System.out.println("stockExchangeMapSell: " + stockExchangeMapSell.size() + " stockExchangeMapBuy: " + stockExchangeMapBuy.size()
//                + " total: " + (stockExchangeMapSell.size() + stockExchangeMapBuy.size()));
//
//        for (Map.Entry<String, String> order : pendingBuy.entrySet()) {
//            stockExchangeMapBuy.put(order.getKey(), order.getValue());
//        }
//        for (Map.Entry<String, String> order : pendingSell.entrySet()) {
//            stockExchangeMapSell.put(order.getKey(), order.getValue());
//        }
//        for (Map.Entry<String, String> order : matchedBuy.entrySet()) {
//            stockExchangeMapBuy.remove(order.getKey());
//        }
//        for (Map.Entry<String, String> order : matchedSell.entrySet()) {
//            stockExchangeMapSell.remove(order.getKey());
//        }
//    }

    public void callAuction() {
        // do call auction
        // 1. sort buy order and sell order by price and timestamp
        System.out.println("Start call auction");

        loadPool();

        metricsDump();

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
                        if (curSellOrders.isEmpty()) {
                            curSellPool.remove(curSellPrice);
                        } else {
                            curSellPool.replace(curSellPrice, curSellOrders);
                        }
                    }
                }
                // put updated sell pool into original pool
                poolS.replace(curStockId, curSellPool);

                if (curBuyOrders.isEmpty()) {
                    curBuyPool.remove(curBuyPrice);
                } else {
                    curBuyPool.replace(curBuyPrice, curBuyOrders);
                }
            }
            // TODO: sanity check, whether this is equal to map size
            // put updated buy pool into original pool
            poolB.replace(curStockId, curBuyPool);

            metricsDump();
        }
    }

    public Map<String, String> continuousStockExchange(String[] orderArr, String direction) {
        long start = System.currentTimeMillis();

        Map<String, String> matchedResult = new HashMap<>();

        Order curOrder = new Order(orderArr);

        // delete stock orderm, index still needs to be deleted
        if (orderArr[Tran_Maint_Code].equals(FILTER_KEY1)) {
            if (direction.equals("")) {
                System.out.println("no order to delete!");
            }

            Order targetOrder = null;

            if (direction.equals("S")) {
                stockExchangeMapSell.remove(orderArr[Order_No]);
                HashMap<Integer, ArrayList<Order>> curSellPool = poolS.getOrDefault(curOrder.getSecCode(), new HashMap<>());
                ArrayList<Order> curSellOrders = curSellPool.getOrDefault(curOrder.getOrderPrice(), new ArrayList<>());

                for (Order order : curSellOrders) {
                    if (order.getOrderNo().equals(curOrder.getOrderNo())) {
                        targetOrder = order;
                    }
                }
                curSellOrders.remove(targetOrder);
                if (curSellOrders.isEmpty()) {
                    curSellPool.remove(curOrder.getOrderPrice());
                } else {
                    curSellPool.replace(curOrder.getOrderPrice(), curSellOrders);
                }
                poolS.replace(curOrder.getSecCode(),curSellPool);
            }
            if (direction.equals("B")) {
                stockExchangeMapBuy.remove(orderArr[Order_No]);
                HashMap<Integer, ArrayList<Order>> curBuyPool = poolB.getOrDefault(curOrder.getSecCode(), new HashMap<>());
                ArrayList<Order> curBuyOrders = curBuyPool.getOrDefault(curOrder.getOrderPrice(), new ArrayList<>());
                for (Order order : curBuyOrders) {
                    if (order.getOrderNo().equals(curOrder.getOrderNo())) {
                        targetOrder = order;
                    }
                }

                curBuyOrders.remove(targetOrder);
                if (curBuyOrders.isEmpty()) {
                    curBuyPool.remove(curOrder.getOrderPrice());
                } else {
                    curBuyPool.replace(curOrder.getOrderPrice(), curBuyOrders);
                }
                poolB.replace(curOrder.getSecCode(),curBuyPool);
            }

            return matchedResult;
        }

//        loadPool(curOrder.getSecCode(), direction);

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
                    if (curSellOrders.isEmpty()) {
                        curSellPool.remove(curSellPrice);
                    } else {
                        curSellPool.replace(curSellPrice, curSellOrders);
                    }
                }
            }

            poolS.replace(curOrder.getSecCode(), curSellPool);

            if (curBuyOrders.isEmpty()) {
                curBuyPool.remove(curBuyPrice);
            } else {
                curBuyPool.replace(curBuyPrice, curBuyOrders);
            }

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
                    if (curBuyOrders.isEmpty()) {
                        curBuyPool.remove(curBuyPrice);
                    } else {
                        curBuyPool.replace(curBuyPrice, curBuyOrders);
                    }
                }
            }

            poolB.replace(curOrder.getSecCode(), curBuyPool);

            if (curSellOrders.isEmpty()) {
                curSellPool.remove(curSellPrice);
            } else {
                curSellPool.replace(curSellPrice, curSellOrders);
            }

            poolS.replace(curOrder.getSecCode(), curSellPool);
        }

//        System.out.println("processing time: " + (System.currentTimeMillis() - start));
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
            stockExchangeMapSell.remove(tradedSellOrder.getOrderNo());
        }

        for (Order tradedBuyOrder : tradedBuyOrders) {
//            System.out.println(stockExchangeMapBuy.containsKey(tradedBuyOrder.getOrderNo()) + " "
//                    + tradedBuyOrder.toString());
            curBuyOrders.remove(tradedBuyOrder);
            stockExchangeMapBuy.remove(tradedBuyOrder.getOrderNo());
        }

        // update orders half traded.
        for (Order halfTradedSellOrder : curSellOrders) {
            stockExchangeMapSell.replace(halfTradedSellOrder.getOrderNo(), halfTradedSellOrder.toString());
        }

        for (Order halfTradedBuyOrder : curBuyOrders) {
            stockExchangeMapBuy.replace(halfTradedBuyOrder.getOrderNo(), halfTradedBuyOrder.toString());
        }
    }

    public void loadPool() {
        // load pool from state backend, then do matchmaking by use old logic
        Iterator buyIter = stockExchangeMapBuy.entrySet().iterator();
        Iterator sellIter = stockExchangeMapSell.entrySet().iterator();

        while (buyIter.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) buyIter.next();
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
            Map.Entry<String, String> entry = (Map.Entry<String, String>) sellIter.next();
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

//    public void loadPool(String stockId, String direction) {
//        // load pool from state backend, then do matchmaking by use old logic
//        if (direction.equals("S")) {
//            Iterator buyIter = stockExchangeMapBuy.entrySet().iterator();
//            while (buyIter.hasNext()) {
//                Map.Entry<String, String> entry = (Map.Entry<String, String>) buyIter.next();
//                String orderNo = entry.getKey();
//                String[] curBuyerOrder = entry.getValue().split("\\|");
//                Order curOrder = new Order(curBuyerOrder);
//                String curSecCode = curOrder.getSecCode();
//                int curOrderPrice=0;
//                try {
//                    curOrderPrice = curOrder.getOrderPrice();
//                } catch (Exception e) {
//                    System.out.println(entry.getValue());
//                }
//
//                ArrayList<Order> curOrderList = specPoolS.getOrDefault(curOrderPrice, new ArrayList<>());
//                // need to keep pool price be sorted, so insert it into pool price
//                curOrderList.add(curOrder);
//                specPoolS.put(curOrderPrice, curOrderList);
//            }
//        } else {
//            Iterator sellIter = stockExchangeMapSell.entrySet().iterator();
//            while (sellIter.hasNext()) {
//                Map.Entry<String, String> entry = (Map.Entry<String, String>) sellIter.next();
//                String orderNo = entry.getKey();
//                String[] curBuyerOrder = entry.getValue().split("\\|");
//                Order curOrder = new Order(curBuyerOrder);
//                String curSecCode = curOrder.getSecCode();
//                if (curSecCode.equals(stockId)) {
//                    int curOrderPrice = curOrder.getOrderPrice();
//
//                    ArrayList<Order> curOrderList = specPoolB.getOrDefault(curOrderPrice, new ArrayList<>());
//                    // need to keep pool price be sorted, so insert it into pool price
//                    curOrderList.add(curOrder);
//                    specPoolB.put(curOrderPrice, curOrderList);
//                }
//            }
//        }
//    }

    public void metricsDump() {
        System.out.println("stockExchangeMapSell: " + stockExchangeMapSell.size() + " stockExchangeMapBuy: " + stockExchangeMapBuy.size()
                + " total: " + (stockExchangeMapSell.size() + stockExchangeMapBuy.size()));

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

    public static void main(String[] args) throws IOException {
        // 1. do call auction, just buffer all tuples, and after reading the CALLAUCTION flag, do call auction
        //      details: store them into map, then sort them all, then for loop to match buyer and seller
        //      don't mind the performance, it is enough to use.
        // 2. do continuous auction based on current state, now the state size is very big, should be that big, otherwise, there are some bugs.
        // 3. in match maker, first sort all orders by its price, then in every price, sort their time, then do matchmake for sell and buy.
        // 4. if is sell, only sort buy, and do matchmaking, if is buy, sort sell, and do matchmaking,
        //    and delete those who has vol=0, append this order to waiting buy, if vol>0
        StockExchangeSimu ses = new StockExchangeSimu();
//        ses.loadPool();

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int noRecSleepCnt = 0;
        stream = new FileReader("/root/SSE-kafka-producer/sb-opening-50ms.txt");
        br = new BufferedReader(stream);

        int interval = 1000000000/1000;

        RandomDataGenerator randomGen = new RandomDataGenerator();


//        boolean continuousAuction = false;
        int continuousAuction = 92500;

        while ((sCurrentLine = br.readLine()) != null) {
            if (sCurrentLine.equals("end")) {
                continue;
            }

            if (sCurrentLine.equals("CALLAUCTIONEND")) {
                // start to do call auction
                ses.callAuction();
            }
            if (sCurrentLine.split("\\|").length < 10) {
                continue;
            }
            String[] orderArr = sCurrentLine.split("\\|");

            if (orderArr[Tran_Maint_Code].equals(FILTER_KEY2) || orderArr[Tran_Maint_Code].equals(FILTER_KEY3)) {
                continue;
            }

            int curTime = Integer.parseInt(orderArr[Last_Upd_Time].replace(":", ""));

            if (curTime < continuousAuction) {
                // store all orders at maps
                if (orderArr[Tran_Maint_Code].equals("D")) {
                    if (orderArr[Trade_Dir].equals("S")) {
                        ses.stockExchangeMapSell.remove(orderArr[Order_No]);
                    } else if (orderArr[Trade_Dir].equals("B")) {
                        ses.stockExchangeMapBuy.remove(orderArr[Order_No]);
                    }
                } else {
                    if (orderArr[Trade_Dir].equals("S")) {
                        ses.stockExchangeMapSell.put(orderArr[Order_No], sCurrentLine);
                    } else if (orderArr[Trade_Dir].equals("B")) {
                        ses.stockExchangeMapBuy.put(orderArr[Order_No], sCurrentLine);
                    } else {
                        System.out.println("++++ error direction");
                    }
                }
            } else {
                Map<String, String> matchedResult = ses.continuousStockExchange(orderArr, orderArr[Trade_Dir]);

//            Order order = new Order(orderArr);
//            List<String> xactResult = ses.stockExchange(order);
//            System.out.println(xactResult);
//            for (Map.Entry<String, String> order : matchedResult.entrySet()) {
//                System.out.println(order.getValue());
//            }
            }
        }
    }
}
