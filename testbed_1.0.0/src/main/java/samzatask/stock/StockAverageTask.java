package samzatask.stock;

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


/**
 * This is a simple task that writes each message to a state store and prints them all out on reload.
 *
 * It is useful for command line testing with the kafka console producer and consumer and text messages.
 */
public class StockAverageTask implements StreamTask, InitableTask {

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;


    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "stock_price");
    private KeyValueStore<String, String> stockAvgPriceMap;

    @SuppressWarnings("unchecked")
    public void init(Context context) {
        this.stockAvgPriceMap = (KeyValueStore<String, String>) context.getTaskContext().getStore("stock-average");
        System.out.println("Contents of store: ");
        KeyValueIterator<String, String> iter = stockAvgPriceMap.all();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            System.out.println(entry.getKey() + " => " + entry.getValue());
        }
        iter.close();
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
//        System.out.println("Adding " + envelope.getMessage() + " => " + envelope.getMessage() + " to the store.");
//        store.put((String) envelope.getMessage(), (Float) envelope.getMessage());
//        coordinator.commit(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
        String stockOrder = (String) envelope.getMessage();
        String[] orderArr = stockOrder.split("\\|");

        Long start = System.nanoTime();
        while (System.nanoTime() - start < 500000) {}

        String average = computeAverage(orderArr);
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, average));
    }

    private String computeAverage(String[] orderArr) {
        if (this.stockAvgPriceMap.get(orderArr[Sec_Code]) == null) {
            stockAvgPriceMap.put(orderArr[Sec_Code], String.valueOf(0));
        }
        float sum = Float.parseFloat(stockAvgPriceMap.get(orderArr[Sec_Code])) + Float.parseFloat(orderArr[Order_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], String.valueOf(sum));
        return orderArr[Sec_Code] + ": " + String.valueOf(sum);
    }
}