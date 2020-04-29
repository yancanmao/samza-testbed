package samzatask.stock;

import java.util.*;

public class utils {
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

    public static Map<Integer, ArrayList<Order>> sortMapBykeyAsc(Map<Integer, ArrayList<Order>> oriMap) {
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
                            return value1 - value2;
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
