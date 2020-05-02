package samzatask.stock;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class Order {
    /**
     * The user that viewed the page
     */
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Last_Upd_Time = 2;
    private static final int Order_Price = 3;
    private static final int Order_Exec_Vol = 4;
    private static final int Order_Vol = 5;
    private static final int Sec_Code = 6;
    private static final int Trade_Dir = 7;

    private static final int Mapped_Order_No = 0;
    private static final int Mapped_Order_Price = 1;
    private static final int Mapped_Order_Vol = 2;

    private String[] orderArr;

    Order(String[] orderArr) {
        this.orderArr = new String[]{orderArr[Order_No], orderArr[Order_Price], orderArr[Order_Vol]};
    }

    Order(String orderNo, String orderPrice, String orderVol) {
        this.orderArr = new String[]{orderNo, orderPrice, orderVol};
    }

    String getOrderNo() {
        return orderArr[Mapped_Order_No];
    }

    int getOrderPrice() {
        Float price = Float.parseFloat(orderArr[Mapped_Order_Price]) * 100000;
        return price.intValue();
    }
    int getOrderVol() {
        Float interOrderVol = Float.parseFloat(orderArr[Mapped_Order_Vol]);
        return interOrderVol.intValue();
    }

    public String getKey(int key) {
        return orderArr[key];
    }

    @Override
    public String toString() {
        return String.join("|", orderArr);
    }

    void updateOrder(int otherOrderVol) {
        orderArr[Mapped_Order_Vol] = (this.getOrderVol() - otherOrderVol) + "";
    }
}


