import java.io.Serializable;

/**
 * Created by ssah22 on 9/11/2017.
 */
public class competitor implements Serializable {
    String prod_id;
    String price;
    String saleEvent;
    String Rival_name;
    String ts;

    public void setProd_id(String prod_id) {
        this.prod_id = prod_id;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public void setSaleEvent(String saleEvent) {
        this.saleEvent = saleEvent;
    }

    public void setRival_name(String rival_name) {
        Rival_name = rival_name;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "competitor{" +
                "prod_id=" + prod_id +
                ", price=" + price +
                ", saleEvent='" + saleEvent + '\'' +
                ", Rival_name='" + Rival_name + '\'' +
                ", ts=" + ts +
                '}';
    }

    public competitor(){}
    public competitor(String prod_id, String price, String saleEvent, String rival_name, String ts) {
        this.prod_id = prod_id;
        this.price = price;
        this.saleEvent = saleEvent;
        Rival_name = rival_name;
        this.ts = ts;
    }



}

