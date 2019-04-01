import java.util.Date;

public class Supplier {
    private int supplierId;
    private String supplierName;
    private Date supplierStartDate;

    public Supplier(int id, String name, Date dt) {
        this.supplierId = id;
        supplierName = name;
        supplierStartDate = dt;
    }

    public int getId() {
        return supplierId;
    }

    public String getName() {
        return supplierName;
    }

    public Date getDate() {
        return supplierStartDate;
    }
}
