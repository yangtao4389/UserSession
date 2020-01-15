package cn.angetech.session;
import scala.math.Ordered;

public class CategorySortKey implements Ordered<CategorySortKey>, java.io.Serializable {
    private Long clickCount;
    private Long orderCount;
    private Long payCount;
    public void set(Long clickCount, Long orderCount, Long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }
    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
       if(clickCount - that.getClickCount() != 0){
           return (int)(clickCount - that.getClickCount());
       }else if(clickCount - that.getClickCount() == 0 && orderCount - that.getOrderCount() != 0){
           return (int)(orderCount-that.getOrderCount());
       }else if(clickCount - that.getClickCount() == 0 && orderCount-that.getOrderCount() ==0 && payCount-that.getPayCount() != 0){
           return (int) (payCount-that.getPayCount());
       }
       return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if(clickCount<that.getClickCount()){
            return true;
        }else if(clickCount == that.getClickCount() && orderCount<that.getOrderCount()){
            return true;
        }else if(clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount<that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if(clickCount>that.getClickCount()){
            return true;
        }else if(clickCount == that.getClickCount() && orderCount>that.getOrderCount()){
            return true;
        }else if(clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount>that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if($less(that)){
            return  true;
        }else if(clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount==that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if($greater(that)){
            return  true;
        }else if(clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount==that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        return compare(that);
    }
}
