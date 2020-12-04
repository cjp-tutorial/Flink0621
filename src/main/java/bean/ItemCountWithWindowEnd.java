package bean;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/4 16:48
 */
public class ItemCountWithWindowEnd {
    private long itemId;
    private long itemCount;
    private long windowEnd;

    public ItemCountWithWindowEnd() {
    }

    public ItemCountWithWindowEnd(long itemId, long itemCount, long windowEnd) {
        this.itemId = itemId;
        this.itemCount = itemCount;
        this.windowEnd = windowEnd;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getItemCount() {
        return itemCount;
    }

    public void setItemCount(long itemCount) {
        this.itemCount = itemCount;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemCountWithWindowEnd{" +
                "itemId=" + itemId +
                ", itemCount=" + itemCount +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
