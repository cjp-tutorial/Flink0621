package bean;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/5 10:51
 */
public class CountByProAdWithWindowEnd {
    private String province;
    private Long adId;
    private Long count;
    private Long windowEnd;

    public CountByProAdWithWindowEnd() {
    }

    public CountByProAdWithWindowEnd(String province, Long adId, Long count, Long windowEnd) {
        this.province = province;
        this.adId = adId;
        this.count = count;
        this.windowEnd = windowEnd;
    }



    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "CountByProAdWithWindowEnd{" +
                "province='" + province + '\'' +
                ", adId=" + adId +
                ", count=" + count +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
