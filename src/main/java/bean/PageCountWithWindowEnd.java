package bean;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/5 9:35
 */
public class PageCountWithWindowEnd implements Comparable<PageCountWithWindowEnd> {
    private String url;
    private long pageCount;
    private long windowEnd;

    public PageCountWithWindowEnd() {
    }

    public PageCountWithWindowEnd(String url, long pageCount, long windowEnd) {
        this.url = url;
        this.pageCount = pageCount;
        this.windowEnd = windowEnd;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getPageCount() {
        return pageCount;
    }

    public void setPageCount(long pageCount) {
        this.pageCount = pageCount;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "PageCountWithWindowEnd{" +
                "url='" + url + '\'' +
                ", pageCount=" + pageCount +
                ", windowEnd=" + windowEnd +
                '}';
    }


    @Override
    public int compareTo(PageCountWithWindowEnd o) {
        return (int) (o.getPageCount() - this.getPageCount());
    }
}
