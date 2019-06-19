package pro.hbase.weibo.constant;

public class Names {

    /**
     * 命名空间
     */
    public final static String NAMESPACE_WEIBO = "weibo";

    /**
     * 表名：命名空间：表名
     */
    public final static String TABLE_WEIBO = "weibo:weibo";
    public final static String TABLE_RELATION = "weibo:relation";
    public final static String TABLE_INBOX = "weibo:inbox";

    /**
     * 列族名
     */
    public final static String WEIBO_FAMILY_DATA = "data";
    public final static String RELATION_FAMILY_DATA = "data";
    public final static String INBOX_FAMILY_DATA = "data";

    /**
     * 列名
     */
    public final static String WEIBO_COLUMN_CONTENT = "content";
    public final static String RELATION_COLUMN_TIME = "time";

    /**
     * 版本数
     */
    public final static Integer INBOX_DATA_VERSIONS = 3;

}
