package org.pagemining.hadoop.infoextract;

public class Config {
    public static String[] data = {
            "name = dianping_shop",
            "pattern = http://www.dianping.com/shop/[0-9]+",
            "title = h1.shop-title",
            "price = .stress",
            "address = [itemprop=street-address]",
            "[regions] = .breadcrumb .bread-name",
            "",
            "name = 5i5j_exchange",
            "pattern = ",
            "[regions] = .path a",
            "price = li.price",
            ""
    };
}
