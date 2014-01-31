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
            "pattern = http://bj.5i5j.com/exchange/[0-9]+",
            "[regions] = .path a",
            "total_price = li.price",
            "unit_price = .sprice:contains(单价)",
            "area = .sprice:contains(面积)",
            "",
            "name = cnki_qikan",
            "pattern = http://lib.cnki.net/cjfd/[A-Z0-9]+.html",
            "[authors] = .author a",
            "from = .from a",
            "abstract = .abstract",
            ""
    };
}
