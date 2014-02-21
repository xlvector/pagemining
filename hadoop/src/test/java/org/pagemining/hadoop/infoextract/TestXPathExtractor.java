package org.pagemining.hadoop.infoextract;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.junit.Test;
import org.pagemining.extractor.xpath.XPathConfig;
import org.pagemining.extractor.xpath.XPathExtractor;

import java.io.UnsupportedEncodingException;

public class TestXPathExtractor {
    @Test
    public void TestConfig() throws UnsupportedEncodingException {
        String configStr = "[{\"_name\":\"dianping-shop\",\"_pattern\":\"http:\\/\\/www.dianping.com\\/shop\\/[0-9]+\",\"nav\":\".breadcrumb .bread-name\",\"title\":\".shop-name h1\",\"price\":\"[itemprop=rating] .stress\",\"address\":\"[itemprop=street-address]\",\"region\":\".region\",\"phone\":\"[itemprop=tel]\",\"info\":{\"name\":\"li:contains(\\u5546\\u6237\\u522b\\u540d)\",\"_root\":\".desc-list\",\"introduction\":\"li:contains(\\u5546\\u6237\\u7b80\\u4ecb)\",\"product\":\"li [rel=tag]\"},\"comment\":{\"_root\":\".comment-list > ul > li\",\"content\":\".comment-txt\",\"price\":\".comm-per, \\u4eba\\u5747(.*)\",\"username\":\".name a\",\"recommend\":\".comment-recommend\"}},{\"_name\":\"p2pzxw\",\"_pattern\":\"http:\\/\\/www.p2pzxw.com\\/index.*Page=[0-9]+\",\"blacklist\":{\"_root\":\".yqsj_kk\",\"username\":\"div div:contains(\\u59d3\\u540d) font\",\"idcard\":\"div div:contains(\\u8bc1\\u4ef6\\u53f7) font\",\"gender\":\"div div:contains(\\u6027\\u522b), \\u6027\\u522b\\uff1a(.*)\",\"idcard_address\":\"div div:contains(\\u8eab\\u4efd\\u8bc1\\u5730\\u5740), \\u8eab\\u4efd\\u8bc1\\u5730\\u5740\\uff1a(.*)\",\"home_address\":\"div div:contains(\\u5bb6\\u5ead\\u5730\\u5740), \\u5bb6\\u5ead\\u5730\\u5740\\uff1a(.*)\",\"phone\":\"div div:contains(\\u8054\\u7cfb\\u7535\\u8bdd), \\u8054\\u7cfb\\u7535\\u8bdd\\uff1a(.*)\"}},{\"_name\":\"ppdai\",\"_pattern\":\"http:\\/\\/www.ppdai.com\\/blacklist\\/OrdinaryMode\\/.*\",\"blacklist\":{\"_root\":\".black_table tr\",\"username\":\"td p:contains(\\u59d3\\u540d) a\",\"email\":\"td p:contains(\\u7535\\u5b50\\u90ae\\u4ef6), \\u7535\\u5b50\\u90ae\\u4ef6:(.*)\",\"phone\":\"td p:contains(\\u624b\\u673a), \\u624b\\u673a\\u53f7\\u7801:(.*)\"}},{\"_name\":\"hc360-b2b-company\",\"_pattern\":\"http:\\/\\/[a-z0-9]+.b2b.hc360.com\\/\",\"company_name\":\"h1\",\"contract_person\":\".renName a\",\"address\":\"h6:contains(\\u5730\\u5740), \\u5730\\u5740\\uff1a(.*)\",\"phone\":\"h6:contains(\\u7535\\u8bdd)\",\"introduction\":\".cAbout\"},{\"_name\":\"aibang-detail\",\"_pattern\":\"http:\\/\\/www.aibang.com\\/detail\\/.*\",\"address\":\"dd:contains(\\u516c\\u4ea4), (.*)\\u516c\\u4ea4 \\u9a7e\\u4e58\",\"price\":\".detail_list dd:contains(\\u4eba\\u5747) .fb\",\"food\":\"#recommend_dish a\",\"introduction\":\"#biz_desc_full\"},{\"_name\":\"qq-weibo\",\"_pattern\":\"http:\\/\\/t.qq.com\\/[a-z0-9]+\",\"username\":\".text_user\",\"summary\":\".summary\",\"location\":\".desc [boss=btnApolloCity]\",\"occupation\":\".desc [boss=btnApolloWork]\"},{\"_name\":\"cnki-article\",\"_pattern\":\"http:\\/\\/lib.cnki.net\\/cjfd\\/[A-Z0-9]+.html\",\"author\":\".author a\",\"abstract\":\".abstract\",\"keywords\":\".fund a strong\",\"fund\":\".fund:contains(\\u57fa\\u91d1) a\",\"source\":\".from a\"},{\"_name\":\"5i5j-exchange\",\"_pattern\":\"http:\\/\\/bj.5i5j.com\\/exchange\\/[0-9]+\",\"title\":\".title h1\",\"price\":\".price b\",\"square\":\".sprice:contains(\\u9762\\u79ef) b\",\"nav\":\".path a\",\"introduction\":\".intro .newDescribe ul li\",\"consultant\":{\"_root\":\".groom\",\"name\":\"dd:contains(\\u987e\\u95ee) span\",\"phone\":\"dd:contains(\\u987e\\u95ee), ([0-9]{11})\"}},{\"_name\":\"soufun-\\u4e8c\\u624b\\u623f\",\"_pattern\":\"http:\\/\\/esf.[a-z0-9]+.soufun.com\\/chushou\\/[0-9]+_[0-9]+.htm\",\"nav\":\".guide .floatl a\",\"title\":\".title h1\",\"price\":\".zongjia1 .red20b\",\"square\":\".base_info dd:contains(\\u9762\\u79ef) span, ([0-9]+)\"},{\"_name\":\"\\u94fe\\u5bb6-\\u4e8c\\u624b\\u623f\",\"_pattern\":\"http:\\/\\/beijing.homelink.com.cn\\/ershoufang\\/[A-Z0-9]+.shtml\",\"nav\":\".nav a\",\"title\":\".xtitle h1\",\"price\":\".shoujia li span:matches([0-9]+)\",\"square\":\".shoujia .hangji:contains(\\u5e73\\u7c73), \\/([0-9]+)\\u5e73\\u7c73\",\"phone\":\".jiageman label\",\"agent\":{\"_root\":\".bot_nine1 dl\",\"name\":\"dd a\",\"phone\":\"dd kbd\"}},{\"_name\":\"\\u8bc4\\u8001\\u5e08-\\u5927\\u5b66\",\"_pattern\":\"http:\\/\\/www.pinglaoshi.com\\/teacherId[0-9]+\\/\",\"username\":\".lech_content h1\",\"school\":\".pei_conright1 p:contains(\\u5927\\u5b66) a\",\"city\":\".pei_conright1 p:contains(\\u57ce\\u5e02) span\"},{\"_name\":\"\\u963f\\u91cc\\u5df4\\u5df4\\u5546\\u4eba\",\"_pattern\":\"http:\\/\\/me.1688.com\\/[a-z0-9]+.htm\",\"username\":\".m-title .name ,(.*) \\u5bf9\",\"location\":\".arae\",\"company\":\".company-info a\",\"tag\":\".fui-tag span\"},{\"_name\":\"\\u77e5\\u4e4e\\u7528\\u6237\",\"_pattern\":\"http:\\/\\/www.zhihu.com\\/people\\/.*\",\"username\":\".zm-profile-header .name\",\"bio\":\".bio\",\"answer\":{\"_root\":\"#zh-profile-answers-inner-list > div\",\"question\":\".question_link\"},\"ask\":{\"_root\":\"#zh-profile-ask-inner-list > div\",\"question\":\".question_link\"},\"activity\":{\"_root\":\"#zh-profile-activity-page-list > div\",\"question\":\".question_link\"}},{\"_name\":\"\\u767e\\u5ea6\\u767e\\u79d1\",\"_pattern\":\"http:\\/\\/baike.baidu.com\\/view\\/[0-9]+.htm\",\"title\":\".title .lemmaTitleH1\",\"tag\":\"#open-tag-item a\",\"entity\":\"a[href~=\\/view\\/[0-9]+.htm]\"}]";
        JSONArray array = (JSONArray)JSONValue.parse(configStr);
        System.out.println(array.size());

        XPathExtractor extractor = new XPathExtractor();
        for(int i = 0; i < array.size(); ++i){
            XPathConfig config = new XPathConfig();
            config.fromString(array.get(i).toString());
            extractor.AddConfig(config);
        }


        String buf = new String("{\"中文\" : \"很好\"}".getBytes(), "UTF-8");
        for(byte b : buf.getBytes()){
            System.out.println((int)b);
        }
        System.out.println();
        StringBuilder sb = new StringBuilder();
        for(byte b : "中文".getBytes()){
            sb.append((int)b);
        }
        System.out.println(sb.toString());
        JSONObject jsonObject = (JSONObject) JSONValue.parse(buf);
        System.out.println(jsonObject.containsKey("中文"));
    }
}
