package org.pagemining.hadoop.linkbase;

/**
 * Created with IntelliJ IDEA.
 * User: macbookpro
 * Date: 14-1-25
 * Time: 下午12:36
 * To change this template use File | Settings | File Templates.
 */
public class LinkSource {
    private String srcLink = "null";
    private String anchorText = "null";

    public String getSrcLink() {
        return srcLink;
    }

    public void setSrcLink(String srcLink) {
        this.srcLink = srcLink;
    }

    public String getAnchorText() {
        return anchorText;
    }

    public void setAnchorText(String anchorText) {
        this.anchorText = anchorText;
    }
}
