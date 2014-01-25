package org.pagemining.hadoop.linkbase;

public class LinkInfo {
    private LinkSource linkSource = new LinkSource();
    private Long updatedAt = 0L;


    public String getAnchorText() {
        return linkSource.getAnchorText();
    }

    public void setAnchorText(String anchorText) {
        this.linkSource.setAnchorText(anchorText);
    }

    public String getSrcLink() {
        return this.linkSource.getSrcLink();
    }

    public void setSrcLink(String srcLink) {
        this.linkSource.setSrcLink(srcLink);
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(updatedAt);
        sb.append("\t");
        sb.append(getSrcLink());
        sb.append("\t");
        sb.append(getAnchorText());
        return sb.toString();
    }

    public void fromString(String buf){
        String [] tks = buf.split("\t");
        if(tks.length != 3) return;
        updatedAt = Long.parseLong(tks[0]);
        setSrcLink(tks[1]);
        setAnchorText(tks[2]);
    }
}
