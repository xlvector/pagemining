package org.pagemining.extractor.xpath;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.junit.Test;

import java.util.List;

public class TestNLPExtractor {
    @Test
    public void Test(){
        List<Term> words = NlpAnalysis.parse("北京市朝阳区建国路88号SOHO现代城C座709（大望路地铁站B口）");
        for(Term word : words){
            System.out.println(word.getName());
        }
    }
}
