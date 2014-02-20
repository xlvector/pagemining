package org.pagemining.extractor.xpath;

import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.recognition.NatureRecognition;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.util.FilterModifWord;
import org.junit.Test;

import java.util.List;

public class TestNLPExtractor {
    @Test
    public void Test(){
        UserDefineLibrary.insertWord("朝阳区", "ud", 1000);
        UserDefineLibrary.insertWord("建国路", "ud", 1000);
        UserDefineLibrary.insertWord("soho现代城", "ud", 1000);
        UserDefineLibrary.insertWord("大望路", "ud", 1000);
        List<Term> words = ToAnalysis.parse("北京市朝阳区建国路88号SOHO现代城C座709（大望路地铁站B口）");
        new NatureRecognition(words).recognition();
        FilterModifWord.modifResult(words);
        System.out.println(words);
    }
}
