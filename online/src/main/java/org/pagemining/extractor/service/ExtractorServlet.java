package org.pagemining.extractor.service;

import net.minidev.json.JSONObject;
import org.pagemining.extractor.xpath.XPathConfig;
import org.pagemining.extractor.xpath.XPathExtractor;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class ExtractorServlet extends HttpServlet {

    public String download(String link){
        StringBuilder html = new StringBuilder();
        try {
            URL url = new URL(link);
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            String line;
            while ((line = reader.readLine()) != null){
                html.append(line);
                html.append("\n");
            }
        } catch (MalformedURLException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
        return html.toString();
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException{
        String link = URLDecoder.decode(request.getParameter("link"), "UTF-8");
        String configString = URLDecoder.decode(request.getParameter("config"), "UTF-8");
        System.out.println(configString);

        XPathConfig config = new XPathConfig();
        config.fromString(configString);

        XPathExtractor extractor = new XPathExtractor();
        extractor.AddConfig(config);

        response.setContentType("text/plain;");
        response.setCharacterEncoding("utf-8");
        response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
        response.setHeader("Pragma", "no-cache");
        JSONObject json = extractor.extract(link);
        if(json == null){
            response.setStatus(404);
            response.getWriter().write("Return JSON is null");
        }
        else{
            response.getWriter().write(json.toString());
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        doPost(request, response);
    }
}
