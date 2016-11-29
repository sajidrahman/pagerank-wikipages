package hadoop.job1.parse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WikiPageParseMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern wikiLinksPattern = Pattern.compile("(?<=[\\[]{2}).+?(?=[\\]])");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
        
        String pageTitle = titleAndText[0];
        String pageBody = titleAndText[1];
        if(isInvalidPage(pageTitle))
            return;
        
        Text page = new Text(pageTitle.replace(' ', '_'));

        //Parse the page body for valid links
        Matcher matcher = wikiLinksPattern.matcher(pageBody);
        
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String otherPage = matcher.group();
            //Filter only wiki pages.
            //- some have [[realPage|linkName]], some single [realPage]
            //- some link to files or external pages.
            //- some link to paragraphs into other pages.
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            context.write(page, new Text(otherPage));
        }
    }
    
    private boolean isInvalidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getWikiPageFromLink(String aLink){
        if(isInvalidWikiLink(aLink)) return null;
        
        int start = 0;
        int endLink = aLink.length();

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        
        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }
        
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = (aLink.contains("&amp;"))? aLink.replace("&amp;", "&"): aLink;
        
        return aLink;
    }
    
    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
      //Find opening <text> tag, excluding potential attributes
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

    private boolean isInvalidWikiLink(String aLink) {
        int minLength = 1;
        int maxLength = 100;

        
        if( aLink.length() < minLength+2 || aLink.length() > maxLength) return true;
        char firstChar = aLink.charAt(minLength);
        
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;
        
        if( aLink.contains(":")) return true; // Matches: external links and translations links
        if( aLink.contains(",")) return true; // Matches: external links and translations links
        if((aLink.indexOf('&') > 0) && !(aLink.substring(aLink.indexOf('&')).startsWith("&amp;"))) return true;
        
        return false;
    }
}
