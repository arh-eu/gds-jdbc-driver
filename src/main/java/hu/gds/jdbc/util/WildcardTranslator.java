package hu.gds.jdbc.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WildcardTranslator {
    private final static String WILDCARD_CHARACTERS = "%_";

    public static String convertPatternToRegex(String pattern, Character escape) {
        Pattern converterPattern;
        if (null == escape) {
            converterPattern = Pattern.compile("[" + WILDCARD_CHARACTERS + "]|[^" + WILDCARD_CHARACTERS + "]+");
        } else {
            if ('%' == escape) {
                throw new IllegalArgumentException("the any (%) wildcard and the escape character are the same");
            }
            if ('_' == escape) {
                throw new IllegalArgumentException("the one (_) wildcard and the escape character are the same");
            }
            converterPattern = Pattern.compile("(?<!" + Pattern.quote(String.valueOf(escape)) + ")[" + WILDCARD_CHARACTERS + "]" +
                    "|((?<=" + Pattern.quote(String.valueOf(escape)) +")[" + WILDCARD_CHARACTERS + "]|[^" + WILDCARD_CHARACTERS + "])+");
        }
        Matcher matcher = converterPattern.matcher(pattern);
        StringBuilder convertedRegex = new StringBuilder();
        while (matcher.find()) {
            String part = pattern.substring(matcher.start(), matcher.end());
            if (part.equals("%")) {
                convertedRegex.append(".*");
            } else if (part.equals("_")) {
                convertedRegex.append(".");
            } else {
                convertedRegex.append(Pattern.quote(part));
            }
        }
        return convertedRegex.toString();
    }
}
