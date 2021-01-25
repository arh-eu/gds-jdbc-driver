package hu.gds.jdbc.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringEscapeUtils {
    private static final CharSequence quote = "'";
    private static final CharSequence quoteReplace = "''";
    private static final CharSequence backslash = "\\";
    private static final CharSequence backslashReplace = "\\\\";

    public StringEscapeUtils() {
    }

    public static String escape(String value) {
        return null == value ? null : value.replace(quote, quoteReplace).replace(backslash, backslashReplace);
    }

    public static String unescape(String value) {
        return null == value ? null : value.replace(quoteReplace, quote).replace(backslashReplace, backslash);
    }

    public static boolean parserShouldFail(String value) {
        Matcher matcher = Pattern.compile("(\\\\)(\\1*)").matcher(value);

        do {
            if (!matcher.find()) {
                matcher = Pattern.compile("[\\\\](')(\\1*)").matcher(value);

                do {
                    if (!matcher.find()) {
                        return false;
                    }
                } while(matcher.group().substring(1).length() % 2 != 1);

                return true;
            }
        } while(matcher.end() != value.length() || matcher.group().length() % 2 != 1);

        return true;
    }
}
