package hu.gds.jdbc.util;

import hu.gds.jdbc.annotation.Stability;

@Stability.Internal
public class CbStrings {
    private CbStrings() {
        throw new AssertionError("not instantiable");
    }

    public static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    public static String emptyToNull(String s) {
        return isNullOrEmpty(s) ? null : s;
    }

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    public static String removeStart(String s, String removeMe) {
        if (s == null || removeMe == null) {
            return s;
        }
        return s.startsWith(removeMe) ? s.substring(removeMe.length()) : s;
    }

    public static String removeEnd(String s, String removeMe) {
        if (s == null || removeMe == null) {
            return s;
        }
        return s.endsWith(removeMe) ? s.substring(0, s.length() - removeMe.length()) : s;
    }
}
