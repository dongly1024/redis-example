package top.keir.redis;

import org.junit.jupiter.api.Test;
import org.springframework.util.AntPathMatcher;

public class AntPathTests {

    AntPathMatcher matcher = new AntPathMatcher();

    @Test
    void test() {
        System.out.println(matcher.match("/com/**/test.js", "/com/a/v/d/test.js"));
        System.out.println(matcher.match("/com/**/test.js", "/com/a/test.js"));
        System.out.println(matcher.match("/com/**/test.js", "/com/test.js"));
        System.out.println(matcher.match("/com/**/test.js", "/com/a/test.js"));
        System.out.println(matcher.match("/**/*.js", "/com/a/test.js"));
    }


}
