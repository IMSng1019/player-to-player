package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnvironmentManifestTest {

    @Test
    void globalHashIsIndependentOfInsertionOrder() {
        Map<String, EnvironmentManifest.Entry> first = new LinkedHashMap<>();
        first.put("b.txt", new EnvironmentManifest.Entry("bb", 2));
        first.put("a.txt", new EnvironmentManifest.Entry("aa", 1));

        Map<String, EnvironmentManifest.Entry> second = new LinkedHashMap<>();
        second.put("a.txt", new EnvironmentManifest.Entry("aa", 1));
        second.put("b.txt", new EnvironmentManifest.Entry("bb", 2));

        assertEquals(new EnvironmentManifest(first).globalHash(),
                new EnvironmentManifest(second).globalHash());
    }
}
