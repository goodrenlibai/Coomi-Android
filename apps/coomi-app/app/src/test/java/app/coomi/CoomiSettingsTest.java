package app.coomi;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.json.JSONObject;
import org.junit.Test;

import java.io.File;

/**
 * CoomiSettings 的 JVM 单元测试（不依赖 Android SDK / Robolectric）。
 *
 * <p>覆盖人工模式开关的：文件往返、字段合并（manual_mode 与其它字段互不覆盖）、
 * 缺失文件兜底、写入失败兜底。</p>
 */
public class CoomiSettingsTest {

    @Test
    public void manualModeRoundTripsThroughFile() throws Exception {
        File file = File.createTempFile("coomi-settings", ".json");
        try {
            JSONObject document = new JSONObject();
            document.put("manual_mode", true);
            assertTrue(CoomiSettings.writeSettings(file, document));

            JSONObject read = CoomiSettings.readSettings(file);
            assertTrue(read.optBoolean("manual_mode", false));
        } finally {
            file.delete();
        }
    }

    @Test
    public void writeMergesAndPreservesOtherFields() throws Exception {
        File file = File.createTempFile("coomi-settings-merge", ".json");
        try {
            JSONObject document = new JSONObject();
            document.put("manual_mode", true);
            assertTrue(CoomiSettings.writeSettings(file, document));

            // 模拟引擎/其它设置的合并写回：加一个字段，manual_mode 不应被覆盖。
            JSONObject merged = CoomiSettings.readSettings(file);
            merged.put("global_memory", true);
            assertTrue(CoomiSettings.writeSettings(file, merged));

            JSONObject read = CoomiSettings.readSettings(file);
            assertTrue(read.optBoolean("manual_mode", false));
            assertTrue(read.optBoolean("global_memory", false));
        } finally {
            file.delete();
        }
    }

    @Test
    public void missingFileReadsAsEmptyAndDisabled() {
        JSONObject read = CoomiSettings.readSettings(new File("/nonexistent/coomi/settings.json"));
        assertFalse(read.optBoolean("manual_mode", false));
    }

    @Test
    public void writeToInvalidParentFailsGracefully() throws Exception {
        // 父路径是一个「文件」而非目录：mkdirs 必然失败，应返回 false 而非抛异常。
        File parentAsFile = File.createTempFile("coomi-parent-file", ".txt");
        try {
            File child = new File(parentAsFile, "settings.json");
            assertFalse(CoomiSettings.writeSettings(child, new JSONObject()));
        } finally {
            parentAsFile.delete();
        }
    }
}
