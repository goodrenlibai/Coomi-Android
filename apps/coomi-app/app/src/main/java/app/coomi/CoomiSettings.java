package app.coomi;

import com.termux.shared.logger.Logger;

import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * 读写引擎侧 ~/.coomi/config/settings.json 的轻量助手。
 *
 * <p>引擎的 settings.json 是「合并写回」风格（global_memory / custom_prompt / manual_mode
 * 等字段互不覆盖），Android 侧在引导阶段（引擎尚未启动时）需要写入 manual_mode 开关，
 * 因此这里同样采用读-改-写合并，只更新 manual_mode 字段，保留其余既有字段。</p>
 *
 * <p>底层 {@link #readSettings(File)} / {@link #writeSettings(File, JSONObject)} 刻意做成
 * 纯文件 IO（不依赖 android.*，失败时不打日志），便于在 JVM 单元测试中直接覆盖。</p>
 */
public final class CoomiSettings {

    private static final String LOG_TAG = "CoomiSettings";

    /** ~/.coomi/config/settings.json */
    private static final String SETTINGS_FILE = CoomiConstants.COOMI_CONFIG_DIR + "/config/settings.json";
    private static final String KEY_MANUAL_MODE = "manual_mode";

    private CoomiSettings() { }

    public static File settingsFile() {
        return new File(SETTINGS_FILE);
    }

    // ── 纯文件 IO（JVM 可测，不依赖 android.*） ──

    /** 读取指定文件；不存在或损坏时返回空对象（不打日志，静默兜底）。 */
    static JSONObject readSettings(File file) {
        if (file == null || !file.isFile()) {
            return new JSONObject();
        }
        try (FileReader reader = new FileReader(file)) {
            StringBuilder json = new StringBuilder();
            char[] buffer = new char[2048];
            int count;
            while ((count = reader.read(buffer)) != -1) {
                json.append(buffer, 0, count);
            }
            return new JSONObject(json.toString());
        } catch (Exception ignored) {
            return new JSONObject();
        }
    }

    /** 把整个文档写入指定文件；失败返回 false（不打日志，静默兜底）。 */
    static boolean writeSettings(File file, JSONObject document) {
        if (file == null || document == null) {
            return false;
        }
        File parent = file.getParentFile();
        if (parent == null || (!parent.isDirectory() && !parent.mkdirs())) {
            return false;
        }
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(document.toString(2));
            file.setReadable(false, false);
            file.setReadable(true, true);
            file.setWritable(false, false);
            file.setWritable(true, true);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    // ── 对外接口（带日志） ──

    /** 读取 settings.json 全文；文件不存在或损坏时返回空对象。 */
    public static JSONObject readSettings() {
        JSONObject document = readSettings(settingsFile());
        if (document.length() == 0 && settingsFile().isFile()) {
            Logger.logError(LOG_TAG, "settings.json is unreadable or corrupt");
        }
        return document;
    }

    /** 合并写回 settings.json（只更新传入字段，保留其余字段）。 */
    public static boolean writeSettings(JSONObject document) {
        boolean ok = writeSettings(settingsFile(), document);
        if (!ok) {
            Logger.logError(LOG_TAG, "Cannot write settings.json");
        }
        return ok;
    }

    /** 是否已开启人工模式（默认关闭）。 */
    public static boolean isManualMode() {
        return readSettings().optBoolean(KEY_MANUAL_MODE, false);
    }

    /** 开启 / 关闭人工模式。 */
    public static boolean setManualMode(boolean enabled) {
        JSONObject document = readSettings();
        try {
            document.put(KEY_MANUAL_MODE, enabled);
        } catch (Exception e) {
            Logger.logError(LOG_TAG, "Cannot update manual_mode: " + e.getMessage());
            return false;
        }
        return writeSettings(document);
    }

    /** 供应商是否已完整配置（复用 CoomiConfig 的判定，便于引导页复用同一口径）。 */
    public static boolean isProviderConfigured() {
        return CoomiConfig.isConfigured();
    }

    /** 供引导页判断「无 API 但可用」：人工模式开启即视为可用。 */
    public static boolean isUsable() {
        return isManualMode() || CoomiConfig.isConfigured();
    }
}
