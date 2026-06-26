package app.crosswatch.companion;

import android.app.Activity;
import android.content.ActivityNotFoundException;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.graphics.Color;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.Path;
import android.graphics.RectF;
import android.graphics.LinearGradient;
import android.graphics.Shader;
import android.graphics.Typeface;
import android.graphics.drawable.GradientDrawable;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.provider.MediaStore;
import android.view.Gravity;
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.widget.Button;
import android.widget.EditText;
import android.widget.FrameLayout;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainActivity extends Activity {
    private static final int REQ_SCAN_QR = 3701;
    private static final int BG = 0xFF080B12;
    private static final int SURFACE = 0xFF101621;
    private static final int SURFACE_2 = 0xFF151C28;
    private static final int ELEVATED = 0xFF1C2432;
    private static final int INK = 0xFFF6F8FF;
    private static final int MUTED = 0xFFAAB4C5;
    private static final int SOFT = 0xFF77849A;
    private static final int LINE = 0xFF2A3444;
    private static final int MINT = 0xFF35D3A7;
    private static final int CYAN = 0xFF57C7FF;
    private static final int BLUE = 0xFF5D7CFF;
    private static final int ROSE = 0xFFFF6B8A;
    private static final int GOLD = 0xFFFFC857;

    private final Handler main = new Handler(Looper.getMainLooper());
    private final ExecutorService io = Executors.newSingleThreadExecutor();
    private SharedPreferences prefs;
    private FrameLayout content;
    private LinearLayout root;
    private Summary summary;
    private String selected = "Dashboard";
    private String serverUrl = "http://10.0.2.2:8787";
    private String mobileToken = "";
    private String status = "Ready";
    private int statusInset = 0;
    private int navInset = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Window w = getWindow();
        w.setStatusBarColor(BG);
        w.setNavigationBarColor(BG);
        statusInset = systemBar("status_bar_height");
        navInset = systemBar("navigation_bar_height");
        prefs = getSharedPreferences("crosswatch-companion", Context.MODE_PRIVATE);
        serverUrl = prefs.getString("server_url", serverUrl);
        mobileToken = prefs.getString("mobile_token", "");
        summary = Summary.sample(serverUrl);
        buildShell();
        if (!handlePairingIntent(getIntent())) refresh();
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        if (!handlePairingIntent(intent)) refresh();
    }

    @Override
    protected void onDestroy() {
        io.shutdownNow();
        super.onDestroy();
    }

    private boolean isTablet() {
        return getResources().getConfiguration().screenWidthDp >= 700;
    }

    private void buildShell() {
        root = new LinearLayout(this);
        root.setOrientation(isTablet() ? LinearLayout.HORIZONTAL : LinearLayout.VERTICAL);
        root.setPadding(0, statusInset, 0, 0);
        root.setBackground(gradient(new int[]{BG, 0xFF0A101A, BG}, 0));

        if (isTablet()) {
            root.addView(navRail(), new LinearLayout.LayoutParams(dp(116), ViewGroup.LayoutParams.MATCH_PARENT));
        }

        LinearLayout mainColumn = new LinearLayout(this);
        mainColumn.setOrientation(LinearLayout.VERTICAL);
        mainColumn.addView(topBar(), new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(isTablet() ? 96 : 88)));
        content = new FrameLayout(this);
        mainColumn.addView(content, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f));

        if (isTablet()) {
            root.addView(mainColumn, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.MATCH_PARENT, 1f));
        } else {
            root.addView(mainColumn, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f));
            root.addView(bottomNav(), new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(76) + navInset));
        }

        setContentView(root);
        renderContent();
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode != REQ_SCAN_QR) return;
        if (resultCode != RESULT_OK || data == null) {
            String err = data == null ? "" : data.getStringExtra("SCAN_ERROR");
            if (err != null && !err.trim().isEmpty()) {
                status = "Scanner error: " + err;
                rebuild();
            }
            return;
        }
        String scanned = data.getStringExtra("SCAN_RESULT");
        if (scanned == null || scanned.trim().isEmpty()) scanned = data.getDataString();
        if (scanned == null || scanned.trim().isEmpty()) {
            status = "No QR result returned";
            rebuild();
            return;
        }
        status = "QR decoded";
        rebuild();
        claimPairing(scanned);
    }

    private View topBar() {
        LinearLayout bar = row();
        bar.setGravity(Gravity.CENTER_VERTICAL);
        bar.setPadding(dp(isTablet() ? 28 : 18), dp(12), dp(isTablet() ? 28 : 18), dp(10));
        bar.setBackgroundColor(BG);

        ImageView icon = new ImageView(this);
        icon.setImageResource(R.drawable.crosswatch_icon);
        icon.setScaleType(ImageView.ScaleType.CENTER_CROP);
        icon.setBackground(round(0xFF162131, 18, 0));
        icon.setPadding(dp(4), dp(4), dp(4), dp(4));
        bar.addView(icon, new LinearLayout.LayoutParams(dp(54), dp(54)));

        LinearLayout titleBlock = col();
        titleBlock.setPadding(dp(12), 0, dp(12), 0);
        ImageView wordmark = new ImageView(this);
        wordmark.setImageResource(R.drawable.crosswatch_wordmark);
        wordmark.setAdjustViewBounds(true);
        wordmark.setScaleType(ImageView.ScaleType.FIT_START);
        titleBlock.addView(wordmark, new LinearLayout.LayoutParams(isTablet() ? dp(220) : dp(174), dp(34)));
        titleBlock.addView(label(statusLine(), 12, Typeface.BOLD, statusColor()));
        bar.addView(titleBlock, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));

        Button refresh = button("Refresh", MINT, BG);
        refresh.setOnClickListener(v -> refresh());
        bar.addView(refresh, new LinearLayout.LayoutParams(dp(isTablet() ? 132 : 112), dp(50)));
        return bar;
    }

    private View bottomNav() {
        LinearLayout wrap = col();
        wrap.setPadding(dp(10), dp(6), dp(10), navInset + dp(8));
        wrap.setBackgroundColor(BG);
        LinearLayout nav = row();
        nav.setGravity(Gravity.CENTER);
        nav.setPadding(dp(6), dp(6), dp(6), dp(6));
        nav.setBackground(round(0xF0111822, 22, 0x242F3D));
        for (String item : sections()) {
            nav.addView(navButton(item, false), new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.MATCH_PARENT, 1f));
        }
        wrap.addView(nav, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(62)));
        return wrap;
    }

    private View navRail() {
        LinearLayout rail = col();
        rail.setPadding(dp(12), dp(18), dp(12), navInset + dp(14));
        rail.setGravity(Gravity.CENTER_HORIZONTAL);
        rail.setBackgroundColor(0xFF0B1018);
        ImageView icon = new ImageView(this);
        icon.setImageResource(R.drawable.crosswatch_icon);
        icon.setPadding(dp(6), dp(6), dp(6), dp(6));
        rail.addView(icon, new LinearLayout.LayoutParams(dp(58), dp(58)));
        spacer(rail, 18);
        for (String item : sections()) {
            LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(72));
            lp.bottomMargin = dp(8);
            rail.addView(navButton(item, true), lp);
        }
        return rail;
    }

    private View navButton(String item, boolean rail) {
        boolean active = item.equals(selected);
        LinearLayout b = col();
        b.setGravity(Gravity.CENTER);
        b.setPadding(dp(4), dp(4), dp(4), dp(4));
        b.setBackground(active ? gradient(new int[]{MINT, CYAN}, rail ? 18 : 17) : round(Color.TRANSPARENT, 16, 0));
        b.setClickable(true);
        b.setOnClickListener(v -> {
            selected = item;
            buildShell();
        });

        TextView marker = label(navMarker(item), rail ? 16 : 14, Typeface.BOLD, active ? BG : SOFT);
        marker.setGravity(Gravity.CENTER);
        b.addView(marker, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        TextView text = label(item, rail ? 11 : 10, Typeface.BOLD, active ? BG : SOFT);
        text.setGravity(Gravity.CENTER);
        text.setSingleLine(true);
        b.addView(text, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        return b;
    }

    private void renderContent() {
        if (content == null) return;
        content.removeAllViews();
        ScrollView scroll = new ScrollView(this);
        scroll.setFillViewport(false);
        LinearLayout page = col();
        int horizontal = isTablet() ? dp(32) : dp(18);
        page.setPadding(horizontal, dp(8), horizontal, dp(isTablet() ? 32 : 18));
        scroll.addView(page, new ScrollView.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));

        if ("Dashboard".equals(selected)) dashboard(page);
        else if ("Activity".equals(selected)) activity(page);
        else if ("Library".equals(selected)) library(page);
        else if ("Tools".equals(selected)) tools(page);
        else settings(page);

        content.addView(scroll, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
    }

    private void dashboard(LinearLayout page) {
        heroHeader(page);
        metricGrid(page, Arrays.asList(
                pair("Sync", summary.syncRunning ? "Running" : "Idle"),
                pair("Scheduler", summary.scheduler),
                pair("Next run", summary.nextRun),
                pair("Warnings", String.valueOf(summary.warnings))
        ), isTablet() ? 4 : 2);
        section(page, "Now");
        nowCard(page);
        section(page, "Providers");
        providerGrid(page, summary.providers, isTablet() ? 3 : 2);
    }

    private void heroHeader(LinearLayout page) {
        LinearLayout hero = col();
        hero.setPadding(dp(18), dp(18), dp(18), dp(18));
        hero.setBackground(gradient(new int[]{0xFF192338, 0xFF123B45, 0xFF13202F}, 24));

        LinearLayout top = row();
        top.setGravity(Gravity.CENTER_VERTICAL);
        LinearLayout copy = col();
        copy.addView(label(summary.demo ? "Companion preview" : "Connected companion", 12, Typeface.BOLD, summary.demo ? GOLD : MINT));
        TextView title = label(summary.serverName, isTablet() ? 30 : 25, Typeface.BOLD, INK);
        title.setSingleLine(false);
        copy.addView(title);
        copy.addView(label(summary.serverUrl, 12, Typeface.NORMAL, 0xFFD2D9E8));
        top.addView(copy, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));

        ImageView icon = new ImageView(this);
        icon.setImageResource(R.drawable.crosswatch_icon);
        icon.setAlpha(0.92f);
        top.addView(icon, new LinearLayout.LayoutParams(dp(isTablet() ? 84 : 68), dp(isTablet() ? 84 : 68)));
        hero.addView(top);

        LinearLayout chips = row();
        chips.setGravity(Gravity.CENTER_VERTICAL);
        chips.addView(chip(hasMobileToken() ? "Paired" : "Pairing needed", hasMobileToken() ? MINT : GOLD, 0x22182410));
        chips.addView(chip(summary.version.isEmpty() ? "Version unknown" : summary.version, CYAN, 0x1F123242));
        LinearLayout.LayoutParams cp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        cp.topMargin = dp(14);
        hero.addView(chips, cp);

        page.addView(hero, blockParams(16));
        if (summary.demo) notice(page, "No live connection yet", "Pair the app from CrossWatch Security, then refresh to show live server data.", GOLD);
    }

    private void nowCard(LinearLayout page) {
        LinearLayout c = card();
        c.setPadding(dp(18), dp(18), dp(18), dp(18));
        c.setBackground(gradient(new int[]{0xFF1A2432, 0xFF142532}, 20));
        c.addView(label("Currently watching", 12, Typeface.BOLD, MUTED));
        TextView value = label(summary.currentlyWatching, 25, Typeface.BOLD, INK);
        value.setSingleLine(false);
        LinearLayout.LayoutParams vlp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        vlp.topMargin = dp(8);
        c.addView(value, vlp);
        c.addView(label(summary.syncRunning ? "Sync is active" : "Quiet right now", 13, Typeface.NORMAL, SOFT));
        page.addView(c, blockParams(18));
    }

    private void activity(LinearLayout page) {
        pageIntro(page, "Recent activity", "Fast read-only view of CrossWatch events.");
        for (ActivityItem item : summary.activity) {
            LinearLayout c = card();
            c.setOrientation(LinearLayout.HORIZONTAL);
            c.setGravity(Gravity.CENTER_VERTICAL);
            TextView badge = label(levelShort(item.level), 11, Typeface.BOLD, levelColor(item.level));
            badge.setGravity(Gravity.CENTER);
            badge.setBackground(round(tint(levelColor(item.level), 0.16f), 14, tint(levelColor(item.level), 0.32f)));
            c.addView(badge, new LinearLayout.LayoutParams(dp(46), dp(46)));
            LinearLayout txt = col();
            txt.setPadding(dp(14), 0, dp(10), 0);
            txt.addView(label(item.title, 16, Typeface.BOLD, INK));
            txt.addView(label(item.detail, 13, Typeface.NORMAL, MUTED));
            c.addView(txt, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));
            c.addView(label(item.time, 12, Typeface.BOLD, SOFT));
            page.addView(c, blockParams(12));
        }
    }

    private void library(LinearLayout page) {
        pageIntro(page, "Library", "A compact companion overview. Detailed edits stay in the web UI.");
        List<Pair> items = new ArrayList<>();
        for (LibraryItem item : summary.library) items.add(pair(item.title, item.value));
        metricGrid(page, items, isTablet() ? 3 : 1);
        for (LibraryItem item : summary.library) {
            LinearLayout c = card();
            c.addView(label(item.title, 13, Typeface.BOLD, MUTED));
            TextView value = label(item.value, 22, Typeface.BOLD, INK);
            LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
            lp.topMargin = dp(8);
            c.addView(value, lp);
            c.addView(label(item.detail, 13, Typeface.NORMAL, SOFT));
            page.addView(c, blockParams(12));
        }
    }

    private void tools(LinearLayout page) {
        pageIntro(page, "Safe tools", "Small companion actions only. Sync pairs and provider auth remain in CrossWatch.");
        tool(page, "Run sync", "Trigger the configured CrossWatch sync run.", "RUN", "/api/mobile/actions/run", MINT);
        tool(page, "Create backup", "Ask CrossWatch to create an app-state backup.", "BAK", "/api/mobile/actions/backup", CYAN);
        tool(page, "Stop watcher", "Stop the watcher for a quiet diagnostics baseline.", "STP", "/api/mobile/actions/watch/stop", ROSE);
        notice(page, "Scope boundary", "This companion app will not take over sync-pair setup or deep provider configuration.", BLUE);
    }

    private void settings(LinearLayout page) {
        pageIntro(page, "Settings", "Pair this device and choose the CrossWatch server.");
        LinearLayout c = card();
        c.addView(label("Server URL", 12, Typeface.BOLD, MUTED));
        EditText input = field(serverUrl, "https://crosswatch.example.com");
        LinearLayout.LayoutParams inputLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(54));
        inputLp.topMargin = dp(8);
        c.addView(input, inputLp);
        Button save = button("Save and refresh", MINT, BG);
        save.setOnClickListener(v -> {
            serverUrl = input.getText().toString().trim().replaceAll("/+$", "");
            prefs.edit().putString("server_url", serverUrl).apply();
            refresh();
        });
        LinearLayout.LayoutParams saveLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(52));
        saveLp.topMargin = dp(12);
        c.addView(save, saveLp);
        c.addView(label(status, 13, Typeface.NORMAL, statusColor()));
        page.addView(c, blockParams(14));

        LinearLayout pair = card();
        pair.addView(label("Mobile pairing", 12, Typeface.BOLD, MUTED));
        pair.addView(label(hasMobileToken() ? "This phone or tablet is paired." : "Scan the QR from CrossWatch Security, or paste the code/URI here.", 14, Typeface.NORMAL, hasMobileToken() ? MINT : SOFT));
        EditText code = field("", "Pairing code or crosswatch://pair URI");
        LinearLayout.LayoutParams codeLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(54));
        codeLp.topMargin = dp(12);
        pair.addView(code, codeLp);
        LinearLayout actions = isTablet() ? row() : col();
        actions.setGravity(Gravity.CENTER_VERTICAL);
        Button scan = button("Scan QR", CYAN, BG);
        scan.setOnClickListener(v -> startQrScan());
        Button claim = button("Pair pasted code", MINT, BG);
        claim.setOnClickListener(v -> claimPairing(code.getText().toString()));
        Button forget = button("Forget token", ELEVATED, MUTED);
        forget.setOnClickListener(v -> {
            mobileToken = "";
            prefs.edit().remove("mobile_token").apply();
            status = "Mobile token removed";
            rebuild();
        });
        if (isTablet()) {
            actions.addView(scan, new LinearLayout.LayoutParams(0, dp(52), 1f));
            LinearLayout.LayoutParams clp = new LinearLayout.LayoutParams(0, dp(52), 1f);
            clp.leftMargin = dp(10);
            actions.addView(claim, clp);
            LinearLayout.LayoutParams flp = new LinearLayout.LayoutParams(0, dp(52), 1f);
            flp.leftMargin = dp(10);
            actions.addView(forget, flp);
        } else {
            actions.addView(scan, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(52)));
            LinearLayout.LayoutParams clp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(52));
            clp.topMargin = dp(10);
            actions.addView(claim, clp);
            LinearLayout.LayoutParams flp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(52));
            flp.topMargin = dp(10);
            actions.addView(forget, flp);
        }
        LinearLayout.LayoutParams actionsLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        actionsLp.topMargin = dp(12);
        pair.addView(actions, actionsLp);
        page.addView(pair, blockParams(14));
    }

    private void startQrScan() {
        startActivityForResult(new Intent(this, QrScanActivity.class), REQ_SCAN_QR);
    }

    private void metricGrid(LinearLayout page, List<Pair> items, int columns) {
        for (int i = 0; i < items.size(); i += columns) {
            LinearLayout r = row();
            for (int j = 0; j < columns; j++) {
                int idx = i + j;
                View child = idx < items.size() ? metric(items.get(idx).a, items.get(idx).b) : new View(this);
                LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(0, dp(112), 1f);
                if (j > 0) lp.leftMargin = dp(10);
                r.addView(child, lp);
            }
            page.addView(r, blockParams(10));
        }
    }

    private View metric(String label, String value) {
        LinearLayout c = card();
        c.setGravity(Gravity.CENTER_VERTICAL);
        c.addView(label(label, 12, Typeface.BOLD, MUTED));
        TextView val = label(value, 20, Typeface.BOLD, INK);
        val.setSingleLine(false);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        lp.topMargin = dp(10);
        c.addView(val, lp);
        return c;
    }

    private void providerGrid(LinearLayout page, List<Provider> providers, int columns) {
        for (int i = 0; i < providers.size(); i += columns) {
            LinearLayout r = row();
            for (int j = 0; j < columns; j++) {
                int idx = i + j;
                View child = idx < providers.size() ? provider(providers.get(idx)) : new View(this);
                LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(0, dp(isTablet() ? 150 : 154), 1f);
                if (j > 0) lp.leftMargin = dp(10);
                r.addView(child, lp);
            }
            page.addView(r, blockParams(10));
        }
    }

    private View provider(Provider item) {
        return new ProviderTileView(this, item);
    }

    private void tool(LinearLayout page, String title, String detail, String marker, String path, int accent) {
        LinearLayout c = card();
        c.setOrientation(isTablet() ? LinearLayout.HORIZONTAL : LinearLayout.VERTICAL);
        c.setGravity(Gravity.CENTER_VERTICAL);
        LinearLayout line = row();
        line.setGravity(Gravity.CENTER_VERTICAL);
        TextView icon = label(marker, 12, Typeface.BOLD, accent);
        icon.setGravity(Gravity.CENTER);
        icon.setBackground(round(tint(accent, 0.15f), 16, tint(accent, 0.32f)));
        line.addView(icon, new LinearLayout.LayoutParams(dp(50), dp(50)));
        LinearLayout texts = col();
        texts.setPadding(dp(14), 0, dp(12), 0);
        texts.addView(label(title, 17, Typeface.BOLD, INK));
        texts.addView(label(detail, 13, Typeface.NORMAL, MUTED));
        line.addView(texts, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));
        c.addView(line, new LinearLayout.LayoutParams(isTablet() ? 0 : ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));
        Button send = button("Send", accent, BG);
        send.setOnClickListener(v -> action(path));
        LinearLayout.LayoutParams sendLp = new LinearLayout.LayoutParams(isTablet() ? dp(112) : ViewGroup.LayoutParams.MATCH_PARENT, dp(50));
        if (!isTablet()) sendLp.topMargin = dp(14);
        c.addView(send, sendLp);
        page.addView(c, blockParams(12));
    }

    private void pageIntro(LinearLayout page, String title, String subtitle) {
        TextView h = label(title, 28, Typeface.BOLD, INK);
        h.setSingleLine(false);
        page.addView(h, blockParams(2));
        page.addView(label(subtitle, 14, Typeface.NORMAL, MUTED), blockParams(18));
    }

    private void section(LinearLayout page, String text) {
        TextView t = label(text, 18, Typeface.BOLD, INK);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        lp.topMargin = dp(8);
        lp.bottomMargin = dp(10);
        page.addView(t, lp);
    }

    private void notice(LinearLayout page, String title, String text, int accent) {
        LinearLayout c = card();
        c.setBackground(round(tint(accent, 0.12f), 18, tint(accent, 0.32f)));
        c.addView(label(title, 15, Typeface.BOLD, INK));
        c.addView(label(text, 13, Typeface.NORMAL, MUTED));
        page.addView(c, blockParams(14));
    }

    private TextView chip(String text, int accent, int bg) {
        TextView chip = label(text, 12, Typeface.BOLD, accent);
        chip.setPadding(dp(10), dp(7), dp(10), dp(7));
        chip.setBackground(round(bg, 18, tint(accent, 0.28f)));
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        lp.rightMargin = dp(8);
        chip.setLayoutParams(lp);
        return chip;
    }

    private void refresh() {
        status = "Refreshing";
        rebuild();
        io.execute(() -> {
            Summary next = fetchSummary(serverUrl);
            main.post(() -> {
                summary = next;
                status = next.demo ? "Preview data" : "Connected";
                rebuild();
            });
        });
    }

    private void action(String path) {
        status = "Sending action";
        rebuild();
        io.execute(() -> {
            boolean ok = post(serverUrl + path);
            main.post(() -> {
                status = ok ? "Action sent" : "Action unavailable";
                refresh();
            });
        });
    }

    private void claimPairing(String raw) {
        final String code = pairingCodeFrom(raw);
        if (code.isEmpty()) {
            status = "Enter a pairing code first";
            rebuild();
            return;
        }
        final String firstBase = serverUrl.replaceAll("/+$", "");
        status = "Pairing device";
        rebuild();
        io.execute(() -> {
            try {
                JSONObject body = new JSONObject();
                body.put("code", code);
                body.put("device_name", "CrossWatch Android");
                String response;
                try {
                    response = postJson(firstBase + "/api/mobile/pairing/claim", body.toString(), false);
                } catch (Exception firstError) {
                    String msg = firstError.getMessage() == null ? "" : firstError.getMessage();
                    if (!firstBase.startsWith("http://") || !msg.toLowerCase(Locale.ROOT).contains("plain http request")) {
                        throw firstError;
                    }
                    String httpsBase = "https://" + firstBase.substring("http://".length());
                    response = postJson(httpsBase + "/api/mobile/pairing/claim", body.toString(), false);
                    serverUrl = httpsBase;
                    prefs.edit().putString("server_url", serverUrl).apply();
                }
                JSONObject obj = new JSONObject(response);
                String token = obj.optString("token", "");
                if (token.isEmpty()) throw new IllegalStateException("missing token");
                main.post(() -> {
                    mobileToken = token;
                    prefs.edit().putString("mobile_token", token).apply();
                    status = "Device paired";
                    refresh();
                });
            } catch (Exception err) {
                final String detail = shortError(err);
                main.post(() -> {
                    status = "Pairing failed: " + detail;
                    rebuild();
                });
            }
        });
    }

    private void rebuild() {
        if (root != null) buildShell();
    }

    private Summary fetchSummary(String base) {
        try {
            String body = get(base.replaceAll("/+$", "") + "/api/mobile/summary");
            return Summary.fromJson(new JSONObject(body), base);
        } catch (Exception ignored) {
            return Summary.sample(base);
        }
    }

    private String get(String value) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(value).openConnection();
        c.setRequestMethod("GET");
        c.setConnectTimeout(5000);
        c.setReadTimeout(5000);
        c.setRequestProperty("Accept", "application/json");
        addMobileAuth(c);
        int code = c.getResponseCode();
        if (code == 401 || code == 403) throw new SecurityException("mobile auth required");
        BufferedReader reader = new BufferedReader(new InputStreamReader(c.getInputStream()));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) sb.append(line);
        reader.close();
        return sb.toString();
    }

    private boolean post(String value) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(value).openConnection();
            c.setRequestMethod("POST");
            c.setConnectTimeout(5000);
            c.setReadTimeout(5000);
            c.setRequestProperty("Accept", "application/json");
            addMobileAuth(c);
            int code = c.getResponseCode();
            return code >= 200 && code < 300;
        } catch (Exception ignored) {
            return false;
        }
    }

    private String postJson(String value, String json, boolean includeAuth) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(value).openConnection();
        c.setRequestMethod("POST");
        c.setConnectTimeout(5000);
        c.setReadTimeout(5000);
        c.setDoOutput(true);
        c.setRequestProperty("Accept", "application/json");
        c.setRequestProperty("Content-Type", "application/json");
        if (includeAuth) addMobileAuth(c);
        OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
        writer.write(json == null ? "{}" : json);
        writer.close();
        int code = c.getResponseCode();
        if (code < 200 || code >= 300) {
            String body = readBody(c.getErrorStream());
            throw new IllegalStateException("HTTP " + code + (body.isEmpty() ? "" : ": " + body));
        }
        return readBody(c.getInputStream());
    }

    private String readBody(InputStream stream) throws Exception {
        if (stream == null) return "";
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) sb.append(line);
        reader.close();
        return sb.toString();
    }

    private String shortError(Exception err) {
        String msg = err == null || err.getMessage() == null ? "unknown error" : err.getMessage();
        msg = msg.replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").trim();
        if (msg.contains("invalid_or_expired_pairing_code")) return "code expired or already used";
        if (msg.length() > 96) return msg.substring(0, 93) + "...";
        return msg;
    }

    private void addMobileAuth(HttpURLConnection c) {
        if (hasMobileToken()) c.setRequestProperty("Authorization", "Bearer " + mobileToken);
    }

    private boolean hasMobileToken() {
        return mobileToken != null && !mobileToken.trim().isEmpty();
    }

    private String pairingCodeFrom(String raw) {
        String value = raw == null ? "" : raw.trim();
        if (value.isEmpty()) return "";
        if (value.startsWith("crosswatch://")) {
            try {
                Uri uri = Uri.parse(value);
                String server = uri.getQueryParameter("server");
                String code = uri.getQueryParameter("code");
                if (server != null && !server.trim().isEmpty()) {
                    serverUrl = server.trim().replaceAll("/+$", "");
                    prefs.edit().putString("server_url", serverUrl).apply();
                }
                return code == null ? "" : code.trim();
            } catch (Exception ignored) {
                return "";
            }
        }
        return value;
    }

    private boolean handlePairingIntent(Intent intent) {
        if (intent == null || intent.getData() == null) return false;
        String raw = intent.getData().toString();
        if (!raw.startsWith("crosswatch://pair")) return false;
        claimPairing(raw);
        return true;
    }

    private LinearLayout row() {
        LinearLayout v = new LinearLayout(this);
        v.setOrientation(LinearLayout.HORIZONTAL);
        return v;
    }

    private LinearLayout col() {
        LinearLayout v = new LinearLayout(this);
        v.setOrientation(LinearLayout.VERTICAL);
        return v;
    }

    private LinearLayout card() {
        LinearLayout c = col();
        c.setPadding(dp(16), dp(16), dp(16), dp(16));
        c.setBackground(round(SURFACE_2, 18, LINE));
        return c;
    }

    private TextView label(String text, int sp, int style, int color) {
        TextView v = new TextView(this);
        v.setText(text == null ? "" : text);
        v.setTextSize(sp);
        v.setTypeface(Typeface.DEFAULT, style);
        v.setTextColor(color);
        v.setIncludeFontPadding(true);
        return v;
    }

    private EditText field(String value, String hint) {
        EditText input = new EditText(this);
        input.setText(value == null ? "" : value);
        input.setSingleLine(true);
        input.setFocusable(true);
        input.setFocusableInTouchMode(true);
        input.setSelectAllOnFocus(false);
        input.setTextColor(INK);
        input.setHintTextColor(SOFT);
        input.setHint(hint);
        input.setTextSize(14);
        input.setPadding(dp(14), 0, dp(14), 0);
        input.setBackground(round(ELEVATED, 14, LINE));
        return input;
    }

    private Button button(String text, int bg, int fg) {
        Button b = new Button(this);
        b.setText(text);
        b.setTextColor(fg);
        b.setTextSize(14);
        b.setTypeface(Typeface.DEFAULT, Typeface.BOLD);
        b.setAllCaps(false);
        b.setMinHeight(0);
        b.setMinWidth(0);
        b.setPadding(dp(10), 0, dp(10), 0);
        b.setBackground(round(bg, 16, 0));
        return b;
    }

    private GradientDrawable round(int color, int radiusDp, int stroke) {
        GradientDrawable d = new GradientDrawable();
        d.setColor(color);
        d.setCornerRadius(dp(radiusDp));
        if (stroke != 0) d.setStroke(dp(1), stroke);
        return d;
    }

    private GradientDrawable gradient(int[] colors, int radiusDp) {
        GradientDrawable d = new GradientDrawable(GradientDrawable.Orientation.TL_BR, colors);
        d.setCornerRadius(dp(radiusDp));
        return d;
    }

    private LinearLayout.LayoutParams blockParams(int bottom) {
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        lp.bottomMargin = dp(bottom);
        return lp;
    }

    private void spacer(LinearLayout parent, int height) {
        parent.addView(new View(this), new LinearLayout.LayoutParams(1, dp(height)));
    }

    private List<String> sections() {
        return Arrays.asList("Dashboard", "Activity", "Library", "Tools", "Settings");
    }

    private String navMarker(String item) {
        if ("Dashboard".equals(item)) return "D";
        if ("Activity".equals(item)) return "A";
        if ("Library".equals(item)) return "L";
        if ("Tools".equals(item)) return "T";
        return "S";
    }

    private String initials(String name) {
        String clean = name == null ? "" : name.trim();
        if (clean.length() <= 2) return clean.toUpperCase(Locale.ROOT);
        return clean.substring(0, 2).toUpperCase(Locale.ROOT);
    }

    private String statusLine() {
        return status + " - " + (hasMobileToken() ? "paired" : "not paired");
    }

    private int statusColor() {
        String s = status == null ? "" : status.toLowerCase(Locale.ROOT);
        if (s.contains("fail") || s.contains("unavailable") || s.contains("removed")) return ROSE;
        if (s.contains("preview") || s.contains("pairing") || s.contains("enter")) return GOLD;
        if (s.contains("connected") || s.contains("paired") || s.contains("sent")) return MINT;
        return MUTED;
    }

    private int levelColor(String level) {
        String v = level == null ? "" : level.toUpperCase(Locale.ROOT);
        if (v.contains("WARN") || v.contains("FAIL")) return GOLD;
        if (v.contains("ERROR")) return ROSE;
        if (v.contains("OK")) return MINT;
        return CYAN;
    }

    private String providerKey(String name) {
        String key = name == null ? "" : name.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "");
        if (key.contains("JELLY")) return "JELLYFIN";
        if (key.contains("MDB")) return "MDBLIST";
        if (key.contains("PUBLIC")) return "PUBLICMETADB";
        if (key.contains("ANI")) return "ANILIST";
        if (key.contains("TMDB")) return "TMDB";
        if (key.contains("SIMKL")) return "SIMKL";
        if (key.contains("TRAKT")) return "TRAKT";
        if (key.contains("EMBY")) return "EMBY";
        if (key.contains("PLEX")) return "PLEX";
        return key.isEmpty() ? "CROSSWATCH" : key;
    }

    private int providerTone(String name) {
        String key = providerKey(name);
        if ("PLEX".equals(key)) return 0xFFE5A000;
        if ("SIMKL".equals(key)) return 0xFF00B8F5;
        if ("TRAKT".equals(key)) return 0xFFED1C24;
        if ("ANILIST".equals(key)) return 0xFF02A9FF;
        if ("TMDB".equals(key)) return 0xFF01B4E4;
        if ("JELLYFIN".equals(key)) return 0xFF7B61FF;
        if ("EMBY".equals(key)) return 0xFF3BB273;
        if ("MDBLIST".equals(key)) return 0xFF2D74DA;
        if ("PUBLICMETADB".equals(key)) return 0xFFF5F5F5;
        if ("TAUTULLI".equals(key)) return 0xFFF59E0B;
        return BLUE;
    }

    private int mix(int a, int b, float t) {
        return Color.rgb(
                Math.round(Color.red(a) + (Color.red(b) - Color.red(a)) * t),
                Math.round(Color.green(a) + (Color.green(b) - Color.green(a)) * t),
                Math.round(Color.blue(a) + (Color.blue(b) - Color.blue(a)) * t)
        );
    }

    private String levelShort(String level) {
        String v = level == null ? "IN" : level.toUpperCase(Locale.ROOT);
        if (v.length() < 2) return v;
        return v.substring(0, 2);
    }

    private int tint(int color, float alpha) {
        return Color.argb(Math.round(alpha * 255), Color.red(color), Color.green(color), Color.blue(color));
    }

    private float sp(int value) {
        return value * getResources().getDisplayMetrics().scaledDensity;
    }

    private int dp(int value) {
        return (int) (value * getResources().getDisplayMetrics().density + 0.5f);
    }

    private int systemBar(String name) {
        int id = getResources().getIdentifier(name, "dimen", "android");
        return id > 0 ? getResources().getDimensionPixelSize(id) : 0;
    }

    private Pair pair(String a, String b) {
        return new Pair(a, b);
    }

    private static class Pair {
        final String a;
        final String b;

        Pair(String a, String b) {
            this.a = a;
            this.b = b;
        }
    }

    private static class Provider {
        final String name;
        final String status;
        final boolean healthy;

        Provider(String name, String status, boolean healthy) {
            this.name = name;
            this.status = status;
            this.healthy = healthy;
        }
    }

    private class ProviderTileView extends View {
        private final Provider provider;
        private final Paint paint = new Paint(Paint.ANTI_ALIAS_FLAG);
        private final RectF rect = new RectF();
        private final Path clip = new Path();

        ProviderTileView(Context context, Provider provider) {
            super(context);
            this.provider = provider;
            setLayerType(View.LAYER_TYPE_SOFTWARE, null);
        }

        @Override
        protected void onDraw(Canvas canvas) {
            super.onDraw(canvas);
            float w = getWidth();
            float h = getHeight();
            float radius = dp(22);
            int tone = providerTone(provider.name);
            int keyTone = provider.healthy ? MINT : GOLD;
            rect.set(0, 0, w, h);
            clip.reset();
            clip.addRoundRect(rect, radius, radius, Path.Direction.CW);
            int save = canvas.save();
            canvas.clipPath(clip);

            paint.setShader(new LinearGradient(0, 0, w, h, new int[]{
                    mix(0xFF222D3C, tone, 0.22f),
                    mix(0xFF111923, tone, 0.12f),
                    0xFF111823
            }, null, Shader.TileMode.CLAMP));
            canvas.drawRect(rect, paint);
            paint.setShader(null);

            drawProviderPattern(canvas, w, h, tone);

            paint.setColor(tint(0xFF000000, 0.20f));
            canvas.drawRect(0, h * 0.56f, w, h, paint);

            drawStatusPill(canvas, w, keyTone);
            drawBigMetric(canvas, w, h, provider.healthy ? "2" : "0");
            drawFeatureBadge(canvas, w, h, provider.healthy ? "M 2" : "IDLE");
            drawProviderFooter(canvas, w, h, tone);

            canvas.restoreToCount(save);

            paint.setStyle(Paint.Style.STROKE);
            paint.setStrokeWidth(dp(1));
            paint.setColor(tint(0xFFFFFFFF, 0.11f));
            canvas.drawRoundRect(rect, radius, radius, paint);
            paint.setStyle(Paint.Style.FILL);
        }

        private void drawProviderPattern(Canvas canvas, float w, float h, int tone) {
            String key = providerKey(provider.name);
            paint.setStyle(Paint.Style.FILL);
            paint.setColor(tint(tone, provider.healthy ? 0.30f : 0.16f));

            Path slash = new Path();
            if ("PLEX".equals(key)) {
                slash.moveTo(w * 0.60f, -h * 0.12f);
                slash.lineTo(w * 0.88f, -h * 0.12f);
                slash.lineTo(w * 0.45f, h * 1.15f);
                slash.lineTo(w * 0.16f, h * 1.15f);
                slash.close();
                canvas.drawPath(slash, paint);
            } else if ("JELLYFIN".equals(key)) {
                slash.moveTo(w * 0.78f, -h * 0.10f);
                slash.lineTo(w * 1.10f, h * 0.10f);
                slash.lineTo(w * 0.50f, h * 0.82f);
                slash.lineTo(w * 0.18f, h * 0.62f);
                slash.close();
                canvas.drawPath(slash, paint);
                paint.setColor(tint(0xFFFFFFFF, 0.10f));
                canvas.drawPath(slash, paint);
            } else if ("TRAKT".equals(key) || "SIMKL".equals(key)) {
                paint.setStrokeWidth(dp(8));
                paint.setStyle(Paint.Style.STROKE);
                for (int i = -2; i < 7; i++) {
                    paint.setColor(tint(tone, 0.24f));
                    canvas.drawLine(i * w * 0.24f, h, i * w * 0.24f + w * 0.72f, 0, paint);
                }
                paint.setStyle(Paint.Style.FILL);
            } else if ("ANILIST".equals(key) || "MDBLIST".equals(key)) {
                paint.setColor(tint(0xFFFFFFFF, 0.10f));
                canvas.drawRoundRect(new RectF(w * 0.06f, h * 0.22f, w * 0.88f, h * 0.42f), dp(12), dp(12), paint);
                paint.setColor(tint(tone, 0.28f));
                canvas.drawRoundRect(new RectF(w * 0.16f, h * 0.52f, w * 1.05f, h * 0.72f), dp(12), dp(12), paint);
            } else {
                paint.setColor(tint(tone, 0.22f));
                canvas.drawCircle(w * 0.22f, h * 0.24f, w * 0.36f, paint);
                paint.setColor(tint(0xFFFFFFFF, 0.08f));
                canvas.drawCircle(w * 0.92f, h * 0.02f, w * 0.36f, paint);
            }

            paint.setTextAlign(Paint.Align.CENTER);
            paint.setTypeface(Typeface.create(Typeface.DEFAULT, Typeface.BOLD));
            paint.setTextSize(sp(104));
            paint.setColor(tint(0xFFFFFFFF, 0.08f));
            canvas.drawText(initials(provider.name), w * 0.50f, h * 0.78f, paint);
        }

        private void drawStatusPill(Canvas canvas, float w, int accent) {
            String text = provider.healthy ? "LIVE" : "IDLE";
            paint.setTextSize(sp(12));
            paint.setTypeface(Typeface.create(Typeface.DEFAULT, Typeface.BOLD));
            float textWidth = paint.measureText(text);
            float pillW = textWidth + dp(38);
            RectF pill = new RectF(dp(12), dp(12), dp(12) + pillW, dp(40));
            paint.setColor(0xAA202938);
            canvas.drawRoundRect(pill, dp(14), dp(14), paint);
            paint.setColor(accent);
            canvas.drawCircle(pill.left + dp(14), pill.centerY(), dp(5), paint);
            paint.setTextAlign(Paint.Align.LEFT);
            paint.setColor(INK);
            canvas.drawText(text, pill.left + dp(25), pill.top + dp(19), paint);
        }

        private void drawBigMetric(Canvas canvas, float w, float h, String value) {
            paint.setTextAlign(Paint.Align.CENTER);
            paint.setTypeface(Typeface.create(Typeface.DEFAULT, Typeface.BOLD));
            paint.setTextSize(sp(52));
            paint.setColor(tint(0xFF000000, 0.36f));
            canvas.drawText(value, w * 0.50f + dp(2), h * 0.62f + dp(3), paint);
            paint.setColor(0xDDE9EEF7);
            canvas.drawText(value, w * 0.50f, h * 0.62f, paint);
        }

        private void drawFeatureBadge(Canvas canvas, float w, float h, String value) {
            paint.setTypeface(Typeface.create(Typeface.DEFAULT, Typeface.BOLD));
            paint.setTextSize(sp(12));
            float bw = Math.max(dp(44), paint.measureText(value) + dp(18));
            RectF badge = new RectF((w - bw) / 2f, h - dp(44), (w + bw) / 2f, h - dp(20));
            paint.setColor(0xAA283241);
            canvas.drawRoundRect(badge, dp(12), dp(12), paint);
            paint.setTextAlign(Paint.Align.CENTER);
            paint.setColor(INK);
            canvas.drawText(value, badge.centerX(), badge.top + dp(17), paint);
        }

        private void drawProviderFooter(Canvas canvas, float w, float h, int tone) {
            paint.setTextAlign(Paint.Align.LEFT);
            paint.setTypeface(Typeface.create(Typeface.DEFAULT, Typeface.BOLD));
            paint.setTextSize(sp(13));
            paint.setColor(INK);
            canvas.drawText(provider.name, dp(14), h - dp(14), paint);
            paint.setTextAlign(Paint.Align.RIGHT);
            paint.setTextSize(sp(11));
            paint.setColor(provider.healthy ? MINT : GOLD);
            canvas.drawText(provider.status, w - dp(14), h - dp(14), paint);
        }
    }

    private static class ActivityItem {
        final String title;
        final String detail;
        final String time;
        final String level;

        ActivityItem(String title, String detail, String time, String level) {
            this.title = title;
            this.detail = detail;
            this.time = time;
            this.level = level;
        }
    }

    private static class LibraryItem {
        final String title;
        final String value;
        final String detail;

        LibraryItem(String title, String value, String detail) {
            this.title = title;
            this.value = value;
            this.detail = detail;
        }
    }

    private static class Summary {
        final String serverName;
        final String serverUrl;
        final String version;
        final boolean syncRunning;
        final String scheduler;
        final String nextRun;
        final String currentlyWatching;
        final int warnings;
        final List<Provider> providers;
        final List<ActivityItem> activity;
        final List<LibraryItem> library;
        final boolean demo;

        Summary(String serverName, String serverUrl, String version, boolean syncRunning, String scheduler, String nextRun, String currentlyWatching, int warnings, List<Provider> providers, List<ActivityItem> activity, List<LibraryItem> library, boolean demo) {
            this.serverName = serverName;
            this.serverUrl = serverUrl;
            this.version = version;
            this.syncRunning = syncRunning;
            this.scheduler = scheduler;
            this.nextRun = nextRun;
            this.currentlyWatching = currentlyWatching;
            this.warnings = warnings;
            this.providers = providers;
            this.activity = activity;
            this.library = library;
            this.demo = demo;
        }

        static Summary sample(String baseUrl) {
            return new Summary(
                    "CrossWatch",
                    baseUrl == null || baseUrl.isEmpty() ? "http://10.0.2.2:8787" : baseUrl,
                    "Companion preview",
                    false,
                    "Ready",
                    "Not connected",
                    "Nothing playing",
                    1,
                    Arrays.asList(
                            new Provider("Plex", "Connected", true),
                            new Provider("Jellyfin", "Connected", true),
                            new Provider("Trakt", "Needs attention", false),
                            new Provider("SIMKL", "Connected", true),
                            new Provider("AniList", "Idle", true),
                            new Provider("MDBList", "Idle", true)
                    ),
                    Arrays.asList(
                            new ActivityItem("Sync route grouped", "Plex -> Trakt history completed", "2 min ago", "OK"),
                            new ActivityItem("Watcher event", "Living Room session observed", "12 min ago", "INFO"),
                            new ActivityItem("Provider warning", "Trakt token should be checked", "31 min ago", "WARN")
                    ),
                    Arrays.asList(
                            new LibraryItem("Unified watchlist", "128 items", "Across connected providers"),
                            new LibraryItem("Playback progress", "14 unfinished", "Plex, Emby, Jellyfin, PublicMetaDB"),
                            new LibraryItem("Recent activity", "42 events", "Grouped mobile view")
                    ),
                    true
            );
        }

        static Summary fromJson(JSONObject obj, String baseUrl) {
            Summary fallback = sample(baseUrl);
            return new Summary(
                    obj.optString("server_name", "CrossWatch"),
                    baseUrl,
                    obj.optString("version", ""),
                    obj.optBoolean("sync_running", false),
                    obj.optString("scheduler", "Unknown"),
                    obj.optString("next_run", "Not scheduled"),
                    obj.optString("currently_watching", "Nothing playing"),
                    obj.optInt("warnings", 0),
                    providers(obj.optJSONArray("providers"), fallback.providers),
                    activity(obj.optJSONArray("activity"), fallback.activity),
                    library(obj.optJSONArray("library"), fallback.library),
                    false
            );
        }

        private static List<Provider> providers(JSONArray arr, List<Provider> fallback) {
            if (arr == null || arr.length() == 0) return fallback;
            List<Provider> out = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.optJSONObject(i);
                if (o != null) out.add(new Provider(o.optString("name", "Provider"), o.optString("status", "Unknown"), o.optBoolean("healthy", false)));
            }
            return out;
        }

        private static List<ActivityItem> activity(JSONArray arr, List<ActivityItem> fallback) {
            if (arr == null || arr.length() == 0) return fallback;
            List<ActivityItem> out = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.optJSONObject(i);
                if (o != null) out.add(new ActivityItem(o.optString("title", "Activity"), o.optString("detail", ""), o.optString("time", ""), o.optString("level", "INFO")));
            }
            return out;
        }

        private static List<LibraryItem> library(JSONArray arr, List<LibraryItem> fallback) {
            if (arr == null || arr.length() == 0) return fallback;
            List<LibraryItem> out = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.optJSONObject(i);
                if (o != null) out.add(new LibraryItem(o.optString("title", "Item"), o.optString("value", ""), o.optString("detail", "")));
            }
            return out;
        }
    }
}
