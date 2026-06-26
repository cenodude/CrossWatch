package app.crosswatch.companion;

import android.app.Activity;
import android.content.ActivityNotFoundException;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
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
import android.text.TextUtils;
import android.view.Gravity;
import android.view.HapticFeedbackConstants;
import android.view.MotionEvent;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
    private boolean refreshInFlight = false;
    private boolean foreground = false;
    private float pullStartY = -1f;
    private float swipeStartX = -1f;
    private float swipeStartY = -1f;
    private boolean pullReady = false;
    private final Map<String, Bitmap> imageCache = new HashMap<>();
    private final Runnable autoRefreshTask = new Runnable() {
        @Override
        public void run() {
            if (!foreground) return;
            if (hasMobileToken() && !refreshInFlight) {
                refresh(false);
            } else {
                scheduleAutoRefresh();
            }
        }
    };

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
        if (!handlePairingIntent(getIntent())) refresh(true);
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        if (!handlePairingIntent(intent)) refresh(true);
    }

    @Override
    protected void onResume() {
        super.onResume();
        foreground = true;
        if (hasMobileToken()) refresh(false);
        scheduleAutoRefresh();
    }

    @Override
    protected void onPause() {
        foreground = false;
        main.removeCallbacks(autoRefreshTask);
        super.onPause();
    }

    @Override
    protected void onDestroy() {
        main.removeCallbacks(autoRefreshTask);
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

    private View bottomNav() {
        LinearLayout wrap = col();
        wrap.setPadding(dp(10), dp(6), dp(10), navInset + dp(8));
        wrap.setBackgroundColor(BG);
        LinearLayout nav = row();
        nav.setGravity(Gravity.CENTER);
        nav.setPadding(dp(6), dp(6), dp(6), dp(6));
        nav.setBackground(round(0xF00F141D, 24, 0x1F2A3443));
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
        rail.setBackgroundColor(0xFF0A0E15);
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
        b.setBackground(active ? round(0xFF202734, rail ? 18 : 18, 0x3349D8B5) : round(Color.TRANSPARENT, 16, 0));
        b.setClickable(true);
        b.setOnClickListener(v -> {
            selected = item;
            haptic();
            buildShell();
        });

        NavIconView icon = new NavIconView(this, item, active);
        b.addView(icon, new LinearLayout.LayoutParams(dp(rail ? 28 : 24), dp(rail ? 28 : 24)));
        TextView text = label(item, rail ? 11 : 10, Typeface.BOLD, active ? INK : SOFT);
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
        LinearLayout scrollBody = col();
        TextView pull = label("Pull to refresh", 12, Typeface.BOLD, SOFT);
        pull.setGravity(Gravity.CENTER);
        pull.setAlpha(0f);
        scrollBody.addView(pull, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(34)));
        LinearLayout page = col();
        int horizontal = isTablet() ? dp(32) : dp(18);
        page.setPadding(horizontal, dp(4), horizontal, dp(isTablet() ? 32 : 18));
        scrollBody.addView(page, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        scroll.addView(scrollBody, new ScrollView.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        attachPullRefresh(scroll, page, pull);

        if ("Dashboard".equals(selected)) dashboard(page);
        else if ("Activity".equals(selected)) activity(page);
        else if ("Library".equals(selected)) library(page);
        else if ("Tools".equals(selected)) tools(page);
        else settings(page);

        content.addView(scroll, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
    }

    private void dashboard(LinearLayout page) {
        statusStrip(page);
        metricGrid(page, Arrays.asList(
                pair("Sync", summary.syncState),
                pair("Scheduler", summary.scheduler),
                pair("Watcher", summary.watcher),
                pair("Webhook", summary.webhook),
                pair("Next run", summary.nextRun)
        ), isTablet() ? 5 : 2);
        if (summary.nowPlaying.active) {
            section(page, "Now");
            nowCard(page);
        }
        section(page, "Providers");
        providerGrid(page, summary.providers, isTablet() ? 3 : 2);
    }

    private void attachPullRefresh(ScrollView scroll, View page, TextView pull) {
        scroll.setOnTouchListener((view, event) -> {
            if (refreshInFlight) return false;
            if (event.getActionMasked() == MotionEvent.ACTION_DOWN) {
                pullStartY = event.getY();
                swipeStartX = event.getX();
                swipeStartY = event.getY();
                pullReady = false;
                return false;
            }
            if (event.getActionMasked() == MotionEvent.ACTION_MOVE && scroll.getScrollY() <= 0 && pullStartY >= 0) {
                float dy = event.getY() - pullStartY;
                if (dy > 0) {
                    float pullDistance = Math.min(dp(92), dy * 0.48f);
                    float progress = Math.min(1f, pullDistance / dp(72));
                    page.setTranslationY(pullDistance);
                    pull.setTranslationY(pullDistance * 0.24f);
                    pull.setAlpha(Math.max(0.18f, progress));
                    pull.setText(progress >= 1f ? "Release to refresh" : "Pull to refresh");
                    pullReady = progress >= 1f;
                    return true;
                }
            }
            if (event.getActionMasked() == MotionEvent.ACTION_UP) {
                float dx = event.getX() - swipeStartX;
                float dy = event.getY() - swipeStartY;
                if (Math.abs(dx) > dp(84) && Math.abs(dx) > Math.abs(dy) * 1.7f) {
                    navigatePage(dx < 0 ? 1 : -1);
                    pullStartY = -1f;
                    swipeStartX = -1f;
                    swipeStartY = -1f;
                    return true;
                }
                if (pullReady) {
                    pull.setText("Refreshing...");
                    pull.setAlpha(1f);
                    settlePull(page, pull);
                    pullReady = false;
                    pullStartY = -1f;
                    swipeStartX = -1f;
                    swipeStartY = -1f;
                    refresh(true);
                    return true;
                }
                settlePull(page, pull);
                pullStartY = -1f;
                swipeStartX = -1f;
                swipeStartY = -1f;
                pullReady = false;
            }
            if (event.getActionMasked() == MotionEvent.ACTION_CANCEL) {
                settlePull(page, pull);
                pullStartY = -1f;
                swipeStartX = -1f;
                swipeStartY = -1f;
                pullReady = false;
            }
            return false;
        });
    }

    private void settlePull(View page, TextView pull) {
        page.animate().translationY(0f).setDuration(170).start();
        pull.animate().translationY(0f).alpha(0f).setDuration(170).start();
    }

    private void navigatePage(int delta) {
        List<String> items = sections();
        int idx = items.indexOf(selected);
        if (idx < 0) idx = 0;
        int next = Math.max(0, Math.min(items.size() - 1, idx + delta));
        if (next == idx) return;
        selected = items.get(next);
        haptic();
        buildShell();
    }

    private void statusStrip(LinearLayout page) {
        LinearLayout strip = row();
        strip.setGravity(Gravity.CENTER_VERTICAL);
        strip.setPadding(dp(14), dp(12), dp(14), dp(12));
        strip.setBackground(round(0xFF111821, 18, 0x22324050));

        TextView dot = label("", 1, Typeface.BOLD, statusColor());
        dot.setBackground(round(statusColor(), 99, 0));
        strip.addView(dot, new LinearLayout.LayoutParams(dp(10), dp(10)));

        LinearLayout copy = col();
        copy.setPadding(dp(10), 0, dp(10), 0);
        copy.addView(label(hasMobileToken() ? "Connected - paired" : "Not paired", 13, Typeface.BOLD, hasMobileToken() ? MINT : GOLD));
        copy.addView(label(refreshInFlight ? "Refreshing..." : (hasMobileToken() ? "Auto refresh active" : "Pull down to refresh"), 12, Typeface.NORMAL, SOFT));
        strip.addView(copy, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));

        TextView version = label(summary.version.isEmpty() ? "CrossWatch" : summary.version, 12, Typeface.BOLD, MUTED);
        version.setGravity(Gravity.RIGHT | Gravity.CENTER_VERTICAL);
        strip.addView(version, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT));
        page.addView(strip, blockParams(14));
    }

    private void nowCard(LinearLayout page) {
        int cardHeight = dp(isTablet() ? 136 : 118);
        int posterWidth = Math.round(cardHeight * 0.68f);
        FrameLayout c = new FrameLayout(this);
        c.setBackground(gradient(new int[]{0xFF172231, 0xFF111923}, 18));

        RoundedImageView backdrop = new RoundedImageView(this, false, 18);
        backdrop.setScaleType(ImageView.ScaleType.CENTER_CROP);
        backdrop.setAlpha(0.22f);
        c.addView(backdrop, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
        loadImageInto(summary.nowPlaying.backdrop, backdrop, false);

        View scrim = new View(this);
        scrim.setBackground(gradient(new int[]{0xEE141C28, 0xD9141A24, 0xF0121721}, 18));
        c.addView(scrim, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));

        LinearLayout row = row();
        row.setGravity(Gravity.CENTER_VERTICAL);
        RoundedImageView poster = new RoundedImageView(this, true, 18);
        poster.setScaleType(ImageView.ScaleType.CENTER_CROP);
        poster.setBackgroundColor(0xFF0A0F18);
        row.addView(poster, new LinearLayout.LayoutParams(posterWidth, cardHeight));
        loadImageInto(summary.nowPlaying.poster, poster);

        LinearLayout copy = col();
        copy.setGravity(Gravity.CENTER_VERTICAL);
        copy.setPadding(dp(16), dp(8), dp(16), dp(8));
        TextView kicker = label(summary.nowPlaying.active ? summary.nowPlaying.state : "Idle", 12, Typeface.BOLD, summary.nowPlaying.active ? MINT : SOFT);
        copy.addView(kicker);
        TextView value = label(summary.nowPlaying.title, 20, Typeface.BOLD, INK);
        value.setSingleLine(false);
        LinearLayout.LayoutParams titleLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        titleLp.topMargin = dp(2);
        copy.addView(value, titleLp);
        String sub = summary.nowPlaying.subtitle.isEmpty() ? (summary.nowPlaying.active ? "Playing now" : "No active watcher session") : summary.nowPlaying.subtitle;
        copy.addView(label(sub, 13, Typeface.NORMAL, MUTED));
        TextView progressText = label(summary.nowPlaying.progressLabel, 14, Typeface.BOLD, INK);
        LinearLayout.LayoutParams progressTextLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        progressTextLp.topMargin = dp(9);
        copy.addView(progressText, progressTextLp);
        LinearLayout.LayoutParams barLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(8));
        barLp.topMargin = dp(6);
        copy.addView(progressBar(summary.nowPlaying.progress, summary.nowPlaying.active ? MINT : LINE), barLp);
        if (!summary.nowPlaying.position.isEmpty() || !summary.nowPlaying.duration.isEmpty()) {
            String timing = summary.nowPlaying.position + " / " + summary.nowPlaying.duration;
            TextView time = label(timing, 11, Typeface.BOLD, SOFT);
            LinearLayout.LayoutParams timeLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
            timeLp.topMargin = dp(5);
            copy.addView(time, timeLp);
        }
        row.addView(copy, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));
        c.addView(row, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, cardHeight));
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, cardHeight);
        lp.bottomMargin = dp(18);
        page.addView(c, lp);
    }

    private View progressBar(int percent, int accent) {
        FrameLayout outer = new FrameLayout(this);
        outer.setBackground(round(0xFF263040, 99, 0));
        View fill = new View(this);
        fill.setBackground(round(accent, 99, 0));
        outer.addView(fill, new FrameLayout.LayoutParams(1, ViewGroup.LayoutParams.MATCH_PARENT));
        outer.post(() -> {
            int bounded = Math.max(0, Math.min(100, percent));
            int width = Math.round(outer.getWidth() * bounded / 100f);
            ViewGroup.LayoutParams lp = fill.getLayoutParams();
            lp.width = bounded <= 0 ? 0 : Math.max(dp(4), width);
            fill.setLayoutParams(lp);
        });
        return outer;
    }

    private void activity(LinearLayout page) {
        pageIntro(page, "Recent activity", "Fast read-only view of CrossWatch events.");
        for (ActivityItem item : summary.activity) {
            LinearLayout c = row();
            c.setOrientation(LinearLayout.HORIZONTAL);
            c.setGravity(Gravity.CENTER_VERTICAL);
            c.setPadding(0, 0, dp(8), 0);
            c.setBackground(round(SURFACE_2, 18, LINE));

            int cardHeight = isTablet() ? dp(112) : dp(90);
            int posterWidth = activityThumbWidth(cardHeight, item);
            FrameLayout media = new FrameLayout(this);
            RoundedImageView poster = new RoundedImageView(this, true, 16);
            poster.setScaleType(ImageView.ScaleType.CENTER_CROP);
            poster.setBackgroundColor(0xFF0A0F18);
            media.addView(poster, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
            loadImageInto(item.poster, poster);
            if (!item.episodeLabel.isEmpty()) {
                TextView badge = label(item.episodeLabel, isTablet() ? 12 : 11, Typeface.BOLD, INK);
                badge.setGravity(Gravity.CENTER);
                badge.setPadding(dp(8), 0, dp(8), 0);
                badge.setBackground(round(0xDD121824, 10, 0x33415062));
                FrameLayout.LayoutParams blp = new FrameLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, dp(28), Gravity.RIGHT | Gravity.BOTTOM);
                blp.rightMargin = dp(8);
                blp.bottomMargin = dp(8);
                media.addView(badge, blp);
            }
            c.addView(media, new LinearLayout.LayoutParams(posterWidth, ViewGroup.LayoutParams.MATCH_PARENT));

            LinearLayout txt = col();
            txt.setGravity(Gravity.CENTER_VERTICAL);
            txt.setPadding(dp(14), 0, dp(8), 0);
            TextView title = label(item.title, isTablet() ? 18 : 16, Typeface.BOLD, INK);
            title.setSingleLine(true);
            title.setEllipsize(TextUtils.TruncateAt.END);
            txt.addView(title);
            TextView detail = label(item.detail, isTablet() ? 14 : 12, Typeface.BOLD, MUTED);
            detail.setSingleLine(true);
            detail.setEllipsize(TextUtils.TruncateAt.END);
            LinearLayout.LayoutParams detailLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
            detailLp.topMargin = dp(3);
            txt.addView(detail, detailLp);
            c.addView(txt, new LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));

            LinearLayout routes = col();
            routes.setGravity(Gravity.CENTER_VERTICAL | Gravity.RIGHT);
            List<String> sourceProviders = activityRouteProviders(item, true);
            List<String> sinkProviders = activityRouteProviders(item, false);
            if (!sourceProviders.isEmpty()) {
                LinearLayout sourceRow = row();
                sourceRow.setGravity(Gravity.RIGHT | Gravity.CENTER_VERTICAL);
                for (String source : sourceProviders) {
                    sourceRow.addView(sourceChip(source));
                }
                routes.addView(sourceRow, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, dp(24)));
            }
            if (!sinkProviders.isEmpty()) {
                LinearLayout sinkRow = row();
                sinkRow.setGravity(Gravity.RIGHT | Gravity.CENTER_VERTICAL);
                for (String source : sinkProviders) {
                    sinkRow.addView(sourceChip(source));
                }
                LinearLayout.LayoutParams sinkLp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, dp(24));
                if (!sourceProviders.isEmpty()) sinkLp.topMargin = dp(5);
                routes.addView(sinkRow, sinkLp);
            }
            c.addView(routes, new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.MATCH_PARENT));

            LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, cardHeight);
            lp.bottomMargin = dp(14);
            page.addView(c, lp);
        }
    }

    private int activityThumbWidth(int cardHeight, ActivityItem item) {
        int screen = getResources().getDisplayMetrics().widthPixels;
        int pagePadding = isTablet() ? dp(64) : dp(36);
        int chipReserve = activityRouteWidth(item) + dp(6);
        int minText = isTablet() ? dp(260) : dp(150);
        int available = Math.max(dp(260), screen - pagePadding);
        int ideal = Math.round(cardHeight * 1.48f);
        int max = Math.max(dp(96), available - chipReserve - minText - dp(30));
        return Math.max(dp(96), Math.min(ideal, max));
    }

    private int activityRouteWidth(ActivityItem item) {
        int count = Math.max(activityRouteProviders(item, true).size(), activityRouteProviders(item, false).size());
        return count <= 0 ? 0 : count * dp(24);
    }

    private List<String> activityRouteProviders(ActivityItem item, boolean mediaSource) {
        List<String> out = new ArrayList<>();
        int limit = mediaSource ? 1 : 3;
        for (String source : item.sources) {
            String key = providerKey(source);
            boolean media = isMediaSource(key);
            if (media != mediaSource || containsProvider(out, key)) continue;
            out.add(key);
            if (out.size() >= limit) break;
        }
        return out;
    }

    private boolean containsProvider(List<String> providers, String key) {
        for (String provider : providers) {
            if (providerKey(provider).equals(providerKey(key))) return true;
        }
        return false;
    }

    private boolean isMediaSource(String provider) {
        String key = providerKey(provider);
        return "PLEX".equals(key) || "EMBY".equals(key) || "JELLYFIN".equals(key);
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
            serverUrl = trimTrailingSlashes(input.getText().toString().trim());
            prefs.edit().putString("server_url", serverUrl).apply();
            refresh(true);
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
                LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(0, dp(76), 1f);
                if (j > 0) lp.leftMargin = dp(10);
                r.addView(child, lp);
            }
            page.addView(r, blockParams(8));
        }
    }

    private View metric(String label, String value) {
        LinearLayout c = card();
        c.setGravity(Gravity.CENTER_VERTICAL);
        c.setPadding(dp(13), dp(10), dp(13), dp(10));
        c.addView(label(label, 11, Typeface.BOLD, MUTED));
        TextView val = label(value, 16, Typeface.BOLD, INK);
        val.setSingleLine(false);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT);
        lp.topMargin = dp(5);
        c.addView(val, lp);
        return c;
    }

    private void providerGrid(LinearLayout page, List<Provider> providers, int columns) {
        if (providers == null || providers.isEmpty()) {
            notice(page, "No configured providers", "Configure providers in CrossWatch first. The companion app only shows providers that are actually set up.", GOLD);
            return;
        }
        for (int i = 0; i < providers.size(); i += columns) {
            LinearLayout r = row();
            for (int j = 0; j < columns; j++) {
                int idx = i + j;
                View child = idx < providers.size() ? provider(providers.get(idx)) : new View(this);
                LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(0, dp(isTablet() ? 126 : 128), 1f);
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

    private void refresh() {
        refresh(true);
    }

    private void refresh(boolean userInitiated) {
        if (refreshInFlight) return;
        main.removeCallbacks(autoRefreshTask);
        if (userInitiated) haptic();
        refreshInFlight = true;
        if (userInitiated) {
            status = "Refreshing";
            rebuild();
        }
        io.execute(() -> {
            Summary next = fetchSummary(serverUrl);
            main.post(() -> {
                boolean keepCurrent = !userInitiated && next.demo && summary != null && !summary.demo;
                if (!keepCurrent) {
                    summary = next;
                    status = next.demo ? "Preview data" : "Connected";
                }
                refreshInFlight = false;
                if (!keepCurrent || userInitiated) rebuild();
                scheduleAutoRefresh();
            });
        });
    }

    private int autoRefreshDelayMs() {
        if (!hasMobileToken()) return 45000;
        if (summary != null && summary.nowPlaying != null && summary.nowPlaying.active) return 12000;
        return "Dashboard".equals(selected) ? 45000 : 60000;
    }

    private void scheduleAutoRefresh() {
        main.removeCallbacks(autoRefreshTask);
        if (!foreground) return;
        main.postDelayed(autoRefreshTask, autoRefreshDelayMs());
    }

    private void haptic() {
        View v = root != null ? root : content;
        if (v != null) v.performHapticFeedback(HapticFeedbackConstants.CLOCK_TICK);
    }

    private void action(String path) {
        status = "Sending action";
        rebuild();
        io.execute(() -> {
            boolean ok = post(serverUrl + path);
            main.post(() -> {
                status = ok ? "Action sent" : "Action unavailable";
                refresh(true);
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
        final String firstBase = trimTrailingSlashes(serverUrl);
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
                    refresh(true);
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
            String body = get(trimTrailingSlashes(base) + "/api/mobile/summary");
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
                    serverUrl = trimTrailingSlashes(server.trim());
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

    private View sourceChip(String source) {
        String key = providerKey(source);
        FrameLayout chip = new FrameLayout(this);
        chip.setBackground(round(0xFF171E2A, 99, 0x33415062));
        Bitmap logo = loadProviderLogo(key);
        if (logo != null) {
            ImageView icon = new ImageView(this);
            icon.setImageBitmap(logo);
            icon.setScaleType(ImageView.ScaleType.FIT_CENTER);
            FrameLayout.LayoutParams ilp = new FrameLayout.LayoutParams(dp(16), dp(16), Gravity.CENTER);
            chip.addView(icon, ilp);
        } else {
            TextView initials = label(sourceInitials(key), 9, Typeface.BOLD, INK);
            initials.setGravity(Gravity.CENTER);
            chip.addView(initials, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
        }
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(dp(23), dp(23));
        lp.leftMargin = dp(4);
        chip.setLayoutParams(lp);
        return chip;
    }

    private String sourceInitials(String source) {
        String key = providerKey(source);
        if (key.length() <= 2) return key;
        if ("MDBLIST".equals(key)) return "M";
        if ("PUBLICMETADB".equals(key)) return "P";
        if ("CROSSWATCH".equals(key)) return "CW";
        return key.substring(0, 1);
    }

    private String providerKey(String name) {
        String key = providerKeyChars(name);
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

    private Bitmap loadProviderLogo(String name) {
        String key = providerKey(name).toLowerCase(Locale.ROOT);
        int id = getResources().getIdentifier("provider_" + key, "drawable", getPackageName());
        if (id <= 0) return null;
        return BitmapFactory.decodeResource(getResources(), id);
    }

    private void loadImageInto(String rawUrl, ImageView target) {
        loadImageInto(rawUrl, target, true);
    }

    private void loadImageInto(String rawUrl, ImageView target, boolean placeholder) {
        if (placeholder) target.setImageResource(R.drawable.crosswatch_icon);
        String url = absoluteUrl(rawUrl);
        if (url.isEmpty() || url.toLowerCase(Locale.ROOT).endsWith(".svg")) return;
        Bitmap cached = imageCache.get(url);
        if (cached != null) {
            target.setImageBitmap(cached);
            return;
        }
        io.execute(() -> {
            try {
                Bitmap bmp = fetchBitmap(url);
                if (bmp == null) return;
                imageCache.put(url, bmp);
                main.post(() -> target.setImageBitmap(bmp));
            } catch (Exception ignored) {
            }
        });
    }

    private String absoluteUrl(String rawUrl) {
        String value = rawUrl == null ? "" : rawUrl.trim();
        if (value.isEmpty()) return "";
        if (value.startsWith("http://") || value.startsWith("https://")) return value;
        if (value.startsWith("/")) return trimTrailingSlashes(serverUrl) + value;
        return value;
    }

    private String trimTrailingSlashes(String value) {
        if (value == null) return "";
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return end == value.length() ? value : value.substring(0, end);
    }

    private String providerKeyChars(String value) {
        if (value == null || value.isEmpty()) return "";
        String upper = value.toUpperCase(Locale.ROOT);
        StringBuilder out = new StringBuilder(upper.length());
        for (int i = 0; i < upper.length(); i++) {
            char ch = upper.charAt(i);
            if ((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
                out.append(ch);
            }
        }
        return out.toString();
    }

    private Bitmap fetchBitmap(String value) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(value).openConnection();
        c.setRequestMethod("GET");
        c.setConnectTimeout(6000);
        c.setReadTimeout(8000);
        c.setRequestProperty("Accept", "image/*,*/*;q=0.8");
        addMobileAuth(c);
        if (c.getResponseCode() < 200 || c.getResponseCode() >= 300) return null;
        InputStream stream = c.getInputStream();
        try {
            return BitmapFactory.decodeStream(stream);
        } finally {
            stream.close();
        }
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
        final String key;
        final String name;
        final String status;
        final boolean healthy;
        final int count;
        final int movies;
        final int shows;
        final int anime;

        Provider(String key, String name, String status, boolean healthy, int count, int movies, int shows, int anime) {
            this.key = key;
            this.name = name;
            this.status = status;
            this.healthy = healthy;
            this.count = count;
            this.movies = movies;
            this.shows = shows;
            this.anime = anime;
        }
    }

    private class NavIconView extends View {
        private final String item;
        private final boolean active;
        private final Paint paint = new Paint(Paint.ANTI_ALIAS_FLAG);
        private final RectF rect = new RectF();

        NavIconView(Context context, String item, boolean active) {
            super(context);
            this.item = item;
            this.active = active;
        }

        @Override
        protected void onDraw(Canvas canvas) {
            super.onDraw(canvas);
            float w = getWidth();
            float h = getHeight();
            float s = Math.min(w, h);
            float cx = w / 2f;
            float cy = h / 2f;
            int color = active ? MINT : SOFT;
            paint.setColor(color);
            paint.setStyle(Paint.Style.STROKE);
            paint.setStrokeWidth(Math.max(2f, dp(2)));
            paint.setStrokeCap(Paint.Cap.ROUND);
            paint.setStrokeJoin(Paint.Join.ROUND);

            if ("Dashboard".equals(item)) {
                paint.setStyle(Paint.Style.FILL);
                float r = s * 0.12f;
                canvas.drawRoundRect(new RectF(cx - s * 0.34f, cy - s * 0.34f, cx - s * 0.06f, cy - s * 0.06f), r, r, paint);
                canvas.drawRoundRect(new RectF(cx + s * 0.06f, cy - s * 0.34f, cx + s * 0.34f, cy - s * 0.06f), r, r, paint);
                canvas.drawRoundRect(new RectF(cx - s * 0.34f, cy + s * 0.06f, cx - s * 0.06f, cy + s * 0.34f), r, r, paint);
                canvas.drawRoundRect(new RectF(cx + s * 0.06f, cy + s * 0.06f, cx + s * 0.34f, cy + s * 0.34f), r, r, paint);
            } else if ("Activity".equals(item)) {
                paint.setStyle(Paint.Style.FILL);
                canvas.drawRoundRect(new RectF(cx - s * 0.34f, cy + s * 0.02f, cx - s * 0.20f, cy + s * 0.34f), s * 0.06f, s * 0.06f, paint);
                canvas.drawRoundRect(new RectF(cx - s * 0.07f, cy - s * 0.28f, cx + s * 0.07f, cy + s * 0.34f), s * 0.06f, s * 0.06f, paint);
                canvas.drawRoundRect(new RectF(cx + s * 0.20f, cy - s * 0.08f, cx + s * 0.34f, cy + s * 0.34f), s * 0.06f, s * 0.06f, paint);
            } else if ("Library".equals(item)) {
                rect.set(cx - s * 0.34f, cy - s * 0.30f, cx + s * 0.34f, cy + s * 0.32f);
                canvas.drawRoundRect(rect, s * 0.10f, s * 0.10f, paint);
                canvas.drawLine(cx - s * 0.12f, cy - s * 0.30f, cx - s * 0.12f, cy + s * 0.32f, paint);
            } else if ("Tools".equals(item)) {
                canvas.drawLine(cx - s * 0.28f, cy + s * 0.28f, cx + s * 0.24f, cy - s * 0.24f, paint);
                canvas.drawCircle(cx + s * 0.26f, cy - s * 0.26f, s * 0.10f, paint);
                canvas.drawCircle(cx - s * 0.30f, cy + s * 0.30f, s * 0.08f, paint);
            } else {
                canvas.drawCircle(cx, cy, s * 0.26f, paint);
                paint.setStyle(Paint.Style.FILL);
                canvas.drawCircle(cx, cy, s * 0.08f, paint);
                for (int i = 0; i < 8; i++) {
                    double a = Math.PI * 2 * i / 8.0;
                    float x1 = cx + (float) Math.cos(a) * s * 0.36f;
                    float y1 = cy + (float) Math.sin(a) * s * 0.36f;
                    float x2 = cx + (float) Math.cos(a) * s * 0.44f;
                    float y2 = cy + (float) Math.sin(a) * s * 0.44f;
                    paint.setStyle(Paint.Style.STROKE);
                    canvas.drawLine(x1, y1, x2, y2, paint);
                }
            }
        }
    }

    private class ProviderTileView extends View {
        private final Provider provider;
        private final Paint paint = new Paint(Paint.ANTI_ALIAS_FLAG);
        private final RectF rect = new RectF();
        private final Path clip = new Path();
        private final Bitmap logo;

        ProviderTileView(Context context, Provider provider) {
            super(context);
            this.provider = provider;
            this.logo = loadProviderLogo(provider.key);
            setLayerType(View.LAYER_TYPE_SOFTWARE, null);
        }

        @Override
        protected void onDraw(Canvas canvas) {
            super.onDraw(canvas);
            float w = getWidth();
            float h = getHeight();
            float radius = dp(22);
            int tone = providerTone(provider.key);
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

            drawProviderArtwork(canvas, w, h, tone);

            paint.setColor(tint(0xFF000000, 0.18f));
            canvas.drawRect(0, h * 0.54f, w, h, paint);

            drawStatusPill(canvas, w, keyTone);
            drawBigMetric(canvas, w, h, String.valueOf(Math.max(0, provider.count)));
            drawFeatureBadges(canvas, w, h);
            drawProviderFooter(canvas, w, h, tone);

            canvas.restoreToCount(save);

            paint.setStyle(Paint.Style.STROKE);
            paint.setStrokeWidth(dp(1));
            paint.setColor(tint(0xFFFFFFFF, 0.11f));
            canvas.drawRoundRect(rect, radius, radius, paint);
            paint.setStyle(Paint.Style.FILL);
        }

        private void drawProviderArtwork(Canvas canvas, float w, float h, int tone) {
            paint.setStyle(Paint.Style.FILL);
            paint.setColor(tint(tone, provider.healthy ? 0.20f : 0.10f));
            canvas.drawCircle(w * 0.12f, h * 0.10f, w * 0.38f, paint);
            paint.setColor(tint(0xFFFFFFFF, provider.healthy ? 0.08f : 0.05f));
            canvas.drawCircle(w * 0.90f, h * 0.04f, w * 0.34f, paint);

            if (logo == null) return;
            float size = Math.min(w, h) * 1.02f;
            float left = (w - size) / 2f;
            float top = h * 0.04f;
            RectF dst = new RectF(left, top, left + size, top + size);
            paint.setAlpha(provider.healthy ? 94 : 64);
            canvas.drawBitmap(logo, null, dst, paint);
            paint.setAlpha(255);
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
            paint.setTextSize(sp(44));
            paint.setColor(tint(0xFF000000, 0.36f));
            canvas.drawText(value, w * 0.50f + dp(2), h * 0.62f + dp(3), paint);
            paint.setColor(0xDDE9EEF7);
            canvas.drawText(value, w * 0.50f, h * 0.62f, paint);
        }

        private void drawFeatureBadges(Canvas canvas, float w, float h) {
            List<Pair> parts = new ArrayList<>();
            if (provider.movies > 0) parts.add(pair("M", String.valueOf(provider.movies)));
            if (provider.shows > 0) parts.add(pair("S", String.valueOf(provider.shows)));
            if (provider.anime > 0) parts.add(pair("A", String.valueOf(provider.anime)));
            if (parts.isEmpty()) {
                if (provider.count <= 0) parts.add(pair("IDLE", ""));
                else parts.add(pair("W", String.valueOf(provider.count)));
            }

            paint.setTypeface(Typeface.create(Typeface.DEFAULT, Typeface.BOLD));
            paint.setTextSize(sp(12));
            float totalW = 0f;
            float gap = dp(4);
            for (Pair p : parts) {
                String text = p.b.isEmpty() ? p.a : p.a + " " + p.b;
                totalW += Math.max(dp(34), paint.measureText(text) + dp(14));
            }
            totalW += gap * Math.max(0, parts.size() - 1);
            float x = (w - totalW) / 2f;
            for (Pair p : parts) {
                String text = p.b.isEmpty() ? p.a : p.a + " " + p.b;
                float bw = Math.max(dp(34), paint.measureText(text) + dp(14));
                RectF badge = new RectF(x, h - dp(38), x + bw, h - dp(16));
                paint.setColor(0xAA202733);
                canvas.drawRoundRect(badge, dp(12), dp(12), paint);
                paint.setTextAlign(Paint.Align.CENTER);
                paint.setColor(INK);
                canvas.drawText(text, badge.centerX(), badge.top + dp(16), paint);
                x += bw + gap;
            }
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

    private class RoundedImageView extends ImageView {
        private final boolean leftOnly;
        private final int radiusDp;
        private final Path clip = new Path();
        private final RectF rect = new RectF();

        RoundedImageView(Context context, boolean leftOnly, int radiusDp) {
            super(context);
            this.leftOnly = leftOnly;
            this.radiusDp = radiusDp;
            setLayerType(View.LAYER_TYPE_SOFTWARE, null);
        }

        @Override
        protected void onDraw(Canvas canvas) {
            float w = getWidth();
            float h = getHeight();
            float r = dp(radiusDp);
            clip.reset();
            rect.set(0, 0, w, h);
            if (!leftOnly) {
                clip.addRoundRect(rect, r, r, Path.Direction.CW);
            } else {
                clip.moveTo(0, r);
                clip.quadTo(0, 0, r, 0);
                clip.lineTo(w, 0);
                clip.lineTo(w, h);
                clip.lineTo(r, h);
                clip.quadTo(0, h, 0, h - r);
                clip.close();
            }
            int save = canvas.save();
            canvas.clipPath(clip);
            super.onDraw(canvas);
            canvas.restoreToCount(save);
        }
    }

    private static class ActivityItem {
        final String title;
        final String detail;
        final String time;
        final String level;
        final String poster;
        final String episodeLabel;
        final List<String> sources;
        final String method;

        ActivityItem(String title, String detail, String time, String level) {
            this(title, detail, time, level, "", "", new ArrayList<>(), "");
        }

        ActivityItem(String title, String detail, String time, String level, String poster, String episodeLabel, List<String> sources, String method) {
            this.title = title;
            this.detail = detail;
            this.time = time;
            this.level = level;
            this.poster = poster == null ? "" : poster;
            this.episodeLabel = episodeLabel == null ? "" : episodeLabel;
            this.sources = sources == null ? new ArrayList<>() : sources;
            this.method = method == null ? "" : method;
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

    private static class NowPlaying {
        final boolean active;
        final String title;
        final String subtitle;
        final String state;
        final String poster;
        final String backdrop;
        final int progress;
        final String progressLabel;
        final String position;
        final String duration;
        final String source;

        NowPlaying(boolean active, String title, String subtitle, String state, String poster, String backdrop, int progress, String progressLabel, String position, String duration, String source) {
            this.active = active;
            this.title = title == null || title.isEmpty() ? "Nothing playing" : title;
            this.subtitle = subtitle == null ? "" : subtitle;
            this.state = state == null || state.isEmpty() ? (active ? "Playing" : "Idle") : state;
            this.poster = poster == null ? "" : poster;
            this.backdrop = backdrop == null ? "" : backdrop;
            this.progress = Math.max(0, Math.min(100, progress));
            this.progressLabel = progressLabel == null || progressLabel.isEmpty()
                    ? (this.progress > 0 ? this.progress + "% watched" : "Progress unavailable")
                    : progressLabel;
            this.position = position == null ? "" : position;
            this.duration = duration == null ? "" : duration;
            this.source = source == null ? "" : source;
        }

        static NowPlaying fromJson(JSONObject obj, String fallbackTitle) {
            if (obj == null) return new NowPlaying(false, fallbackTitle, "No active watcher session", "Idle", "", "", 0, "Progress unavailable", "", "", "");
            boolean active = obj.optBoolean("active", false);
            String title = obj.optString("title", fallbackTitle == null || fallbackTitle.isEmpty() ? "Nothing playing" : fallbackTitle);
            int progress = obj.optInt("progress", 0);
            return new NowPlaying(
                    active,
                    title,
                    obj.optString("subtitle", ""),
                    obj.optString("state", active ? "Playing" : "Idle"),
                    obj.optString("poster", ""),
                    obj.optString("backdrop", ""),
                    progress,
                    obj.optString("progress_label", progress > 0 ? progress + "% watched" : "Progress unavailable"),
                    obj.optString("position", ""),
                    obj.optString("duration", ""),
                    obj.optString("source", "")
            );
        }
    }

    private static class Summary {
        final String serverName;
        final String serverUrl;
        final String version;
        final boolean syncRunning;
        final String syncState;
        final String scheduler;
        final String watcher;
        final String webhook;
        final String nextRun;
        final String currentlyWatching;
        final NowPlaying nowPlaying;
        final int warnings;
        final List<Provider> providers;
        final List<ActivityItem> activity;
        final List<LibraryItem> library;
        final boolean demo;

        Summary(String serverName, String serverUrl, String version, boolean syncRunning, String syncState, String scheduler, String watcher, String webhook, String nextRun, String currentlyWatching, NowPlaying nowPlaying, int warnings, List<Provider> providers, List<ActivityItem> activity, List<LibraryItem> library, boolean demo) {
            this.serverName = serverName;
            this.serverUrl = serverUrl;
            this.version = version;
            this.syncRunning = syncRunning;
            this.syncState = syncState;
            this.scheduler = scheduler;
            this.watcher = watcher;
            this.webhook = webhook;
            this.nextRun = nextRun;
            this.currentlyWatching = currentlyWatching;
            this.nowPlaying = nowPlaying;
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
                    "Idle",
                    "Disabled",
                    "Disabled",
                    "Enabled",
                    "Not connected",
                    "Nothing playing",
                    new NowPlaying(false, "Nothing playing", "No active watcher session", "Idle", "", "", 0, "Progress unavailable", "", "", ""),
                    1,
                    Arrays.asList(
                            new Provider("ANILIST", "AniList", "Live", true, 2, 2, 0, 0),
                            new Provider("EMBY", "Emby", "Live", true, 2, 2, 0, 0),
                            new Provider("JELLYFIN", "Jellyfin", "Live", true, 2, 2, 0, 0),
                            new Provider("MDBLIST", "MDBList", "Live", true, 2, 2, 0, 0),
                            new Provider("PLEX", "Plex", "Live", true, 2, 2, 0, 0),
                            new Provider("PUBLICMETADB", "PublicMetaDB", "Live", true, 2, 2, 0, 0)
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
                    obj.optString("sync_state", obj.optBoolean("sync_running", false) ? "Running" : "Idle"),
                    obj.optString("scheduler", "Unknown"),
                    obj.optString("watcher", "Unknown"),
                    obj.optString("webhook", "Unknown"),
                    obj.optString("next_run", "Not scheduled"),
                    obj.optString("currently_watching", "Nothing playing"),
                    NowPlaying.fromJson(obj.optJSONObject("now_playing"), obj.optString("currently_watching", "Nothing playing")),
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
                if (o != null) {
                    JSONObject breakdown = o.optJSONObject("breakdown");
                    out.add(new Provider(
                            o.optString("key", o.optString("name", "Provider")).toUpperCase(Locale.ROOT),
                            o.optString("label", o.optString("name", "Provider")),
                            o.optString("status", "Unknown"),
                            o.optBoolean("healthy", false),
                            o.optInt("count", 0),
                            breakdown == null ? 0 : breakdown.optInt("movies", 0),
                            breakdown == null ? 0 : breakdown.optInt("shows", 0),
                            breakdown == null ? 0 : breakdown.optInt("anime", 0)
                    ));
                }
            }
            return out;
        }

        private static List<ActivityItem> activity(JSONArray arr, List<ActivityItem> fallback) {
            if (arr == null || arr.length() == 0) return fallback;
            List<ActivityItem> out = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.optJSONObject(i);
                if (o != null) {
                    List<String> sources = new ArrayList<>();
                    JSONArray rawSources = o.optJSONArray("sources");
                    if (rawSources != null) {
                        for (int j = 0; j < rawSources.length(); j++) {
                            JSONObject src = rawSources.optJSONObject(j);
                            String provider = src == null ? rawSources.optString(j, "") : src.optString("provider", "");
                            if (provider != null && !provider.trim().isEmpty()) sources.add(provider.trim());
                        }
                    }
                    out.add(new ActivityItem(
                            o.optString("title", "Activity"),
                            o.optString("detail", ""),
                            o.optString("time", ""),
                            o.optString("level", "INFO"),
                            o.optString("poster", ""),
                            o.optString("episode_label", ""),
                            sources,
                            o.optString("method", "")
                    ));
                }
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
