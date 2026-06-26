package app.crosswatch.companion;

import android.Manifest;
import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Rect;
import android.graphics.RectF;
import android.hardware.Camera;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.view.Gravity;
import android.view.SurfaceHolder;
import android.view.SurfaceView;
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.widget.Button;
import android.widget.FrameLayout;
import android.widget.TextView;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.BinaryBitmap;
import com.google.zxing.DecodeHintType;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.NotFoundException;
import com.google.zxing.PlanarYUVLuminanceSource;
import com.google.zxing.Result;
import com.google.zxing.common.GlobalHistogramBinarizer;
import com.google.zxing.common.HybridBinarizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class QrScanActivity extends Activity implements SurfaceHolder.Callback, Camera.PreviewCallback {
    private static final int REQ_CAMERA = 4102;
    private final Handler main = new Handler(Looper.getMainLooper());
    private final MultiFormatReader reader = new MultiFormatReader();
    private SurfaceView preview;
    private Camera camera;
    private boolean surfaceReady;
    private boolean decoding;
    private boolean focusing;
    private final Runnable focusRunnable = new Runnable() {
        @Override
        public void run() {
            requestFocusSweep();
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Window window = getWindow();
        window.setStatusBarColor(Color.BLACK);
        window.setNavigationBarColor(Color.BLACK);

        Map<DecodeHintType, Object> hints = new EnumMap<>(DecodeHintType.class);
        hints.put(DecodeHintType.POSSIBLE_FORMATS, Arrays.asList(BarcodeFormat.QR_CODE));
        hints.put(DecodeHintType.TRY_HARDER, Boolean.TRUE);
        hints.put(DecodeHintType.ALSO_INVERTED, Boolean.TRUE);
        reader.setHints(hints);

        buildUi();
        if (android.os.Build.VERSION.SDK_INT >= 23 && checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.CAMERA}, REQ_CAMERA);
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        startCameraIfReady();
    }

    @Override
    protected void onPause() {
        stopCamera();
        super.onPause();
    }

    @Override
    protected void onDestroy() {
        reader.reset();
        super.onDestroy();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode != REQ_CAMERA) return;
        if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
            startCameraIfReady();
        } else {
            finishWithError("camera_permission_denied");
        }
    }

    private void buildUi() {
        FrameLayout root = new FrameLayout(this);
        root.setBackgroundColor(Color.BLACK);
        preview = new SurfaceView(this);
        preview.getHolder().addCallback(this);
        preview.getHolder().setKeepScreenOn(true);
        root.addView(preview, new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));
        root.addView(new ScannerOverlay(this), new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.MATCH_PARENT));

        TextView title = new TextView(this);
        title.setText("Scan CrossWatch pairing QR");
        title.setTextColor(0xFFF6F8FF);
        title.setTextSize(20);
        title.setGravity(Gravity.CENTER);
        title.setTypeface(android.graphics.Typeface.DEFAULT, android.graphics.Typeface.BOLD);
        title.setPadding(dp(20), dp(28), dp(20), dp(8));
        FrameLayout.LayoutParams titleLp = new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT, Gravity.TOP);
        root.addView(title, titleLp);

        TextView hint = new TextView(this);
        hint.setText("Place the QR code inside the frame");
        hint.setTextColor(0xFFB8C3D6);
        hint.setTextSize(14);
        hint.setGravity(Gravity.CENTER);
        FrameLayout.LayoutParams hintLp = new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT, Gravity.TOP);
        hintLp.topMargin = dp(70);
        root.addView(hint, hintLp);

        Button close = new Button(this);
        close.setText("Cancel");
        close.setAllCaps(false);
        close.setTextColor(0xFF080B12);
        close.setTextSize(14);
        close.setTypeface(android.graphics.Typeface.DEFAULT, android.graphics.Typeface.BOLD);
        close.setBackgroundColor(0xFF35D3A7);
        close.setOnClickListener(v -> finish());
        FrameLayout.LayoutParams closeLp = new FrameLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(52), Gravity.BOTTOM);
        closeLp.setMargins(dp(18), 0, dp(18), dp(24));
        root.addView(close, closeLp);

        setContentView(root);
    }

    private void startCameraIfReady() {
        if (!surfaceReady || camera != null) return;
        if (android.os.Build.VERSION.SDK_INT >= 23 && checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) return;
        try {
            camera = Camera.open();
            camera.setDisplayOrientation(90);
            Camera.Parameters params = camera.getParameters();
            Camera.Size size = choosePreviewSize(params.getSupportedPreviewSizes());
            if (size != null) params.setPreviewSize(size.width, size.height);
            List<String> scenes = params.getSupportedSceneModes();
            if (scenes != null && scenes.contains(Camera.Parameters.SCENE_MODE_BARCODE)) {
                params.setSceneMode(Camera.Parameters.SCENE_MODE_BARCODE);
            }
            List<String> modes = params.getSupportedFocusModes();
            if (modes != null) {
                if (modes.contains(Camera.Parameters.FOCUS_MODE_MACRO)) params.setFocusMode(Camera.Parameters.FOCUS_MODE_MACRO);
                else if (modes.contains(Camera.Parameters.FOCUS_MODE_AUTO)) params.setFocusMode(Camera.Parameters.FOCUS_MODE_AUTO);
                else if (modes.contains(Camera.Parameters.FOCUS_MODE_CONTINUOUS_PICTURE)) params.setFocusMode(Camera.Parameters.FOCUS_MODE_CONTINUOUS_PICTURE);
                else if (modes.contains(Camera.Parameters.FOCUS_MODE_CONTINUOUS_VIDEO)) params.setFocusMode(Camera.Parameters.FOCUS_MODE_CONTINUOUS_VIDEO);
            }
            Camera.Area center = new Camera.Area(new Rect(-450, -450, 450, 450), 1000);
            if (params.getMaxNumFocusAreas() > 0) {
                params.setFocusAreas(Collections.singletonList(center));
            }
            if (params.getMaxNumMeteringAreas() > 0) {
                params.setMeteringAreas(Collections.singletonList(center));
            }
            camera.setParameters(params);
            camera.setPreviewDisplay(preview.getHolder());
            camera.setPreviewCallback(this);
            camera.startPreview();
            main.postDelayed(focusRunnable, 300);
        } catch (Exception exc) {
            stopCamera();
            finishWithError("camera_unavailable");
        }
    }

    private Camera.Size choosePreviewSize(List<Camera.Size> sizes) {
        if (sizes == null || sizes.isEmpty()) return null;
        Camera.Size best = null;
        int bestScore = Integer.MAX_VALUE;
        int targetW = 1280;
        int targetH = 720;
        for (Camera.Size size : sizes) {
            int pixels = size.width * size.height;
            if (pixels < 640 * 480 || pixels > 1920 * 1080) continue;
            int aspectPenalty = Math.abs((size.width * 1000 / Math.max(1, size.height)) - (targetW * 1000 / targetH));
            int sizePenalty = Math.abs(size.width - targetW) + Math.abs(size.height - targetH);
            int score = aspectPenalty * 4 + sizePenalty;
            if (score < bestScore) {
                best = size;
                bestScore = score;
            }
        }
        return best != null ? best : sizes.get(0);
    }

    private void requestFocusSweep() {
        Camera active = camera;
        if (active == null || focusing) return;
        focusing = true;
        try {
            active.autoFocus((success, cam) -> {
                focusing = false;
                if (camera != null) main.postDelayed(focusRunnable, success ? 1300 : 650);
            });
        } catch (Exception ignored) {
            focusing = false;
            if (camera != null) main.postDelayed(focusRunnable, 1200);
        }
    }

    private void stopCamera() {
        Camera old = camera;
        camera = null;
        decoding = false;
        focusing = false;
        main.removeCallbacks(focusRunnable);
        if (old == null) return;
        try { old.setPreviewCallback(null); } catch (Exception ignored) {}
        try { old.stopPreview(); } catch (Exception ignored) {}
        try { old.release(); } catch (Exception ignored) {}
    }

    @Override
    public void surfaceCreated(SurfaceHolder holder) {
        surfaceReady = true;
        startCameraIfReady();
    }

    @Override
    public void surfaceChanged(SurfaceHolder holder, int format, int width, int height) {
        if (surfaceReady && camera == null) startCameraIfReady();
    }

    @Override
    public void surfaceDestroyed(SurfaceHolder holder) {
        surfaceReady = false;
        stopCamera();
    }

    @Override
    public void onPreviewFrame(byte[] data, Camera cam) {
        if (decoding || data == null || cam == null) return;
        decoding = true;
        try {
            Camera.Size size = cam.getParameters().getPreviewSize();
            Result result = decode(data, size.width, size.height);
            if (result != null && result.getText() != null && !result.getText().trim().isEmpty()) {
                finishWithResult(result.getText().trim());
                return;
            }
        } catch (Exception ignored) {
        } finally {
            reader.reset();
            main.postDelayed(() -> decoding = false, 80);
        }
    }

    private Result decode(byte[] data, int width, int height) throws Exception {
        Result got = decodeVariants(data, width, height);
        if (got != null) return got;

        byte[] clockwise = rotateLuminanceClockwise(data, width, height);
        got = decodeVariants(clockwise, height, width);
        if (got != null) return got;

        byte[] counterClockwise = rotateLuminanceCounterClockwise(data, width, height);
        got = decodeVariants(counterClockwise, height, width);
        if (got != null) return got;

        byte[] upsideDown = rotateLuminance180(data, width, height);
        return decodeVariants(upsideDown, width, height);
    }

    private Result decodeVariants(byte[] data, int width, int height) throws Exception {
        Result full = decodeBitmap(data, width, height, 0, 0, width, height);
        if (full != null) return full;

        int side = Math.min(width, height);
        int crop = Math.max(1, Math.round(side * 0.82f));
        int left = Math.max(0, (width - crop) / 2);
        int top = Math.max(0, (height - crop) / 2);
        Result center = decodeBitmap(data, width, height, left, top, Math.min(crop, width - left), Math.min(crop, height - top));
        if (center != null) return center;

        crop = Math.max(1, Math.round(side * 0.62f));
        left = Math.max(0, (width - crop) / 2);
        top = Math.max(0, (height - crop) / 2);
        return decodeBitmap(data, width, height, left, top, Math.min(crop, width - left), Math.min(crop, height - top));
    }

    private Result decodeBitmap(byte[] data, int width, int height, int left, int top, int cropWidth, int cropHeight) throws Exception {
        PlanarYUVLuminanceSource source = new PlanarYUVLuminanceSource(data, width, height, left, top, cropWidth, cropHeight, false);
        Result result = decodeSource(source);
        if (result != null) return result;
        try {
            return decodeSource(source.invert());
        } catch (Exception ignored) {
            return null;
        }
    }

    private Result decodeSource(com.google.zxing.LuminanceSource source) throws Exception {
        Result result = decodeBinary(new BinaryBitmap(new HybridBinarizer(source)));
        if (result != null) return result;
        return decodeBinary(new BinaryBitmap(new GlobalHistogramBinarizer(source)));
    }

    private Result decodeBinary(BinaryBitmap bitmap) throws Exception {
        try {
            return reader.decodeWithState(bitmap);
        } catch (NotFoundException missing) {
            return null;
        } finally {
            reader.reset();
        }
    }

    private byte[] rotateLuminanceClockwise(byte[] data, int width, int height) {
        byte[] out = new byte[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                out[x * height + height - y - 1] = data[y * width + x];
            }
        }
        return out;
    }

    private byte[] rotateLuminanceCounterClockwise(byte[] data, int width, int height) {
        byte[] out = new byte[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                out[(width - x - 1) * height + y] = data[y * width + x];
            }
        }
        return out;
    }

    private byte[] rotateLuminance180(byte[] data, int width, int height) {
        byte[] out = new byte[width * height];
        int n = width * height;
        for (int i = 0; i < n; i++) {
            out[n - i - 1] = data[i];
        }
        return out;
    }

    private void finishWithResult(String value) {
        Intent out = new Intent();
        out.putExtra("SCAN_RESULT", value);
        setResult(RESULT_OK, out);
        finish();
    }

    private void finishWithError(String value) {
        Intent out = new Intent();
        out.putExtra("SCAN_ERROR", value);
        setResult(RESULT_CANCELED, out);
        finish();
    }

    private int dp(int value) {
        return (int) (value * getResources().getDisplayMetrics().density + 0.5f);
    }

    private class ScannerOverlay extends View {
        private final Paint paint = new Paint(Paint.ANTI_ALIAS_FLAG);
        private final RectF frame = new RectF();

        ScannerOverlay(Context context) {
            super(context);
        }

        @Override
        protected void onDraw(Canvas canvas) {
            super.onDraw(canvas);
            float w = getWidth();
            float h = getHeight();
            float side = Math.min(w * 0.74f, h * 0.44f);
            frame.set((w - side) / 2f, (h - side) / 2f, (w + side) / 2f, (h + side) / 2f);

            paint.setStyle(Paint.Style.FILL);
            paint.setColor(0x99000000);
            canvas.drawRect(0, 0, w, frame.top, paint);
            canvas.drawRect(0, frame.bottom, w, h, paint);
            canvas.drawRect(0, frame.top, frame.left, frame.bottom, paint);
            canvas.drawRect(frame.right, frame.top, w, frame.bottom, paint);

            paint.setStyle(Paint.Style.STROKE);
            paint.setStrokeWidth(dp(3));
            paint.setColor(0xFF35D3A7);
            canvas.drawRoundRect(frame, dp(22), dp(22), paint);

            paint.setStrokeWidth(dp(7));
            paint.setStrokeCap(Paint.Cap.ROUND);
            float corner = dp(42);
            canvas.drawLine(frame.left + dp(18), frame.top + dp(18), frame.left + corner, frame.top + dp(18), paint);
            canvas.drawLine(frame.left + dp(18), frame.top + dp(18), frame.left + dp(18), frame.top + corner, paint);
            canvas.drawLine(frame.right - dp(18), frame.top + dp(18), frame.right - corner, frame.top + dp(18), paint);
            canvas.drawLine(frame.right - dp(18), frame.top + dp(18), frame.right - dp(18), frame.top + corner, paint);
            canvas.drawLine(frame.left + dp(18), frame.bottom - dp(18), frame.left + corner, frame.bottom - dp(18), paint);
            canvas.drawLine(frame.left + dp(18), frame.bottom - dp(18), frame.left + dp(18), frame.bottom - corner, paint);
            canvas.drawLine(frame.right - dp(18), frame.bottom - dp(18), frame.right - corner, frame.bottom - dp(18), paint);
            canvas.drawLine(frame.right - dp(18), frame.bottom - dp(18), frame.right - dp(18), frame.bottom - corner, paint);
            paint.setStrokeCap(Paint.Cap.BUTT);
            paint.setStyle(Paint.Style.FILL);
        }
    }
}
