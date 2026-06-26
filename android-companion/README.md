# CrossWatch Companion for Android

A small Android companion app built as a side project for CrossWatch.

This is an early preview app, not a production-ready mobile release yet. Basically, DO NOT USE IT!
The app is meant for quickly checking CrossWatch from a phone or tablet. It does not replace the full web interface. You can view the current status, check recent activity, monitor providers, run a few basic actions, and change some app settings.

## Current features
* Native Android app
* Bottom navigation on phones
* Navigation rail and wider layouts on tablets
* Support for a CrossWatch server URL
* Status information from `/api/mobile/summary`
* Sample data when the server cannot be reached
* QR/code device pairing using scoped mobile tokens
* Actions for running a sync, creating a backup, and stopping the watcher

Sync pair configuration is not included. This remains part of the main CrossWatch web interface.

## Pairing

1. Open an authenticated CrossWatch web session
2. Go to Settings -> UI and Security -> Security
3. Use the CrossWatch companion app block to add a device
4. CrossWatch returns a QR code, short pairing code, and URI similar to:

```text
crosswatch://pair?server=...&code=...
```

5. Scan the QR code in the Android app, or paste the code/URI manually
6. When a URI is opened directly on Android, the app opens and completes the pairing automatically
7. The app claims the code through `/api/mobile/pairing/claim`
8. The returned device token is stored on the device and sent with requests as:

```http
Authorization: Bearer <token>
```

CrossWatch stores a hash of each device token under `mobile_auth.devices`.

Authenticated users can view and revoke paired devices through `/api/mobile/devices`.

## Distribution status

The manual build currently produces a debug-signed APK for testing:

```text
app\build\manual\crosswatch-companion-debug.apk
```

Use that APK for sideload testing. Do not publish it as a production artifact. A production release should use a dedicated release keystore, a versioned build pipeline, changelog/release notes, and a final pass over mobile auth behavior.

## Building the app locally

Normally, the project can be opened and built using Android Studio.

A manual build script is also included:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\build-manual-apk.ps1
```

The generated APK can be found here:

```text
app\build\manual\crosswatch-companion-debug.apk
```

The script uses the installed Android SDK and JDK. It can also use the workspace-local SDK cache when direct SDK access is unavailable.
