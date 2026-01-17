## Jellyfin

Connect with your **Server URL** and a **user token** (or generate one from username/password). Pick the correct **user** and (optionally) **library scope**. If things get weird, regenerate the token and verify the URL.

![Jellyfin connection screen](https://github.com/user-attachments/assets/f6e4c9f6-ffaf-48be-a4c5-f4f619d922bd)

**Required steps**
1. Fill in your **Server URL**, **Username**, and **Password**.
2. Click **Sign In**; you should see the *Jellyfin Connected* banner. If not, repeat step 1.
3. Click **Settings**.
4. Make sure your **Server URL**, **Username**, and **User ID** are filled in. Click **Auto-Fetch** to retrieve your User ID.
5. If you don’t see your libraries, click **Load Libraries**. If you see your libraries, you're good to go. Click the **Save** button.

## Emby

Same playbook as Jellyfin: use your **Server URL** and **API key / user token**. Select the right **user** and (optionally) **library scope**. If requests fail, issue a new token from the dashboard and confirm the base URL (HTTP/HTTPS) is reachable from CrossWatch.

![Emby connection screen](https://github.com/user-attachments/assets/0af1c3ba-8d14-4ee5-84cb-b930d85d997a)

**Required steps**
1. Fill in your **Server URL**, **Username**, and **Password**.
2. Click **Sign In**; you should see the *Emby Connected* banner. If not, repeat step 1.
3. Click **Settings**.
4. Make sure your **Server URL**, **Username**, and **User ID** are filled in. Click **Auto-Fetch** to retrieve your User ID.
5. If you don’t see your libraries, click **Load Libraries**. If you see your libraries, you're good to go. Click the **Save** button.

## Plex

Sign in and approve the app. We store your **account token** (not your password). Prefer a **local PMS address** when available. If you run multiple servers, set the **Server UUID** where needed.

![Plex connection screen](https://github.com/user-attachments/assets/88d19074-d979-4a23-8f12-ce94dce725ad)

**Required steps**
1. Click on **CONNECT PLEX**.
2. A new tab/browser will open with Plex to enter the PIN. Copy/paste the PIN from the **Link code (PIN)** field.
3. Within ~10 seconds the **Current Token** should have a value. If not, repeat step 1 after 5 minutes.
4. Click on **Settings**.
5. Make sure your **Server URL**, **Username** and **Account ID** are filled in. For PMS owner this should be **ID 1**.
6. Click on **Load Libraries**. If you see your libraries, you're good to go. Click on the **Save** button.

*Tip: if you have issues, use your local server IP address, such as `http://192.168.1.1:32400`.*


## SIMKL
Approve the app (OAuth or device PIN). You get a **long‑lived access token**. If lists look stale, reconnect or trigger a fresh read to bust caches.

## Trakt
Approve the app in your browser. If access is revoked or expired, reconnect. We attempt **automatic refresh** where supported.

## MDBList
Connect your API, that's it.

## Tautulli

Enter your **Tautulli Server URL** (for example: `http://192.168.2.133:8181`) and your **API key**.

**Where to find the API key**
In Tautulli go to: **Settings -> Web Interface -> API**
- Enable the API (if it isn’t already)
- Copy the API key
- Paste it into CrossWatch

**User ID (recommended)**
If you don’t set a User ID, CrossWatch will import **history for all users** on that Tautulli instance.

To find your User ID:
1. In Tautulli, open **Users**
2. Click the user you want
3. Look at the browser URL, it will contain something like:

`/user?user_id=9999999`
That number (`9999999`) is the **User ID** to enter in CrossWatch.
