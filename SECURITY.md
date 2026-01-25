# Security Policy

CrossWatch is NOT meant to be exposed directly to the public internet.

* Do **NOT** port-forward `8787` from your router or expose the web UI directly to WAN.
* Run CrossWatch on your **local network**, or access it via:
  * a **VPN** (WireGuard, Tailscale, etc.)
* Anyone who can reach the web UI can change sync pairs, tokens and settings.
  * use CrossWatch authentication to set a username/password

## Supported Versions
> Security fixes land on `main` first, then the next release.

## Reporting a Vulnerability

**Please do not open public GitHub Issues for security bugs.**\
Instead, use **GitHub Security Advisories** (preferred):\
Repo **Security** → **Advisories** → **New draft security advisory**.

If Advisories are unavailable for some reason:

* open a GitHub Issue titled **“Security: need private contact”**
* include **no technical details**
* ask for a private channel

### What to include

* A clear description of the issue and impact
* Reproduction steps or a minimal PoC
* Any relevant logs **with secrets removed**
* Suggested fix (optional but appreciated)

### What you can expect

* Acknowledgement: typically within a few days
* Fix/mitigation: as fast as practical depending on severity
* Coordinated disclosure: we prefer responsible disclosure (often up to \~90 days for public disclosure, sooner if actively exploited)

## CrossWatch Security Notes

### 1) Don’t expose CrossWatch to the public internet

CrossWatch is intended for **local use**. If you expose it publicly everybody can access your configurations and UI.

Recommended:

* bind to localhost / LAN only
* firewall the port
* if you must access remotely: use a VPN (WireGuard/Tailscale) or a reverse proxy with **strong authentication**

### 2) Treat the API/UI as “trusted user only”

CrossWatch includes endpoints that read/write configuration and orchestrate sync runs. Keep access restricted.

### 3) Secrets are stored locally

CrossWatch stores tokens/keys in a local `config.json` (location depends on `CONFIG_BASE`, and typically `/config` in containers).

* protect the config directory with proper OS permissions
* don’t commit config files to git
* rotate tokens immediately if you suspect exposure

### 4) TLS verification

Some providers support `verify_ssl=false` for self-signed setups. That’s convenient, but it weakens transport security.

## Scope

In scope:

* vulnerabilities in CrossWatch code (API/UI, sync logic, file handling, auth flows, etc.)
* sensitive data exposure (tokens, credentials, personal data)
* RCE, SSRF, path traversal, authz bypass, deserialization bugs, etc.

Out of scope:

* vulnerabilities in third-party services (Plex/Trakt/SIMKL/Jellyfin/Emby/etc.)
* “my reverse proxy is misconfigured” (we can still help, but it’s not a product vulnerability)

## Thanks

If you report something responsibly, you’ll get credit in the advisory/release notes (unless you prefer to stay anonymous).
