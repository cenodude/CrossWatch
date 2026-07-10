# Security Policy

CrossWatch is designed for trusted local use and should not be exposed directly to the public internet.

Use it on your local network, through a VPN such as WireGuard or Tailscale, or behind a reverse proxy with strong authentication and TLS. CrossWatch authentication and its built in self signed certificate can also be enabled.

## Supported Versions

Security fixes are applied to `main` first and included in the next release.

## Reporting a Vulnerability

Do not report security issues through public GitHub Issues.

Use GitHub Security Advisories:

Repository, **Security**, **Advisories**, **New draft security advisory**

Include:

* A description of the issue and its impact
* Reproduction steps or a minimal proof of concept
* Relevant logs with secrets removed
* A suggested fix, when available

When Security Advisories are unavailable, open an issue titled **Security: need private contact** without technical details.

## Security Notes

### Access

Restrict access to trusted users. CrossWatch can read and modify configuration, credentials, provider data and sync state.

Do not port forward port `8787` directly to the internet.

### Credentials

Sensitive values are encrypted in `config.json` and masked in logs. This reduces accidental exposure but does not protect against a compromised host.

Protect the configuration directory, do not commit configuration files, and rotate credentials after suspected exposure.

### TLS

Disabling SSL verification can support self signed provider setups, but it weakens transport security. Use it only on trusted networks.

## Scope

In scope:

* CrossWatch API, UI, authentication, sync logic and file handling
* Credential or personal data exposure
* Remote code execution
* Server side request forgery
* Path traversal
* Authorization bypass

Out of scope:

* Vulnerabilities in third party services
* Reverse proxy, firewall or network configuration errors

Responsible reports may receive credit in the advisory or release notes, unless anonymity is requested.
