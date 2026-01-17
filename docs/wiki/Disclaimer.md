# Early development
CrossWatch is still evolving and **can break**.  
**Use provider backups (SIMKL / Trakt / etc.) and CrossWatch tracker exports** before you start experimenting.
</div>

# Disclaimer
This is an independent, community-maintained project and is not affiliated with, endorsed by, or sponsored by Plex, Emby, Jellyfin, Trakt, Simkl or MDBlist. Use at your own risk. All product names, logos, and brands are property of their respective owners and used for identification only. Interacts with third-party services; **you are responsible for complying with their Terms of Use and API rules.** **Provided “as is,” without warranties or guarantees.**

# Security
CrossWatch is NOT meant to be exposed directly to the public internet.
During the current development stage there is also NO authentication built in, so treat it as a LAN/VPN-only tool.

- Do **NOT** port-forward `8787` from your router or expose the web UI directly to WAN.
- Run CrossWatch on your **local network**, or access it via:
  - a **VPN** (WireGuard, Tailscale, etc.)
- Anyone who can reach the web UI can change sync pairs, tokens and settings.

# Dependencies
- **fastapi** API server  
- **pydantic** request/response models
- **uvicorn** ASGI server to run FastAPI  
- **requests** HTTP client for external APIs  
- **plexapi** Plex API client - third party project
- **websocket-client** WebSocket client (sync/events where applicable)  
- **websockets** asyncio WebSocket support  
- **python-multipart** multipart/form-data support  
- **packaging** version parsing/feature gating  

# Additional information 
_I didn’t think I’d ever need to write this, but here we are_.<br>
A small, but increasingly loud, minority of people (especially on Reddit) show up with demands, as if this project is required to follow their rules. It isn’t. Let me be absolutely clear: I don’t owe anyone anything. This is a hobby project I work on in my free time. Use it if it fits your needs; if it doesn’t, move on. I’m not interested in lectures about your principles, what I can or can’t do, whether I can use AI or not, or any other opinion pieces aimed at my project. You’re are not entitled to receive support here; i’m not your customer support agent, but I will absolutely help people who are respectful and constructive.<br>

**If you’re here to demand, complain, or moralize: LEAVE!**




