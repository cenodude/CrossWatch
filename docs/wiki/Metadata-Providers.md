## What metadata does
It’s the translator. Different services use different IDs or titles; metadata maps them to canonical identifiers (IMDb/TMDB/TVDB and anime sources) and normalizes titles/years. The Orchestrator relies on this to match items reliably.

Currently we support only TMDB.

## How it works
1. **Input** – Title + year, or an external ID.
2. **Resolve** – Look up canonical IDs in one or more sources.
3. **Normalize** – Unify type (movie/show/anime), title, year/first-aired, and IDs.
4. **Merge** – Combine results with a defined priority (e.g., TMDB for movies, TVDB for shows; anime via AniList/MAL/Kitsu).
5. **Deliver** – Return a clean, canonical object for planning and writing.

**Common sources**
- Movies/TV: TMDB

## Interface

<img alt="image" src="https://github.com/user-attachments/assets/cec1b654-1248-4e91-8c35-1904ca136921" />

**Advanced Settings**
- **Language/Locale** – Preferred language for titles/overviews. (see supported locales)
- **TTL hour** - how long posters etc should be cached. 

## Supported locales (metadata / translator)
These locale codes control the language/region used for metadata lookups (titles, overviews, and other localized fields).

| Locale | Language | Country/Region |
|---|---|---|
| af-ZA | Afrikaans | South Africa |
| ar-AE | Arabic | United Arab Emirates |
| ar-SA | Arabic | Saudi Arabia |
| be-BY | Belarusian | Belarus |
| bg-BG | Bulgarian | Bulgaria |
| bn-BD | Bengali | Bangladesh |
| ca-ES | Catalan | Spain |
| ch-GU | Chamorro | Guam |
| cn-CN | Chinese | China |
| cs-CZ | Czech | Czech Republic |
| cy-GB | Welsh | United Kingdom |
| da-DK | Danish | Denmark |
| de-AT | German | Austria |
| de-CH | German | Switzerland |
| de-DE | German | Germany |
| el-GR | Greek | Greece |
| en-AU | English | Australia |
| en-CA | English | Canada |
| en-GB | English | United Kingdom |
| en-IE | English | Ireland |
| en-NZ | English | New Zealand |
| en-US | English | United States |
| eo-EO | Esperanto | — |
| es-ES | Spanish | Spain |
| es-MX | Spanish | Mexico |
| et-EE | Estonian | Estonia |
| eu-ES | Basque | Spain |
| fa-IR | Persian | Iran |
| fi-FI | Finnish | Finland |
| fr-CA | French | Canada |
| fr-FR | French | France |
| ga-IE | Irish | Ireland |
| gd-GB | Scottish Gaelic | United Kingdom |
| gl-ES | Galician | Spain |
| he-IL | Hebrew | Israel |
| hi-IN | Hindi | India |
| hr-HR | Croatian | Croatia |
| hu-HU | Hungarian | Hungary |
| id-ID | Indonesian | Indonesia |
| it-IT | Italian | Italy |
| ja-JP | Japanese | Japan |
| ka-GE | Georgian | Georgia |
| kk-KZ | Kazakh | Kazakhstan |
| kn-IN | Kannada | India |
| ko-KR | Korean | South Korea |
| ky-KG | Kyrgyz | Kyrgyzstan |
| lt-LT | Lithuanian | Lithuania |
| lv-LV | Latvian | Latvia |
| ml-IN | Malayalam | India |
| mr-IN | Marathi | India |
| ms-MY | Malay | Malaysia |
| ms-SG | Malay | Singapore |
| nb-NO | Norwegian Bokmål | Norway |
| nl-BE | Dutch | Belgium |
| nl-NL | Dutch | Netherlands |
| no-NO | Norwegian | Norway |
| pa-IN | Punjabi | India |
| pl-PL | Polish | Poland |
| pt-BR | Portuguese | Brazil |
| pt-PT | Portuguese | Portugal |
| ro-RO | Romanian | Romania |
| ru-RU | Russian | Russia |
| si-LK | Sinhala | Sri Lanka |
| sk-SK | Slovak | Slovakia |
| sl-SI | Slovenian | Slovenia |
| sq-AL | Albanian | Albania |
| sr-RS | Serbian | Serbia |
| sv-SE | Swedish | Sweden |
| ta-IN | Tamil | India |
| te-IN | Telugu | India |
| th-TH | Thai | Thailand |
| tl-PH | Filipino (Tagalog) | Philippines |
| tr-TR | Turkish | Turkey |
| uk-UA | Ukrainian | Ukraine |
| vi-VN | Vietnamese | Vietnam |
| zh-CN | Chinese | China |
| zh-HK | Chinese | Hong Kong |
| zh-SG | Chinese | Singapore |
| zh-TW | Chinese | Taiwan |
| zu-ZA | Zulu | South Africa |

**Metadata troubleshooting**
- **Wrong match** → Override with an exact ID or adjust priority.
- **Anime off-by-one** → Switch to an anime source or use anime-friendly mapping.
- **Rate limits** → Slow down heavy backfills or enable caching.
