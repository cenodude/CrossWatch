/* assets/helpers/profile-select.js */
(function () {
  const CW = (window.CW ||= {});
  const text = (v, fallback = "") => String(v ?? fallback).trim();
  const keyOf = (v) => CW.ProviderMeta?.keyOf?.(v) || text(v).toUpperCase();
  const labelOf = (v, option) => text(option?.dataset?.label || option?.textContent) || CW.ProviderMeta?.label?.(v) || keyOf(v) || "-";
  const logoOf = (v, option) => text(option?.dataset?.icon || option?.dataset?.logo) || CW.ProviderMeta?.logLogoPath?.(v) || CW.ProviderMeta?.logoPath?.(v) || "";

  function providerOption(value, option) {
    const key = keyOf(value || option?.dataset?.provider);
    const logo = logoOf(key, option);
    return { label: labelOf(key, option), icons: [logo ? { src: logo, alt: "" } : { text: text(key).slice(0, 2) || "?" }] };
  }

  function profileOption(value, option) {
    const label = labelOf(value, option) || "Default";
    return { label, icons: [{ symbol: "account_circle" }], note: text(option?.dataset?.note), showNote: false };
  }

  function enhanceProvider(select, cfg = {}) {
    return CW.IconSelect?.enhance?.(select, { ...cfg, className: `cw-profile-select cw-provider-select ${text(cfg.className)}`.trim(), getOptionData: cfg.getOptionData || providerOption });
  }

  function enhanceProfile(select, cfg = {}) {
    return CW.IconSelect?.enhance?.(select, { ...cfg, className: `cw-profile-select cw-profile-instance-select ${text(cfg.className)}`.trim(), getOptionData: cfg.getOptionData || profileOption });
  }

  function enhancePair(providerSelect, profileSelect, cfg = {}) {
    return { provider: enhanceProvider(providerSelect, cfg.provider || {}), profile: enhanceProfile(profileSelect, cfg.profile || {}) };
  }

  CW.ProfileSelect = { enhanceProvider, enhanceProfile, enhancePair, providerOption, profileOption };
})();
