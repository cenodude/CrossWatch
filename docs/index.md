---
layout: default
title: CrossWatch
---

{% capture readme %}{% include_relative README.md %}{% endcapture %}
{{ readme | markdownify }}

<script src="{{ '/assets/js/cw-lightbox.js' | relative_url }}" defer></script>
