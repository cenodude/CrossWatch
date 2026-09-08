/* assets/js/events/page.js */
/* CrossWatch - Events page navigation and retained view */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

const viewURL = new URL('./index.js', import.meta.url);
viewURL.searchParams.set('v', new URL(import.meta.url).searchParams.get('v') || window.APP_VERSION || '1');
const { default: EventsView } = await import(viewURL.href);

export function readEventsRoute(hash = window.location.hash) {
  const query = new URLSearchParams(String(hash).split('?')[1] || '');
  const props = Object.fromEntries(['groupId', 'runId', 'domain', 'visibility', 'mode', 'returnTo', 'returnContext'].map(key => [key, query.get(key) || '']));
  if (!/^\d+$/.test(props.groupId)) props.groupId = '';
  return props;
}

let host = null;
let context = '';
let pending = null;
const contextKey = () => {
  const auth = window.CW?.AuthState?.read?.();
  return JSON.stringify([auth?.user?.id, auth?.user?.username, auth?.isManaged, auth?.permissions,
    window.CW?.OverviewProfile?.id || '', readEventsRoute()]);
};

const EventsPage = {
  async mount(root) {
    if (!root) return;
    const next = contextKey();
    if (host === root && context === next && (pending || root.children.length)) {
      await pending;
      EventsView.show?.();
      return;
    }
    host = root;
    context = next;
    pending = EventsView.mount(root, readEventsRoute());
    try { await pending; } catch (error) { context = ''; throw error; }
  },
  hide() { EventsView.hide?.(); },
  unmount() {
    EventsView.unmount();
    host?.replaceChildren();
    host = null;
    context = '';
    pending = null;
  },
};
window.EventsPage = EventsPage;
for (const name of ['cw:overview-profile-changed', 'cw:auth-state-changed']) {
  window.addEventListener(name, () => {
    if (context === contextKey()) return;
    const root = host;
    EventsPage.unmount();
    if (root && document.documentElement.dataset.tab === 'events') {
      EventsPage.mount(root).catch(() => {
        root.innerHTML = '<div class="cw-page-load-error">Events failed to load. Refresh the page and try again.</div>';
      });
    }
  });
}
export default EventsPage;
