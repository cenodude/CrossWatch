/* helpers/events.js – mini event bus + DOM bridge */
(function(){ const bus=new Map(); function on(t,fn){ if(!bus.has(t)) bus.set(t,new Set()); bus.get(t).add(fn); return ()=>bus.get(t)?.delete(fn);} function emit(t,d){ (bus.get(t)||[]).forEach(fn=>{try{fn(d);}catch(e){console.warn(e);}}); document.dispatchEvent(new CustomEvent(t,{detail:d})); }
  (window.CW ||= {}); window.CW.Events={on,emit};
})();