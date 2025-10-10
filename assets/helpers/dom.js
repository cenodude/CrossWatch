/* helpers/dom.js – small DOM utils */
(function(){ const $=(id,root=document)=>root.getElementById?root.getElementById(id):document.getElementById(id);
  const qs=(s,root=document)=>root.querySelector(s); const qsa=(s,root=document)=>Array.from(root.querySelectorAll(s));
  function on(el,ev,fn,opt){ if(el) el.addEventListener(ev,fn,opt||false); }
  function toast(msg, ok=true){ try{ const el=$("save_msg")||qs(".save-toast"); if(!el) return console.log(msg); el.textContent=msg; el.classList.remove("hide","error","ok"); el.classList.add(ok?"ok":"error"); el.classList.remove("hide"); setTimeout(()=>el.classList.add("hide"),1600);}catch{} }
  function fixLabels(root=document){ let uid=0; qsa("label",root).forEach(l=>{ if(l.hasAttribute("for")) return; const owned=l.querySelector("input,select,textarea"); if(owned) return; let c=l.nextElementSibling; while(c && !c.matches?.("input,select,textarea")) c=c.nextElementSibling; if(!c) c=l.parentElement?.querySelector?.("input,select,textarea"); if(!c) return; if(!c.id) c.id="auto_lbl_"+(++uid); l.setAttribute("for",c.id); }); }
  (window.CW ||= {}); window.CW.DOM={$,qs,qsa,on,showToast:toast,fixFormLabels:fixLabels};
})();