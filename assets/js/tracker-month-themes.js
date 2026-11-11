/* assets/js/tracker-month-themes.js */
/* CrossWatch - Shared monthly poster and banner themes */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

export const themes = [
  {id:"new-year",name:"New Year",a:"#ffe2a1",b:"#a6bcff",ink:"#232b57",motif:"fireworks"},
  {id:"valentine",name:"Valentine's Day",a:"#ffb5c8",b:"#ff749a",ink:"#532237",motif:"hearts"},
  {id:"patrick",name:"St. Patrick's Day",a:"#b9efa6",b:"#efcc78",ink:"#174d39",motif:"clover"},
  {id:"easter",name:"Easter",a:"#ffe5a7",b:"#c9adff",ink:"#493858",motif:"eggs"},
  {id:"spring",name:"Spring",a:"#ffc9dc",b:"#b4e6b4",ink:"#35483d",motif:"blossoms"},
  {id:"summer",name:"Summer",a:"#ffdc89",b:"#74e3dd",ink:"#154c59",motif:"sun"},
  {id:"autumn",name:"Autumn",a:"#ffc07e",b:"#e18c99",ink:"#563139",motif:"leaves"},
  {id:"winter",name:"Winter",a:"#c2efff",b:"#acb9ff",ink:"#263b59",motif:"snow"},
  {id:"thanksgiving",name:"Thanksgiving",a:"#ffd08a",b:"#d5996c",ink:"#51362a",motif:"harvest"},
  {id:"carnival",name:"Carnival",a:"#ffc86d",b:"#cb9cff",ink:"#42265d",motif:"mask"},
  {id:"diwali",name:"Diwali",a:"#ffda81",b:"#ff93bb",ink:"#562647",motif:"lamps"},
  {id:"lunar-new-year",name:"Lunar New Year",a:"#ffe09a",b:"#ff9094",ink:"#642b32",motif:"lanterns"},
  {id:"eid",name:"Eid",a:"#f2dea4",b:"#9cddcf",ink:"#194b4d",motif:"crescent"},
  {id:"halloween",name:"Halloween",a:"#ffb777",b:"#be9aff",ink:"#49304e",motif:"bats"},
  {id:"christmas",name:"Christmas",a:"#f7d49a",b:"#98dcbf",ink:"#244b40",motif:"evergreen"},
];
export const holidays = themes.filter(t=>!["spring","summer","autumn","winter"].includes(t.id));
const byId = new Map(themes.map(t=>[t.id,t]));
const common = ["new-year","valentine","patrick","easter","halloween","christmas"];
const memory = new Map(), calendarCache = new Map();
const diwaliMonths = {2024:10,2025:10,2026:11,2027:10};

export function preferenceKey(instance = "default") {
  const auth = globalThis.window?.CW?.AuthState?.read?.() || {}, view = globalThis.window?.CW?.OverviewProfile;
  return `cw-month-style:${JSON.stringify([auth.user?.id || auth.user?.username || auth.role || "local",auth.profileId || "",auth.isAdmin ? view?.id || "" : "",instance])}`;
}

export function readPreferences(instance) {
  const key = preferenceKey(instance);
  let value = memory.get(key) || {};
  try {value = JSON.parse(localStorage.getItem(key)) || value;} catch {}
  if (!value || typeof value !== "object") value = {};
  const overrides = Object.fromEntries(Object.entries(value.overrides || {}).filter(([month,id])=>/^\d{4}-\d{2}$/.test(month) && byId.has(id)).slice(-48));
  return {hemisphere:value.hemisphere === "south" ? "south" : "north",effects:value.effects !== false,
    enabled:Array.isArray(value.enabled) ? value.enabled.filter(id=>holidays.some(t=>t.id === id)) : [...common],
    thanksgiving:value.thanksgiving === "canada" ? "canada" : "us",overrides};
}

export function savePreferences(instance, patch) {
  const key = preferenceKey(instance), next = {...readPreferences(instance),...patch};
  memory.set(key,next);
  try {localStorage.setItem(key,JSON.stringify(next));} catch {}
  globalThis.window?.dispatchEvent(new CustomEvent("cw:month-theme-changed",{detail:{key}}));
}

function easter(year) {
  const a=year%19,b=Math.floor(year/100),c=year%100,d=Math.floor(b/4),e=b%4,f=Math.floor((b+8)/25),g=Math.floor((b-f+1)/3),h=(19*a+b-d-g+15)%30,i=Math.floor(c/4),k=c%4,l=(32+2*e+2*i-h-k)%7,m=Math.floor((a+11*h+22*l)/451);
  return new Date(Date.UTC(year,Math.floor((h+l-7*m+114)/31)-1,(h+l-7*m+114)%31+1,12));
}

function lunarMonths(year, calendar, predicate) {
  const result = new Set();
  try {
    const format = new Intl.DateTimeFormat(`en-u-ca-${calendar}`,{month:"numeric",day:"numeric",timeZone:"UTC"});
    if (format.resolvedOptions().calendar !== calendar) return [];
    for (let day=0;day<366;day++) {
      const date = new Date(Date.UTC(year,0,day+1,12));
      if (date.getUTCFullYear() !== year) break;
      const parts = Object.fromEntries(format.formatToParts(date).map(p=>[p.type,p.value]));
      if (predicate(Number(parts.month),Number(parts.day))) result.add(date.getUTCMonth()+1);
    }
  } catch {}
  return [...result];
}

export function holidayMonths(id, year, thanksgiving = "us") {
  const key = `${id}:${year}:${thanksgiving}`;
  if (calendarCache.has(key)) return calendarCache.get(key);
  const fixed = {"new-year":[1],valentine:[2],patrick:[3],halloween:[10],christmas:[12],thanksgiving:[thanksgiving === "canada" ? 10 : 11]};
  let result = fixed[id] || [];
  if (id === "easter" || id === "carnival") {const date=easter(year); if (id === "carnival") date.setUTCDate(date.getUTCDate()-47); result=[date.getUTCMonth()+1];}
  if (id === "diwali") result=diwaliMonths[year] ? [diwaliMonths[year]] : [];
  if (id === "lunar-new-year") result=lunarMonths(year,"chinese",(m,d)=>m===1&&d===1);
  if (id === "eid") result=lunarMonths(year,"islamic-umalqura",(m,d)=>m===10&&d===1 || m===12&&d===10);
  calendarCache.set(key,result);
  if (calendarCache.size>128) calendarCache.delete(calendarCache.keys().next().value);
  return result;
}

export function resolveTheme(month, prefs) {
  if (byId.has(prefs.overrides?.[month])) return byId.get(prefs.overrides[month]);
  const [year,number] = String(month).split("-").map(Number);
  const enabled = [...(prefs.enabled || [])].sort((a,b)=>Number(common.includes(a))-Number(common.includes(b)));
  const holiday = enabled.find(id=>holidayMonths(id,year,prefs.thanksgiving).includes(number));
  if (holiday) return byId.get(holiday);
  const shifted = prefs.hemisphere === "south" ? (number+5)%12+1 : number;
  return byId.get(shifted>=3&&shifted<=5 ? "spring" : shifted>=6&&shifted<=8 ? "summer" : shifted>=9&&shifted<=11 ? "autumn" : "winter");
}

const star = '<path d="m0-16 4 12 12 4-12 4-4 12-4-12-12-4 12-4Z"/>';
const leaf = '<path d="M0 38C-60 10-30-45 0-48 30-45 60 10 0 38ZM0-35v90m0-35-21-20m21 4L21-8"/>';
const heart = '<path d="M0 35C-75-8-35-65 0-30 35-65 75-8 0 35Z"/>';
const flake = `<g>${[0,60,120].map(a=>`<path transform="rotate(${a})" d="M0-45v90m0-75-12-9m12 9 12-9M0 30l-12 9m12-9 12 9"/>`).join("")}</g>`;
const blossom = `<g>${[0,72,144,216,288].map(a=>`<ellipse transform="rotate(${a})" cy="-20" rx="14" ry="24"/>`).join("")}<circle r="8" class="mr-decor-secondary"/></g>`;
const lantern = '<path d="M0-82v30m-19 0h38m-38 82h38M0 34v28m-8-7v19m16-19v19"/><ellipse cy="-10" rx="38" ry="42"/><ellipse cy="-10" rx="18" ry="42"/><path d="M-36-10h72"/>';

function motif(kind) {
  if (kind === "fireworks") return `<g class="mr-firework">${Array.from({length:12},(_,i)=>`<path transform="rotate(${i*30})" d="M0-16v-20m0-10v-9"/>`).join("")}<circle r="5"/></g>`;
  if (kind === "hearts") return heart;
  if (kind === "clover") return `<g>${[0,120,240].map(a=>`<path transform="rotate(${a})" d="M0 8C-55-22-25-63 0-38 25-63 55-22 0 8Z"/>`).join("")}<path d="M0 7q-6 35 18 47"/></g>`;
  if (kind === "eggs") return '<path d="M-30 12C-34-45 0-70 16-29 48 24 31 47 0 46-20 46-30 30-30 12Z"/><path class="mr-decor-secondary" d="m-27 2 14-8L0 2l14-8 14 8M-26 22h53"/><circle cy="-22" r="5"/>';
  if (kind === "blossoms") return blossom;
  if (kind === "sun") return `<circle r="30"/>${Array.from({length:12},(_,i)=>`<path transform="rotate(${i*30})" d="M0-43v-12"/>`).join("")}<path class="mr-decor-secondary" d="M-70 65q18-14 36 0t36 0t36 0t36 0M-70 78q18-14 36 0t36 0t36 0t36 0"/>`;
  if (kind === "leaves") return leaf;
  if (kind === "snow") return flake;
  if (kind === "harvest") return `<g>${[-60,-30,0,30,60].map(a=>`<ellipse transform="rotate(${a} 0 25)" cy="-15" rx="13" ry="44"/>`).join("")}<path class="mr-decor-secondary" d="M-38 28q38 52 76 0Z"/></g>`;
  if (kind === "mask") return '<path d="M-65-15q35-35 65-6 30-29 65 6L50 18Q25 45 0 10q-25 35-50 8Z"/><path class="mr-decor-cut" d="M-45-10q24-10 32 8-22 17-32-8Zm90 0q-24-10-32 8 22 17 32-8Z"/><path d="m-55 22-25 22m135-22 25 22M-42-28l-8-24m92 24 8-24"/>';
  if (kind === "lamps") return '<path d="M-47 13q47 63 94 0Z"/><path d="M-43 13q43-20 86 0M-29 30h58"/><path class="mr-flame mr-decor-secondary" d="M0 7C-29-9-2-30 0-51 30-25 29-4 0 7Z"/><path d="m-60-10-8-10m128 10 8-10M0-67v-12"/>';
  if (kind === "lanterns") return lantern;
  if (kind === "crescent" || kind === "bats") return `<path d="M18-49a51 51 0 1 0 33 78A53 53 0 0 1 18-49Z"/>${kind === "bats" ? '<g class="mr-decor-cut" transform="translate(-70 -30) scale(.65)"><path d="m-32 0 17 5 10-12 5 8 5-8 10 12 17-5-10 22-22-7-22 7Z"/></g>' : `<g class="mr-decor-secondary" transform="translate(42 -38) scale(.7)">${star}</g>`}`;
  return '<path d="M0-68-28-30h15L-42 5h23l-38 43H57L19 5h23L13-30h15ZM0 48v20"/><g class="mr-decor-secondary"><circle cx="-9" cy="-20" r="4"/><circle cx="13" cy="9" r="4"/><circle cx="-20" cy="30" r="4"/><path d="m0-88 3 9 9 3-9 3-3 9-3-9-9-3 9-3Z"/></g>';
}

export function decorations(theme, compact = false) {
  const main = motif(theme.motif), secondary = ["winter","christmas"].includes(theme.id) ? flake : ["spring","easter"].includes(theme.id) ? blossom : star;
  const placements = compact ? [[840,90,.85,-8],[960,22,.32,12]] : [[100,130,1.2,-12],[865,130,1.1,12],[35,315,.5,20],[950,325,.65,-20]];
  const details = Array.from({length:compact?7:14},(_,i)=>`<g class="mr-decor-spark" style="--d:${i%5}s" transform="translate(${compact?590+i*55:(i%2?750:25)+(i*29)%220} ${25+(i*47)%(compact?140:360)}) scale(${.12+(i%3)*.06})">${secondary}</g>`).join("");
  const accents = theme.id === "christmas" ? '<g class="mr-ornaments mr-decor-secondary"><path d="M200 0v50m600-50v37m270-37v65"/><circle cx="200" cy="59" r="9"/><circle cx="600" cy="46" r="9"/><circle cx="870" cy="74" r="9"/></g>' : "";
  return `<svg class="mr-season-art mr-themed-art" viewBox="0 0 1000 ${compact?180:420}" preserveAspectRatio="xMidYMid slice" aria-hidden="true" focusable="false"><g class="mr-decor">${placements.map(([x,y,s,r],i)=>`<g transform="translate(${x} ${y}) rotate(${r}) scale(${s})"><g class="mr-decor-float" style="--d:${i*2}s">${main}</g></g>`).join("")}${details}${accents}</g></svg>`;
}

export function paintTheme(host, month, instance, compact = false) {
  const prefs=readPreferences(instance), theme=resolveTheme(month,prefs);
  for (const key of ["a","b","ink"]) host.style.setProperty(`--mr-${key}`,theme[key]);
  host.dataset.monthTheme=theme.id;
  host.classList.toggle("mr-effects-off",!prefs.effects);
  const slot=host.querySelector(compact ? ".trk-month-decoration" : ".mr-decoration");
  if (slot) slot.innerHTML=decorations(theme,compact);
  return theme;
}
