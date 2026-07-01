/* ==========================================================================
   Learn Duckle - cinematic no-scroll player engine
   Scenes are stepped manually (Prev/Next or arrow keys). Each scene animates
   in on arrival via GSAP. Quiz scenes gate advance. Component-grid scenes are
   generated from window.CATALOG (all 359 components). Degrades without GSAP.
   ========================================================================== */
(function () {
  "use strict";
  var root = document.documentElement;
  var HAS_GSAP = !!window.gsap;
  var REDUCE = false;
  try { REDUCE = window.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch (e) {}
  var gsap = window.gsap;

  /* -------- family metadata for generated grids -------- */
  var FAM = {
    source:   { label: "Sources",    color: "var(--k-source)",    verb: "read data in",     n: 109, ic: '<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/>' },
    transform:{ label: "Transforms", color: "var(--k-transform)", verb: "reshape data",      n: 133, ic: '<path d="M3 4h18l-7 8v7l-4-2v-5z"/>' },
    sink:     { label: "Sinks",      color: "var(--k-sink)",      verb: "write data out",    n: 66,  ic: '<path d="M12 3v12m0 0l-4-4m4 4l4-4M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2"/>' },
    quality:  { label: "Quality",    color: "var(--k-quality)",   verb: "guard data",        n: 25,  ic: '<path d="M12 3l7 3v6c0 4-3 7-7 9-4-2-7-5-7-9V6z"/><path d="M9 12l2 2 4-4"/>' },
    control:  { label: "Control",    color: "var(--k-control)",   verb: "orchestrate steps", n: 19,  ic: '<circle cx="6" cy="6" r="2.5"/><circle cx="6" cy="18" r="2.5"/><circle cx="18" cy="12" r="2.5"/><path d="M8 6h6a4 4 0 0 1 4 4M8 18h6a4 4 0 0 0 4-4"/>' },
    code:     { label: "Code",       color: "var(--k-code)",      verb: "drop to code",      n: 7,   ic: '<path d="M8 8l-4 4 4 4M16 8l4 4-4 4"/>' }
  };

  /* -------- expand grid markers into scenes from the catalog -------- */
  function expandGrids() {
    var cat = window.CATALOG || {};
    var markers = [].slice.call(document.querySelectorAll("[data-grid]"));
    markers.forEach(function (m) {
      var fam = m.getAttribute("data-grid");
      var per = parseInt(m.getAttribute("data-per") || "15", 10);
      var items = cat[fam] || [];
      var meta = FAM[fam] || { label: fam, color: "var(--orange)" };
      var pages = Math.max(1, Math.ceil(items.length / per));
      for (var p = 0; p < pages; p++) {
        var slice = items.slice(p * per, p * per + per);
        var sec = document.createElement("section");
        sec.className = "scene";
        sec.setAttribute("data-chapter", meta.label);
        var cards = slice.map(function (it) {
          var av = it[3] === "p" ? "p" : it[3] === "v" ? "v" : "";
          return '<div class="ccard" data-e style="--kc:' + meta.color + '">' +
            '<div class="cid"><span class="dot ' + av + '"></span>' + esc(it[0]) + '</div>' +
            '<div class="csum">' + esc(it[2] || it[1]) + '</div></div>';
        }).join("");
        sec.innerHTML = '<div class="scene-inner">' +
          '<div class="famhead"><span class="ic" style="background:' + meta.color + '">' +
            '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' + (meta.ic || "") + '</svg></span>' +
            '<div><div class="n" data-e>' + meta.label + ' &middot; ' + (meta.verb || "") + '</div>' +
            '<h2 data-e>' + meta.label + '</h2></div></div>' +
          '<div class="cgrid">' + cards + '</div>' +
          '<div class="cgrid-cap"><span>' + items.length + ' ' + meta.label.toLowerCase() + ' &middot; scroll-free, one page at a time</span>' +
          '<span class="pg" data-e>' + meta.label + ' &middot; ' + (p + 1) + ' / ' + pages + '</span></div>' +
          '</div>';
        m.parentNode.insertBefore(sec, m);
      }
      m.parentNode.removeChild(m);
    });
  }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]; }); }

  /* -------- quizzes -------- */
  function initQuizzes() {
    [].forEach.call(document.querySelectorAll("[data-quiz]"), function (scene) {
      var opts = [].slice.call(scene.querySelectorAll(".q-opt"));
      var wrap = scene.querySelector(".q-opts");
      var explain = scene.querySelector(".q-explain");
      opts.forEach(function (o) {
        o.addEventListener("click", function () {
          if (scene.dataset.answered === "1") return;
          scene.dataset.answered = "1";
          wrap.classList.add("locked");
          var correct = o.hasAttribute("data-correct");
          opts.forEach(function (x) {
            if (x.hasAttribute("data-correct")) x.classList.add("correct");
            else if (x === o) x.classList.add("wrong");
            else x.classList.add("dim");
          });
          if (explain) explain.classList.add("show");
          updateHud();
        });
      });
    });
  }

  /* -------- interactive widgets -------- */
  function initCbmap() { [].forEach.call(document.querySelectorAll('[data-widget="cbmap"]'), buildCbmap); }
  function initTimeTravel() { [].forEach.call(document.querySelectorAll('[data-widget="timetravel"]'), buildTimeTravel); }

  /* ============================ PLAYER ============================ */
  var scenes = [], cur = 0, chapters = [], introEl;
  var elCount, elChapter, btnPrev, btnNext, segWrap;
  function introVisible() { return introEl && !introEl.classList.contains("gone"); }

  function isQuiz(s) { return s.hasAttribute("data-quiz"); }
  function answered(s) { return s.dataset.answered === "1"; }

  function buildPlayer() {
    scenes = [].slice.call(document.querySelectorAll(".scene"));
    elCount = document.getElementById("ctrlCount");
    elChapter = document.getElementById("hudChapter");
    btnPrev = document.getElementById("btnPrev");
    btnNext = document.getElementById("btnNext");
    segWrap = document.getElementById("segs");

    // chapters (ordered unique)
    var seen = {};
    scenes.forEach(function (s) { var c = s.getAttribute("data-chapter") || ""; if (!(c in seen)) { seen[c] = chapters.length; chapters.push(c); } });
    if (segWrap) {
      chapters.forEach(function (c, i) {
        var seg = document.createElement("button"); seg.className = "seg"; seg.type = "button";
        seg.title = c; seg.innerHTML = "<i></i>";
        seg.addEventListener("click", function () { var idx = scenes.findIndex(function (s) { return (s.getAttribute("data-chapter") || "") === c; }); if (idx >= 0) activate(idx); });
        segWrap.appendChild(seg);
      });
    }

    if (btnPrev) btnPrev.addEventListener("click", function () { step(-1); });
    if (btnNext) btnNext.addEventListener("click", function () { step(1); });
    window.addEventListener("keydown", function (e) {
      if (introVisible()) return;
      if (e.target && /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName)) return;
      if (e.key === "ArrowRight" || e.key === "PageDown") { e.preventDefault(); step(1); }
      else if (e.key === "ArrowLeft" || e.key === "PageUp") { e.preventDefault(); step(-1); }
    });

    activate(0);
    var rzt; window.addEventListener("resize", function () { clearTimeout(rzt); rzt = setTimeout(fitActive, 150); });
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(fitActive);
  }

  /* ---- cinematic intro ---- */
  function initIntro() {
    introEl = document.getElementById("intro");
    if (!introEl) return;
    var begun = false;
    function begin() {
      if (begun) return; begun = true;
      introEl.classList.add("gone");
      activate(cur);                       // replay the first scene's reveal on entry
    }
    var b = document.getElementById("introBegin"); if (b) b.addEventListener("click", begin);
    var sk = document.getElementById("introSkip"); if (sk) sk.addEventListener("click", begin);
    introEl.addEventListener("click", function (e) { if (e.target === introEl) begin(); });
    window.addEventListener("keydown", function (e) {
      if (!introVisible()) return;
      if (e.key === "ArrowRight" || e.key === "Enter" || e.key === " " || e.key === "Escape") { e.preventDefault(); begin(); }
    });
  }

  /* ---- auto-fit: scale a scene's content so it never needs scrolling ---- */
  function fitScene(scene) {
    if (!scene) return;
    var inner = scene.querySelector(".scene-inner"); if (!inner) return;
    inner.style.transform = "none";                    // reset to measure natural height
    var cs = window.getComputedStyle(scene);
    var avail = scene.clientHeight - parseFloat(cs.paddingTop) - parseFloat(cs.paddingBottom);
    var h = inner.offsetHeight;
    if (h > avail && h > 0) {
      var scale = Math.max(0.5, avail / h);
      inner.style.transform = "scale(" + scale.toFixed(3) + ")";
    }
  }
  function fitActive() { if (scenes[cur]) fitScene(scenes[cur]); }

  function step(d) {
    var n = cur + d;
    if (n < 0 || n >= scenes.length) return;
    if (d > 0 && isQuiz(scenes[cur]) && !answered(scenes[cur])) return; // gate on quiz
    activate(n);
  }

  function activate(i) {
    scenes.forEach(function (s, j) {
      var on = j === i;
      s.classList.toggle("active", on);
      if (!on) s.classList.remove("revealed");
    });
    cur = i;
    var sc = scenes[i];
    var es = sc.querySelectorAll("[data-e]");
    for (var k = 0; k < es.length; k++) es[k].style.transitionDelay = Math.min(k * 0.05, 0.6) + "s";
    void sc.offsetWidth;                 // reflow so re-entry re-triggers the CSS transition
    sc.classList.add("revealed");
    updateHud();
    fitScene(sc);
    requestAnimationFrame(function () { fitScene(sc); });   // re-measure once layout settles
    playEnter(sc);
  }

  function updateHud() {
    if (elCount) elCount.innerHTML = "<b>" + String(cur + 1).padStart(2, "0") + "</b> / " + scenes.length;
    var ch = scenes[cur].getAttribute("data-chapter") || "";
    if (elChapter) elChapter.innerHTML = "<b>" + (chapters.indexOf(ch) + 1).toString().padStart(2, "0") + "</b> &nbsp; " + ch;
    if (btnPrev) btnPrev.disabled = cur === 0;
    if (btnNext) {
      var lockQuiz = isQuiz(scenes[cur]) && !answered(scenes[cur]);
      btnNext.disabled = cur === scenes.length - 1 || lockQuiz;
      btnNext.querySelector(".lbl").textContent = lockQuiz ? "Answer to continue" : (cur === scenes.length - 1 ? "Done" : "Next");
    }
    // segment states
    if (segWrap) {
      var curCh = chapters.indexOf(ch);
      [].forEach.call(segWrap.children, function (seg, k) {
        seg.classList.toggle("cur", k === curCh);
        seg.classList.toggle("done", k < curCh);
      });
    }
  }

  /* ---- per-scene enter animation (primary reveal is CSS; these add flair) ---- */
  function playEnter(scene) {
    var anim = scene.getAttribute("data-anim");
    if (anim && HAS_GSAP && !REDUCE) {
      if (anim === "canvas") animCanvas(scene);
      else if (anim === "count") animCount(scene);
      else if (anim === "xform") animXform(scene);
    }
    // safety net: guarantee the final state even if the frame loop is throttled
    if (scene._fb) clearTimeout(scene._fb);
    if (anim) scene._fb = setTimeout(function () { finalizeScene(scene); }, 2200);
  }

  function finalizeScene(scene) {
    [].forEach.call(scene.querySelectorAll(".node"), function (n) { n.style.opacity = 1; n.style.transform = "none"; });
    [].forEach.call(scene.querySelectorAll(".wire i"), function (w) { w.style.transform = "scaleX(1)"; });
    [].forEach.call(scene.querySelectorAll(".codep .l"), function (l) { l.style.opacity = 1; });
    var c = scene.querySelector("[data-count]"); if (c) c.textContent = (parseFloat(c.getAttribute("data-count")) || 0).toLocaleString();
    [].forEach.call(scene.querySelectorAll(".xf-out [data-n]"), function (x) { x.textContent = x.getAttribute("data-n"); });
    [].forEach.call(scene.querySelectorAll(".xf-out tbody tr"), function (r) { r.style.opacity = 1; r.style.transform = "none"; });
  }

  function animCanvas(scene) {
    var nodes = scene.querySelectorAll(".node"), wires = scene.querySelectorAll(".wire i"), lines = scene.querySelectorAll(".codep .l");
    gsap.set(nodes, { opacity: 0, y: 12, scale: 0.95 }); gsap.set(wires, { scaleX: 0 }); gsap.set(lines, { opacity: 0.1 });
    var tl = gsap.timeline({ delay: 0.15 });
    var na = [].slice.call(nodes), wa = [].slice.call(wires);
    na.forEach(function (n, i) {
      tl.to(n, { opacity: 1, y: 0, scale: 1, duration: 0.42, ease: "back.out(1.5)" }, i * 0.34);
      if (wa[i]) tl.to(wa[i], { scaleX: 1, duration: 0.3, ease: "power2.out" }, ">-0.05");
    });
    tl.to(lines, { opacity: 1, duration: 0.5, stagger: 0.1 }, ">");
  }

  function animCount(scene) {
    var el = scene.querySelector("[data-count]"); if (!el) return;
    var t = parseFloat(el.getAttribute("data-count")) || 0, o = { v: 0 };
    gsap.to(o, { v: t, duration: 1.5, ease: "power2.out", onUpdate: function () { el.textContent = Math.round(o.v).toLocaleString(); } });
  }

  function animXform(scene) {
    var outRows = scene.querySelectorAll(".xf-out tbody tr"), counts = scene.querySelectorAll(".xf-out [data-n]"), drops = scene.querySelectorAll("tr.drop");
    gsap.set(outRows, { opacity: 0, x: -8 });
    gsap.to(outRows, { opacity: 1, x: 0, duration: 0.42, ease: "power2.out", stagger: 0.09, delay: 0.5 });
    if (drops.length) gsap.fromTo(drops, { backgroundColor: "rgba(255,84,104,0)" }, { backgroundColor: "rgba(255,84,104,0.14)", duration: 0.4, stagger: 0.05, delay: 0.3, yoyo: true, repeat: 1 });
    counts.forEach(function (c) {
      var t = parseFloat(c.getAttribute("data-n")) || 0, o = { v: 0 };
      gsap.to(o, { v: t, duration: 0.9, delay: 0.65, ease: "power2.out", onUpdate: function () { c.textContent = Math.round(o.v); } });
    });
  }

  /* ---- codebase map ---- */
  function crateData() { return {
    engine: { lang: "Rust crate", name: "crates/duckdb-engine", body: "The heart. Turns a pipeline graph into a plan of DuckDB SQL stages and executes them - planning, per-component SQL builders, external-driver connectors, the executor, merged TLS roots. Every other surface calls into this one crate.", files: ["plan/mod.rs", "plan/builders.rs", "plan/graph.rs", "connectors.rs", "executor", "tls.rs"] },
    metadata: { lang: "Rust crate", name: "crates/metadata", body: "The shared vocabulary. Defines the PipelineNode, NodeData, Schema and edge types the engine, runner, MCP server and desktop back end all speak. One source of truth for what a pipeline is on disk.", files: ["PipelineNode", "NodeData", "Schema", "Edge"] },
    runner: { lang: "Rust binary", name: "crates/duckle-runner", body: "The headless CLI. Runs a pipeline .json with no GUI - ideal for cron, systemd or CI. Its serve mode hosts a web console with Operations, Pipelines, Runs history and an interval + cron scheduler.", files: ["main.rs", "serve.rs", "panel.html"] },
    mcp: { lang: "Rust binary", name: "crates/duckle-mcp", body: "The LLM bridge. A stdio MCP server that lets any client list components, fetch a schema, create a validated pipeline, run it, read logs, build a standalone binary - plus lineage, verify and trust review tools.", files: ["main.rs", "catalog.json", "tools"] },
    lance: { lang: "Rust sidecar", name: "crates/duckle-lance", body: "The vector sidecar. Owns LanceDB and Vortex behind a Parquet bridge, isolating heavier Arrow / DataFusion / protoc dependencies from the main engine so the core stays lean.", files: ["lancedb", "vortex", "parquet-bridge"] },
    desktop: { lang: "Tauri app", name: "apps/desktop", body: "The studio shell. A Tauri (Rust) back end that hosts the canvas, manages the workspace, encrypts connection secrets at rest, runs Duckie (local Qwen via llama.cpp), and bridges the front end to the engine and MCP.", files: ["main.rs", "secrets.rs", "workspace_git.rs", "llama"] },
    frontend: { lang: "React + Vite", name: "frontend", body: "The canvas you draw on - the node graph, the palette, the Visual Mapper, the properties panel, live preview, and the Runs / Console / Plan tabs. Compiled and embedded so the desktop app and duckle serve share the exact same UI.", files: ["App.tsx", "workflow-ui", "PropertiesPanel", "component-manifests"] }
  }; }
  function buildCbmap(el) {
    var C = crateData();
    var btns = [].slice.call(el.querySelectorAll(".crate-btn"));
    var detail = el.querySelector(".crate-detail");
    function show(key, btn) {
      var c = C[key]; if (!c) return;
      btns.forEach(function (b) { b.classList.toggle("on", b === btn); });
      detail.innerHTML = '<div class="lang">' + c.lang + '</div><h4>' + c.name + '</h4><p>' + c.body + '</p><div class="files">' + c.files.map(function (f) { return "<code>" + f + "</code>"; }).join("") + "</div>";
    }
    btns.forEach(function (b) { b.addEventListener("click", function () { show(b.getAttribute("data-crate"), b); }); });
    if (btns[0]) show(btns[0].getAttribute("data-crate"), btns[0]);
  }

  /* ---- time-travel scrubber ---- */
  function buildTimeTravel(el) {
    var range = el.querySelector(".tt-range"), fill = el.querySelector(".tt-fill"), asof = el.querySelector(".tt-asof"), tbody = el.querySelector("tbody");
    var SNAP = [
      ["2024-01-05", [["Austin", "12,400"], ["Denver", "0"], ["Miami", "3,100"]]],
      ["2024-02-05", [["Austin", "18,900"], ["Denver", "6,250"], ["Miami", "4,700"]]],
      ["2024-03-05", [["Austin", "24,100"], ["Denver", "11,800"], ["Miami", "9,300"]]],
      ["2024-04-05", [["Austin", "29,750"], ["Denver", "15,400"], ["Miami", "13,900"]]]
    ];
    function render(i) {
      var s = SNAP[i];
      if (asof) asof.textContent = s[0];
      if (fill) fill.style.width = (i / (SNAP.length - 1) * 100) + "%";
      if (tbody) tbody.innerHTML = s[1].map(function (r) { return "<tr><td class='key'>" + r[0] + "</td><td class='num'>" + r[1] + "</td></tr>"; }).join("");
    }
    if (range) { range.min = 0; range.max = SNAP.length - 1; range.step = 1; range.value = SNAP.length - 1; range.addEventListener("input", function () { render(parseInt(range.value, 10)); }); }
    render(SNAP.length - 1);
  }

  /* ============================ BOOT ============================ */
  try { expandGrids(); } catch (e) { if (window.console) console.warn("[learn] grids", e); }
  try { initQuizzes(); } catch (e) {}
  try { initCbmap(); } catch (e) {}
  try { initTimeTravel(); } catch (e) {}
  buildPlayer();
  try { initIntro(); } catch (e) {}
})();
