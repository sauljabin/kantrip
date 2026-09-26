"use strict";

// Progressive enhancement only: without this script the page shows the full
// static transcript and the install command as plain text.

function setUpCopyButtons() {
  for (const button of document.querySelectorAll("[data-copy]")) {
    const source = document.getElementById(button.dataset.copy);
    const status = button.closest(".install")?.querySelector(".copy-status");
    if (!source || !status || !navigator.clipboard) continue;
    button.hidden = false;
    button.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(source.textContent);
        status.textContent = "Copied to clipboard";
      } catch {
        status.textContent = "Copy failed; select the command instead";
      }
    });
  }
}

// The newest stable release, or the newest pre-release while none is stable.
// The API lists releases newest first and hides drafts from anonymous requests.
function currentRelease(releases) {
  const published = releases.filter((release) => !release.draft && release.tag_name);
  return published.find((release) => !release.prerelease) ?? published[0];
}

async function loadRelease() {
  const line = document.querySelector("[data-release]");
  if (!line) return;
  try {
    const response = await fetch(
      "https://api.github.com/repos/sauljabin/kantrip/releases?per_page=100",
      { headers: { Accept: "application/vnd.github+json" } },
    );
    if (!response.ok) return;
    const release = currentRelease(await response.json());
    if (!release) return;
    const link = line.querySelector("a");
    link.textContent = release.tag_name;
    link.href = `${link.href}/tag/${encodeURIComponent(release.tag_name)}`;
    line.querySelector(".release-label").textContent = release.prerelease
      ? "latest pre-release"
      : "latest release";
    line.dataset.state = release.prerelease ? "pre" : "stable";
  } catch {
    // Offline or rate limited: keep the link to the Releases page.
  }
}

// The animated layer is aria-hidden and rebuilt from the static transcript,
// which stays in the accessibility tree while the replay types over it.
function setUpDemo() {
  const demo = document.querySelector("[data-demo]");
  const controls = document.querySelector(".demo-controls");
  if (!demo || !controls || !("IntersectionObserver" in window)) return;
  const transcript = demo.querySelector(".transcript");
  const toggle = controls.querySelector("[data-demo-toggle]");
  const replay = controls.querySelector("[data-demo-replay]");
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)");
  const spinnerFrames = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"];

  const layer = document.createElement("pre");
  layer.setAttribute("aria-hidden", "true");
  const screen = document.createElement("code");
  layer.append(screen);
  const cursor = document.createElement("span");
  cursor.className = "cursor";
  cursor.textContent = " ";

  let queue = [];
  let index = 0;
  let timer = 0;
  let due = 0;
  let remaining = 0;
  let state = "idle";

  function newLine(className, ...children) {
    cursor.remove();
    if (screen.childNodes.length) screen.append("\n");
    const line = document.createElement("span");
    line.className = className;
    line.append(...children);
    screen.append(line);
    return line;
  }

  function typingDelay(character) {
    return character === " " ? 70 : 30 + ((character.charCodeAt(0) * 7) % 28);
  }

  function buildQueue() {
    const steps = [];
    const at = (delay, action) => steps.push([delay, action]);
    let line = null;
    for (const source of transcript.querySelectorAll(".ln")) {
      if (source.classList.contains("cmd")) {
        const prompt = source.querySelector(".t-prompt");
        const command = source.textContent.slice(prompt.textContent.length);
        at(steps.length ? 650 : 400, () => {
          line = newLine(source.className, prompt.cloneNode(true));
        });
        for (const character of command) {
          at(typingDelay(character), () => line.append(character));
        }
        if (source.dataset.spinner) queueSpinner(at, source.dataset.spinner);
        else at(300, () => {});
      } else {
        at(45, () => {
          line = newLine(source.className, ...source.cloneNode(true).childNodes);
        });
        if (source.dataset.wait) at(Number(source.dataset.wait), () => {});
      }
    }
    at(1200, finish);
    return steps;
  }

  // Rich's transient status: a moon spinner that disappears before the result.
  function queueSpinner(at, message) {
    let spinner = null;
    let frame = null;
    at(300, () => {
      frame = document.createElement("span");
      frame.className = "t-primary";
      spinner = newLine("ln out", frame, ` ${message}`);
    });
    for (let step = 0; step < 16; step += 1) {
      at(80, () => {
        frame.textContent = spinnerFrames[step % spinnerFrames.length];
      });
    }
    at(80, () => {
      spinner.previousSibling.remove();
      spinner.remove();
    });
  }

  function tick() {
    const [, action] = queue[index];
    index += 1;
    action();
    if (state !== "playing") return;
    screen.append(cursor);
    schedule(queue[index][0]);
  }

  function schedule(delay) {
    due = performance.now() + delay;
    timer = window.setTimeout(tick, delay);
  }

  function render() {
    toggle.textContent = state === "paused" ? "Resume" : "Pause";
    toggle.disabled = state === "done";
  }

  function play() {
    window.clearTimeout(timer);
    screen.replaceChildren();
    demo.querySelector(".demo-stage").append(layer);
    demo.classList.add("is-playing");
    controls.hidden = false;
    queue = buildQueue();
    index = 0;
    state = "playing";
    render();
    schedule(queue[0][0]);
  }

  function finish() {
    window.clearTimeout(timer);
    state = "done";
    layer.remove();
    demo.classList.remove("is-playing");
    if (document.activeElement === toggle) replay.focus();
    render();
  }

  toggle.addEventListener("click", () => {
    if (state === "playing") {
      window.clearTimeout(timer);
      remaining = Math.max(0, due - performance.now());
      state = "paused";
    } else if (state === "paused") {
      state = "playing";
      schedule(remaining);
    }
    render();
  });
  replay.addEventListener("click", play);

  const observer = new IntersectionObserver(
    (entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      observer.disconnect();
      if (!reduced.matches) play();
    },
    { threshold: 0.35 },
  );
  if (!reduced.matches) {
    // Hide the static text before the replay starts so it does not flash.
    demo.classList.add("is-playing");
    demo.querySelector(".demo-stage").append(layer);
    screen.append(cursor);
    observer.observe(demo);
  }
  reduced.addEventListener("change", () => {
    if (!reduced.matches) return;
    observer.disconnect();
    finish();
    controls.hidden = true;
  });
}

setUpCopyButtons();
setUpDemo();
loadRelease();
