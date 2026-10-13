"use strict";

// Everything shown from the server goes in as text, never as HTML.

const MAX_DOCUMENTS = 500;
const STEPS = ["type", "documents", "reasons", "review", "done"];

const CHANGE_TYPES = [
  {
    key: "removed",
    title: "Remove documents",
    text: "Take documents out of a corpus at its next release.",
    ready: true,
  },
  {
    key: "added",
    title: "Add documents",
    text: "Not available here yet. Additions come from the corpus build.",
    ready: false,
  },
  {
    key: "content-corrected",
    title: "Correct content",
    text: "Not available yet. Upload a corrected XML.",
    ready: false,
  },
  {
    key: "metadata-corrected",
    title: "Correct metadata",
    text: "Not available yet. Fix a DOI, a date or a URL.",
    ready: false,
  },
];

const state = {
  step: "type",
  user: "",
  canSubmit: false,
  type: "removed",
  text: "",
  csvReasons: new Map(),
  results: [],
  selected: new Set(),
  sharedReason: "",
  reasons: new Map(),
  rows: [],
  checks: [],
  proposals: [],
  confirmed: false,
  outcomes: [],
  busy: false,
};

// --- small helpers ---------------------------------------------------------

function h(tag, props, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props || {})) {
    if (value === undefined || value === null || value === false) continue;
    if (key === "class") node.className = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (key === "value" || key === "checked" || key === "disabled") node[key] = value;
    else node.setAttribute(key, value === true ? "" : String(value));
  }
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : String(child));
  }
  return node;
}

function plural(n) {
  return n === 1 ? "" : "s";
}

function repoName(repoId) {
  return repoId.split("/").pop();
}

function badge(kind, text) {
  return h("span", { class: `badge ${kind}` }, text || kind);
}

function tierBadge(tier) {
  return badge(tier, tier);
}

function heading(text) {
  return h("h2", { tabindex: "-1" }, text);
}

function thead(...titles) {
  return h("thead", {}, h("tr", {}, titles.map((t) => h("th", { scope: "col" }, t))));
}

function safeUrl(url) {
  return typeof url === "string" && url.startsWith("https://") ? url : null;
}

function showError(message) {
  const banner = document.getElementById("banner");
  banner.textContent = message;
  banner.hidden = false;
}

function clearError() {
  const banner = document.getElementById("banner");
  banner.textContent = "";
  banner.hidden = true;
}

async function api(path, body) {
  const options =
    body === undefined
      ? {}
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        };
  const response = await fetch(path, options);
  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }
  if (!response.ok) {
    const error = new Error(messageOf(data) || `${response.status} ${response.statusText}`);
    error.status = response.status;
    error.data = data;
    throw error;
  }
  return data;
}

function messageOf(data) {
  if (!data) return "";
  if (typeof data.detail === "string") return data.detail;
  if (Array.isArray(data.detail)) return data.detail.map((d) => d.msg).join("; ");
  return "";
}

async function busy(work) {
  clearError();
  state.busy = true;
  render();
  try {
    await work();
  } catch (error) {
    showError(error.message);
  } finally {
    state.busy = false;
    render();
  }
}

function go(step) {
  clearError();
  state.step = step;
  render();
  const title = document.querySelector("#view h2");
  if (title) title.focus();
}

// --- input -----------------------------------------------------------------

function parseIds(text) {
  return text
    .split(/[\s,;]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"' && text[i + 1] === '"') {
        field += '"';
        i++;
      } else if (c === '"') {
        quoted = false;
      } else {
        field += c;
      }
    } else if (c === '"') {
      quoted = true;
    } else if (c === ",") {
      row.push(field);
      field = "";
    } else if (c === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (c !== "\r") {
      field += c;
    }
  }
  if (field !== "" || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((r) => r.some((f) => f.trim() !== ""));
}

async function loadCsv(file) {
  if (!file) return;
  clearError();
  const rows = parseCsv(await file.text());
  if (!rows.length) return showError("The CSV is empty.");
  const header = rows[0].map((c) => c.trim().toLowerCase());
  const idColumn = header.includes("uid") ? header.indexOf("uid") : header.indexOf("id");
  const reasonColumn = header.includes("reason")
    ? header.indexOf("reason")
    : header.indexOf("change_reason");
  if (idColumn < 0) return showError("The CSV needs a header row with an id or uid column.");
  const ids = [];
  for (const row of rows.slice(1)) {
    const id = (row[idColumn] || "").trim();
    if (!id) continue;
    ids.push(id);
    const reason = reasonColumn >= 0 ? (row[reasonColumn] || "").trim() : "";
    if (reason) state.csvReasons.set(id, reason);
  }
  state.text = [state.text.trim(), ...ids].filter(Boolean).join("\n");
  render();
}

function reasonFor(index) {
  return state.reasons.get(index) ?? state.sharedReason;
}

function requestsFor(rows) {
  return rows.map((i) => ({
    query: state.results[i].locations[0].uid,
    reason: reasonFor(i),
  }));
}

// --- actions ---------------------------------------------------------------

async function findDocuments() {
  const queries = parseIds(state.text);
  if (!queries.length) return showError("Enter at least one document id.");
  if (queries.length > MAX_DOCUMENTS) {
    return showError(`That is ${queries.length} ids. Send at most ${MAX_DOCUMENTS} at a time.`);
  }
  await busy(async () => {
    const data = await api("/api/lookup", { queries });
    state.results = data.results;
    state.selected = new Set(
      data.results.flatMap((r, i) => (r.status === "found" ? [i] : [])),
    );
    state.reasons = new Map();
    state.checks = [];
    state.proposals = [];
  });
}

async function checkRemovals() {
  state.rows = [...state.selected].sort((a, b) => a - b);
  const without = state.rows.filter((i) => !reasonFor(i).trim()).length;
  if (without) {
    return showError(
      `${without} document${plural(without)} ${without === 1 ? "has" : "have"} no reason. ` +
        "Give one reason for all of them, or one each.",
    );
  }
  await busy(async () => {
    const data = await api("/api/removals/check", { requests: requestsFor(state.rows) });
    state.checks = data.checks;
    state.proposals = data.proposals;
    state.confirmed = false;
    state.step = "review";
  });
}

async function leaveOut(position) {
  state.selected.delete(state.rows[position]);
  if (!state.selected.size) {
    state.checks = [];
    state.proposals = [];
    return go("documents");
  }
  await checkRemovals();
}

async function submitRemovals() {
  const reviewed = Object.fromEntries(state.proposals.map((p) => [p.repo_id, p.digest]));
  let changed = "";
  await busy(async () => {
    try {
      const data = await api("/api/removals/submit", {
        requests: requestsFor(state.rows),
        reviewed,
      });
      state.outcomes = data.results;
      state.step = "done";
    } catch (error) {
      if (error.status !== 409) throw error;
      changed = error.message;
    }
  });
  if (changed) {
    await checkRemovals();
    showError(`${changed} They have been checked again; review them before submitting.`);
  }
}

function reset() {
  Object.assign(state, {
    text: "",
    csvReasons: new Map(),
    results: [],
    selected: new Set(),
    sharedReason: "",
    reasons: new Map(),
    rows: [],
    checks: [],
    proposals: [],
    confirmed: false,
    outcomes: [],
  });
  go("type");
}

// --- views -----------------------------------------------------------------

function viewType() {
  return [
    heading("What do you want to change?"),
    h(
      "p",
      { class: "lead" },
      "Each request opens a pull request on the dataset repo for someone to review. " +
        "Nothing in the published dataset changes until a release applies it.",
    ),
    h(
      "div",
      { class: "choices", role: "radiogroup", "aria-label": "Change type" },
      CHANGE_TYPES.map((t) =>
        h(
          "label",
          { class: t.ready ? "choice" : "choice disabled" },
          h("input", {
            type: "radio",
            name: "type",
            value: t.key,
            checked: state.type === t.key,
            disabled: !t.ready,
            onchange: () => {
              state.type = t.key;
            },
          }),
          h("strong", {}, t.title),
          h("p", {}, t.text),
        ),
      ),
    ),
    h(
      "div",
      { class: "actions" },
      h("span", { class: "spacer" }),
      h("button", { class: "primary", onclick: () => go("documents") }, "Continue"),
    ),
  ];
}

function viewDocuments() {
  const found = state.selected.size;
  return [
    heading("Which documents?"),
    h(
      "p",
      { class: "lead" },
      "Paste document ids, or load a CSV. An id can be bare, like 10.1101_2022.01.01.000000, " +
        "or carry its source, like biorxiv__10.1101_2022.01.01.000000.",
    ),
    h(
      "div",
      { class: "field" },
      h("label", { for: "ids" }, "Document ids"),
      h(
        "span",
        { class: "hint", id: "ids-hint" },
        `Up to ${MAX_DOCUMENTS}, separated by new lines, spaces or commas.`,
      ),
      h("textarea", {
        id: "ids",
        value: state.text,
        spellcheck: "false",
        "aria-describedby": "ids-hint",
        oninput: (e) => {
          state.text = e.target.value;
        },
      }),
    ),
    h(
      "div",
      { class: "field" },
      h("label", { for: "csv" }, "Or load a CSV"),
      h(
        "span",
        { class: "hint", id: "csv-hint" },
        "It needs a header row with an id or uid column. A reason column fills in each " +
          "document's reason.",
      ),
      h("input", {
        type: "file",
        id: "csv",
        accept: ".csv,text/csv",
        "aria-describedby": "csv-hint",
        onchange: (e) => loadCsv(e.target.files[0]),
      }),
    ),
    h(
      "div",
      { class: "actions" },
      h("button", { onclick: () => go("type") }, "Back"),
      h("span", { class: "spacer" }),
      h(
        "button",
        {
          class: state.results.length ? "" : "primary",
          onclick: findDocuments,
          disabled: state.busy,
        },
        state.busy ? "Finding…" : "Find documents",
      ),
    ),
    state.results.length ? lookupTable() : null,
    state.results.length
      ? h(
          "div",
          { class: "actions" },
          h("span", { class: "spacer" }),
          h(
            "button",
            {
              class: "primary",
              id: "continue-documents",
              disabled: !found,
              onclick: () => go("reasons"),
            },
            continueLabel(),
          ),
        )
      : null,
  ];
}

function continueLabel() {
  const n = state.selected.size;
  return `Continue with ${n} document${plural(n)}`;
}

function lookupTable() {
  const counts = {};
  for (const r of state.results) counts[r.status] = (counts[r.status] || 0) + 1;
  const summary = ["found", "missing", "ambiguous", "error"]
    .filter((s) => counts[s])
    .map((s) => `${counts[s]} ${s}`)
    .join(" · ");
  return [
    h("h3", {}, "What was found"),
    h(
      "p",
      { class: "summary" },
      summary,
      ". Only documents found in exactly one repo can be included.",
    ),
    h(
      "div",
      { class: "table-scroll" },
      h(
        "table",
        {},
        thead("Include", "Document", "Status", "Repo", "Split", "Shard", "Licence", "DOI", "Published"),
        h("tbody", {}, state.results.map(lookupRow)),
      ),
    ),
  ];
}

function lookupRow(result, index) {
  const where = result.status === "found" ? result.locations[0] : null;
  const box = h("input", {
    type: "checkbox",
    checked: state.selected.has(index),
    disabled: !where,
    "aria-label": `Include ${result.query}`,
    onchange: (e) => {
      if (e.target.checked) state.selected.add(index);
      else state.selected.delete(index);
      const next = document.getElementById("continue-documents");
      next.textContent = continueLabel();
      next.disabled = !state.selected.size;
    },
  });
  if (!where) {
    return h(
      "tr",
      {},
      h("td", {}, box),
      h("td", { class: "mono" }, result.query || "(blank)"),
      h("td", {}, badge(result.status)),
      h("td", { class: "detail", colspan: "6" }, result.detail),
    );
  }
  const m = where.metadata;
  return h(
    "tr",
    {},
    h("td", {}, box),
    h("td", { class: "mono" }, where.uid),
    h("td", {}, badge(result.status)),
    h("td", {}, repoName(where.repo_id), " ", tierBadge(where.tier)),
    h("td", {}, where.split),
    h("td", { class: "mono" }, where.parquet_file_name),
    h("td", {}, m.licence ?? ""),
    h("td", { class: "mono" }, m.doi ?? ""),
    h("td", {}, m.pub_date ?? ""),
  );
}

function viewReasons() {
  const rows = [...state.selected].sort((a, b) => a - b);
  for (const i of rows) {
    const query = state.results[i].query.trim();
    if (!state.reasons.has(i) && state.csvReasons.has(query)) {
      state.reasons.set(i, state.csvReasons.get(query));
    }
  }
  return [
    heading("Why are they being removed?"),
    h(
      "p",
      { class: "lead" },
      "Give one reason for all of them, and change it for any document that needs its own. " +
        "The reason is recorded with the change and shown to the reviewer.",
    ),
    h(
      "div",
      { class: "field" },
      h("label", { for: "shared" }, "Reason for all documents"),
      h("textarea", {
        id: "shared",
        class: "reason",
        maxlength: "2000",
        value: state.sharedReason,
        oninput: (e) => {
          state.sharedReason = e.target.value;
        },
      }),
    ),
    h("h3", {}, "Reasons for individual documents"),
    h(
      "div",
      { class: "table-scroll" },
      h(
        "table",
        {},
        thead("Document", "Own reason (optional)"),
        h(
          "tbody",
          {},
          rows.map((i) => {
            const uid = state.results[i].locations[0].uid;
            return h(
              "tr",
              {},
              h("td", { class: "mono" }, uid),
              h(
                "td",
                {},
                h("input", {
                  type: "text",
                  maxlength: "2000",
                  value: state.reasons.get(i) ?? "",
                  placeholder: "Uses the reason for all documents",
                  "aria-label": `Reason for ${uid}`,
                  oninput: (e) => {
                    if (e.target.value.trim()) state.reasons.set(i, e.target.value);
                    else state.reasons.delete(i);
                  },
                }),
              ),
            );
          }),
        ),
      ),
    ),
    h(
      "div",
      { class: "actions" },
      h("button", { onclick: () => go("documents") }, "Back"),
      h("span", { class: "spacer" }),
      h(
        "button",
        { class: "primary", onclick: checkRemovals, disabled: state.busy },
        state.busy ? "Checking…" : "Check these removals",
      ),
    ),
  ];
}

function viewReview() {
  const passed = state.checks.filter((c) => c.ok).length;
  const allPass = state.checks.length > 0 && passed === state.checks.length;
  const prs = state.proposals.length;
  return [
    heading("Check and review"),
    h(
      "p",
      { class: "lead" },
      allPass
        ? `All ${passed} documents can be proposed. Review the change to each repo below.`
        : `${passed} of ${state.checks.length} documents can be proposed. Leave out the ` +
            "others, or go back and fix them, before submitting.",
    ),
    h(
      "div",
      { class: "table-scroll" },
      h(
        "table",
        {},
        thead("Document", "Result", "Repo", "Reason", ""),
        h("tbody", {}, state.checks.map(checkRow)),
      ),
    ),
    prs ? h("h3", {}, "What each pull request adds") : null,
    state.proposals.map(proposalPanel),
    state.canSubmit
      ? null
      : h(
          "p",
          { class: "notice" },
          "This is a preview against a local build, so submitting is switched off.",
        ),
    h(
      "label",
      { class: "confirm" },
      h("input", {
        type: "checkbox",
        checked: state.confirmed,
        disabled: !allPass || !prs,
        onchange: (e) => {
          state.confirmed = e.target.checked;
          render();
        },
      }),
      h(
        "span",
        {},
        `I have reviewed these removals. Open ${prs} pull request${plural(prs)} for ` +
          "review. The documents stay in the dataset until a release applies the change.",
      ),
    ),
    h(
      "div",
      { class: "actions" },
      h("button", { onclick: () => go("reasons") }, "Back"),
      h("span", { class: "spacer" }),
      h(
        "button",
        {
          class: "primary",
          onclick: submitRemovals,
          disabled: state.busy || !(allPass && prs && state.confirmed && state.canSubmit),
        },
        state.busy ? "Opening…" : `Open ${prs} pull request${plural(prs)}`,
      ),
    ),
  ];
}

function checkRow(check, position) {
  const where = check.location;
  return h(
    "tr",
    {},
    h("td", { class: "mono" }, where ? where.uid : check.query),
    h(
      "td",
      {},
      check.ok
        ? badge("ok", "Can be proposed")
        : [
            badge("error", "Cannot be proposed"),
            h("ul", { class: "errors" }, check.errors.map((e) => h("li", {}, e))),
          ],
    ),
    h("td", {}, where ? [repoName(where.repo_id), " ", tierBadge(where.tier)] : ""),
    h("td", {}, check.reason),
    h(
      "td",
      {},
      check.ok
        ? null
        : h(
            "button",
            { class: "small", onclick: () => leaveOut(position), disabled: state.busy },
            "Leave out",
          ),
    ),
  );
}

function proposalPanel(p) {
  const added = p.added
    .replace(/\n$/, "")
    .split("\n")
    .map((line) => `+ ${line}`)
    .join("\n");
  return h(
    "section",
    { class: "proposal", "aria-label": `Change to ${p.repo_id}` },
    h(
      "header",
      {},
      h("strong", {}, p.repo_id),
      " ",
      tierBadge(p.tier),
      h(
        "p",
        {},
        `${p.ids.length} removal${plural(p.ids.length)} added to ${p.path}` +
          (p.before === null ? ", which this creates." : "."),
      ),
    ),
    h("pre", { class: "added" }, added),
    p.before === null
      ? null
      : h("details", {}, h("summary", {}, "The file as it is now"), h("pre", {}, p.before)),
  );
}

function viewDone() {
  const opened = state.outcomes.filter((o) => o.ok).length;
  const all = opened === state.outcomes.length;
  return [
    heading(all ? "Submitted for review" : "Only partly submitted"),
    h(
      "p",
      { class: "lead" },
      "Each pull request needs a reviewer to merge it. The documents leave the dataset " +
        "at the next release after that.",
    ),
    h(
      "ul",
      { class: "outcomes" },
      state.outcomes.map((o) =>
        h(
          "li",
          {},
          o.ok
            ? [
                badge("ok", "Opened"),
                " ",
                h("strong", {}, o.repo_id),
                " · ",
                h(
                  "a",
                  { href: safeUrl(o.pr_url), target: "_blank", rel: "noopener" },
                  `Pull request #${o.pr_num}`,
                ),
                h("span", { class: "hint" }, o.title),
              ]
            : [
                badge("error", "Not opened"),
                " ",
                h("strong", {}, o.repo_id),
                h("p", { class: "errors" }, o.error),
              ],
        ),
      ),
    ),
    h(
      "div",
      { class: "actions" },
      all ? null : h("button", { onclick: checkRemovals }, "Check the rest again"),
      h("span", { class: "spacer" }),
      h("button", { class: "primary", onclick: reset }, "Start a new request"),
    ),
  ];
}

const VIEWS = {
  type: viewType,
  documents: viewDocuments,
  reasons: viewReasons,
  review: viewReview,
  done: viewDone,
};

function render() {
  const current = STEPS.indexOf(state.step);
  for (const item of document.querySelectorAll("#steps li")) {
    const index = STEPS.indexOf(item.dataset.step);
    item.className = index === current ? "current" : index < current ? "past" : "";
    if (index === current) item.setAttribute("aria-current", "step");
    else item.removeAttribute("aria-current");
  }
  const view = document.getElementById("view");
  view.replaceChildren(...[VIEWS[state.step]()].flat(Infinity).filter(Boolean));
}

async function start() {
  render();
  const who = document.getElementById("who");
  try {
    const me = await api("/api/whoami");
    state.user = me.user;
    state.canSubmit = me.can_submit;
    who.textContent = me.can_submit
      ? `Signed in to Hugging Face as ${me.user}`
      : `Previewing a local build as ${me.user}`;
  } catch (error) {
    who.textContent = "Not signed in";
    showError(`${error.message}. Set HF_TOKEN or run "hf auth login", then reload.`);
  }
  render();
}

start();
