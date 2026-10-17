import * as Turbo from "turbo";

// -- Custom confirm dialog (replaces browser confirm) --
Turbo.config.forms.confirm = (message, element) => {
  const dialog = document.getElementById("pgbus-confirm-dialog");
  const messageEl = document.getElementById("pgbus-confirm-message");
  const titleEl = document.getElementById("pgbus-confirm-title");
  const confirmBtn = document.getElementById("pgbus-confirm-btn");
  const iconEl = document.getElementById("pgbus-confirm-icon");

  const turboMethod = element.getAttribute("data-turbo-method");
  const isDelete = turboMethod === "delete";

  const i18n = document.getElementById("pgbus-i18n");
  titleEl.textContent = isDelete
    ? (i18n?.dataset.deleteTitle || "Delete")
    : (i18n?.dataset.confirmTitle || "Are you sure?");
  messageEl.textContent = message;

  confirmBtn.className = "rounded-md px-4 py-2 text-sm font-medium focus:outline-none focus:ring-2";
  if (isDelete) {
    confirmBtn.classList.add("bg-red-600", "text-white", "hover:bg-red-500", "focus:ring-red-500");
    confirmBtn.textContent = i18n?.dataset.deleteLabel || "Delete";
    iconEl.className = "flex-shrink-0 flex items-center justify-center h-10 w-10 rounded-full bg-red-100 dark:bg-red-900/30";
  } else {
    // White on yellow fails contrast; yellow buttons carry dark text (ButtonHelper's warning variant).
    confirmBtn.classList.add("bg-yellow-400", "text-yellow-950", "hover:bg-yellow-300", "focus:ring-yellow-500");
    confirmBtn.textContent = i18n?.dataset.confirmLabel || "Confirm";
    iconEl.className = "flex-shrink-0 flex items-center justify-center h-10 w-10 rounded-full bg-yellow-100 dark:bg-yellow-900/30";
  }

  dialog.showModal();

  return new Promise((resolve) => {
    dialog.addEventListener("close", () => {
      resolve(dialog.returnValue === "confirm");
    }, { once: true });
  });
};

// -- Toast notifications --
function showToast(message, type = "success") {
  const container = document.getElementById("pgbus-toast-container");
  if (!container) return;
  const toast = document.createElement("div");

  const colors = {
    success: "bg-green-50 dark:bg-green-900/30 text-green-800 dark:text-green-300 border-green-200 dark:border-green-800",
    error: "bg-red-50 dark:bg-red-900/30 text-red-800 dark:text-red-300 border-red-200 dark:border-red-800",
    info: "bg-blue-50 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300 border-blue-200 dark:border-blue-800",
  };

  toast.className = `rounded-md border p-3 text-sm shadow-lg transition-all duration-300 ${colors[type] || colors.info}`;
  toast.textContent = message;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = "0";
    toast.style.transform = "translateX(100%)";
    setTimeout(() => toast.remove(), 300);
  }, 5000);
}

function renderFlashToasts() {
  document.querySelectorAll("template[data-pgbus-toast]").forEach(tpl => {
    showToast(tpl.content.textContent.trim(), tpl.dataset.pgbusToast);
    tpl.remove();
  });
}
renderFlashToasts();
document.addEventListener("turbo:load", renderFlashToasts);

// -- Bulk checkbox selection --
function initBulkSelect() {
  document.querySelectorAll("[data-bulk-select-all]").forEach(selectAll => {
    if (selectAll.dataset.bulkInitialized === "true") return;
    selectAll.dataset.bulkInitialized = "true";

    const scope = selectAll.closest("[data-bulk-scope]") || document;
    const checkboxes = () => scope.querySelectorAll("input[data-bulk-item]");
    // Look for bulk-actions in the parent page container (outside the table scope)
    const pageScope = scope.closest("[data-bulk-page]") || scope.closest("turbo-frame") || scope.parentElement?.closest("div") || document;
    const countEl = pageScope.querySelector("[data-bulk-count]");
    const actions = pageScope.querySelector("[data-bulk-actions]");

    function updateUI() {
      const checked = scope.querySelectorAll("input[data-bulk-item]:checked");
      if (countEl) countEl.textContent = checked.length;
      if (actions) actions.classList.toggle("hidden", checked.length === 0);

      const all = checkboxes();
      selectAll.checked = all.length > 0 && checked.length === all.length;
      selectAll.indeterminate = checked.length > 0 && checked.length < all.length;
    }

    let inSelectAll = false;
    selectAll.addEventListener("change", () => {
      inSelectAll = true;
      checkboxes().forEach(cb => {
        cb.checked = selectAll.checked;
        // Dispatch change event so inline onchange handlers fire (e.g., hidden input sync)
        cb.dispatchEvent(new Event("change", { bubbles: false }));
      });
      inSelectAll = false;
      updateUI();
    });

    scope.addEventListener("change", (e) => {
      if (e.target.matches("input[data-bulk-item]")) updateUI();
    });
  });

}
initBulkSelect();
document.addEventListener("turbo:load", initBulkSelect);
document.addEventListener("turbo:frame-load", initBulkSelect);

// -- Auto-refresh (pauses when user is interacting) --
const refreshInterval = parseInt(document.body?.dataset.pgbusRefreshInterval || "0", 10);
if (refreshInterval > 0) {
  let timer;

  function hasUserInteraction() {
    // Pause when any checkbox is checked
    const checked = document.querySelector("input[data-bulk-item]:checked, input[data-bulk-select-all]:checked");
    if (checked) return true;
    // Pause when any job detail row is expanded
    const openDetails = document.querySelector("turbo-frame[data-auto-refresh] details[open]");
    if (openDetails) return true;
    return false;
  }

  function refreshFrames() {
    if (document.hidden) return;
    if (hasUserInteraction()) return;
    document.querySelectorAll("turbo-frame[data-auto-refresh]")
      .forEach(frame => {
        try {
          if (!frame.src && frame.dataset.src) frame.src = frame.dataset.src;
          if (frame.src) frame.reload();
        } catch (_) { /* Turbo may abort in-flight fetches during navigation */ }
      });
  }
  function start() { timer = setInterval(refreshFrames, refreshInterval); }
  function stop() { clearInterval(timer); }
  document.addEventListener("visibilitychange", () => document.hidden ? stop() : start());
  start();
}
