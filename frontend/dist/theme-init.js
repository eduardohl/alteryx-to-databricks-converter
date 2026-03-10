// Apply theme before React renders to prevent flash of wrong theme
(function () {
  var stored = localStorage.getItem("a2d-theme");
  if (stored === "light") {
    document.documentElement.classList.remove("dark");
  } else {
    document.documentElement.classList.add("dark");
  }
})();
