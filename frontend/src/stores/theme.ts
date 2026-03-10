import { create } from "zustand";

type Theme = "light" | "dark";

interface ThemeStore {
  theme: Theme;
  toggle: () => void;
}

// Apply theme class on initial load to prevent flash
const initialTheme: Theme =
  (localStorage.getItem("a2d-theme") as Theme) || "dark";

if (initialTheme === "dark") {
  document.documentElement.classList.add("dark");
} else {
  document.documentElement.classList.remove("dark");
}

export const useThemeStore = create<ThemeStore>((set) => ({
  theme: initialTheme,
  toggle: () =>
    set((state) => {
      const next: Theme = state.theme === "dark" ? "light" : "dark";
      localStorage.setItem("a2d-theme", next);
      if (next === "dark") {
        document.documentElement.classList.add("dark");
      } else {
        document.documentElement.classList.remove("dark");
      }
      return { theme: next };
    }),
}));
