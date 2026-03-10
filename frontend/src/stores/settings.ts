import { create } from "zustand";

interface SettingsStore {
  format: string;
  catalogName: string;
  schemaName: string;
  includeComments: boolean;
  setFormat: (f: string) => void;
  setCatalogName: (v: string) => void;
  setSchemaName: (v: string) => void;
  setIncludeComments: (v: boolean) => void;
}

function load<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(`a2d-${key}`);
    return v !== null ? JSON.parse(v) : fallback;
  } catch {
    return fallback;
  }
}

function save(key: string, value: unknown) {
  localStorage.setItem(`a2d-${key}`, JSON.stringify(value));
}

export const useSettingsStore = create<SettingsStore>((set) => ({
  format: load("format", "pyspark"),
  catalogName: load("catalogName", "main"),
  schemaName: load("schemaName", "default"),
  includeComments: load("includeComments", true),

  setFormat: (f) => {
    save("format", f);
    set({ format: f });
  },
  setCatalogName: (v) => {
    save("catalogName", v);
    set({ catalogName: v });
  },
  setSchemaName: (v) => {
    save("schemaName", v);
    set({ schemaName: v });
  },
  setIncludeComments: (v) => {
    save("includeComments", v);
    set({ includeComments: v });
  },
}));
