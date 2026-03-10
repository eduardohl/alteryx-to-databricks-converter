import { PageHeader } from "@/components/layout/page-header";
import { Card } from "@/components/ui/card";
import { useSettingsStore } from "@/stores/settings";
import { CheckCircle } from "lucide-react";

export function SettingsPage() {
  const settings = useSettingsStore();

  return (
    <div className="space-y-6">
      <PageHeader
        title="Settings"
        description="Configure conversion defaults. Settings are persisted in your browser."
      />

      {/* Output format */}
      <Card>
        <h3 className="text-sm font-semibold text-[var(--fg)] mb-4">
          Default Output Format
        </h3>
        <div className="flex gap-2">
          {["pyspark", "dlt", "sql"].map((f) => (
            <button
              key={f}
              onClick={() => settings.setFormat(f)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                settings.format === f
                  ? "bg-[var(--ring)] text-white"
                  : "bg-[var(--bg-sidebar)] text-[var(--fg-muted)] hover:text-[var(--fg)]"
              }`}
            >
              {f === "pyspark" ? "PySpark" : f === "dlt" ? "Delta Live Tables" : "Spark SQL"}
            </button>
          ))}
        </div>
      </Card>

      {/* Databricks catalog */}
      <Card>
        <h3 className="text-sm font-semibold text-[var(--fg)] mb-4">
          Unity Catalog
        </h3>
        <p className="text-xs text-[var(--fg-muted)] mb-4">
          Specify the catalog and schema used in generated code.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <label
              htmlFor="catalog"
              className="block text-xs font-medium text-[var(--fg-muted)] mb-1"
            >
              Catalog Name
            </label>
            <input
              id="catalog"
              type="text"
              value={settings.catalogName}
              onChange={(e) => settings.setCatalogName(e.target.value)}
              className="w-full rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-sm text-[var(--fg)] focus:outline-none focus:ring-2 focus:ring-[var(--ring)]"
            />
          </div>
          <div>
            <label
              htmlFor="schema"
              className="block text-xs font-medium text-[var(--fg-muted)] mb-1"
            >
              Schema Name
            </label>
            <input
              id="schema"
              type="text"
              value={settings.schemaName}
              onChange={(e) => settings.setSchemaName(e.target.value)}
              className="w-full rounded-lg border border-[var(--border)] bg-[var(--bg)] px-3 py-2 text-sm text-[var(--fg)] focus:outline-none focus:ring-2 focus:ring-[var(--ring)]"
            />
          </div>
        </div>
      </Card>

      {/* Code generation options */}
      <Card>
        <h3 className="text-sm font-semibold text-[var(--fg)] mb-4">
          Code Generation
        </h3>
        <label className="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={settings.includeComments}
            onChange={(e) => settings.setIncludeComments(e.target.checked)}
            className="h-4 w-4 rounded border-[var(--border)] accent-[var(--ring)]"
          />
          <div>
            <span className="text-sm text-[var(--fg)]">Include comments</span>
            <p className="text-xs text-[var(--fg-muted)]">
              Add explanatory comments to the generated code
            </p>
          </div>
        </label>
      </Card>

      <div className="flex items-center gap-2 text-sm text-[var(--fg-muted)]">
        <CheckCircle className="h-4 w-4 text-green-500" />
        All settings are saved automatically to your browser.
      </div>
    </div>
  );
}
