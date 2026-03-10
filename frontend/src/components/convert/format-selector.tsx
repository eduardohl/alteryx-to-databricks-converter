import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface FormatSelectorProps {
  value: string;
  onChange: (value: string) => void;
}

export function FormatSelector({ value, onChange }: FormatSelectorProps) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger className="w-48">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="pyspark">
          <div>
            <span>PySpark</span>
            <span className="block text-[10px] text-[var(--fg-muted)]">Databricks notebooks</span>
          </div>
        </SelectItem>
        <SelectItem value="dlt">
          <div>
            <span>Delta Live Tables</span>
            <span className="block text-[10px] text-[var(--fg-muted)]">Managed pipelines</span>
          </div>
        </SelectItem>
        <SelectItem value="sql">
          <div>
            <span>Spark SQL</span>
            <span className="block text-[10px] text-[var(--fg-muted)]">SQL views &amp; CTEs</span>
          </div>
        </SelectItem>
      </SelectContent>
    </Select>
  );
}
