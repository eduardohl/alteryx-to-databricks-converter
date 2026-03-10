import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  Label,
} from "recharts";

interface CoverageChartProps {
  coverage: number;
}

export function CoverageChart({ coverage }: CoverageChartProps) {
  const data = [
    { name: "Covered", value: coverage },
    { name: "Uncovered", value: 100 - coverage },
  ];

  return (
    <div className="flex flex-col items-center">
      <ResponsiveContainer width={160} height={160}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={50}
            outerRadius={70}
            startAngle={90}
            endAngle={-270}
            paddingAngle={2}
            dataKey="value"
          >
            <Cell fill="var(--ring)" strokeWidth={2} stroke="var(--ring)" />
            <Cell fill="var(--border)" strokeWidth={2} stroke="var(--border)" strokeDasharray="4 2" />
            <Label
              value={`${coverage.toFixed(0)}%`}
              position="center"
              className="text-lg font-bold fill-[var(--fg)]"
            />
          </Pie>
          <Tooltip
            contentStyle={{
              backgroundColor: "var(--bg-card)",
              border: "1px solid var(--border)",
              borderRadius: "8px",
              fontSize: "12px",
            }}
          />
        </PieChart>
      </ResponsiveContainer>
      <div className="text-center -mt-4">
        <p className="text-2xl font-bold text-[var(--fg)]">{coverage.toFixed(1)}%</p>
        <p className="text-xs text-[var(--fg-muted)]">Avg Coverage</p>
      </div>
    </div>
  );
}
