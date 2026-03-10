import { PageHeader } from "@/components/layout/page-header";
import { ComingSoon } from "@/components/shared/coming-soon";
import { Map } from "lucide-react";

export function MigrationPlanPage() {
  return (
    <div className="space-y-6">
      <PageHeader
        title="Migration Plan"
        description="Plan and track your Alteryx-to-Databricks migration"
      />
      <ComingSoon
        icon={<Map className="h-12 w-12" />}
        title="Migration Planning Coming Soon"
        description="Generate a phased migration plan based on workflow complexity, dependencies, and team capacity. Prioritize workflows by business impact and track migration progress."
      />
    </div>
  );
}
