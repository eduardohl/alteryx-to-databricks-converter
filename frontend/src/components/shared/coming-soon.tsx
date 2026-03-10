import type { ReactNode } from "react";
import { Card } from "@/components/ui/card";
import { Construction } from "lucide-react";

interface ComingSoonProps {
  icon?: ReactNode;
  title: string;
  description: string;
}

export function ComingSoon({ icon, title, description }: ComingSoonProps) {
  return (
    <Card className="flex flex-col items-center justify-center py-16 text-center">
      <div className="mb-4 text-[var(--fg-muted)]">
        {icon ?? <Construction className="h-12 w-12" />}
      </div>
      <h2 className="text-lg font-semibold text-[var(--fg)]">{title}</h2>
      <p className="mt-2 max-w-md text-sm text-[var(--fg-muted)]">{description}</p>
    </Card>
  );
}
