import { useCallback, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
} from "@xyflow/react";
import dagre from "dagre";
import type { DagData, DagNode } from "@/lib/api";
import "@xyflow/react/dist/style.css";

interface WorkflowGraphProps {
  dagData: DagData;
  onNodeSelect?: (nodeId: number) => void;
}

function getBorderColor(confidence: number): string {
  if (confidence >= 0.9) return "#22c55e";
  if (confidence >= 0.6) return "#eab308";
  return "#ef4444";
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max) + "..." : s;
}

function useAutoLayout(dagData: DagData) {
  return useMemo(() => {
    const allZero = dagData.nodes.every(
      (n) => n.position_x === 0 && n.position_y === 0,
    );

    const buildNodes = (getPos: (n: DagNode) => { x: number; y: number }): Node[] =>
      dagData.nodes.map((n) => {
        const pos = getPos(n);
        return {
          id: String(n.node_id),
          position: pos,
          data: {
            label: n.annotation
              ? `${n.tool_type}\n${truncate(n.annotation, 24)}`
              : n.tool_type,
            dagNode: n,
          },
          style: {
            border: `2px solid ${getBorderColor(n.conversion_confidence)}`,
            borderRadius: "8px",
            padding: "8px 12px",
            fontSize: "12px",
            background: "var(--bg-card)",
            color: "var(--fg)",
            width: 180,
            whiteSpace: "pre-line" as const,
            cursor: "pointer",
          },
        };
      });

    const edges: Edge[] = dagData.edges.map((e, i) => ({
      id: `e-${i}`,
      source: String(e.source_id),
      target: String(e.target_id),
      animated: true,
      style: { stroke: "var(--border)" },
    }));

    if (allZero) {
      const g = new dagre.graphlib.Graph();
      g.setDefaultEdgeLabel(() => ({}));
      g.setGraph({ rankdir: "LR", nodesep: 60, ranksep: 120 });
      for (const n of dagData.nodes) g.setNode(String(n.node_id), { width: 180, height: 50 });
      for (const e of dagData.edges) g.setEdge(String(e.source_id), String(e.target_id));
      dagre.layout(g);

      return {
        nodes: buildNodes((n) => {
          const pos = g.node(String(n.node_id));
          return { x: pos.x - 90, y: pos.y - 25 };
        }),
        edges,
      };
    }

    const xs = dagData.nodes.map((n) => n.position_x);
    const ys = dagData.nodes.map((n) => n.position_y);
    const minX = Math.min(...xs);
    const minY = Math.min(...ys);

    return {
      nodes: buildNodes((n) => ({
        x: (n.position_x - minX) * 1.2 + 50,
        y: (n.position_y - minY) * 1.2 + 50,
      })),
      edges,
    };
  }, [dagData]);
}

export function WorkflowGraph({ dagData, onNodeSelect }: WorkflowGraphProps) {
  const { nodes, edges } = useAutoLayout(dagData);
  const [tooltip, setTooltip] = useState<{ x: number; y: number; node: DagNode } | null>(null);

  const onNodeClick = useCallback(
    (_: unknown, node: Node) => {
      const dagNode = node.data?.dagNode as DagNode | undefined;
      if (dagNode && onNodeSelect) onNodeSelect(dagNode.node_id);
    },
    [onNodeSelect],
  );

  const onNodeMouseEnter = useCallback(
    (_: React.MouseEvent, node: Node) => {
      const dagNode = node.data?.dagNode as DagNode | undefined;
      if (dagNode) {
        setTooltip({
          x: (node.position?.x ?? 0) + 190,
          y: (node.position?.y ?? 0),
          node: dagNode,
        });
      }
    },
    [],
  );

  const onNodeMouseLeave = useCallback(() => setTooltip(null), []);

  return (
    <div className="relative h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        minZoom={0.2}
        maxZoom={2}
        onNodeClick={onNodeClick}
        onNodeMouseEnter={onNodeMouseEnter}
        onNodeMouseLeave={onNodeMouseLeave}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
        <MiniMap
          style={{ background: "var(--bg-sidebar)" }}
          maskColor="rgba(0,0,0,0.2)"
        />
      </ReactFlow>

      {/* Legend */}
      <div className="absolute bottom-3 left-3 flex items-center gap-3 rounded-lg bg-[var(--bg-card)]/90 backdrop-blur px-3 py-2 text-[11px] text-[var(--fg-muted)] border border-[var(--border)]">
        <span className="font-medium">Confidence:</span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded border-2 border-green-500" /> High
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded border-2 border-yellow-500" /> Medium
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded border-2 border-red-500" /> Low
        </span>
      </div>

      {/* Tooltip */}
      {tooltip && (
        <div
          className="absolute z-10 rounded-lg bg-[var(--bg-card)] border border-[var(--border)] shadow-lg px-3 py-2 text-xs pointer-events-none max-w-[220px]"
          style={{ left: tooltip.x, top: tooltip.y }}
        >
          <div className="font-medium text-[var(--fg)]">{tooltip.node.tool_type}</div>
          {tooltip.node.annotation && (
            <div className="text-[var(--fg-muted)] mt-0.5">{tooltip.node.annotation}</div>
          )}
          <div className="mt-1 space-y-0.5 text-[var(--fg-muted)]">
            <div>Confidence: {(tooltip.node.conversion_confidence * 100).toFixed(0)}%</div>
            <div>Method: {tooltip.node.conversion_method}</div>
            <div>Node ID: {tooltip.node.node_id}</div>
          </div>
        </div>
      )}
    </div>
  );
}
