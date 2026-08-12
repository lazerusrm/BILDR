import { type ReactNode, useMemo, useState } from "react";

export function VirtualRows<T>({
  items,
  rowHeight = 44,
  height = 352,
  overscan = 8,
  renderRow,
}: {
  items: readonly T[];
  rowHeight?: number;
  height?: number;
  overscan?: number;
  renderRow: (item: T, index: number) => ReactNode;
}) {
  const [scrollTop, setScrollTop] = useState(0);
  const range = useMemo(() => {
    const first = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
    const visible = Math.ceil(height / rowHeight) + overscan * 2;
    return [first, Math.min(items.length, first + visible)] as const;
  }, [height, items.length, overscan, rowHeight, scrollTop]);
  return (
    <div
      aria-label="Virtualized rows"
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
      style={{ height, overflow: "auto" }}
    >
      <div style={{ height: items.length * rowHeight, position: "relative" }}>
        {items.slice(...range).map((item, offset) => {
          const index = range[0] + offset;
          return (
            <div key={index} style={{ height: rowHeight, left: 0, position: "absolute", right: 0, top: index * rowHeight }}>
              {renderRow(item, index)}
            </div>
          );
        })}
      </div>
    </div>
  );
}
