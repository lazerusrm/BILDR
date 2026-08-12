import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { VirtualRows } from "./VirtualRows";

describe("VirtualRows", () => {
  it("renders a bounded initial window for a large trace", () => {
    const markup = renderToStaticMarkup(
      <VirtualRows<number>
        items={Array.from({ length: 10_000 }, (_, index) => index)}
        height={44}
        rowHeight={44}
        renderRow={(item) => createElement("span", null, item)}
      />,
    );
    expect((markup.match(/<span>/g) || []).length).toBeLessThan(30);
  });
});
