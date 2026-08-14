import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AttentionCenter } from "./AttentionCenter";

describe("AttentionCenter", () => {
  it("states that acknowledgement cannot resolve or resume work", () => {
    const markup = renderToStaticMarkup(createElement(AttentionCenter));
    expect(markup).toContain("Attention &amp; return view");
    expect(markup).toContain("Loading attention");
    expect(markup).toContain("Artifacts are evidence records");
    expect(markup).toContain("does not poll a provider, wake work, or execute a result");
  });
});
