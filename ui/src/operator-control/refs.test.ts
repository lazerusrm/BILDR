import { describe, expect, it } from "vitest";
import { humanizeRefs, refLabel } from "./refs";

const labels = {
  "01M0VH0NXEZYSA95SQ80SMWATR": "Ornith BILDR atomic-ledger benchmark",
};

describe("humanizeRefs", () => {
  it("replaces a known identifier with its name", () => {
    expect(
      humanizeRefs(
        "A controller approval is pending for run 01M0VH0NXEZYSA95SQ80SMWATR.",
        labels,
      ),
    ).toBe(
      "A controller approval is pending for run Ornith BILDR atomic-ledger benchmark.",
    );
  });

  it("shortens an identifier it cannot resolve instead of inventing one", () => {
    expect(humanizeRefs("Blocks 01M0VH5GM8P88WGYTJSFPZYWMW now", {})).toBe(
      "Blocks …ZYWMW now",
    );
  });

  it("replaces every identifier in one string", () => {
    expect(
      humanizeRefs(
        "01M0VH0NXEZYSA95SQ80SMWATR blocks 01M0VH5GM8P88WGYTJSFPZYWMW",
        labels,
      ),
    ).toBe("Ornith BILDR atomic-ledger benchmark blocks …ZYWMW");
  });

  it("leaves ordinary prose untouched", () => {
    const text = "Review the source approval before work can continue.";
    expect(humanizeRefs(text, labels)).toBe(text);
  });

  it("does not mangle short hex or SHA-like tokens", () => {
    const text = "head 0123456789abcdef and branch main";
    expect(humanizeRefs(text, labels)).toBe(text);
  });

  it("labels a single reference", () => {
    expect(refLabel("01M0VH0NXEZYSA95SQ80SMWATR", labels)).toBe(
      "Ornith BILDR atomic-ledger benchmark",
    );
    expect(refLabel("01M0VH5GM8P88WGYTJSFPZYWMW", labels)).toBe("…ZYWMW");
  });
});

describe("refLabel", () => {
  it("leaves a readable identifier alone instead of abbreviating it", () => {
    expect(refLabel("task-core-001", {})).toBe("task-core-001");
    expect(refLabel("CORE-001", {})).toBe("CORE-001");
  });
});
