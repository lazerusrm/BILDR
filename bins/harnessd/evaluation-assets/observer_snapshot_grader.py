import hashlib
import json
import sys

EXPECTED = {
    "historical": {
        "signal": "historical_bug_reproduced",
        "base": "6bc83a51d83a82fb5ba4e5722db683de830533ca",
        "original": "20bd01c5986aa93a41f6b592c803d8aabd02bb4c9ce00d4b5367bf42842436d1",
        "overlay": "af74bc7c8d281c290b9b697bb20e367bd639d8e97988e7d5bab163346f99a6a4",
    },
    "fixed": {
        "signal": "fixed_bound_enforced",
        "base": "5f70f85a45ce358df135543617c2925dcbaf127f",
        "original": "4f724a7fc2ef1f5bbbaf4eb05495f40a07cf51a914823986aa9d4d4d040c5def",
        "overlay": "4015dfb6f8d789716d19c7e654c2f1e80fd5f135cfb4b0bc81270be51a2f6b1a",
    },
}

def validate(value):
    if value.get("schema") != "harness.eval.materialization-artifact.v1":
        return False
    arm = value.get("arm")
    expected = EXPECTED.get(arm)
    if not expected or value.get("signal") != expected["signal"]:
        return False
    if value.get("base_checkout_sha") != expected["base"]:
        return False
    if value.get("target_path") != "crates/harness-store/src/queries.rs":
        return False
    try:
        original = bytes.fromhex(value["original_target_hex"]).decode()
        overlay = bytes.fromhex(value["overlay_hex"]).decode()
    except (KeyError, ValueError, UnicodeDecodeError):
        return False
    if hashlib.sha256(original.encode()).hexdigest() != expected["original"]:
        return False
    if hashlib.sha256(overlay.encode()).hexdigest() != expected["overlay"]:
        return False
    closing = original.rfind("\n}")
    if closing < 0:
        return False
    actual = (original[:closing] + "\n\n" + overlay + "\n}\n").encode()
    return hashlib.sha256(actual).hexdigest() == value.get("resulting_target_digest")

if sys.argv[1:] == ["--self-test"]:
    # Neither a swapped arm/base nor fabricated original bytes can pass the
    # baked controller custody tuple, even if their internal digest matches.
    swapped = {"schema": "harness.eval.materialization-artifact.v1", "arm": "historical", "signal": "historical_bug_reproduced", "base_checkout_sha": EXPECTED["fixed"]["base"], "target_path": "crates/harness-store/src/queries.rs", "original_target_hex": "", "overlay_hex": "", "resulting_target_digest": ""}
    fabricated = {"schema": "harness.eval.materialization-artifact.v1", "arm": "fixed", "signal": "fixed_bound_enforced", "base_checkout_sha": EXPECTED["fixed"]["base"], "target_path": "crates/harness-store/src/queries.rs", "original_target_hex": "66616272696361746564", "overlay_hex": "", "resulting_target_digest": ""}
    sys.exit(0 if not validate(swapped) and not validate(fabricated) else 1)

value = json.load(open("/work/artifact", "rb"))
arm = value.get("arm")
signal = value.get("signal")
if not validate(value):
    sys.exit(1)
print(json.dumps({"schema": "harness.eval.materialization-grade.v1", "arm": arm, "signal": signal, "result": "pass"}, sort_keys=True))
