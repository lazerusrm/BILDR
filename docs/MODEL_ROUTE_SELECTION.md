# Immutable model-route selection

Run admission must resolve model choices into controller-owned receipts. A UI
selection is not runtime authority, and a model name alone is insufficient
because a name may exist behind more than one provider.

## Selection modes

`uniform_local` is used for Qwodex. The admission request selects one dynamic
catalog entry and one supported reasoning level. The controller expands that
single route to every normal role, persists the catalog-profile digest, and
rejects native children whose route cannot be bound. When advisory supervision
or expert consultation is enabled, it receives that same persisted local route
in a fresh, independently bounded context; it cannot introduce a second model
silently. Unbound provider-native children are quarantined and cancelled.

`hosted_preset` is for the operator-approved hosted catalog. The user selects
one controller profile and, optionally, one named role preset. Both are IDs
from operator configuration, not arbitrary client-supplied route maps. The
configuration resolves only model and reasoning effort; each repository
profile retains ownership of its role sandbox, network posture, approvals, and
token bounds.

## Required v3 custody shape

The current v2 record deliberately has one route and therefore cannot safely
represent a hosted role preset. Its replacement is a route-set header plus
one immutable row per normal role:

```text
run route-set header
  provider, selection mode, selected controller-profile ID,
  selected preset ID, catalog/config digest, route-set digest

run route rows
  role key, model, effort, local-profile digest (if applicable), route digest

agent binding
  session ID, run ID, role key, route-set digest, exact route digest
```

Admission writes the run, header, and all rows in one transaction. Before a
thread starts, its binding must match both the persisted role row and the
controller session's role. Fresh-attempt authorization includes the role key
and route-set digest; continuation checks the same binding. This prevents a
retry, an incidental profile reload, or a similarly named model from changing
the selected route.

This is intentionally a greenfield migration: an existing v2 receipt must be
rejected rather than backfilled from mutable profile configuration. Start a
fresh database for v3; do not invent historical role authority.
