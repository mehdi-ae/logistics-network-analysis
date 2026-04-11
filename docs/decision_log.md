# Decision Log

This document records the significant architectural and design decisions made during the build of the EU Logistics Network Analytics Platform. Each entry explains what was decided, why, and what alternatives were considered.

---

## ADR-001 — Build a logistics network rather than use a public dataset

**Date:** March 2026  
**Status:** Decided

**Decision:** Build a synthetic EU logistics network from scratch using Python generation scripts rather than using a public dataset such as Olist or another available source.

**Reasoning:** This is the first analytics engineering project in a portfolio targeting supply chain and logistics clients. The domain is familiar — five years of operational experience at Amazon EU across fulfilment centre operations and network planning. The problems modeled here — lane profitability, container compliance, transit time governance — are problems that generated measurable cost savings in a real production environment. Building from a familiar domain produces more credible analytical output and more defensible business framing than adapting a generic retail dataset.

A public dataset would have constrained the data model to someone else's schema. Building synthetically allowed full control over the network topology, the quality issues injected, the compliance model, and the business rules — producing a richer environment for practicing defensive SQL and end-to-end pipeline design.

**Alternatives considered:** Olist (Brazilian e-commerce), NYC taxi data, a Kaggle logistics dataset. All remain candidates for future projects targeting retail and e-commerce clients.

---


## ADR-002 — Replace lane/route model with transportation/shipment_details model

**Date:** March 2026  
**Status:** Decided

**Decision:** Replace the previous model (lanes, routes, shipments as separate entities with FK relationships) with a simpler two-table transactional model: one row per truck run in `tms_transportation`, one row per shipment leg in `wms_shipment_details`.

**Reasoning:** The previous model attempted to pre-define all possible routes as static entities, then attach shipments to them. This created complexity without analytical value — the route definitions were redundant once you had the actual truck run data. The new model is closer to how a real TMS and WMS would export data: the truck run record captures everything about the movement, and the shipment detail record traces the package. The sort_code field on the shipment detail row replaces the need for a separate routes table entirely.

**Alternatives considered:** Keeping a separate routes dimension table. Rejected because route identity can be derived from (origin, destination, sort_code) on the shipment detail row without a separate entity.

---

## ADR-003 — All Bronze columns loaded as STRING

**Date:** March 2026  
**Status:** Decided

**Decision:** Load all source CSV columns as STRING in the Bronze layer with no type casting or transformation at ingestion.

**Reasoning:** Bronze is an immutable record of what was received. A numeric value that fails a FLOAT cast would be silently dropped or error at load time if typed at ingestion — instead it is preserved as a string and handled in Silver where the decision about how to flag or correct it is explicit and documented. This also means Bronze can always be used as a recovery point if a Silver transformation introduces a bug.

---

## ADR-004 — Quality issues injected as additive rows, not in-place corruption

**Date:** April 2026  
**Status:** Decided

**Decision:** Data quality issues that affect structural fields (node_type, coordinates) are injected as additional rows appended to the dataset rather than by modifying existing clean rows.

**Reasoning:** The initial implementation corrupted existing rows — for example, changing a valid node_type to a typo. This caused `lanes.py` to drop those nodes when building the network, resulting in only 14 origins, 5 hubs, and 29 delivery nodes instead of 15, 6, and 30. Downstream generation scripts produced data for an incomplete network that could not be recovered in Silver.

The correct approach is additive injection: the clean row remains intact and a duplicate row with the corrupted value is appended. This preserves the full network for generation while still providing Silver with realistic quality issues to handle — a duplicate row with an invalid node_type is a more realistic real-world scenario than a clean record being silently corrupted.

---

## ADR-005 — lanes.py as an internal module, not a CSV output

**Date:** March 2026  
**Status:** Decided

**Decision:** The lane connectivity logic lives in `lanes.py` as an importable Python module with no CSV output, rather than as a generation script that produces a lanes table.

**Reasoning:** Lane assignments are not a source system table — they are routing rules internal to the generation logic. In a real TMS, routing rules live inside the system as configuration, not as an exported CSV. Exporting them would have created a file that looks like a data source but is actually a generation artifact, which would have been misleading in the portfolio and would have needed to be loaded into BigQuery unnecessarily. Other generation scripts import `lanes.py` directly to access the network topology.

---

## ADR-006 — Cross-border connections always via consolidation hub

**Date:** March 2026  
**Status:** Decided

**Decision:** Cross-border shipments always route through a consolidation hub. Direct cross-border lanes to delivery nodes are not permitted.

**Reasoning:** A direct cross-border lane to a single delivery node would rarely be economically justifiable — the volume to a single foreign delivery node is unlikely to fill a truck consistently. In a real network, cross-border flows are consolidated before dispatch to maximise truck utilisation. This rule also keeps the compliance model clean: cross-border indirect shipments always have two legs, and the hub is always accountable for the outbound container type.

---

## ADR-007 — Container compliance applied to all three lane types including leg1

**Date:** April 2026  
**Status:** Decided

**Decision:** The container compliance model applies to origin → consolidation hub lanes (leg1) as well as direct lanes and leg2 lanes.

**Reasoning:** Initial design excluded leg1 from compliance on the grounds that consolidation hubs accept both container types. However, the network standard requires metallic containers on all inbound hub lanes because they are cheaper to process. An origin shipping BOX containers to a hub creates a processing cost that could be avoided. Making leg1 compliance measurable allows the platform to identify origins that are non-compliant across all their lanes — both direct and hub-bound — rather than only on direct lanes.

---

## ADR-008 — Consolidation hub MTL standard for leg1 inbound

**Date:** April 2026  
**Status:** Decided

**Decision:** The correct container type for all origin → consolidation hub lanes is MTL (metallic). Sending BOX containers to a consolidation hub is a compliance failure regardless of the final delivery node's capability.

**Reasoning:** Metallic containers are reusable, structurally stronger, and cheaper per unit to process at a sorting facility than cardboard boxes. Standardising on MTL inbound simplifies hub operations and reduces procurement costs. The compliance metric on leg1 lanes measures whether origins are following this network standard.

---

## ADR-009 — Thin lanes capped at one truck per day

**Date:** April 2026  
**Status:** Decided

**Decision:** Thin lanes (lowest volume profile, 25% of active lanes) run a maximum of one truck per day, with an 80% daily probability of running at all.

**Reasoning:** A thin lane is defined as one where daily CBM is consistently below 10 CBM. Running multiple trucks on such a lane would produce unrealistic data — no operator would dispatch two trucks at 15% fill rate on the same day. The 80% daily run probability reflects the reality that thin lanes are irregular: they don't run every day, but when they do run they still incur the full fixed cost of the truck. This makes thin lanes analytically interesting for the Lane Health dashboard — high cost per package, irregular frequency, a clear candidate for network optimisation.

---

## ADR-010 — Arrival time generated first, departure back-calculated

**Date:** April 2026  
**Status:** Decided

**Decision:** For direct lanes and leg1 lanes, arrival time is generated first within a target window, and departure time is back-calculated as arrival minus transit time. For leg2 lanes, departure is generated first within a hard window (22:00–05:00) and arrival is forward-calculated.

**Reasoning:** The business SLA is defined on arrival time (delivery before 10:00, hub receipt before 21:00). Generating arrival first and back-calculating departure ensures the arrival distribution matches the SLA target directly, without needing to iterate. For leg2, the constraint is on departure (the hub dispatch window is operationally fixed), so departure is generated first and arrival emerges from transit time variance — producing natural late arrivals that make the Transit Time Governance dashboard meaningful.

---

## ADR-011 — Packages per CBM reduced to 3–5

**Date:** April 2026  
**Status:** Decided

**Decision:** Packages per CBM was reduced from the initial range of 8–15 to 3–5.

**Reasoning:** At 8–15 packages per CBM, the `wms_shipment_details` table would have exceeded 130 million rows, breaching the BigQuery 10GB free storage tier across Bronze and Silver copies. Reducing to 3–5 packages per CBM brings the table to approximately 37 million rows and total warehouse storage to around 10–12GB — acceptable given the minimal monthly cost of the marginal overage. The analytical value of the dataset does not depend on package density — compliance rates, transit time distributions, and lane cost metrics are all meaningful at lower package volumes.

---

## ADR-012 — Data contract triggers realignment, not silent fixes

**Date:** April 2026  
**Status:** Decided

**Decision:** When the Silver layer encounters data that is not covered by the data contract, the correct response is to flag it as `invalid_value`, update the contract, and document the decision. Silent corrections applied without updating the contract are not acceptable.

**Reasoning:** A silent fix creates hidden knowledge — the Silver layer knows something that the contract does not document, which means any consumer reading the contract has an incomplete picture of the data. In a team environment this creates bugs, misaligned expectations, and untraceable decisions. The contract is the source of truth; if reality diverges from it, the contract is updated first.
