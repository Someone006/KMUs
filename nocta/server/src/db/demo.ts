/**
 * Populate a plausible night: several Rundes across zones, some shared by
 * groups and some solo, placed and ready for dispatch. Used for the demo,
 * the screenshots, and to give the operator console real numbers to show.
 */

const BASE = process.env.NOCTA_API ?? "http://localhost:8787";

interface Person { name: string; picks: Array<[string, number]>; }

const SCENARIOS: Array<{ zone: string; address: string; people: Person[] }> = [
  {
    zone: "z-k4", address: "Langstrasse 84, 8004 Zürich",
    people: [
      { name: "Nina", picks: [["p-rb-250", 2], ["p-zw-pap-175", 1]] },
      { name: "Jonas", picks: [["p-cc-500", 2], ["p-har-200", 1]] },
      { name: "Alia", picks: [["p-bj-465", 1], ["p-val-500", 2]] },
      { name: "Timo", picks: [["p-pri-165", 1], ["p-ict-500", 1]] },
    ],
  },
  {
    zone: "z-k5", address: "Josefstrasse 142, 8005 Zürich",
    people: [
      { name: "Selin", picks: [["p-mon-500", 2], ["p-dor-170", 1]] },
      { name: "Marc", picks: [["p-riv-500", 2], ["p-tob-100", 1]] },
      { name: "Lea", picks: [["p-emm-230", 2], ["p-rag-50", 2]] },
    ],
  },
  {
    zone: "z-k1", address: "Niederdorfstrasse 21, 8001 Zürich",
    people: [
      { name: "Fabio", picks: [["p-rb-250", 1], ["p-zw-nus-200", 1]] },
      { name: "Chiara", picks: [["p-ccz-500", 1], ["p-mm-200", 1]] },
    ],
  },
  {
    zone: "z-k3", address: "Birmensdorferstrasse 55, 8003 Zürich",
    // Deliberately a lone customer with a thin basket: this is the drop that
    // shows red in the console, and the reason the Runde exists.
    people: [{
      name: "Robin",
      picks: [["p-rb-250", 1], ["p-val-500", 2], ["p-har-200", 1], ["p-pri-165", 1], ["p-rag-50", 1]],
    }],
  },
  {
    zone: "z-k6", address: "Universitätstrasse 88, 8006 Zürich",
    people: [
      { name: "Sven", picks: [["p-mag-110", 2], ["p-cc-500", 2]] },
      { name: "Yara", picks: [["p-lin-100", 1], ["p-evi-500", 2]] },
      { name: "Deniz", picks: [["p-bre-6", 1], ["p-mic-500", 1]] },
      { name: "Amir", picks: [["p-iso-500", 2], ["p-ban-4", 1]] },
      { name: "Pia", picks: [["p-cai-81", 2], ["p-zw-nat-175", 1]] },
    ],
  },
];

async function post(path: string, body?: unknown) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`POST ${path} failed: ${res.status} ${await res.text()}`);
  }
  return res.json();
}

async function main() {
  let placed = 0;
  for (const scenario of SCENARIOS) {
    const host = scenario.people[0];
    const created = await post("/api/runde", {
      zoneId: scenario.zone,
      address: scenario.address,
      hostName: host.name,
    }) as { runde: { code: string }; you: { id: string } };

    const code = created.runde.code;
    const ids: Array<{ id: string; picks: Person["picks"] }> = [
      { id: created.you.id, picks: host.picks },
    ];

    for (const person of scenario.people.slice(1)) {
      const joined = await post(`/api/runde/${code}/join`, { name: person.name }) as { you: { id: string } };
      ids.push({ id: joined.you.id, picks: person.picks });
    }

    for (const { id, picks } of ids) {
      for (const [productId, qty] of picks) {
        await post(`/api/runde/${code}/items`, { participantId: id, productId, qty });
      }
    }

    await post(`/api/runde/${code}/place`);
    placed++;
    console.log(`  placed ${code} · ${scenario.address} · ${scenario.people.length} sharer(s)`);
  }

  const assigned = await post("/api/ops/dispatch/assign") as { runs: number; dropsAssigned: number };
  console.log(`Placed ${placed} Rundes; dispatched ${assigned.dropsAssigned} drops across ${assigned.runs} runs.`);
}

main().catch((err) => {
  console.error("Demo population failed:", err.message);
  process.exit(1);
});
