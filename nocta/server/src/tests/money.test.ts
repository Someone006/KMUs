import { test, describe } from "node:test";
import assert from "node:assert/strict";
import {
  formatCHF, parseCHF, roundToFiveRappen, splitExact, vatIncludedIn, netOf,
} from "../domain/money.js";
import { unitPrice, packLabel } from "../domain/pbv.js";

describe("splitExact", () => {
  test("shares always sum to the original amount", () => {
    // Every amount x every party count. If this ever fails, a Runde's
    // shares stop reconciling and the product is broken.
    for (let amount = 0; amount <= 2000; amount += 7) {
      for (let n = 1; n <= 12; n++) {
        const shares = splitExact(amount, n);
        assert.equal(
          shares.reduce((a, b) => a + b, 0), amount,
          `${amount} split ${n} ways did not reconcile`,
        );
        assert.equal(shares.length, n);
      }
    }
  });

  test("distributes the remainder one Rappen at a time", () => {
    assert.deepEqual(splitExact(590, 4), [148, 148, 147, 147]);
    assert.deepEqual(splitExact(100, 3), [34, 33, 33]);
  });

  test("proportional split honours weights and still reconciles", () => {
    const shares = splitExact(590, 3, [1000, 500, 500]);
    assert.equal(shares.reduce((a, b) => a + b, 0), 590);
    assert.ok(shares[0] > shares[1], "the larger basket carries more fee");
  });

  test("falls back to an equal split when every weight is zero", () => {
    const shares = splitExact(600, 3, [0, 0, 0]);
    assert.deepEqual(shares, [200, 200, 200]);
  });

  test("rejects a non-positive number of shares", () => {
    assert.throws(() => splitExact(100, 0), RangeError);
    assert.throws(() => splitExact(100, -2), RangeError);
  });

  test("rejects a weight list that does not match the share count", () => {
    assert.throws(() => splitExact(100, 3, [1, 1]), RangeError);
  });
});

describe("VAT", () => {
  test("extracts 2.6% contained in a food price", () => {
    // CHF 10.00 gross at 2.6% -> 10.00 * 0.026/1.026 = 25.34 Rappen
    assert.equal(vatIncludedIn(1000, "food"), 25);
    assert.equal(netOf(1000, "food"), 975);
  });

  test("extracts 8.1% contained in a delivery fee", () => {
    // CHF 5.90 gross at 8.1% -> 5.90 * 0.081/1.081 = 44.2 Rappen
    assert.equal(vatIncludedIn(590, "service"), 44);
  });

  test("a zero amount carries zero VAT", () => {
    assert.equal(vatIncludedIn(0, "food"), 0);
    assert.equal(vatIncludedIn(0, "service"), 0);
  });
});

describe("cash rounding", () => {
  test("settles to the nearest 5 Rappen", () => {
    assert.equal(roundToFiveRappen(1234), 1235);
    assert.equal(roundToFiveRappen(1232), 1230);
    assert.equal(roundToFiveRappen(1235), 1235);
    assert.equal(roundToFiveRappen(0), 0);
  });
});

describe("formatting", () => {
  test("renders Rappen as Swiss francs", () => {
    assert.equal(formatCHF(1234), "CHF 12.34");
    assert.equal(formatCHF(5), "CHF 0.05");
    assert.equal(formatCHF(0), "CHF 0.00");
    assert.equal(formatCHF(-250), "-CHF 2.50");
  });

  test("round-trips through parseCHF", () => {
    assert.equal(parseCHF("CHF 12.34"), 1234);
    assert.equal(parseCHF("12,34"), 1234);
  });

  test("rejects a non-monetary string", () => {
    assert.throws(() => parseCHF("abc"), RangeError);
  });
});

describe("PBV unit pricing", () => {
  test("computes CHF per litre for a beverage", () => {
    // CHF 3.20 for 500ml -> CHF 6.40/l
    const up = unitPrice(320, { unitSize: 500, unitMeasure: "ml", unitCount: 1 });
    assert.equal(up?.label, "CHF 6.40/l");
  });

  test("computes CHF per kilogram for a snack", () => {
    // CHF 5.90 for 175g -> CHF 33.71/kg
    const up = unitPrice(590, { unitSize: 175, unitMeasure: "g", unitCount: 1 });
    assert.equal(up?.label, "CHF 33.71/kg");
  });

  test("accounts for multipacks", () => {
    // CHF 3.00 for 3 x 27g = 81g -> CHF 37.04/kg
    const up = unitPrice(300, { unitSize: 27, unitMeasure: "g", unitCount: 3 });
    assert.equal(up?.label, "CHF 37.04/kg");
  });

  test("imposes no unit price on goods counted per piece", () => {
    assert.equal(unitPrice(300, { unitSize: 1, unitMeasure: "piece", unitCount: 1 }), null);
  });

  test("labels packs readably", () => {
    assert.equal(packLabel({ unitSize: 500, unitMeasure: "ml", unitCount: 1 }), "500ml");
    assert.equal(packLabel({ unitSize: 27, unitMeasure: "g", unitCount: 3 }), "3 × 27g");
  });
});
