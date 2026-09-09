---
name: anatomy
description: Dissect a product or codebase into its layers and trace how one real request flows end to end. Use to document an existing system, onboard to it, or prove a design actually connects.
---

# Anatomy

Anatomy is the vertical slice: not what the parts are, but how one real thing travels through all of them.

## Method

1. **Pick one real transaction.** Not "a user logs in" — "customer at Langstrasse 84 orders 6 beers at 20:47 on a Friday". Concreteness exposes gaps that abstraction hides.
2. **Trace it through every layer,** naming the file and function at each hop. If you cannot name the file, that layer does not exist yet.
3. **At every hop, record three things:** what data enters, what decision is made, what data leaves.
4. **Mark the cut points** — where the transaction could fail, stall, or be denied. These are the interesting parts of any system.
5. **Repeat for the unhappy path.** The same transaction at 21:03 in Geneva. The same transaction where the customer is 15.

## Layer template

| Layer | Responsibility | Owns | Must not know |
|---|---|---|---|
| Surface | capture intent | pixels, input state | business rules |
| Contract | transport & validation | shapes, codes | storage details |
| Engine | decide | rules, math | I/O, framework |
| Ledger | remember | persistence, audit | why |

## Output

An anatomy document reads as a narrative with code references (`file.ts:42`), not a bullet list of components. The reader should be able to set a breakpoint at any named hop and see the exact data described.

## Test of a good anatomy

Someone who has never seen the codebase reads it, then correctly predicts what happens when you change one rule. If they cannot, the document describes structure but not mechanism — rewrite it around causation.
