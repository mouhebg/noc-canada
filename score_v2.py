"""
score_v2.py — Multi-pass calibrated AI exposure scorer using Google Gemini.

Uses gemini-2.0-flash-lite (fast + cheap for bulk scoring).
No extra SDK needed — uses plain HTTP requests.

Usage:
    export GEMINI_API_KEY=your_key_here
    python scripts/score_v2.py
    python scripts/score_v2.py --force        # re-score everything
    python scripts/score_v2.py --code 21223   # score one occupation
"""

import argparse, json, os, sys, time, urllib.request, urllib.error
from pathlib import Path

ROOT      = Path(__file__).parent.parent
PAGES_DIR = ROOT / "data" / "pages"
DATA_DIR  = ROOT / "data"
SCORES_V2 = DATA_DIR / "scores_v2.json"
FLAGS_FILE = DATA_DIR / "flagged_disagreements.json"

sys.path.insert(0, str(Path(__file__).parent))
from noc_list import NOC_UNIT_GROUPS, BOC_NAMES, TEER_DESCRIPTIONS

GEMINI_MODEL = "gemini-2.0-flash-lite"
GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={{api_key}}"
)

def gemini(prompt, api_key, temperature=1.0):
    url  = GEMINI_URL.format(api_key=api_key)
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": 600}
    }).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    return data["candidates"][0]["content"]["parts"][0]["text"].strip()


ANCHORS = """
CALIBRATION ANCHORS — your fixed reference frame, do not deviate:
  Data entry clerks (14100)      final=9.0  (D1=10,D2=10,D3=10,D4=6)
  Software developers (21223)    final=8.5  (D1=10,D2=9, D3=7, D4=8)
  Financial auditors (11100)     final=8.0  (D1=9, D2=9, D3=9, D4=5)
  Lawyers (41101)                final=7.0  (D1=8, D2=8, D3=6, D4=6)
  General practitioners (31102)  final=5.5  (D1=6, D2=6, D3=5, D4=5)
  Registered nurses (31300)      final=4.5  (D1=5, D2=5, D3=5, D4=3)
  Plumbers (72020)               final=2.5  (D1=2, D2=2, D3=4, D4=2)
  Roofers and shinglers (72120)  final=1.5  (D1=1, D2=1, D3=3, D4=1)
  Athletes (51220)               final=1.0  (D1=1, D2=1, D3=1, D4=1)
""".strip()

SYSTEM_CTX = f"""You are a labour economist scoring Canadian NOC 2021 occupations for AI exposure.

Score on FOUR dimensions (each 0-10), then compute final = mean(D1,D2,D3,D4).

DIMENSIONS:
  D1 task_digitality   — What fraction of core tasks produce digital output or manipulate
                         information? (0=entirely physical, 10=entirely digital)
  D2 physical_barrier  — INVERTED: how much does physical presence PROTECT this job from AI?
                         10=no protection (could be done fully remotely), 0=irreducibly physical
  D3 routine_structure — How routine, rule-based, pattern-following are the core tasks?
                         (0=highly unpredictable/creative, 10=highly routine/structured)
  D4 substitute_risk   — Is AI a substitute (10=replaces worker) or complement (0=augments worker)?

{ANCHORS}

Return ONLY a valid JSON object, no markdown fences, no extra text.
Required fields: code, D1, D2, D3, D4, final, wage_low, wage_high, employment_k, outlook, rationale
  wage_low/high: typical Canadian annual salary in dollars (integers, e.g. 55000)
  employment_k: Canadian employment in thousands (integer)
  outlook: one of growing | stable | declining | uncertain
  rationale: 2-3 sentences on AI risk, which dimension drives the score, how the role may evolve"""


def build_prompt(code, title, boc, teer, description=None):
    boc_name  = BOC_NAMES.get(boc, f"BOC {boc}")
    teer_desc = TEER_DESCRIPTIONS.get(teer, "")
    base = (f"{SYSTEM_CTX}\n\nNOC Code: {code}\nTitle: {title}\n"
            f"BOC: {boc_name}\nTEER {teer}: {teer_desc}")
    if description:
        desc = description[:3500] + ("\n[truncated]" if len(description) > 3500 else "")
        base += f"\n\nFull occupation description:\n---\n{desc}\n---"
    base += (f'\n\nReturn JSON: {{"code":"{code}","D1":N,"D2":N,"D3":N,"D4":N,'
             f'"final":N,"wage_low":N,"wage_high":N,"employment_k":N,'
             f'"outlook":"...","rationale":"..."}}')
    return base


def call_once(api_key, code, title, boc, teer, description=None, temperature=1.0):
    prompt = build_prompt(code, title, boc, teer, description)
    raw    = gemini(prompt, api_key, temperature)
    raw    = raw.replace("```json","").replace("```","").strip()
    start  = raw.find("{"); end = raw.rfind("}") + 1
    if start >= 0 and end > start:
        raw = raw[start:end]
    result = json.loads(raw)
    for dim in ("D1","D2","D3","D4"):
        result[dim] = max(0.0, min(10.0, float(result.get(dim, 5.0))))
    computed = round(sum(result[d] for d in ("D1","D2","D3","D4")) / 4, 1)
    claimed  = float(result.get("final", computed))
    result["final"]        = computed if abs(computed - claimed) > 0.3 else round(claimed, 1)
    result["wage_low"]     = int(result.get("wage_low", 35000))
    result["wage_high"]    = int(result.get("wage_high", 75000))
    result["employment_k"] = max(1, int(result.get("employment_k", 10)))
    result["outlook"]      = result.get("outlook", "stable")
    result["rationale"]    = str(result.get("rationale", ""))
    return result


def score_occupation(api_key, code, title, boc, teer):
    md_file     = PAGES_DIR / f"{code}.md"
    description = md_file.read_text(encoding="utf-8", errors="replace") if md_file.exists() else None
    has_desc    = description is not None
    pass1 = call_once(api_key, code, title, boc, teer, description, temperature=1.0)
    time.sleep(0.4)
    pass2 = call_once(api_key, code, title, boc, teer, description, temperature=0.7)
    diff    = abs(pass1["final"] - pass2["final"])
    flagged = diff > 1.5
    merged  = {
        "code": code, "title": title, "boc": boc, "teer": teer,
        "D1":   round((pass1["D1"]+pass2["D1"])/2, 1),
        "D2":   round((pass1["D2"]+pass2["D2"])/2, 1),
        "D3":   round((pass1["D3"]+pass2["D3"])/2, 1),
        "D4":   round((pass1["D4"]+pass2["D4"])/2, 1),
        "final":       round((pass1["final"]+pass2["final"])/2, 1),
        "wage_low":    (pass1["wage_low"]+pass2["wage_low"])//2,
        "wage_high":   (pass1["wage_high"]+pass2["wage_high"])//2,
        "employment_k":(pass1["employment_k"]+pass2["employment_k"])//2,
        "outlook":     pass2["outlook"],
        "rationale":   pass2["rationale"],
        "has_description": has_desc,
        "flagged":     flagged,
        "pass1_final": pass1["final"],
        "pass2_final": pass2["final"],
    }
    return merged, flagged


def load_scores(): return json.loads(SCORES_V2.read_text()) if SCORES_V2.exists() else {}
def save_scores(s): SCORES_V2.write_text(json.dumps(s, indent=2, ensure_ascii=False))
def load_flags():   return json.loads(FLAGS_FILE.read_text()) if FLAGS_FILE.exists() else []
def save_flags(f):  FLAGS_FILE.write_text(json.dumps(f, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force",   action="store_true")
    parser.add_argument("--code",    help="Score a single NOC code only")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY","")
    if not api_key and not args.dry_run:
        print("ERROR: export GEMINI_API_KEY=your_key_here"); sys.exit(1)

    scores = load_scores()
    flags  = load_flags()

    if args.code:
        to_score = [(c,t,b,r) for c,t,b,r in NOC_UNIT_GROUPS if c == args.code]
    else:
        to_score = [(c,t,b,r) for c,t,b,r in NOC_UNIT_GROUPS if args.force or c not in scores]

    md_count = len(list(PAGES_DIR.glob("*.md"))) if PAGES_DIR.exists() else 0
    print(f"Scoring {len(to_score)} occupations  ({len(NOC_UNIT_GROUPS)-len(to_score)} already done)")
    print(f"Full ESDC descriptions available: {md_count}/{len(NOC_UNIT_GROUPS)}")
    print(f"Model: {GEMINI_MODEL}  |  2 passes per occupation\n")

    ok = errors = newly_flagged = 0

    for i, (code, title, boc, teer) in enumerate(to_score):
        pct     = (i+1)/len(to_score)*100
        has_md  = (PAGES_DIR/f"{code}.md").exists()
        print(f"  [{i+1:3d}/{len(to_score)}] {pct:5.1f}%  {code}  "
              f"[{'desc' if has_md else 'title'}]  {title[:48]}")

        if args.dry_run:
            print(f"         → [dry-run]"); continue

        try:
            result, flagged = score_occupation(api_key, code, title, boc, teer)
            scores[code] = result
            ok += 1
            p1,p2,fin = result["pass1_final"],result["pass2_final"],result["final"]
            print(f"         → {fin:.1f}  (p1={p1:.1f} p2={p2:.1f})  "
                  f"D1={result['D1']} D2={result['D2']} "
                  f"D3={result['D3']} D4={result['D4']}  "
                  f"{result['outlook']}{'  ⚑ FLAGGED' if flagged else ''}")
            if flagged:
                newly_flagged += 1
                flags = [f for f in flags if f["code"] != code]
                flags.append({"code":code,"title":title,
                               "pass1":p1,"pass2":p2,"diff":round(abs(p1-p2),1),"final":fin})
                save_flags(flags)
            if ok % 10 == 0:
                save_scores(scores)
            time.sleep(0.5)

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8","replace")
            print(f"         → HTTP {e.code}: {body[:120]}")
            errors += 1; save_scores(scores)
            if e.code in (429, 503):
                print("  Rate limited — waiting 20s..."); time.sleep(20)
        except Exception as e:
            print(f"         → ERROR: {e}")
            errors += 1; save_scores(scores)

    save_scores(scores)
    print(f"\n{'='*60}")
    print(f"Done. {ok} scored, {errors} errors, {newly_flagged} newly flagged.")
    print(f"Total in scores_v2.json: {len(scores)}/{len(NOC_UNIT_GROUPS)}")
    if scores:
        finals = [v["final"] for v in scores.values()]
        print(f"Average final score: {sum(finals)/len(finals):.2f}/10")


if __name__ == "__main__":
    main()
