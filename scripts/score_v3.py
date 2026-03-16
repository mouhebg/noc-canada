"""
score_v3.py — Task-level AI exposure scorer with GC Job Bank outlook anchoring.

Changes from v2:
  - Uses the master prompt (task decomposition, substitution vs augmentation)
  - Feeds in Job Bank 2025-2027 employment outlook as a grounding input
  - 3-year time horizon aligned with Job Bank
  - Returns richer output: core_tasks, factors, bands, summary
  - Still does 2-pass scoring for consistency checking

Usage:
    export GEMINI_API_KEY=your_key_here
    python scripts/score_v3.py
    python scripts/score_v3.py --force
    python scripts/score_v3.py --code 21223
"""

import argparse, json, os, sys, time, urllib.request, urllib.error
from pathlib import Path

ROOT      = Path(__file__).parent.parent
PAGES_DIR = ROOT / "data" / "pages"
DATA_DIR  = ROOT / "data"
SCORES_V3 = DATA_DIR / "scores_v3.json"
FLAGS_FILE = DATA_DIR / "flagged_v3.json"

sys.path.insert(0, str(Path(__file__).parent))
from noc_list import NOC_UNIT_GROUPS, BOC_NAMES, TEER_DESCRIPTIONS

GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={{api_key}}"
)

def gemini(prompt, api_key, temperature=1.0):
    url  = GEMINI_URL.format(api_key=api_key)
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": 2000,
            "responseMimeType": "application/json"
        }
    }).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=45) as r:
        data = json.loads(r.read())
    return data["candidates"][0]["content"]["parts"][0]["text"].strip()


# ── Master system prompt (task-level methodology) ─────────────────────────────
SYSTEM_PROMPT = """You are a Canadian occupational AI exposure analyst.

Your job is to estimate how exposed a Canadian occupation is to AI using NOC 2021 
occupation descriptions and, when available, its main duties, skills, work activities,
knowledge, abilities, work context, employment requirements, and Canadian labour market notes.

Your analysis must be occupation-level, Canada-specific, conservative, and methodical.

CORE RULE:
AI exposure means the extent to which current and near-term AI systems could materially
perform, accelerate, automate, or reshape the occupation's core tasks.
AI exposure is NOT a prediction of layoffs, unemployment, or inevitable job loss.

TIME HORIZON: 3 years (aligned with GC Job Bank Employment Outlook methodology).

AI capabilities to consider:
- Large language models and multimodal AI
- Speech-to-text and text-to-speech systems
- Document automation and workflow automation
- Search, retrieval and classification systems
- Recommendation and prediction systems
- Coding copilots and computer vision (where relevant)

IMPORTANT INTERPRETATION RULES:
1. Score tasks first, then aggregate to the occupation.
2. Separate substitution from augmentation.
3. Do not assume full robotics deployment unless the occupation is already digital/screen-based.
4. Physical presence, licensing, regulation, liability, safety, and interpersonal complexity reduce replacement potential.
5. A job can have high AI impact even if replacement risk is only moderate.
6. Hands-on work, chaotic environments, frontline care, crisis response, manual dexterity,
   and trust-heavy human interaction usually reduce substitution exposure.
7. Templated digital work, repetitive documentation, routine analysis, scheduling,
   summarization, structured communication, and information processing usually increase exposure.
8. When evidence is mixed, choose the more conservative score.

SCORING ANCHORS (use as your fixed reference frame):
- Athletes, dancers (51220, 51123):          very low  — irreducibly physical/human
- Roofers, shinglers (72120):                very low  — physically demanding outdoor work
- Plumbers (72020):                          low       — physically complex, variable environments
- Registered nurses (31300):                 moderate  — hands-on care + cognitive mix
- General practitioners (31102):             moderate  — clinical judgment + AI diagnostic tools
- Lawyers (41101):                           high      — research/drafting AI-amenable; advocacy human
- Financial auditors/accountants (11100):    high      — structured cognitive work, highly automatable
- Software developers/programmers (21223):   high      — AI coding copilots already transforming role
- Data entry clerks (14100):                 very high — structured digital input, fully replaceable

Return valid JSON only matching this exact schema — no markdown, no extra text."""


def build_prompt(code, title, boc, teer, jobbank_outlook, description=None):
    boc_name     = BOC_NAMES.get(boc, f"BOC {boc}")
    teer_desc    = TEER_DESCRIPTIONS.get(teer, "")
    outlook_str  = jobbank_outlook.get("label", "Undetermined")
    outlook_note = (
        f"GC Job Bank 2025-2027 National Employment Outlook: {outlook_str} "
        f"({jobbank_outlook.get('stars', 0)}/5 stars). "
        "This is the government's assessment of labour demand — use it to inform "
        "your outlook field but do NOT let it directly override the AI exposure score. "
        "An occupation can be high AI exposure AND have good demand (e.g. software developers)."
    )

    desc_section = ""
    if description:
        desc = description[:3000] + ("\n[truncated]" if len(description) > 3000 else "")
        desc_section = f"\nFull occupation description:\n---\n{desc}\n---\n"

    schema = '''{
  "noc_code": "string",
  "occupation_title": "string",
  "core_tasks": [
    {"task": "string", "task_importance": 0.0,
     "substitution_exposure": 0.0, "augmentation_exposure": 0.0,
     "rationale": "string"}
  ],
  "factors": {
    "physical_presence_constraint": 0.0,
    "regulatory_constraint": 0.0,
    "liability_constraint": 0.0,
    "interpersonal_trust_constraint": 0.0,
    "unpredictability_constraint": 0.0,
    "data_structure_advantage": 0.0,
    "digital_workflow_advantage": 0.0
  },
  "scores": {
    "raw_substitution_score": 0.0,
    "raw_augmentation_score": 0.0,
    "adjusted_ai_exposure_score": 0.0,
    "adjusted_ai_impact_score": 0.0,
    "adjusted_substitution_risk_score": 0.0
  },
  "bands": {
    "ai_exposure_band": "very low|low|moderate|high|very high",
    "ai_impact_band": "very low|low|moderate|high|very high",
    "substitution_risk_band": "very low|low|moderate|high|very high"
  },
  "wage_low": 0,
  "wage_high": 0,
  "employment_k": 0,
  "outlook": "growing|stable|declining|uncertain",
  "jobbank_outlook": "very_good|good|moderate|limited|very_limited|undetermined",
  "summary": {
    "most_exposed_tasks": ["string"],
    "least_exposed_tasks": ["string"],
    "why_this_job_is_exposed": "string",
    "why_this_job_is_not_fully_replaceable": "string",
    "plain_language_take": "string"
  },
  "confidence": 0.0
}'''

    return f"""{SYSTEM_PROMPT}

NOC Code:  {code}
Title:     {title}
BOC:       {boc_name}
TEER {teer}: {teer_desc}
{outlook_note}
{desc_section}
METHOD:
Step 1: Extract 8-15 core tasks that define this occupation in practice in Canada.
Step 2: For each task assign task_importance (0.05-1.00), substitution_exposure (0-1), augmentation_exposure (0-1), rationale.
Step 3: Assess friction/enablement factors (0-1 each).
Step 4: Compute weighted task scores.
Step 5: Adjust for real-world constraints. adjusted_ai_exposure_score reflects overall AI-driven change exposure.
Step 6: Assign bands: very low (0-0.19), low (0.20-0.39), moderate (0.40-0.59), high (0.60-0.79), very high (0.80-1.00).
Step 7: Write plain language summary.

wage_low/wage_high: typical Canadian annual salary in dollars (integers).
employment_k: estimated Canadian employment in thousands.
outlook: your assessment of this occupation's trajectory — growing|stable|declining|uncertain.
jobbank_outlook: echo back the GC Job Bank rating provided above.
confidence: your confidence in this assessment (0.0-1.0).

JSON SCHEMA TO FOLLOW EXACTLY:
{schema}"""


def load_scores(): return json.loads(SCORES_V3.read_text()) if SCORES_V3.exists() else {}
def save_scores(s): SCORES_V3.write_text(json.dumps(s, indent=2, ensure_ascii=False))
def load_flags():   return json.loads(FLAGS_FILE.read_text()) if FLAGS_FILE.exists() else []
def save_flags(f):  FLAGS_FILE.write_text(json.dumps(f, indent=2))

def load_jobbank():
    jb_file = DATA_DIR / "jobbank_outlooks.json"
    if jb_file.exists():
        data = json.loads(jb_file.read_text())
        return data.get("occupations", {})
    return {}


def call_once(api_key, code, title, boc, teer, jobbank_outlook, description=None, temperature=1.0):
    prompt = build_prompt(code, title, boc, teer, jobbank_outlook, description)
    raw    = gemini(prompt, api_key, temperature)
    raw    = raw.replace("```json","").replace("```","").strip()
    start  = raw.find("{"); end = raw.rfind("}") + 1
    if start >= 0 and end > start:
        raw = raw[start:end]
    result = json.loads(raw)

    # Validate scores are in range
    scores = result.get("scores", {})
    for k in ("adjusted_ai_exposure_score","adjusted_ai_impact_score","adjusted_substitution_risk_score",
              "raw_substitution_score","raw_augmentation_score"):
        if k in scores:
            scores[k] = round(max(0.0, min(1.0, float(scores[k]))), 3)

    result["wage_low"]     = int(result.get("wage_low", 35000))
    result["wage_high"]    = int(result.get("wage_high", 75000))
    result["employment_k"] = max(1, int(result.get("employment_k", 10)))
    result["confidence"]   = round(max(0.0, min(1.0, float(result.get("confidence", 0.7)))), 2)
    return result


def score_occupation(api_key, code, title, boc, teer, jobbank_outlook):
    md_file     = PAGES_DIR / f"{code}.md"
    description = md_file.read_text(encoding="utf-8", errors="replace") if md_file.exists() else None
    has_desc    = description is not None

    pass1 = call_once(api_key, code, title, boc, teer, jobbank_outlook, description, temperature=0.8)
    time.sleep(0.5)
    pass2 = call_once(api_key, code, title, boc, teer, jobbank_outlook, description, temperature=0.5)

    # Compare adjusted_ai_exposure_score between passes
    s1 = pass1.get("scores", {}).get("adjusted_ai_exposure_score", 0.5)
    s2 = pass2.get("scores", {}).get("adjusted_ai_exposure_score", 0.5)
    diff    = abs(s1 - s2)
    flagged = diff > 0.15  # flag if passes disagree by >0.15 (equivalent to 1.5 on 0-10 scale)

    # Use pass2 (lower temperature = more conservative/consistent) as primary
    # but annotate with pass1 scores for transparency
    result = pass2
    result["has_description"] = has_desc
    result["flagged"]         = flagged
    result["pass1_exposure"]  = round(s1, 3)
    result["pass2_exposure"]  = round(s2, 3)
    result["exposure_diff"]   = round(diff, 3)

    # Convenience field: final_score on 0-10 for backward compatibility with site
    result["final"] = round(result["scores"]["adjusted_ai_exposure_score"] * 10, 1)

    return result, flagged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force",   action="store_true")
    parser.add_argument("--code",    help="Score a single NOC code only")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY","")
    if not api_key and not args.dry_run:
        print("ERROR: export GEMINI_API_KEY=your_key_here"); sys.exit(1)

    scores   = load_scores()
    flags    = load_flags()
    jobbank  = load_jobbank()

    if not jobbank:
        print("WARNING: No Job Bank data found. Run scripts/fetch_jobbank.py first.")
        print("  Proceeding without Job Bank outlook anchoring.\n")

    if args.code:
        to_score = [(c,t,b,r) for c,t,b,r in NOC_UNIT_GROUPS if c == args.code]
    else:
        to_score = [(c,t,b,r) for c,t,b,r in NOC_UNIT_GROUPS if args.force or c not in scores]

    md_count = len(list(PAGES_DIR.glob("*.md"))) if PAGES_DIR.exists() else 0
    print(f"Scoring {len(to_score)} occupations ({len(NOC_UNIT_GROUPS)-len(to_score)} already done)")
    print(f"Job Bank data loaded: {len(jobbank)} occupations")
    print(f"Full ESDC descriptions: {md_count}/{len(NOC_UNIT_GROUPS)}")
    print(f"Model: {GEMINI_MODEL}  |  2 passes  |  3-year horizon  |  task-level methodology\n")

    ok = errors = newly_flagged = 0

    for i, (code, title, boc, teer) in enumerate(to_score):
        pct    = (i+1)/len(to_score)*100
        has_md = (PAGES_DIR/f"{code}.md").exists()
        jb     = jobbank.get(code, {"label":"Undetermined","stars":0})
        print(f"  [{i+1:3d}/{len(to_score)}] {pct:5.1f}%  {code}  "
              f"[{'desc' if has_md else 'title'}]  [JB:{jb.get('label','?')}]  {title[:42]}")

        if args.dry_run:
            print(f"         → [dry-run]"); continue

        try:
            result, flagged = score_occupation(api_key, code, title, boc, teer, jb)
            scores[code] = result
            ok += 1

            exp   = result["scores"].get("adjusted_ai_exposure_score", 0)
            sub   = result["scores"].get("adjusted_substitution_risk_score", 0)
            band  = result.get("bands",{}).get("ai_exposure_band","?")
            conf  = result.get("confidence", 0)
            ntasks = len(result.get("core_tasks", []))
            print(f"         → exp={exp:.2f} sub={sub:.2f} [{band}]  "
                  f"{ntasks} tasks  conf={conf:.2f}  {result.get('outlook','?')}"
                  f"{'  ⚑ FLAGGED' if flagged else ''}")

            if flagged:
                newly_flagged += 1
                flags = [f for f in flags if f["code"] != code]
                flags.append({"code":code,"title":title,
                               "pass1":result["pass1_exposure"],
                               "pass2":result["pass2_exposure"],
                               "diff":result["exposure_diff"]})
                save_flags(flags)

            if ok % 5 == 0:
                save_scores(scores)

            time.sleep(0.8)

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8","replace")
            print(f"         → HTTP {e.code}: {body[:100]}")
            errors += 1; save_scores(scores)
            if e.code in (429, 503):
                print("  Rate limited — waiting 30s..."); time.sleep(30)
        except Exception as e:
            print(f"         → ERROR: {e}")
            errors += 1; save_scores(scores)

    save_scores(scores)
    print(f"\n{'='*60}")
    print(f"Done. {ok} scored, {errors} errors, {newly_flagged} flagged.")
    print(f"Total in scores_v3.json: {len(scores)}/{len(NOC_UNIT_GROUPS)}")

    if scores:
        exposure_vals = [v["scores"]["adjusted_ai_exposure_score"] 
                        for v in scores.values() if "scores" in v]
        if exposure_vals:
            avg = sum(exposure_vals)/len(exposure_vals)
            print(f"Average AI exposure: {avg:.3f} ({avg*10:.1f}/10)")
            bands = {}
            for v in scores.values():
                b = v.get("bands",{}).get("ai_exposure_band","unknown")
                bands[b] = bands.get(b,0) + 1
            print("Band distribution:")
            for band in ["very high","high","moderate","low","very low"]:
                print(f"  {band:10s}: {bands.get(band,0)}")


if __name__ == "__main__":
    main()
