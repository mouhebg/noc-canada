"""
build.py — Merge all data sources into site/data.json.

Priority order for scores:
  1. scores_v2.json  (multi-pass, calibrated, sub-dimension)
  2. scores.json     (v1 single-pass fallback)
"""
import json, sys, shutil
from pathlib import Path
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
SITE_DIR = ROOT / "site"
SITE_DIR.mkdir(exist_ok=True)
sys.path.insert(0, str(Path(__file__).parent))
from noc_list import NOC_UNIT_GROUPS, BOC_NAMES, TEER_DESCRIPTIONS
try:
    from validate import STATCAN_CAIOE
except ImportError:
    STATCAN_CAIOE = {}

def main():
    sv2_file = DATA_DIR / "scores_v2.json"
    sv1_file = DATA_DIR / "scores.json"
    scores_v2 = json.loads(sv2_file.read_text()) if sv2_file.exists() else {}
    scores_v1 = json.loads(sv1_file.read_text()) if sv1_file.exists() else {}
    if scores_v2: print(f"Using scores_v2.json ({len(scores_v2)} occupations)")
    lfs_file = DATA_DIR / "lfs.json"
    if lfs_file.exists():
        lfs = json.loads(lfs_file.read_text())
        employment = lfs.get("occupations", {})
        lfs_source = lfs.get("source","estimates")
    else:
        employment = {}; lfs_source = "fallback estimates"
    val_file = DATA_DIR / "validation_report.json"
    validation = json.loads(val_file.read_text()) if val_file.exists() else None
    occupations = []
    for code, title, boc, teer in NOC_UNIT_GROUPS:
        sv2 = scores_v2.get(code, {})
        sv1 = scores_v1.get(code, {})
        if sv2:
            score=round(float(sv2.get("final",sv2.get("score",5.0))),1); wage_low=int(sv2.get("wage_low",35000)); wage_high=int(sv2.get("wage_high",75000)); outlook=sv2.get("outlook","stable"); rationale=sv2.get("rationale",""); D1=round(float(sv2.get("D1",0)),1); D2=round(float(sv2.get("D2",0)),1); D3=round(float(sv2.get("D3",0)),1); D4=round(float(sv2.get("D4",0)),1); flagged=bool(sv2.get("flagged",False)); has_desc=bool(sv2.get("has_description",False))
        elif sv1:
            score=round(float(sv1.get("score",5.0)),1); wage_low=int(sv1.get("wage_low",35000)); wage_high=int(sv1.get("wage_high",75000)); outlook=sv1.get("outlook","stable"); rationale=sv1.get("rationale",""); D1=D2=D3=D4=0.0; flagged=False; has_desc=False
        else:
            score=5.0; wage_low=35000; wage_high=75000; outlook="stable"; rationale=""; D1=D2=D3=D4=0.0; flagged=False; has_desc=False
        emp=round(float(employment.get(code) or (sv2.get("employment_k") if sv2 else None) or (sv1.get("employment_k") if sv1 else None) or 10.0),1)
        caioe=STATCAN_CAIOE.get(code,"")
        occupations.append([code,title,boc,teer,score,emp,wage_low,wage_high,outlook,rationale,D1,D2,D3,D4,caioe,int(flagged),int(has_desc)])
    scored=[o for o in occupations if o[0] in scores_v2 or o[0] in scores_v1]
    avg_score=sum(o[4] for o in scored)/len(scored) if scored else 0
    total_emp=sum(o[5] for o in occupations)
    val_summary=None
    if validation:
        val_summary={"statcan_agreement_pct":validation["statcan_caioe"].get("agreement_pct"),"rbc_pearson_r":validation["rbc"].get("pearson_r")}
    out={"meta":{"total_occupations":len(occupations),"scored_v2":len(scores_v2),"avg_ai_score":round(avg_score,2),"total_employment_k":round(total_emp,0),"flagged_disagreements":sum(1 for o in occupations if o[15]),"has_full_description":sum(1 for o in occupations if o[16]),"employment_source":lfs_source,"validation":val_summary,"scoring_method":"Multi-pass calibrated (v2)" if scores_v2 else "Single-pass (v1)","boc_names":BOC_NAMES,"teer_descriptions":{str(k):v for k,v in TEER_DESCRIPTIONS.items()},"caioe_legend":{"HL":"High exposure, low complementarity (displacement risk)","HH":"High exposure, high complementarity (AI augments workers)","L":"Low exposure","":"Not classified"},"columns":["code","title","boc","teer","score","employment_k","wage_low","wage_high","outlook","rationale","D1_digitality","D2_physical_barrier","D3_routine","D4_substitute","caioe","flagged","has_description"]},"occupations":occupations}
    out_file=SITE_DIR/"data.json"
    out_file.write_text(json.dumps(out,ensure_ascii=False,separators=(",",":")))
    print(f"Built site/data.json — {len(occupations)} occupations, avg={avg_score:.2f}/10, {out_file.stat().st_size//1024}KB")

  # Copy index.html into site/ for GitHub Pages
  shutil.copy(ROOT / "index.html", SITE_DIR / "index.html")
  print("Copied index.html -> site/index.html")
if __name__=="__main__":
    main()
