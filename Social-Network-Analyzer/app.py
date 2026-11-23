from __future__ import annotations
import json, os, re, shutil, subprocess
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"
JAR = ROOT / "spark" / "target" / "scala-2.12" / "social-network-graphx_2.12-1.0.0.jar"
APP_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');
.stApp {background: radial-gradient(ellipse at 10% 0%, #17243c 0%, #0a1020 42%, #080d18 100%); color:#e8eef9; font-family:'DM Sans',sans-serif;}
[data-testid="stSidebar"] {background:#0b1424; border-right:1px solid #223149;}
.block-container {padding-top:2rem; max-width:1450px;}
h1,h2,h3 {font-family:'Space Grotesk',sans-serif!important; letter-spacing:-.03em;}
.hero {padding:25px 28px; border:1px solid #263b58; border-radius:20px; background:linear-gradient(120deg,rgba(28,53,83,.95),rgba(17,28,49,.88)); margin-bottom:20px;}
.hero-kicker {color:#5eead4; text-transform:uppercase; letter-spacing:.14em; font-size:11px; font-weight:700;}
.hero h1 {font-size:34px; margin:6px 0 4px; color:#f2f6ff;}
.hero p {color:#aabbd3; margin:0;}
.metric {background:linear-gradient(145deg,#14243a,#101a2c);border:1px solid #263a55;border-radius:16px;padding:18px 20px;min-height:112px;}
.metric-label {font-size:12px;color:#9db0ca;text-transform:uppercase;letter-spacing:.08em;}
.metric-value {font-family:'Space Grotesk';font-size:29px;font-weight:700;color:#f4f7ff;margin-top:8px;}
.metric-sub {font-size:11px;color:#6ee7d2;margin-top:1px;}
.section-note {color:#91a5c0;font-size:13px;}
div[data-testid="stDataFrame"] {border:1px solid #263a55;border-radius:12px;overflow:hidden;}
.stButton>button {border-radius:10px;border:1px solid #2dd4bf;background:#0f766e;color:white;font-weight:600;}
.stButton>button:hover {background:#115e59;border-color:#5eead4;color:white;}
</style>
"""
st.set_page_config(page_title="Social Network Analyzer | GraphX", page_icon="🕸️", layout="wide", initial_sidebar_state="expanded")
st.markdown(APP_CSS, unsafe_allow_html=True)


def spark_submit_command() -> list[str] | None:
    spark_home = os.environ.get("SPARK_HOME")
    candidates = [Path(spark_home) / "bin" / "spark-submit.cmd"] if spark_home else []
    candidates += [Path(shutil.which("spark-submit"))] if shutil.which("spark-submit") else []
    for candidate in candidates:
        if candidate.exists(): return [str(candidate)]
    return None


def parse_upload(upload) -> pd.DataFrame:
    name = upload.name.lower()
    if name.endswith(".csv"):
        raw = pd.read_csv(upload, comment="#", dtype=str, header=None)
        if raw.shape[1] < 2: raise ValueError("CSV must contain at least two columns: source and target.")
        # Accept both headered and headerless CSV files.
        first = [str(v).strip().lower() for v in raw.iloc[0, :2].tolist()]
        headered = first[0] in {"source", "src", "from"} and first[1] in {"target", "dst", "to"}
        if headered: raw = raw.iloc[1:].reset_index(drop=True)
        df = raw.iloc[:, :2].copy(); df.columns = ["source", "target"]
    elif name.endswith(".txt"):
        upload.seek(0)
        df = pd.read_csv(upload, sep=r"[\s,]+", engine="python", comment="#", header=None, usecols=[0,1], dtype=str)
        df.columns = ["source", "target"]
    else: raise ValueError("Only .txt and .csv files are supported.")
    df = df.dropna()
    df["source"] = df["source"].astype(str).str.strip(); df["target"] = df["target"].astype(str).str.strip()
    df = df[(df.source != "") & (df.target != "") & (df.source != df.target)]
    df["_a"] = df[["source", "target"]].min(axis=1); df["_b"] = df[["source", "target"]].max(axis=1)
    df = df.drop_duplicates(["_a", "_b"])[["source", "target"]].reset_index(drop=True)
    if df.empty: raise ValueError("No valid relationships found. Each row needs two different user IDs.")
    if len(df) > 2_000_000: raise ValueError("This laptop demo is limited to 2,000,000 unique relationships.")
    return df


def run_graphx(df: pd.DataFrame) -> tuple[bool, str]:
    submit = spark_submit_command()
    if submit is None: return False, "Spark submit was not found. Configure SPARK_HOME or add spark-submit to PATH."
    if not JAR.exists(): return False, "Scala GraphX JAR is missing. Open a terminal in the spark folder and run: sbt clean package"
    INPUT.mkdir(exist_ok=True); OUTPUT.mkdir(exist_ok=True)
    # Remove old results first so a failed new run can never display stale GraphX output.
    for old_result in (OUTPUT / "pagerank_results.csv", OUTPUT / "communities_results.csv", OUTPUT / "graph_stats.json"):
        old_result.unlink(missing_ok=True)
    input_path = INPUT / "current_edges.csv"
    df.to_csv(input_path, index=False)
    cmd = submit + ["--class", "com.socialanalyzer.SocialNetworkAnalyzer", "--master", "local[*]", "--conf", "spark.driver.memory=2g", str(JAR), str(input_path), str(OUTPUT)]
    try:
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=900, encoding="utf-8", errors="replace")
        log = (proc.stdout or "") + "\n" + (proc.stderr or "")
        (OUTPUT / "spark_run.log").write_text(log, encoding="utf-8")
        if proc.returncode != 0 or "GRAPHX_SUCCESS" not in log:
            return False, log[-9000:] or f"Spark exited with code {proc.returncode}."
        return True, log[-3000:]
    except subprocess.TimeoutExpired:
        return False, "Spark execution exceeded 15 minutes. Try a smaller edge list or increase the timeout in app.py."
    except Exception as exc: return False, f"Could not launch Spark: {exc}"


def load_results():
    required = [OUTPUT / "pagerank_results.csv", OUTPUT / "communities_results.csv", OUTPUT / "graph_stats.json"]
    if not all(p.exists() for p in required): return None
    ranks = pd.read_csv(required[0], dtype={"user_id": str})
    comm = pd.read_csv(required[1], dtype={"user_id": str, "component_id": str})
    stats = json.loads(required[2].read_text(encoding="utf-8"))
    merged = ranks.merge(comm, on="user_id", how="inner")
    if merged.empty: return None
    merged["degree"] = pd.to_numeric(merged["degree"], errors="coerce").fillna(0).astype(int)
    merged["pagerank"] = pd.to_numeric(merged["pagerank"], errors="coerce")
    return merged, stats


def metric(label, value, sub=""):
    st.markdown(f'<div class="metric"><div class="metric-label">{label}</div><div class="metric-value">{value}</div><div class="metric-sub">{sub}</div></div>', unsafe_allow_html=True)


def make_network(df: pd.DataFrame, limit: int = 180, selected_component: str | None = None):
    view = df.copy()
    if selected_component is not None: view = view[view.component_id.astype(str) == str(selected_component)]
    if len(view) > limit: view = view.nlargest(limit, "degree")
    ids = view.user_id.astype(str).tolist(); idset=set(ids)
    # Deterministic circular placement grouped by component; no Python graph algorithm is used.
    components = sorted(view.component_id.astype(str).unique())
    positions={}
    for ci, comp in enumerate(components):
        members=view[view.component_id.astype(str)==comp].sort_values("pagerank",ascending=False).user_id.astype(str).tolist()
        center_x=(ci-(len(components)-1)/2)*3.4
        for j, uid in enumerate(members):
            angle=2*3.1415926535*j/max(1,len(members)); radius=1.15+0.08*(j%3)
            positions[uid]=(center_x+radius*__import__('math').cos(angle),radius*__import__('math').sin(angle))
    edges=[]
    # Read the same uploaded edge list for rendering only; all metrics remain Spark-produced.
    edge_file=INPUT/"current_edges.csv"
    if edge_file.exists():
        e=pd.read_csv(edge_file,dtype=str)
        for row in e.itertuples(index=False):
            a,b=str(row[0]),str(row[1])
            if a in idset and b in idset:
                edges.append((a,b))
    ex=[];ey=[]
    for a,b in edges:
        x1,y1=positions[a];x2,y2=positions[b];ex += [x1,x2,None];ey += [y1,y2,None]
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=ex,y=ey,mode="lines",line=dict(width=1,color="rgba(119,145,180,.30)"),hoverinfo="skip",name="Relationships"))
    colors=px.colors.qualitative.Bold
    for i,comp in enumerate(components):
        part=view[view.component_id.astype(str)==comp]
        xs=[positions[str(u)][0] for u in part.user_id]; ys=[positions[str(u)][1] for u in part.user_id]
        fig.add_trace(go.Scatter(x=xs,y=ys,mode="markers+text",text=part.user_id.astype(str),textposition="top center",textfont=dict(size=9,color="#dce7f8"),
            marker=dict(size=[max(10,min(30,9+float(v)*250)) for v in part.pagerank],color=colors[i%len(colors)],line=dict(width=1,color="#dbeafe")),
            customdata=part[["user_id","pagerank","degree","component_id"]].astype(str).values,
            hovertemplate="User: %{customdata[0]}<br>PageRank: %{customdata[1]}<br>Degree: %{customdata[2]}<br>Community: %{customdata[3]}<extra></extra>",name=f"Community {comp}"))
    fig.update_layout(template="plotly_dark",paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(10,16,30,.45)",height=560,margin=dict(l=10,r=10,t=30,b=10),
        xaxis=dict(visible=False),yaxis=dict(visible=False,scaleanchor="x",scaleratio=1),legend=dict(orientation="h",y=1.08),hoverlabel=dict(bgcolor="#14243a"))
    return fig


st.sidebar.markdown("## 🕸️ Social Analyzer")
st.sidebar.caption("SPARK GRAPHX · BIG DATA ESSENTIALS")
page=st.sidebar.radio("NAVIGATION",["Overview","Network Visualization","PageRank / Influential Users","Connected Components / Communities","User Explorer","About Project / Methodology"])
st.sidebar.divider()
st.sidebar.markdown("**Analysis engine**")
st.sidebar.success("Apache Spark GraphX · Scala")
st.sidebar.caption("Results are only labelled GraphX after a successful Scala Spark run. No Python algorithm fallback is used.")
st.markdown('<div class="hero"><div class="hero-kicker">BIG DATA ESSENTIALS · PROJECT 30</div><h1>Social Network Analyzer</h1><p>Discover influential users and connected communities with Apache Spark GraphX.</p></div>',unsafe_allow_html=True)

with st.expander("Dataset upload & processing", expanded=not (OUTPUT/"graph_stats.json").exists()):
    c1,c2=st.columns([2,1])
    with c1:
        uploaded=st.file_uploader("Upload an edge-list dataset (.txt or .csv)",type=["txt","csv"],help="One relationship per row. CSV may use source,target column names.")
        use_sample=st.checkbox("Use included synthetic sample dataset",value=uploaded is None)
    with c2:
        st.markdown("**Expected format**")
        st.code("source,target\nuser_1,user_2\nuser_2,user_3",language="text")
        st.caption("Undirected relationships. Duplicate edges and self-links are removed.")
    if uploaded is not None and not use_sample:
        try: candidate=parse_upload(uploaded); st.success(f"Validated: {len(candidate):,} unique relationships · {pd.unique(candidate.values.ravel()).size:,} users"); st.dataframe(candidate.head(8),use_container_width=True)
        except Exception as e: candidate=None; st.error(str(e))
    elif use_sample:
        candidate=pd.read_csv(DATA/"sample_edges.txt",sep=r"\s+",comment="#",header=None,names=["source","target"],dtype=str)
        candidate=candidate.dropna().drop_duplicates().reset_index(drop=True)
        st.info("Using the included synthetic demonstration graph. It is not real-world Facebook/SNAP data.")
        st.caption(f"{pd.unique(candidate[['source','target']].values.ravel()).size:,} distinct users · {len(candidate):,} sample edge rows")
    else: candidate=None
    if st.button("⚡ Run Spark GraphX Analysis",type="primary",disabled=candidate is None):
        with st.spinner("Spark is constructing the GraphX graph and running PageRank + Connected Components..."):
            ok,log=run_graphx(candidate)
        if ok: st.success("GraphX analysis completed successfully."); st.code(log,language="text"); st.rerun()
        else: st.error("Spark GraphX analysis did not complete."); st.code(log,language="text")

result=load_results()
if result is None:
    st.warning("No completed GraphX results are available yet. Upload a dataset (or select the sample) and run the analysis above.")
    st.stop()
df,stats=result
st.caption(f"<span style='color:#5eead4'>● GRAPHX VERIFIED RUN</span> &nbsp; Engine: {stats.get('engine','Apache Spark GraphX')} · Vertices: {stats['vertices']:,} · Edges: {stats['edges']:,}",unsafe_allow_html=True)

if page=="Overview":
    top=df.sort_values("pagerank",ascending=False).iloc[0]
    cols=st.columns(4)
    with cols[0]: metric("TOTAL USERS",f"{stats['vertices']:,}","GraphX vertices")
    with cols[1]: metric("RELATIONSHIPS",f"{stats['edges']:,}","Unique undirected edges")
    with cols[2]: metric("COMMUNITIES",f"{stats['components']:,}","Connected components")
    with cols[3]: metric("TOP INFLUENCER",str(top.user_id),f"PageRank {top.pagerank:.5f}")
    left,right=st.columns([1.5,1])
    with left:
        st.subheader("Network at a glance"); st.plotly_chart(make_network(df,140),use_container_width=True)
    with right:
        st.subheader("Top 5 influential users"); top5=df.nlargest(5,"pagerank")[["user_id","pagerank","degree","component_id"]].copy(); top5.columns=["User","PageRank","Connections","Community"]
        st.dataframe(top5,hide_index=True,use_container_width=True)
        fig=px.bar(top5.sort_values("PageRank"),x="PageRank",y="User",orientation="h",template="plotly_dark",color="PageRank",color_continuous_scale="Tealgrn")
        fig.update_layout(height=260,margin=dict(l=0,r=0,t=10,b=0),paper_bgcolor="rgba(0,0,0,0)"); st.plotly_chart(fig,use_container_width=True)
elif page=="Network Visualization":
    st.subheader("Interactive relationship graph"); comps=sorted(df.component_id.astype(str).unique()); chosen=st.selectbox("Filter by community",["All communities"]+comps)
    st.caption("Zoom, pan and hover over a node to inspect its Spark-computed metrics. Large networks are limited to the 180 highest-degree nodes for browser performance.")
    st.plotly_chart(make_network(df,180,None if chosen=="All communities" else chosen),use_container_width=True)
elif page=="PageRank / Influential Users":
    st.subheader("Influence ranking"); st.markdown('<div class="section-note">PageRank scores are calculated by Spark GraphX, then sorted in this dashboard.</div>',unsafe_allow_html=True)
    top=df.nlargest(10,"pagerank").sort_values("pagerank",ascending=True)
    fig=px.bar(top,x="pagerank",y="user_id",orientation="h",color="pagerank",color_continuous_scale="Tealgrn",template="plotly_dark",labels={"pagerank":"PageRank score","user_id":"User ID"})
    fig.update_layout(height=430,paper_bgcolor="rgba(0,0,0,0)",margin=dict(l=10,r=10,t=25,b=10)); st.plotly_chart(fig,use_container_width=True)
    display=df.sort_values("pagerank",ascending=False).reset_index(drop=True); display.index+=1; display.index.name="Rank"; st.dataframe(display,use_container_width=True)
    st.download_button("⬇ Download PageRank CSV",(OUTPUT/"pagerank_results.csv").read_bytes(),"pagerank_results.csv","text/csv")
elif page=="Connected Components / Communities":
    st.subheader("Detected connected components"); sizes=df.groupby("component_id").size().sort_values(ascending=False).rename("members").reset_index()
    a,b=st.columns([1,2]);
    with a: metric("TOTAL COMPONENTS",f"{len(sizes):,}","GraphX connected components")
    with b:
        fig=px.bar(sizes.head(20),x="component_id",y="members",color="members",template="plotly_dark",labels={"component_id":"Component ID","members":"Users"},color_continuous_scale="Tealgrn"); fig.update_layout(paper_bgcolor="rgba(0,0,0,0)",height=300); st.plotly_chart(fig,use_container_width=True)
    st.dataframe(sizes,hide_index=True,use_container_width=True)
    chosen=st.selectbox("Explore a community",sizes.component_id.astype(str).tolist()); members=df[df.component_id.astype(str)==chosen].sort_values("pagerank",ascending=False)
    st.write(f"Community **{chosen}** contains **{len(members)} users**."); st.dataframe(members,use_container_width=True)
    st.plotly_chart(make_network(df,180,chosen),use_container_width=True)
    st.download_button("⬇ Download communities CSV",(OUTPUT/"communities_results.csv").read_bytes(),"communities_results.csv","text/csv")
elif page=="User Explorer":
    st.subheader("Explore an individual user"); uid=st.selectbox("Select user ID",sorted(df.user_id.astype(str).tolist())); row=df[df.user_id.astype(str)==uid].iloc[0]
    a,b,c=st.columns(3)
    with a: metric("PAGERANK",f"{row.pagerank:.6f}","Spark GraphX score")
    with b: metric("COMMUNITY",str(row.component_id),"Connected component ID")
    with c: metric("DIRECT CONNECTIONS",str(row.degree),"GraphX degree")
    edge_file=INPUT/"current_edges.csv"; edges=pd.read_csv(edge_file,dtype=str); neighbors=edges.loc[edges.source==uid,"target"].tolist()+edges.loc[edges.target==uid,"source"].tolist()
    st.markdown("**Directly connected users**"); st.write(", ".join(sorted(set(neighbors))) if neighbors else "No direct connections found.")
    st.markdown("**Local community view**"); st.plotly_chart(make_network(df,180,str(row.component_id)),use_container_width=True)
    st.caption("The displayed local view shows the selected user's connected component; the selected user is highlighted in the metrics above.")
elif page=="About Project / Methodology":
    st.subheader("Project methodology")
    st.markdown("""
**Objective.** Apply graph processing to social relationship data to identify influential users and connected communities.

**Graph model.** Each distinct user is a vertex. Each unique relationship is an undirected edge. External string IDs are mapped to GraphX `VertexId` values (`Long`) and restored when exporting.

**PageRank.** Spark GraphX iteratively distributes rank through incoming links, with damping/reset probability, until the configured tolerance is reached. A high score indicates structural importance in this graph; it is not a measure of a person's real-world worth.

**Connected Components.** GraphX propagates component labels so every vertex reachable through a path receives the same component ID. Disconnected groups are separate components. These are connectivity groups, not necessarily communities in the statistical/modularity sense.

**Data flow.** Streamlit validates and writes the edge list → `spark-submit` runs the Scala JAR → GraphX exports CSV/JSON → Pandas reads the output → Plotly renders the dashboard.

**Execution honesty.** This dashboard has no Python graph-algorithm fallback. Results are shown only when the Scala backend returns a successful `GRAPHX_SUCCESS` marker and its output files exist.
""")
    st.subheader("Dataset attribution")
    info=json.loads((DATA/"dataset_info.json").read_text()); st.json(info)
    st.markdown("For real-world testing, the Stanford SNAP Facebook dataset is available from [SNAP: Facebook social circles](https://snap.stanford.edu/data/ego-Facebook.html). Respect the dataset's terms and cite the original SNAP authors. The included dataset is synthetic and labelled as such.")
    st.subheader("Pseudocode")
    st.code("GraphX_PageRank(G):\n  rankGraph = G.pageRank(tolerance = 0.0001)\n  return rankGraph.vertices sorted by rank descending",language="text")
    st.code("GraphX_ConnectedComponents(G):\n  componentGraph = G.connectedComponents()\n  return each vertex with its component ID",language="text")
    st.download_button("Download graph statistics JSON",(OUTPUT/"graph_stats.json").read_bytes(),"graph_stats.json","application/json")
