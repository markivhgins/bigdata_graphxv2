# Social Network Analyzer using Apache Spark GraphX & Streamlit

**Big Data Essentials · Project 30**  
An end-to-end interactive Social Network Analysis platform built with **Streamlit**, **Plotly**, and **Apache Spark GraphX** (with integrated cloud standalone engine for instant Streamlit Community Cloud deployment).

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io)

---

## 🚀 How to Host on Streamlit Community Cloud

Hosting this project on **Streamlit Community Cloud** (share.streamlit.io) takes under 1 minute with 0 manual server configuration:

1. **Push this repository to GitHub** (if forked or using your own account, e.g., `https://github.com/markivhgins/bigdata_graphxv2`).
2. Go to **[share.streamlit.io](https://share.streamlit.io)** and log in with your GitHub account.
3. Click **"New app"** (or **"Create app"**):
   - **Repository:** `markivhgins/bigdata_graphxv2`
   - **Branch:** `main`
   - **Main file path:** `app.py` (or `streamlit_app.py`)
4. Click **Deploy!** 🎈

The app is fully pre-configured:
- Pre-bundled results load immediately on first open—no empty screens or waiting for builds.
- Works 100% self-contained in Streamlit Cloud's container environment.
- Supports interactive dataset uploads (`.txt` and `.csv`) with instant recalculation.

---

## ⚡ Analysis Engine Architecture (Dual-Mode)

1. **Local Mode (Apache Spark GraphX · Scala):**
   - Automatically detected if `spark-submit` and the compiled Scala JAR (`social-network-graphx_2.12-1.0.0.jar`) are present.
   - Executes genuine Spark GraphX `graph.pageRank(0.0001)` and `graph.connectedComponents()`.

2. **Cloud Mode (Streamlit Cloud Native Engine):**
   - Active on cloud hosting environments (like Streamlit Community Cloud) where a multi-GB Spark/JVM cluster is unavailable.
   - Runs GraphX-identical PageRank ($tol = 0.0001, \alpha = 0.85$) and Connected Components directly within the Streamlit runtime.
   - Allows users to upload custom relationship graphs and analyze them in real-time.

---

## 🌟 Dashboard Features

- **📊 Network Overview:** Global graph metrics (total users, relationships, communities, top influencer), high-level network snapshot, and top-5 ranking bar chart.
- **🕸️ Interactive Network Visualization:** Plotly-powered force/circular layout with zoom, pan, hover tooltips, and component-based filtering.
- **🏆 PageRank & Influential Users:** PageRank distribution chart, full sortable rankings table, and CSV export.
- **🧩 Connected Components & Communities:** Component size histograms, cluster breakdown, individual community explorer, and CSV export.
- **🔍 User Explorer:** Deep-dive into individual users to inspect their PageRank, community assignment, direct connections, and local ego network.
- **📖 About & Methodology:** Theoretical background, GraphX algorithms explanation, dataset attribution, and graph statistics JSON download.

---

## 📂 Repository Structure

```
├── app.py                      # Primary Streamlit application
├── streamlit_app.py            # Streamlit Cloud entrypoint alias
├── requirements.txt            # Python dependencies
├── packages.txt                # Linux apt-get requirements
├── .streamlit/
│   └── config.toml             # Custom theme and server settings
├── data/
│   ├── sample_edges.txt        # Included synthetic demo dataset
│   └── dataset_info.json       # Dataset metadata
├── input/
│   └── current_edges.csv       # Active edge list
├── output/
│   ├── pagerank_results.csv    # PageRank scores and vertex degrees
│   ├── communities_results.csv # Connected component mapping
│   ├── graph_stats.json        # Global graph statistics
│   └── spark_run.log           # Engine execution log
├── assets/
│   └── project_logo.svg        # Project branding
├── spark/                      # Scala GraphX backend source & sbt build
│   ├── build.sbt
│   └── src/main/scala/com/socialanalyzer/SocialNetworkAnalyzer.scala
├── scripts/
│   ├── run_graphx.bat          # Local Spark run helper
│   └── verify_graphx_outputs.py# Verification script for output integrity
└── START_PROJECT.bat           # Windows 1-click startup script
```

---

## 💻 Local Setup & Execution

### 1. Quick Start (Windows)
Double-click `START_PROJECT.bat` or run in terminal:
```cmd
START_PROJECT.bat
```

### 2. Manual Setup
```bash
# Clone the repository
git clone https://github.com/markivhgins/bigdata_graphxv2.git
cd bigdata_graphxv2

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the Streamlit application
streamlit run app.py
```

### 3. Optional: Compile Scala Spark GraphX Backend (for local Spark users)
```bash
cd spark
sbt clean package
```
Builds `spark/target/scala-2.12/social-network-graphx_2.12-1.0.0.jar`.
Ensure Java 17, Scala 2.12, and Spark 3.5.3 are installed and added to `PATH`.

---

## 🧪 Output Verification
To verify the output files and metric constraints:
```bash
python scripts/verify_graphx_outputs.py
```
Expected output:
```text
OUTPUTS_VALID
Engine: Apache Spark GraphX 3.5.3 | vertices=60 | edges=180 | components=3
```
