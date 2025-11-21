import streamlit as st
import pandas as pd
import time
from io import BytesIO
import csv
import math
import re

from processing import (
    stage1_pipeline_1,
    stage1_pipeline_2,
    stage1_pipeline_3,
    stage1_pipeline_4,
    stage1_pipeline_5,
    stage1_pipeline_6,
    stage1_pipeline_7,
    stage1_pipeline_7_1,
    stage1_pipeline_8,
    stage1_pipeline_9,
    stage1_pipeline_10,
    stage1_pipeline_11,
    parse_component_functions,
    stage1_pipeline_12,
    identify_blank_cells,
    stage1_pipeline_14,
    stage1_pipeline_15,
    stage1_pipeline_16,
    stage1_pipeline_17,
    stage1_pipeline_18,
    stage1_pipeline_19,
    stage1_pipeline_20,
    stage1_pipeline_21,
    stage1_pipeline_22,
    stage1_pipeline_23,
    stage1_pipeline_24,
    stage1_pipeline_25,
    stage1_pipeline_26,
    stage2_pipeline_1,
    stage2_pipeline_2,
    stage2_pipeline_4

)

st.set_page_config(
    page_title="Advansor Wireset Helper",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
.stApp {background: linear-gradient(135deg, #0f1419 0%, #1 50%, #0f1419 100%);}
.main .block-container {padding-top: 2rem; padding-bottom: 2rem;}
.stMarkdown, p {color: #e2e8f0;}
.main-title {font-family: 'Inter', sans-serif; font-size: 3.5rem; font-weight: 700; text-align: center; margin-bottom: 0.5rem; background: linear-gradient(135deg, #00d4aa 0%, #00a693 30%, #0ea5e9 70%, #0284c7 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; text-shadow: 0 4px 20px rgba(0, 212, 170, 0.3);}
.subtitle {font-family: 'Inter', sans-serif; text-align: center; color: #94a3b8; font-size: 1.3rem; font-weight: 400; margin-bottom: 3rem;}
.electric-line {height: 2px; background: linear-gradient(90deg, transparent 0%, #00d4aa 20%, #0ea5e9 50%, #00d4aa 80%, transparent 100%); margin: 1rem auto 2rem auto; width: 60%; box-shadow: 0 0 10px rgba(0, 212, 170, 0.5);}
.upload-container {border: 2px dashed #334155; border-radius: 16px; padding: 3rem 2rem; text-align: center; background: linear-gradient(135deg, rgba(15, 23, 42, 0.8) 0%, rgba(30, 41, 59, 0.6) 100%); margin: 2rem 0; backdrop-filter: blur(10px); transition: all 0.3s ease;}
.upload-container:hover {border-color: #00d4aa;}
.status-success {background: linear-gradient(135deg, #00d4aa 0%, #059669 100%); color: white; padding: 1rem; border-radius: 12px;}
.status-info {background: linear-gradient(135deg, #0ea5e9 0%, #0284c7 100%); color: white; padding: 1rem; border-radius: 12px;}
.status-warning {background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); color: white; padding: 1rem; border-radius: 12px;}
.stMetric {background: linear-gradient(135deg, rgba(30,41,59,0.8) 0%, rgba(51,65,85,0.6) 100%); padding: 1rem; border-radius: 8px;}
.stButton > button {background: linear-gradient(135deg, #00d4aa 0%, #0ea5e9 100%); color: white; border-radius: 12px; padding: 0.75rem 2rem; font-weight: 600; font-family: 'Inter', sans-serif; transition: all 0.3s;}
.stButton > button:hover {transform: translateY(-2px);}
.success-message {color: #22c55e; font-weight: 600; font-size: 0.9rem;}
.blank-cell-highlight {background-color: #fef3c7 !important; border: 2px solid #f59e0b !important;}
#MainMenu, footer, header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-title">⚡ Advansor Wireset Helper</h1>', unsafe_allow_html=True)
st.markdown('<div class="electric-line"></div>', unsafe_allow_html=True)
st.markdown(
    '<p class="subtitle">Intelligent Excel Processing • Sustainable Data Solutions • The Future is Electric</p>',
    unsafe_allow_html=True)

# ── NAVIGATION ──────────────────────────────────────────────────────────────
st.markdown("<div style='text-align:center; margin-bottom:2rem;'>", unsafe_allow_html=True)
# Centered dual-button navigation
if "stage" not in st.session_state:
    st.session_state.stage = None

# Use three columns: left spacer, center (with both buttons), right spacer
col_left, col_center, col_right = st.columns([4, 2, 4])
with col_center:
    # The two buttons, stacked vertically and centered
    if st.button("🚀 Convert for EPLAN", key="btn_eplan", use_container_width=True):
        st.session_state.stage = "eplan"
    st.write("")  # vertical spacing
    if st.button("🔧 Convert for KOMAX", key="btn_komax", use_container_width=True):
        st.session_state.stage = "komax"

st.markdown("---")

# ── STAGE 1 EPLAN UI ────────────────────────────────────────────────────────
if st.session_state.stage == "eplan":
    st.header("Stage 1: Convert for EPLAN")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### 📁 Upload your MAIN Excel file")
        uploaded_file = st.file_uploader(
            "Main Excel file (PCSchematic export)...",
            type=['xlsx', 'xls'],
            help="Upload your main Excel file",
            label_visibility="collapsed",
            key="mainfile"
        )
        st.markdown("### 📥 Upload the ADV_WS_functions file")
        uploaded_advws = st.file_uploader(
            "Advansor component functions file...",
            type=['xlsx', 'xls'],
            help="Upload the ADV_WS_functions.xlsx file",
            label_visibility="collapsed",
            key="advwsfile"
        )

        requirements_ready = uploaded_file is not None and uploaded_advws is not None

        if uploaded_file is not None:
            short_filename = (
                uploaded_file.name if len(uploaded_file.name) <= 20
                else uploaded_file.name[:17] + "..."
            )
            try:
                start_time = time.perf_counter()
                df = pd.read_excel(uploaded_file)
                end_time = time.perf_counter()
            except Exception as e:
                st.error(f"⚠️ SYSTEM ERROR: {e}")
                st.stop()

            st.markdown(
                '<div class="status-success">🔋 Main file uploaded! Waiting for component functions file.</div>',
                unsafe_allow_html=True,
            )
            st.markdown("### 📊 Main File Preview")
            st.dataframe(df.head(10), use_container_width=True, height=350)

        if uploaded_advws is not None:
            try:
                df_component = pd.read_excel(uploaded_advws)
            except Exception as e:
                st.error(f"⚠️ SYSTEM ERROR with ADV_WS_functions: {e}")
                st.stop()

            st.markdown(
                '<div class="status-success">✅ ADV_WS_functions file uploaded!</div>',
                unsafe_allow_html=True,
            )
            st.markdown("### 🧩 Component Functions Preview")
            st.dataframe(df_component.head(10), use_container_width=True, height=200)

        # -------- Processing Block -------- #
        if requirements_ready:
            st.markdown("### 🚦 All files uploaded, ready for processing!")

        if st.button("🚀 RUN STAGE 1 TRANSFORMATION", type="primary"):
                df_stage1, removed_duplicates = stage1_pipeline_1(df.copy())
                df_stage1 = stage1_pipeline_2(df_stage1)
                df_stage1 = stage1_pipeline_3(df_stage1)
                df_stage1 = stage1_pipeline_4(df_stage1)
                df_stage1 = stage1_pipeline_5(df_stage1)
                df_stage1 = stage1_pipeline_6(df_stage1)
                df_stage1 = stage1_pipeline_7(df_stage1)
                df_stage1 = stage1_pipeline_8(df_stage1)
                df_stage1 = stage1_pipeline_9(df_stage1)
                group_symbols = parse_component_functions(df_component)
                df_stage1 = stage1_pipeline_16(df_stage1)
                df_stage1 = stage1_pipeline_7_1(df_stage1)
                df_stage1 = stage1_pipeline_10(df_stage1, group_symbols)
                df_stage1 = stage1_pipeline_11(df_stage1)
                df_stage1 = stage1_pipeline_14(df_stage1)
                df_stage1 = stage1_pipeline_15(df_stage1)
                df_stage1 = stage1_pipeline_17(df_stage1)
                df_stage1 = stage1_pipeline_18(df_stage1)
                df_stage1 = stage1_pipeline_19(df_stage1)
                df_stage1 = stage1_pipeline_20(df_stage1)
                df_stage1 = stage1_pipeline_21(df_stage1)
                df_stage1 = stage1_pipeline_22(df_stage1)
                df_stage1 = stage1_pipeline_23(df_stage1)
                df_stage1 = stage1_pipeline_24(df_stage1)
                df_stage1 = stage1_pipeline_25(df_stage1)
                df_stage1 = stage1_pipeline_26(df_stage1)
                # ── ADD THIS SNIPPET TO CALCULATE AND DISPLAY -XPE TERMINALS ─────────
                # Count rows where Line-Function is GNYE
                gnyc_count = (df_stage1['Line-Function'] == 'GNYE').sum()
                # Divide by 2 and round up
                xpe_terminals = -(-gnyc_count // 2)
                
                # Display in Streamlit
                st.markdown("### Required Protective‐Earth (-XPE) Terminals")
                st.metric("Number of -XPE terminals", xpe_terminals)
                # ── Terminal Count Statistics ────────────────────────────────────────────────
                terminal_list = [
                    "-X0101:230VL", "-X0101:230VN", "-X0100:L3", "-X0100:230VL2",
                    "-X0100:N", "-X0100:230VN2", "-X0102:0VDC", "-X0102:24VDC",
                    "-X0102:24VDC1", "-X0102:24VDC2"
                ]
                # Count occurrences in Name or Name.1
                counts = {}
                for term in terminal_list:
                    counts[term] = (
                            (df_stage1['Name'] == term).sum() +
                            (df_stage1['Name.1'] == term).sum()
                    )
                # Compute required terminal blocks (6 per block, round up)

                blocks_needed = {term: math.ceil(count / 6) for term, count in counts.items()}

                # Display in UI
                st.markdown("### 🔌 Terminal Blocks Required")
                for term, blocks in blocks_needed.items():
                    if counts[term] > 0:
                        st.write(f"{term}: {counts[term]} occurrences → {blocks} block(s)")


                # Prepare for editing and identify blank cells
                df_stage1 = stage1_pipeline_12(df_stage1)
                blank_cells = identify_blank_cells(df_stage1)

                # Store in session state
                st.session_state["stage1_data"] = df_stage1
                st.session_state["blank_cells"] = blank_cells

                st.markdown("### ✅ Stage 1 Transformation Result Preview")
                st.dataframe(df_stage1.head(20), use_container_width=True, height=400)

                # Show blank cell statistics
                total_blank_cells = sum(len(rows) for rows in blank_cells.values())
                total_rows = df_stage1.shape[0]
                total_cols = df_stage1.shape[1]

                st.markdown("### 📊 Data Overview")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("🔢 Total Rows", f"{total_rows:,}")
                c2.metric("📋 Columns", f"{total_cols}")
                c3.metric("⚠️ Blank Cells", f"{total_blank_cells:,}")
                c4.metric("🗑️ Removed Duplicates", f"{removed_duplicates}")

                if total_blank_cells > 0:
                    st.markdown(
                        f'<div class="status-warning">⚠️ **{total_blank_cells}** blank cells detected across **{len(blank_cells)}** columns. Please fill them in the editor below.</div>',
                        unsafe_allow_html=True,
                    )

    # -------- Blank cell editor & download block -------- #
    if "stage1_data" in st.session_state and st.session_state["stage1_data"] is not None:
        st.markdown("---")
        full_df = st.session_state["stage1_data"].copy()
        blank_cells = st.session_state.get("blank_cells", {})
        total_blank_cells = sum(len(rows) for rows in blank_cells.values())
        if total_blank_cells > 0:
            st.markdown("## 🛠️ **Interactive Data Editor**")
            st.markdown("**Instructions:** Fill in missing values below. Only rows with blanks are shown. Changes are auto-saved.")

            summary_df = pd.DataFrame({
                "Column": list(blank_cells.keys()),
                "Row Numbers": [", ".join(map(str, rows)) for rows in blank_cells.values()]
            }).reset_index(drop=True)
            st.dataframe(summary_df, use_container_width=True)

            # Show only rows with blanks in the editor
            all_blank_row_indices = set()
            for rows in blank_cells.values():
                all_blank_row_indices.update(rows)
            if all_blank_row_indices:
                all_blank_row_indices = sorted(list(all_blank_row_indices))
                st.session_state["all_blank_row_indices"] = all_blank_row_indices
                df_with_blanks = full_df.loc[all_blank_row_indices].copy()
                df_with_blanks = stage1_pipeline_12(df_with_blanks)

                # Friendly column headers
                column_config = {}
                for col in df_with_blanks.columns:
                    if col in ("Name", "Name.1"):
                        column_config[col] = st.column_config.TextColumn(
                            col, help=f"Component identifier – {len(blank_cells.get(col, []))} blank cells", width="medium", required=True)
                    elif col == "Wireno":
                        column_config[col] = st.column_config.TextColumn("Wire-Tag", help=f"Wire tag – {len(blank_cells.get(col, []))} blank cells", width="small")
                    elif col == "Line-Name":
                        column_config[col] = st.column_config.TextColumn("Cross-Section", help=f"Cross-section – {len(blank_cells.get(col, []))} blank cells", width="small")
                    elif col == "Line-Function":
                        column_config[col] = st.column_config.TextColumn("Wire-Color", help=f"Wire colour – {len(blank_cells.get(col, []))} blank cells", width="small")
                    elif col == "DaisyNo":
                        column_config[col] = st.column_config.TextColumn(col, help="Group identifier", width="small")
                    else:
                        column_config[col] = st.column_config.TextColumn(col, help=f"{len(blank_cells.get(col, []))} blank cells", width="medium")

                # Editor + save on change
                def update_blank_cells():
                    editor_state = st.session_state.data_editor
                    if "edited_rows" in editor_state and editor_state["edited_rows"]:
                        full_df = st.session_state["stage1_data"]
                        all_blank_row_indices = st.session_state.get("all_blank_row_indices", [])
                        for row_idx, changes in editor_state["edited_rows"].items():
                            for col, new_value in changes.items():
                                if row_idx < len(all_blank_row_indices):
                                    original_idx = all_blank_row_indices[row_idx]
                                    full_df.at[original_idx, col] = new_value
                        st.session_state["stage1_data"] = full_df
                        st.session_state["blank_cells"] = identify_blank_cells(full_df)

                st.data_editor(
                    df_with_blanks,
                    column_config=column_config,
                    use_container_width=True,
                    height=600,
                    num_rows="fixed",
                    key="data_editor",
                    on_change=update_blank_cells
                )

        # Download section
        st.markdown("---")
        buffer_final = BytesIO()
        full_df.to_excel(buffer_final, index=False)
        base = uploaded_file.name[:8]
        download_name = f"{base}_ADV_EPLAN_IMPORT.xlsx"
        st.download_button(
            "📥 Download Final Data",
            buffer_final.getvalue(),
            file_name=download_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ── Stage 2: KOMAX UI ─────────────────────────────────────────────────────────
elif st.session_state.stage == "komax":
    st.header("Stage 2: Convert for KOMAX")

    uploaded_csv = st.file_uploader(
        "📁 Upload your KOMAX CSV file (auto‐detected delimiter)…",
        type=["csv"],
        key="komax_csv",
    )
    if uploaded_csv:
        try:
            df_stage2 = stage2_pipeline_1(uploaded_csv)
            df_stage2 = stage2_pipeline_2(df_stage2)
            # df_stage2 = stage2_pipeline_3(df_stage2)
            df_stage2 = stage2_pipeline_4(df_stage2)
        except Exception as e:
            st.error(f"Error processing CSV: {e}")
            st.stop()

        st.markdown("### ✅ Stage 2 Result (with Daisy Chain Detection)")
        st.dataframe(df_stage2.head(10), use_container_width=True, height=300)

        buf = BytesIO()
        df_stage2.to_csv(buf, index=False)
        base2 = uploaded_csv.name[:8]
        download_name2 = f"{base2}_ADV_DLW_IMPORT.csv"
        st.download_button(
            "📥 Download Processed CSV",
            buf.getvalue(),
            file_name=download_name2,
            mime="text/csv"
        )



# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center; padding:1rem 0; color:#64748b;">
  🌱 Sustainable Data Solutions • ⚡ The Future is Electric
</div>
""", unsafe_allow_html=True)
