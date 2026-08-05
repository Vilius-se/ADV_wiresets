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
    stage1_pipeline_11,
    stage1_pipeline_12,
    stage1_pipeline_13,
    parse_component_functions,
    stage1_pipeline_14,
    identify_blank_cells,
    stage1_pipeline_15,
    stage1_pipeline_16,
    stage1_pipeline_17,
    stage1_pipeline_18,
    stage1_pipeline_20,
    stage1_pipeline_21,
    stage1_pipeline_22,
    stage1_pipeline_24,
    stage1_pipeline_25,
    stage1_pipeline_26,
    stage1_pipeline_27,
    stage1_pipeline_28,
    stage1_pipeline_29,
    stage2_pipeline_1,
    stage2_pipeline_2,
    stage2_pipeline_4,
    stage2_pipeline_5,
    stage2_final_text_to_columns,
    validate_distribution_terminals,
    validate_duplicate_endpoint_wirenos,
)

st.set_page_config(
    page_title="Advansor Wireset Testas",
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
.stMetric {background: linear-gradient(135deg, rgba(71,85,105,0.72) 0%, rgba(100,116,139,0.58) 100%); padding: 1rem; border-radius: 8px;}
.stButton > button {background: linear-gradient(135deg, #00d4aa 0%, #0ea5e9 100%); color: white; border-radius: 12px; padding: 0.75rem 2rem; font-weight: 600; font-family: 'Inter', sans-serif; transition: all 0.3s;}
.stButton > button:hover {transform: translateY(-2px);}
.success-message {color: #22c55e; font-weight: 600; font-size: 0.9rem;}
.blank-cell-highlight {background-color: #fef3c7 !important; border: 2px solid #f59e0b !important;}
.section-heading {
    color: #2d3748 !important;
    font-family: 'Inter', sans-serif;
    font-size: 1.45rem;
    font-weight: 700;
    margin: 1.15rem 0 0.55rem 0;
    padding: 0;
}
.group-column-title {
    color: #374151 !important;
    font-family: 'Inter', sans-serif;
    font-size: 1rem;
    font-weight: 700;
    margin: 0 0 0.35rem 0;
    padding: 0;
}
.terminal-group-title {
    color: #3f3f3f !important;
    font-family: monospace;
    font-weight: 700;
    margin: 0;
    padding: 0;
    line-height: 1.15;
}
.terminal-row {
    color: #3f3f3f !important;
    font-family: monospace;
    font-weight: 500;
    margin: 0;
    padding: 0;
    line-height: 1.15;
    white-space: pre;
}
#MainMenu, footer, header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-title">⚡ Advansor Wireset testas</h1>', unsafe_allow_html=True)
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

        requirements_ready = (
            uploaded_file is not None
            and uploaded_advws is not None
        )

        if uploaded_file is not None:
            short_filename = (
                uploaded_file.name
                if len(uploaded_file.name) <= 20
                else uploaded_file.name[:17] + "..."
            )

            try:
                df = pd.read_excel(uploaded_file)
                df_original = df.copy()
            except Exception as e:
                st.error(f"⚠️ SYSTEM ERROR: {e}")
                st.stop()

            st.markdown(
                '<div class="status-success">'
                '🔋 Main file uploaded!'
                '</div>',
                unsafe_allow_html=True,
            )

        if uploaded_advws is not None:
            try:
                df_component = pd.read_excel(uploaded_advws)
            except Exception as e:
                st.error(
                    f"⚠️ SYSTEM ERROR with ADV_WS_functions: {e}"
                )
                st.stop()

            st.markdown(
                '<div class="status-success">'
                '✅ ADV_WS_functions file uploaded!'
                '</div>',
                unsafe_allow_html=True,
            )

        # ---------------------------------------------------------
        # ORIGINALAUS FAILO TERMINALŲ IR SPALVŲ PATIKRA
        # ---------------------------------------------------------
        if uploaded_file is not None:
            st.markdown("### 🔎 Distribution Terminal Check")

            distribution_errors = validate_distribution_terminals(
                df_original
            )

            if distribution_errors.empty:
                st.success(
                    "✅ X0100 / X0101 / X0102 terminalai ir "
                    "paskirstymo grandinių spalvos yra tinkamos."
                )
            else:
                st.error(
                    f"❌ Rastos {len(distribution_errors)} "
                    "paskirstymo grandinių problemos."
                )

                st.dataframe(
                    distribution_errors,
                    use_container_width=True,
                    hide_index=True,
                    height=min(
                        500,
                        80 + len(distribution_errors) * 35,
                    ),
                )

        # -------- Processing Block -------- #
        if requirements_ready:
            st.markdown("### 🚦 All files uploaded, ready for processing!")

            if st.button(
                "🚀 RUN STAGE 1 TRANSFORMATION",
                type="primary",
                use_container_width=True,
            ):
                transformation_start = time.perf_counter()

                df_stage1, removed_duplicates = stage1_pipeline_1(df.copy())
                df_stage1 = stage1_pipeline_2(df_stage1)
                df_stage1 = stage1_pipeline_3(df_stage1)
                df_stage1 = stage1_pipeline_4(df_stage1)
                df_stage1 = stage1_pipeline_5(df_stage1)
                df_stage1 = stage1_pipeline_6(df_stage1)
                df_stage1 = stage1_pipeline_7(df_stage1)
                df_stage1 = stage1_pipeline_8(df_stage1)
                df_stage1 = stage1_pipeline_9(df_stage1)
                # --- FIX EPLAN STRUCTURE (=POWER+X-F128 → =POWER-F128) ---
                df_component_fixed = df_component.copy()
                col_name = df_component_fixed.columns[0]

                df_component_fixed[col_name] = (
                    df_component_fixed[col_name]
                    .astype(str)
                    .str.strip()
                    .apply(lambda x: re.sub(r'^(=[^+\-]+)\+[^-]*', r'\1', x) if isinstance(x, str) and x.strip().startswith("=") else x)
                )
                group_symbols = parse_component_functions(df_component_fixed)
                # Convert group_symbols (GROUP -> [components])
                # into component_to_group (component -> GROUP)
                component_to_group = {}
                for group, symbols in group_symbols.items():
                    for sym in symbols:
                        component_to_group[sym] = group.upper()
                df_stage1 = stage1_pipeline_16(df_stage1)
                df_stage1 = stage1_pipeline_7_1(df_stage1)
                df_stage1 = stage1_pipeline_11(df_stage1, group_symbols)
                df_stage1 = stage1_pipeline_12(df_stage1, group_symbols)
                df_stage1 = stage1_pipeline_13(df_stage1)
                df_stage1 = stage1_pipeline_15(df_stage1)
                df_stage1 = stage1_pipeline_17(df_stage1)
                df_stage1 = stage1_pipeline_18(df_stage1)
                df_stage1 = stage1_pipeline_20(df_stage1)
                df_stage1 = stage1_pipeline_21(df_stage1)
                df_stage1 = stage1_pipeline_22(df_stage1)
                df_stage1 = stage1_pipeline_24(df_stage1)
                df_stage1 = stage1_pipeline_25(df_stage1, df_original)
                df_stage1 = stage1_pipeline_26(df_stage1)
                df_stage1 = stage1_pipeline_27(df_stage1)
                df_stage1 = stage1_pipeline_28(df_stage1, component_to_group)
                df_stage1 = stage1_pipeline_29(df_stage1, df_original)

                # ---------------------------------------------------------
                # GALUTINIO FAILO ENDPOINT / WIRENO PATIKRA
                # Funkcija yra tik informacinė ir duomenų nekeičia.
                # ---------------------------------------------------------
                endpoint_errors = validate_duplicate_endpoint_wirenos(
                    df_stage1
                )


                st.markdown(
                    '<div class="section-heading">🔎 Result Validation</div>',
                    unsafe_allow_html=True,
                )

                if endpoint_errors.empty:
                    st.success(
                        "✅ Galutiniame faile nerasta komponentų kontaktų, "
                        "naudojamų su keliais skirtingais Wireno."
                    )
                else:
                    st.warning(
                        f"⚠️ Galutiniame faile rasta "
                        f"{len(endpoint_errors)} kontaktų su keliais "
                        "skirtingais Wireno."
                    )

                    st.dataframe(
                        endpoint_errors,
                        use_container_width=True,
                        hide_index=True,
                        height=min(
                            500,
                            80 + len(endpoint_errors) * 35,
                        ),
                    )
                # ── ADD THIS SNIPPET TO CALCULATE AND DISPLAY -XPE TERMINALS ─────────
                # Count rows where Line-Function is GNYE
                gnyc_count = (df_stage1['Line-Function'] == 'GNYE').sum()
                # Divide by 2 and round up
                xpe_terminals = -(-gnyc_count // 2)
                
                group_counts = {
                    group: len(symbols)
                    for group, symbols in group_symbols.items()
                }

                standard_groups_df = pd.DataFrame(
                    [
                        {
                            "Group": group,
                            "Components": count,
                        }
                        for group, count in group_counts.items()
                        if "SWING" not in str(group).upper()
                    ]
                )

                swing_groups_df = pd.DataFrame(
                    [
                        {
                            "Group": group,
                            "Components": count,
                        }
                        for group, count in group_counts.items()
                        if "SWING" in str(group).upper()
                    ]
                )

                if not standard_groups_df.empty:
                    standard_groups_df = (
                        standard_groups_df
                        .sort_values(
                            by=["Components", "Group"],
                            ascending=[False, True],
                        )
                        .reset_index(drop=True)
                    )

                if not swing_groups_df.empty:
                    swing_groups_df = (
                        swing_groups_df
                        .sort_values(
                            by=["Components", "Group"],
                            ascending=[False, True],
                        )
                        .reset_index(drop=True)
                    )

                st.markdown(
                    '<div class="section-heading">🧩 ADV_WS Group Summary</div>',
                    unsafe_allow_html=True,
                )

                all_groups_col, swing_groups_col = st.columns(2)

                with all_groups_col:
                    st.markdown(
                        '<div class="group-column-title">Standard Groups</div>',
                        unsafe_allow_html=True,
                    )

                    if standard_groups_df.empty:
                        st.info("No standard groups found.")
                    else:
                        st.dataframe(
                            standard_groups_df,
                            use_container_width=True,
                            hide_index=True,
                            height=min(
                                500,
                                40 + len(standard_groups_df) * 35,
                            ),
                        )

                with swing_groups_col:
                    st.markdown(
                        '<div class="group-column-title">SWING Groups</div>',
                        unsafe_allow_html=True,
                    )

                    if swing_groups_df.empty:
                        st.info("No SWING groups found.")
                    else:
                        st.dataframe(
                            swing_groups_df,
                            use_container_width=True,
                            hide_index=True,
                            height=min(
                                500,
                                40 + len(swing_groups_df) * 35,
                            ),
                        )

                st.markdown(
                    '<div class="section-heading">🛡️ Required XPE Terminals</div>',
                    unsafe_allow_html=True,
                )
                st.metric("Required terminals", xpe_terminals)
                # ── Terminal Count Statistics ───────────────────────────
                terminal_groups = {
                    "X0100": [
                        "-X0100:L3",
                        "-X0100:N",
                        "-X0100:230VL2",
                        "-X0100:230VN2",
                    ],
                    "X0101": [
                        "-X0101:230VL",
                        "-X0101:230VN",
                    ],
                    "X0102": [
                        "-X0102:24VDC",
                        "-X0102:24VDC1",
                        "-X0102:24VDC2",
                        "-X0102:24VDC3",
                        "-X0102:0VDC",
                    ],
                }

                counts = {}

                for terminal_list in terminal_groups.values():
                    for terminal in terminal_list:
                        counts[terminal] = (
                            (df_stage1["Name"] == terminal).sum()
                            + (df_stage1["Name.1"] == terminal).sum()
                        )

                blocks_needed = {
                    terminal: math.ceil(count / 6)
                    for terminal, count in counts.items()
                }

                st.markdown(
                    '<div class="section-heading">🔌 Terminal Blocks Required</div>',
                    unsafe_allow_html=True,
                )

                max_terminal_length = max(
                    len(terminal)
                    for terminal_list in terminal_groups.values()
                    for terminal in terminal_list
                )

                for group_name, terminal_list in terminal_groups.items():
                    used_terminals = [
                        terminal
                        for terminal in terminal_list
                        if counts.get(terminal, 0) > 0
                    ]

                    if not used_terminals:
                        continue

                    st.markdown(
                        f'<div class="terminal-group-title">'
                        f'════════════ {group_name} ════════════'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    for terminal in used_terminals:
                        count = counts[terminal]
                        blocks = blocks_needed[terminal]
                        block_word = "block" if blocks == 1 else "blocks"
                        occurrence_word = (
                            "occurrence"
                            if count == 1
                            else "occurrences"
                        )
                        aligned_terminal = terminal.ljust(
                            max_terminal_length
                        )

                        st.markdown(
                            f'<div class="terminal-row">'
                            f'{aligned_terminal} : '
                            f'{count} {occurrence_word} '
                            f'→ {blocks} {block_word}'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                # Prepare for editing and identify blank cells
                df_stage1 = stage1_pipeline_14(df_stage1)
                blank_cells = identify_blank_cells(df_stage1)

                # Store in session state
                st.session_state["stage1_data"] = df_stage1
                st.session_state["blank_cells"] = blank_cells

                # Show blank cell statistics
                total_blank_cells = sum(len(rows) for rows in blank_cells.values())
                total_rows = df_stage1.shape[0]
                total_cols = df_stage1.shape[1]

                st.markdown(
                    '<div class="section-heading">📊 Data Overview</div>',
                    unsafe_allow_html=True,
                )
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
            st.markdown(
                '<div class="section-heading">🛠️ Interactive Data Editor</div>',
                unsafe_allow_html=True,
            )
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
                df_with_blanks = stage1_pipeline_14(df_with_blanks)

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
            df_stage2 = stage2_pipeline_5(df_stage2)
        except Exception as e:
            st.error(f"Error processing CSV: {e}")
            st.stop()

        st.markdown("### ✅ Stage 2 Result (with Daisy Chain Detection)")
        st.dataframe(df_stage2.head(10), use_container_width=True, height=300)

        buf = BytesIO()
        df_stage2.to_csv(
            buf,
            index=False,
            sep=";",
            encoding="utf-8-sig"
        )

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
