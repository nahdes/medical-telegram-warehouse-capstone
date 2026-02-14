"""Streamlit dashboard for exploring AI agent outputs."""
import streamlit as st
import pandas as pd
import os

st.title("Medical Telegram AI Agent Dashboard")

# load data if exists
csv_path = 'data/processed/yolo_detections.csv'
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
    st.write("### YOLO Detections Sample")
    st.dataframe(df.head())
    st.write(f"Total detections: {len(df)}")
else:
    st.warning("YOLO results CSV not found; run detection first.")

# price extractions
price_path = 'data/processed/price_extractions.csv'
if os.path.exists(price_path):
    pdf = pd.read_csv(price_path)
    st.write("### Price Extractions Sample")
    st.dataframe(pdf.head())
else:
    st.info("Price extraction results not available.")

st.sidebar.header("Actions")
if st.sidebar.button("Reload data"):
    st.experimental_rerun()

# placeholder for explainability
st.sidebar.markdown("---")
if st.sidebar.button("Compute explainability"):
    import ai_agent.explain as expl
    with st.spinner("Running SHAP..."):
        X, shap_vals = expl.compute_dummy_shap()
        st.write("#### Summary plot")
        expl.plot_summary(shap_vals)
        st.write("*(open console for plot output)*")
    
    st.sidebar.write("Explainability generated.")
