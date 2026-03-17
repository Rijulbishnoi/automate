# Broker Payout Toolkit

## Streamlit Frontend
For Streamlit Cloud/local Streamlit app use:

```bash
python -m venv .venv-streamlit
source .venv-streamlit/bin/activate
pip install -r requirements.streamlit.txt
streamlit run streamlit_app.py
```

Open `http://localhost:8501`.

Streamlit modes:
- Upload `.xlsx` for preview + pipeline execution.
- Upload `.csv` for preview-only mode.

If page keeps showing a loading skeleton:
- Stop old Streamlit processes: `pkill -f "streamlit run"`
- Clear cache: `streamlit cache clear`
- Start correct app file: `streamlit run streamlit_app.py --server.port 8501`
- Do not run `streamlit run main.py` (Streamlit may treat it as ASGI/FastAPI).

Quick stable launcher:

```bash
./scripts/run_streamlit.sh
```

Streamlit Cloud setup:
- Main file path: `streamlit_app.py`
- Requirements file: `requirements.streamlit.txt`

## Web Frontend
Run the upload + output preview UI:

```bash
uv sync
uv run uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

Open `http://127.0.0.1:8000`.

The UI lets you:
- Upload `.xlsx` or `.csv` input
- Inspect workbook sheet details and preview rows
- Run legacy or pandas engine (`.xlsx` only; `.csv` is preview mode)
- Optional parity compare
- Preview generated output files
- Download generated output files

## CLI
```bash
uv run python pipeline.py test_data/HM_DIGIT_MAR26_GRID.xlsx --dry-run
```

