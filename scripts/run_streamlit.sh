#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -x ".venv-streamlit/bin/python" ]]; then
  echo "Missing .venv-streamlit. Create it first:"
  echo "  python -m venv .venv-streamlit"
  echo "  source .venv-streamlit/bin/activate"
  echo "  pip install -r requirements.streamlit.txt"
  exit 1
fi

# Stop stale processes that can leave browser in endless loading state.
pkill -f "streamlit run streamlit_app.py" 2>/dev/null || true
pkill -f "streamlit run main.py" 2>/dev/null || true

".venv-streamlit/bin/streamlit" cache clear >/dev/null 2>&1 || true

exec ".venv-streamlit/bin/streamlit" run streamlit_app.py --server.port 8501 --server.address 127.0.0.1
