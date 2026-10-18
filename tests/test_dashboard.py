"""Smoke test: every dashboard page renders from the real pipeline outputs.

Requires the outputs in outputs/spark/ and the project venv (streamlit).
Run: .venv/bin/python -m pytest tests/test_dashboard.py -q
"""
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parent.parent / "dashboard" / "app.py")
PAGES = ["Overview & Data Quality", "Skill Trends", "Emerging Skills"]


def test_all_pages_render():
    for page in PAGES:
        at = AppTest.from_file(APP, default_timeout=90)
        at.run()
        if page != PAGES[0]:
            at.sidebar.radio[0].set_value(page).run()
        assert not at.exception, f"{page} raised: {at.exception}"
        assert at.title, f"{page} has no title"


def test_overview_metrics_come_from_pipeline():
    at = AppTest.from_file(APP, default_timeout=90)
    at.run()
    values = {m.label: m.value for m in at.metric}
    assert values["Postings analysed"] == "775,275"
    assert values["Job-skill rows"] == "3,608,325"
