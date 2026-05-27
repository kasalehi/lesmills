"""Build a non-technical PowerPoint presentation summarizing the project README.

Each slide also contains detailed *speaker notes* aimed at stakeholders so they
understand both the business story and the practical "how to run / how to debug"
steps for the Les Mills Retention Prediction Platform.
"""

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN

# ---------- Colour palette ----------
PRIMARY = RGBColor(0xE4, 0x00, 0x2B)   # Les Mills red
DARK = RGBColor(0x1A, 0x1A, 0x1A)
LIGHT = RGBColor(0xF5, 0xF5, 0xF5)
ACCENT = RGBColor(0x00, 0x6E, 0xB8)
TEXT = RGBColor(0x2B, 0x2B, 0x2B)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
MUTED = RGBColor(0x6B, 0x6B, 0x6B)
GREEN = RGBColor(0x2E, 0x8B, 0x57)
ORANGE = RGBColor(0xE6, 0x7E, 0x22)

prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)

BLANK = prs.slide_layouts[6]

TOTAL_SLIDES = 26  # updated to match the deck

# =========================================================
# Helpers
# =========================================================

def add_background(slide, color=WHITE):
    bg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
    bg.line.fill.background()
    bg.fill.solid()
    bg.fill.fore_color.rgb = color
    bg.shadow.inherit = False
    return bg


def add_side_bar(slide, color=PRIMARY, width=Inches(0.35)):
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, width, prs.slide_height)
    bar.line.fill.background()
    bar.fill.solid()
    bar.fill.fore_color.rgb = color


def add_textbox(slide, left, top, width, height, text, *,
                font_size=18, bold=False, color=TEXT, align=PP_ALIGN.LEFT,
                font_name="Calibri"):
    tb = slide.shapes.add_textbox(left, top, width, height)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = Inches(0.05)
    tf.margin_right = Inches(0.05)

    lines = text.split("\n") if isinstance(text, str) else text
    for i, line in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        run = p.add_run()
        run.text = line
        run.font.size = Pt(font_size)
        run.font.bold = bold
        run.font.color.rgb = color
        run.font.name = font_name
    return tb


def add_bullets(slide, left, top, width, height, items, *,
                font_size=18, color=TEXT, bullet_color=PRIMARY, line_spacing=1.25):
    tb = slide.shapes.add_textbox(left, top, width, height)
    tf = tb.text_frame
    tf.word_wrap = True
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = PP_ALIGN.LEFT
        p.line_spacing = line_spacing
        bullet = p.add_run()
        bullet.text = "● "
        bullet.font.size = Pt(font_size)
        bullet.font.color.rgb = bullet_color
        bullet.font.bold = True
        run = p.add_run()
        run.text = item
        run.font.size = Pt(font_size)
        run.font.color.rgb = color
        run.font.name = "Calibri"
    return tb


def add_footer(slide, page_num, total=TOTAL_SLIDES):
    add_textbox(slide, Inches(0.5), Inches(7.05), Inches(8), Inches(0.35),
                "Les Mills • Retention Prediction Platform",
                font_size=10, color=MUTED)
    add_textbox(slide, Inches(11.8), Inches(7.05), Inches(1.2), Inches(0.35),
                f"{page_num} / {total}",
                font_size=10, color=MUTED, align=PP_ALIGN.RIGHT)


def add_title_block(slide, title, subtitle=None):
    add_side_bar(slide)
    add_textbox(slide, Inches(0.7), Inches(0.4), Inches(12), Inches(0.8),
                title, font_size=32, bold=True, color=DARK)
    line = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                                  Inches(0.7), Inches(1.15),
                                  Inches(1.2), Inches(0.06))
    line.line.fill.background()
    line.fill.solid()
    line.fill.fore_color.rgb = PRIMARY
    if subtitle:
        add_textbox(slide, Inches(0.7), Inches(1.3), Inches(12), Inches(0.5),
                    subtitle, font_size=16, color=MUTED)


def add_notes(slide, text):
    """Attach speaker notes to a slide."""
    notes_tf = slide.notes_slide.notes_text_frame
    notes_tf.text = text


def add_code_block(slide, left, top, width, height, lines, *, font_size=14):
    """Render a fake terminal / code block."""
    box = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                                 left, top, width, height)
    box.line.color.rgb = RGBColor(0x33, 0x33, 0x33)
    box.fill.solid(); box.fill.fore_color.rgb = RGBColor(0x1E, 0x1E, 0x1E)

    inner = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.15),
                                     width - Inches(0.4), height - Inches(0.3))
    tf = inner.text_frame
    tf.word_wrap = True
    for i, line in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        run = p.add_run()
        run.text = line
        run.font.name = "Consolas"
        run.font.size = Pt(font_size)
        run.font.color.rgb = RGBColor(0xDC, 0xDC, 0xDC)


# =========================================================
# SLIDE 1 — TITLE
# =========================================================
s = prs.slides.add_slide(BLANK)
add_background(s, DARK)
block = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, Inches(2.2), Inches(0.6), Inches(3.0))
block.line.fill.background(); block.fill.solid(); block.fill.fore_color.rgb = PRIMARY

add_textbox(s, Inches(1.0), Inches(2.0), Inches(11), Inches(1.0),
            "Les Mills", font_size=28, color=PRIMARY, bold=True)
add_textbox(s, Inches(1.0), Inches(2.6), Inches(11), Inches(1.4),
            "Retention Prediction Platform", font_size=54, bold=True, color=WHITE)
add_textbox(s, Inches(1.0), Inches(4.0), Inches(11), Inches(0.8),
            "Predicting customer churn with Machine Learning",
            font_size=24, color=LIGHT)
add_textbox(s, Inches(1.0), Inches(4.8), Inches(11), Inches(0.5),
            "An overview for non-technical audiences",
            font_size=18, color=MUTED)
add_textbox(s, Inches(1.0), Inches(6.4), Inches(11), Inches(0.4),
            "Prepared by Keyvan Salehi  •  Les Mills New Zealand",
            font_size=14, color=MUTED)

add_notes(s, """Welcome the audience.

Talking points:
- This platform predicts which Les Mills members are most likely to cancel.
- It uses Machine Learning, but we will explain everything in plain language.
- The presentation has two parts:
  1) What the platform does and why it matters (slides 2 to 13).
  2) How the team runs and debugs it day-to-day (slides 15 to 25), including visual flowcharts and the actual Airflow DAGs.
- Encourage stakeholders to interrupt with questions.
""")


# =========================================================
# SLIDE 2 — WHAT IS THIS PROJECT
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "What is this project?",
                "A simple way to know which members are likely to leave — before they do.")
add_bullets(s, Inches(0.7), Inches(2.0), Inches(12), Inches(4.5), [
    "An end-to-end system that predicts the risk of a customer cancelling their membership.",
    "Combines our member data, an automated pipeline, and a Machine Learning model.",
    "Provides results through an easy-to-use dashboard — no coding required.",
    "Helps the business take action early to retain members and reduce churn.",
], font_size=20)
add_footer(s, 2)

add_notes(s, """Goal of this slide: explain in one breath what the project is.

Plain-language framing:
- Think of it like a smoke detector for membership cancellations.
- It looks at member behaviour and flags people who look "at risk" of leaving.
- The business can then reach out with a discount, a class invite, or a phone call.

Avoid jargon. If asked "what is ML?" just say:
- "It's a way for computers to learn patterns from past data and use those patterns to make a prediction about new data."
""")


# =========================================================
# SLIDE 3 — WHY IT MATTERS
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Why it matters",
                "Retention is cheaper than acquisition.")
cards = [
    ("Reduce Churn", "Spot at-risk members early and act before they cancel."),
    ("Save Costs", "Keeping a member is far cheaper than acquiring a new one."),
    ("Smarter Decisions", "Marketing and operations use data, not guesswork."),
    ("Personalised Action", "Target the right members with the right offer."),
]
left = Inches(0.7); top = Inches(2.0); card_w = Inches(2.95); card_h = Inches(2.6); gap = Inches(0.15)
for i, (title, body) in enumerate(cards):
    x = left + (card_w + gap) * i
    card = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, top, card_w, card_h)
    card.line.color.rgb = RGBColor(0xE0, 0xE0, 0xE0)
    card.fill.solid(); card.fill.fore_color.rgb = LIGHT
    add_textbox(s, x + Inches(0.2), top + Inches(0.25), card_w - Inches(0.4), Inches(0.5),
                title, font_size=18, bold=True, color=PRIMARY)
    add_textbox(s, x + Inches(0.2), top + Inches(0.9), card_w - Inches(0.4), card_h - Inches(1.1),
                body, font_size=14, color=TEXT)
add_footer(s, 3)

add_notes(s, """Why this matters to the business:
- Acquiring a new member typically costs 5x to 7x more than retaining one.
- Even a 1% improvement in retention has a meaningful impact on annual revenue.
- The platform turns "we lost a member" into "we knew this was coming and tried to stop it."

Use these talking points if asked for ROI:
- We can quantify benefit by tracking the conversion rate of saved members vs. control group.
""")


# =========================================================
# SLIDE 4 — HOW IT WORKS
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "How it works — at a glance",
                "Data flows automatically from our systems to insights.")
steps = [
    ("Data", "Member info from\nour database"),
    ("Pipeline", "Automated workflow\nprepares the data"),
    ("Model", "Machine Learning\npredicts churn risk"),
    ("Results", "Saved as a clean,\nready-to-use dataset"),
    ("Dashboard", "Business users view\nand explore results"),
]
left = Inches(0.6); top = Inches(2.6); box_w = Inches(2.2); box_h = Inches(2.0); gap = Inches(0.25)
for i, (title, body) in enumerate(steps):
    x = left + (box_w + gap) * i
    box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, top, box_w, box_h)
    box.line.color.rgb = PRIMARY
    box.fill.solid(); box.fill.fore_color.rgb = WHITE
    add_textbox(s, x, top + Inches(0.25), box_w, Inches(0.5),
                title, font_size=18, bold=True, color=PRIMARY, align=PP_ALIGN.CENTER)
    add_textbox(s, x + Inches(0.1), top + Inches(0.9), box_w - Inches(0.2), Inches(1.0),
                body, font_size=12, color=TEXT, align=PP_ALIGN.CENTER)
    if i < len(steps) - 1:
        arrow = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW,
                                   x + box_w + Inches(0.02), top + Inches(0.85),
                                   Inches(0.22), Inches(0.3))
        arrow.line.fill.background(); arrow.fill.solid(); arrow.fill.fore_color.rgb = PRIMARY
add_footer(s, 4)

add_notes(s, """Walk left-to-right through the pipeline:

1) Data: We read member records from SQL Server (visits, joins, cancellations, etc.).
2) Pipeline: An automated workflow (Apache Airflow, running inside Docker via "Astro") prepares and cleans the data.
3) Model: A trained Machine Learning model assigns each member a "risk of leaving" score.
4) Results: The output is saved as a CSV file (predictions.csv) so it can be read easily.
5) Dashboard: A web page (built with Streamlit) lets the business view results without using code.

The entire chain is automated — it runs without anyone touching it.
""")


# =========================================================
# SLIDE 5 — BUILDING BLOCKS
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "The building blocks",
                "Standard, industry-proven tools — explained simply.")
tools = [
    ("SQL Server", "Where our member data lives.", "Source of truth"),
    ("Apache Airflow (Astro)", "An automation tool that runs the pipeline on a schedule.", "Automation"),
    ("Docker", "Lets the project run reliably on any computer or server.", "Portability"),
    ("Python & ML Model", "The brain — learns patterns and predicts churn.", "Intelligence"),
    ("Streamlit", "A friendly web dashboard for the team.", "User interface"),
]
top = Inches(1.95); row_h = Inches(0.85)
for i, (name, desc, tag) in enumerate(tools):
    y = top + row_h * i
    bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.7), y + Inches(0.05),
                             Inches(0.12), Inches(0.7))
    bar.line.fill.background(); bar.fill.solid(); bar.fill.fore_color.rgb = PRIMARY
    add_textbox(s, Inches(0.95), y, Inches(3.4), Inches(0.4),
                name, font_size=18, bold=True, color=DARK)
    add_textbox(s, Inches(0.95), y + Inches(0.4), Inches(7.5), Inches(0.4),
                desc, font_size=14, color=TEXT)
    pill = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                              Inches(10.6), y + Inches(0.15),
                              Inches(2.2), Inches(0.5))
    pill.line.fill.background()
    pill.fill.solid(); pill.fill.fore_color.rgb = ACCENT
    add_textbox(s, Inches(10.6), y + Inches(0.22), Inches(2.2), Inches(0.4),
                tag, font_size=12, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
add_footer(s, 5)

add_notes(s, """Stakeholder-friendly analogies:
- SQL Server: a giant filing cabinet of member info.
- Apache Airflow: a robot that follows a recipe at scheduled times.
- Docker: a "shipping container" that packages the whole system so it works the same on any computer or server.
- Python / ML model: the brain that has learned from history.
- Streamlit: the storefront — what business users actually click and see.

These are all open, well-known industry tools — not something custom and risky.
""")


# =========================================================
# SLIDE 6 — DATA JOURNEY
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "The data journey",
                "From raw records to actionable predictions.")
journey = [
    "Pull data from SQL Server",
    "Take a snapshot of members",
    "Combine and clean features",
    "Load the trained ML model",
    "Predict churn probability",
    "Save results for the team",
]
top = Inches(2.0)
for i, step in enumerate(journey):
    y = top + Inches(0.7) * i
    circle = s.shapes.add_shape(MSO_SHAPE.OVAL, Inches(1.0), y, Inches(0.55), Inches(0.55))
    circle.line.fill.background(); circle.fill.solid(); circle.fill.fore_color.rgb = PRIMARY
    add_textbox(s, Inches(1.0), y + Inches(0.08), Inches(0.55), Inches(0.4),
                str(i + 1), font_size=18, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    add_textbox(s, Inches(1.8), y + Inches(0.05), Inches(10), Inches(0.5),
                step, font_size=20, color=TEXT)
add_footer(s, 6)

add_notes(s, """Walk through the six steps in plain English:

1) Pull data: read raw member records from SQL Server.
2) Snapshot: freeze the data at a point in time so the model sees a consistent picture.
3) Combine and clean: merge tables, fill in missing values, prepare "features" the model understands.
4) Load model: open the trained ML model file (model.pkl).
5) Predict: each member gets a risk score between 0 (will stay) and 1 (high churn risk).
6) Save: results are written to include/data/predictions.csv for the dashboard to read.
""")


# =========================================================
# SLIDE 7 — STREAMLIT DASHBOARD
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "The Streamlit dashboard",
                "A friendly window into the predictions.")
add_bullets(s, Inches(0.7), Inches(2.0), Inches(7.5), Inches(4.5), [
    "Upload a CSV file of members.",
    "Run predictions with one click.",
    "See who is most at risk of leaving.",
    "Download the results for the team.",
    "No coding or technical knowledge needed.",
], font_size=20)

mock = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                          Inches(8.5), Inches(2.0), Inches(4.2), Inches(4.5))
mock.line.color.rgb = RGBColor(0xCC, 0xCC, 0xCC)
mock.fill.solid(); mock.fill.fore_color.rgb = LIGHT
header = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                            Inches(8.5), Inches(2.0), Inches(4.2), Inches(0.6))
header.line.fill.background(); header.fill.solid(); header.fill.fore_color.rgb = PRIMARY
add_textbox(s, Inches(8.7), Inches(2.1), Inches(4.0), Inches(0.4),
            "Retention Dashboard", font_size=14, bold=True, color=WHITE)
add_textbox(s, Inches(8.7), Inches(2.8), Inches(3.8), Inches(0.4),
            "Members at risk", font_size=14, bold=True, color=DARK)
for i, (name, risk, color) in enumerate([
    ("Member A", "92%", PRIMARY),
    ("Member B", "81%", PRIMARY),
    ("Member C", "47%", ACCENT),
    ("Member D", "12%", GREEN),
]):
    y = Inches(3.3) + Inches(0.55) * i
    add_textbox(s, Inches(8.7), y, Inches(2.5), Inches(0.4),
                name, font_size=13, color=TEXT)
    add_textbox(s, Inches(11.2), y, Inches(1.4), Inches(0.4),
                risk, font_size=13, bold=True, color=color, align=PP_ALIGN.RIGHT)
add_footer(s, 7)

add_notes(s, """The dashboard is the touch-point for non-technical users.

Demo flow if you have access:
- Open http://localhost:8501 in a browser.
- The page shows uploaded data and predictions.
- Users can upload their own CSV and see results instantly.
- Predictions are colour-coded:
  - Red = high risk
  - Blue = medium risk
  - Green = low risk

The dashboard reads from the same predictions file the pipeline produces.
""")


# =========================================================
# SLIDE 8 — AUTOMATION
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Automation behind the scenes",
                "The pipeline runs itself — reliably and on schedule.")
add_bullets(s, Inches(0.7), Inches(2.0), Inches(12), Inches(4.5), [
    "Airflow runs the workflow automatically — no manual steps.",
    "Each step is monitored — if something fails, the team is alerted.",
    "Docker ensures the system behaves the same everywhere.",
    "New data is processed without anyone needing to be present.",
    "Predictions are always fresh and ready for the business.",
], font_size=20)
add_footer(s, 8)

add_notes(s, """Key message: the system is hands-off and reliable.

Behind the scenes:
- Airflow uses a "DAG" (Directed Acyclic Graph) which is just a recipe of steps in order.
- If a step fails, Airflow retries it automatically a few times before alerting.
- Logs from every run are stored, so we can always look back and explain what happened.
""")


# =========================================================
# SLIDE 9 — HOW THE MODEL LEARNS
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "How the model learns",
                "Trained on historical patterns — improved over time.")
add_bullets(s, Inches(0.7), Inches(2.0), Inches(12), Inches(4.5), [
    "We feed the model years of past member behaviour.",
    "It learns which patterns are linked to people who left.",
    "Algorithms used: Random Forest and Gradient Boosting (proven, reliable techniques).",
    "The trained model is saved and re-used for new predictions.",
    "Can be re-trained periodically to stay accurate as behaviour changes.",
], font_size=20)
add_footer(s, 9)

add_notes(s, """Plain-English explanation of training:

- Imagine looking at thousands of past members and asking: "what did the people who left have in common?"
- The model finds those patterns automatically (e.g. visits dropped sharply, stopped attending favourite class, billing failures).
- Random Forest = many simple decision trees voting together.
- Gradient Boosting = trees that learn from each other's mistakes.
- Both are well-understood, mainstream methods — not experimental.
- We use GridSearchCV to automatically test combinations of settings and pick the best one.
""")


# =========================================================
# SLIDE 10 — BUSINESS BENEFITS
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Business benefits",
                "What this means for Les Mills.")
benefits = [
    ("Higher retention", "Members stay longer when we engage them at the right moment."),
    ("Lower marketing costs", "Targeted offers instead of broad campaigns."),
    ("Better member experience", "Personalised outreach feels more relevant."),
    ("Data-driven culture", "Decisions backed by evidence, not guesses."),
]
left = Inches(0.7); top = Inches(2.0); card_w = Inches(5.95); card_h = Inches(2.2); gap = Inches(0.25)
for i, (title, body) in enumerate(benefits):
    row = i // 2; col = i % 2
    x = left + (card_w + gap) * col
    y = top + (card_h + gap) * row
    card = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, card_w, card_h)
    card.line.color.rgb = RGBColor(0xE0, 0xE0, 0xE0)
    card.fill.solid(); card.fill.fore_color.rgb = WHITE
    bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(0.15), card_h)
    bar.line.fill.background(); bar.fill.solid(); bar.fill.fore_color.rgb = PRIMARY
    add_textbox(s, x + Inches(0.4), y + Inches(0.3), card_w - Inches(0.6), Inches(0.5),
                title, font_size=20, bold=True, color=DARK)
    add_textbox(s, x + Inches(0.4), y + Inches(0.95), card_w - Inches(0.6), Inches(1.2),
                body, font_size=15, color=TEXT)
add_footer(s, 10)

add_notes(s, """Tie each benefit to a concrete activity:
- Higher retention -> proactive call campaigns to top-100 at-risk members.
- Lower marketing costs -> stop blanket-emailing the entire base.
- Better member experience -> outreach that feels timely, not random.
- Data-driven culture -> weekly review of results and actions taken.
""")


# =========================================================
# SLIDE 11 — SECURITY & TRUST
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Security and trust",
                "Built with safe, modern practices.")
add_bullets(s, Inches(0.7), Inches(2.0), Inches(12), Inches(4.5), [
    "Sensitive credentials are kept out of the code — managed via secure environment variables.",
    "Member data stays inside trusted systems (SQL Server, internal infrastructure).",
    "The pipeline runs in isolated containers — predictable and auditable.",
    "All steps are logged for transparency and troubleshooting.",
], font_size=20)
add_footer(s, 11)

add_notes(s, """Reassurance points:
- Passwords and keys are never committed to the source code repository.
- We use environment variables (".env" files) on the host machine instead.
- All actions are logged to disk, so any member-data access can be reviewed.
- Docker isolation means the project cannot accidentally affect other systems.
""")


# =========================================================
# SLIDE 12 — TYPICAL DAY
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "A typical day with the platform",
                "Simple, repeatable, business-friendly.")
steps = [
    "The pipeline runs automatically or manually By RunDate.",
    "Fresh predictions are saved and ready by RunDate.",
    "The business team opens the Streamlit dashboard.",
    "They review the members at highest risk.",
    "Marketing or operations launches a retention action.",
]
top = Inches(2.0)
for i, step in enumerate(steps):
    y = top + Inches(0.75) * i
    circle = s.shapes.add_shape(MSO_SHAPE.OVAL, Inches(1.0), y, Inches(0.6), Inches(0.6))
    circle.line.fill.background(); circle.fill.solid(); circle.fill.fore_color.rgb = ACCENT
    add_textbox(s, Inches(1.0), y + Inches(0.1), Inches(0.6), Inches(0.4),
                str(i + 1), font_size=18, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    add_textbox(s, Inches(1.85), y + Inches(0.08), Inches(11), Inches(0.5),
                step, font_size=20, color=TEXT)
add_footer(s, 12)

add_notes(s, """Show that the day-to-day is simple:
- Nobody has to "start" the pipeline each morning.
- Business team only needs the dashboard URL and a browser.
- Engineering team only steps in if Airflow flags an issue.
""")


# =========================================================
# SLIDE 13 — WHAT'S NEXT
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "What's next",
                "Where we can take this platform.")
add_bullets(s, Inches(0.7), Inches(2.0), Inches(12), Inches(4.5), [
    "Model versioning — track and compare model improvements over time.",
    "Prediction monitoring — make sure results stay accurate.",
    "Automated re-training — model updates itself as new data arrives.",
    "Drift detection — alerts when member behaviour shifts.",
    "Richer dashboards — deeper analytics for the business team.",
], font_size=20)
add_footer(s, 13)

add_notes(s, """Future roadmap to share with stakeholders.

Quick wins next quarter:
- Schedule weekly automatic re-training.
- Add an executive dashboard with churn KPIs.

Longer-term:
- Hook into CRM so high-risk members trigger automatic actions.
- A/B test retention offers and feed results back into the model.
""")


# =========================================================
# SLIDE 14 — SECTION BREAK: HOW TO RUN & DEBUG
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s, DARK)
block = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, Inches(2.4), Inches(0.6), Inches(2.6))
block.line.fill.background(); block.fill.solid(); block.fill.fore_color.rgb = PRIMARY

add_textbox(s, Inches(1.0), Inches(2.4), Inches(11), Inches(1.0),
            "Part 2", font_size=24, color=PRIMARY, bold=True)
add_textbox(s, Inches(1.0), Inches(2.9), Inches(11), Inches(1.4),
            "How to run & debug the platform", font_size=44, bold=True, color=WHITE)
add_textbox(s, Inches(1.0), Inches(4.5), Inches(11), Inches(0.6),
            "A step-by-step walk-through for stakeholders and new team members",
            font_size=18, color=LIGHT)
add_footer(s, 14)

add_notes(s, """Transition slide.

Say something like:
- "Now that we have seen WHAT the platform does, let's see HOW it is run and supported."
- "Even if you don't run it yourself, knowing the basics helps you ask the right questions and understand what the engineering team is doing when something goes wrong."
""")


# =========================================================
# SLIDE 15 — RUN OVERVIEW (4 STEPS)
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Running the project — overview",
                "Four simple steps from zero to running.")

run_steps = [
    ("1", "Install prerequisites", "Docker, Astro CLI, Python, Git."),
    ("2", "Start Airflow", "One command launches everything."),
    ("3", "Trigger the pipeline", "Run the workflow from the Airflow UI."),
    ("4", "Open the dashboard", "View predictions in Streamlit."),
]
left = Inches(0.6); top = Inches(2.2); box_w = Inches(2.95); box_h = Inches(3.0); gap = Inches(0.2)
for i, (num, title, body) in enumerate(run_steps):
    x = left + (box_w + gap) * i
    box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, top, box_w, box_h)
    box.line.color.rgb = PRIMARY
    box.fill.solid(); box.fill.fore_color.rgb = WHITE
    circle = s.shapes.add_shape(MSO_SHAPE.OVAL,
                                x + box_w/2 - Inches(0.45), top + Inches(0.3),
                                Inches(0.9), Inches(0.9))
    circle.line.fill.background()
    circle.fill.solid(); circle.fill.fore_color.rgb = PRIMARY
    add_textbox(s, x + box_w/2 - Inches(0.45), top + Inches(0.45),
                Inches(0.9), Inches(0.6),
                num, font_size=28, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    add_textbox(s, x + Inches(0.2), top + Inches(1.4), box_w - Inches(0.4), Inches(0.6),
                title, font_size=16, bold=True, color=DARK, align=PP_ALIGN.CENTER)
    add_textbox(s, x + Inches(0.2), top + Inches(2.0), box_w - Inches(0.4), Inches(0.9),
                body, font_size=13, color=TEXT, align=PP_ALIGN.CENTER)
add_footer(s, 15)

add_notes(s, """Set expectations: only 4 high-level steps.

Time to complete the first time: ~15-20 minutes (mostly Docker downloading images).
Time to complete every subsequent run: less than 1 minute.

The next four slides break each step down with the exact commands.
""")


# =========================================================
# SLIDE 16 — STEP 1: PREREQUISITES
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Step 1 — Install prerequisites",
                "What needs to be on your computer first.")

add_bullets(s, Inches(0.7), Inches(2.0), Inches(6.5), Inches(3.0), [
    "Docker — runs the containers.",
    "Astronomer CLI (astro) — manages Airflow.",
    "Git — to download the project.",
    "Python 3.10 or newer.",
], font_size=18)

add_textbox(s, Inches(7.5), Inches(2.0), Inches(5.3), Inches(0.5),
            "Verify the tools are installed:",
            font_size=14, bold=True, color=DARK)
add_code_block(s, Inches(7.5), Inches(2.5), Inches(5.3), Inches(2.6), [
    "$ docker --version",
    "$ astro version",
    "$ git --version",
    "$ python --version",
    "",
    "# Clone the project",
    "$ git clone https://github.com/kasalehi/lesmills.git",
    "$ cd lesmills",
])
add_footer(s, 16)

add_notes(s, """Walk through each prerequisite:

1) Docker Desktop must be installed AND running (look for the whale icon in the system tray).
2) Astronomer CLI install guide: https://docs.astronomer.io/astro/cli/install-cli
3) Git is needed to download (clone) the project from the company repository.
4) Python 3.10+ is needed for any local script runs outside Docker.

Common pitfalls to mention:
- "astro: command not found" -> the CLI was not added to PATH; restart the terminal.
- "Cannot connect to the Docker daemon" -> Docker Desktop is not started yet.
- On Windows, install via PowerShell as Administrator the first time.
""")


# =========================================================
# SLIDE 17 — STEP 2: START AIRFLOW
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Step 2 — Start Airflow with Astro",
                "One command launches the whole environment.")

add_code_block(s, Inches(0.7), Inches(2.0), Inches(7.0), Inches(2.0), [
    "# from inside the project folder",
    "$ astro dev start",
    "",
    "# Airflow UI:  http://localhost:8080",
    "# username: admin   password: admin",
])

add_textbox(s, Inches(8.0), Inches(2.0), Inches(4.8), Inches(0.5),
            "This starts these containers:",
            font_size=14, bold=True, color=DARK)
add_bullets(s, Inches(8.0), Inches(2.5), Inches(4.8), Inches(3.0), [
    "Airflow Scheduler",
    "Airflow Webserver",
    "DAG Processor",
    "Triggerer",
    "PostgreSQL metadata DB",
], font_size=14)

add_textbox(s, Inches(0.7), Inches(4.5), Inches(12), Inches(0.5),
            "If a port is in use or something is wrong, Astro tells you in the terminal output.",
            font_size=14, color=MUTED)
add_footer(s, 17)

add_notes(s, """Walking through what happens:

- "astro dev start" downloads Docker images the first time (can take several minutes).
- After it finishes you'll see a message confirming Airflow is up.
- Open a browser at http://localhost:8080 and log in with admin / admin.

Common issues:
- "Port 8080 already in use" -> stop the conflicting app, or run "astro dev start --webserver-port 8081".
- "Image pull failed" -> check internet connection or proxy settings.
- "astro dev start" hangs -> make sure Docker Desktop is running and has enough memory (at least 4 GB).
- To stop everything cleanly: "astro dev stop".
- To start fresh: "astro dev kill" then "astro dev start".
""")


# =========================================================
# SLIDE 18 — STEP 3: TRIGGER PIPELINE
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Step 3 — Trigger the pipeline",
                "Run the workflow once to generate predictions.")

add_bullets(s, Inches(0.7), Inches(2.0), Inches(6.8), Inches(4.5), [
    "Open http://localhost:8080 in a browser.",
    "Log in with admin / admin.",
    "Find the DAG named 'lesmills'.",
    "Toggle it ON (the slider on the left).",
    "Click 'Trigger DAG' (the play button).",
    "Watch each step turn green as it completes.",
], font_size=17)

add_textbox(s, Inches(8.0), Inches(2.0), Inches(4.8), Inches(0.5),
            "Output saved to:",
            font_size=14, bold=True, color=DARK)
add_code_block(s, Inches(8.0), Inches(2.5), Inches(4.8), Inches(1.0), [
    "include/data/predictions.csv",
])

add_textbox(s, Inches(8.0), Inches(3.7), Inches(4.8), Inches(0.5),
            "If a step fails:",
            font_size=14, bold=True, color=DARK)
add_bullets(s, Inches(8.0), Inches(4.2), Inches(4.8), Inches(3.0), [
    "Click the red square in the UI.",
    "Open the 'Logs' tab.",
    "Read the error message.",
    "Click 'Clear' to retry the step.",
], font_size=14)
add_footer(s, 18)

add_notes(s, """Demo this live if possible.

The DAG performs:
1) Read data from SQL Server
2) Generate snapshots
3) Merge dataset
4) Load trained ML model
5) Run predictions
6) Save results to include/data/predictions.csv

If a task is red:
- Click the failed task box.
- Open the "Logs" tab — the bottom of the log usually has the error.
- Common failures:
  - SQL connection refused -> check VPN / database credentials in .env.
  - File not found: model.pkl -> the model artifact is missing in include/artifacts/.
  - PermissionError on output -> ensure include/data/ is writable.
- After fixing the cause, click "Clear" on the failed task to retry only that step (no need to re-run everything).
""")


# =========================================================
# SLIDE 19 — STEP 4: STREAMLIT
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Step 4 — Run the Streamlit dashboard",
                "Open the friendly UI for the business team.")

add_code_block(s, Inches(0.7), Inches(2.0), Inches(8.0), Inches(2.5), [
    "# enter the running container",
    "$ astro dev bash",
    "",
    "# launch streamlit",
    "$ streamlit run include/lesmills_project/streamlit.py \\",
    "    --server.port 8501 --server.address 0.0.0.0",
    "",
    "# Open in browser:  http://localhost:8501",
])

add_textbox(s, Inches(9.0), Inches(2.0), Inches(3.8), Inches(0.5),
            "What you can do:",
            font_size=14, bold=True, color=DARK)
add_bullets(s, Inches(9.0), Inches(2.5), Inches(3.8), Inches(4.0), [
    "Upload a CSV.",
    "Run predictions.",
    "View results.",
    "Download outputs.",
], font_size=14)
add_footer(s, 19)

add_notes(s, """Two ways to start Streamlit:

A) Inside the Astro container (recommended, matches production):
   astro dev bash
   streamlit run include/lesmills_project/streamlit.py --server.port 8501 --server.address 0.0.0.0

B) On your local machine (faster for development):
   pip install -r requirements.txt
   streamlit run include/lesmills_project/streamlit.py

Then visit http://localhost:8501 in any browser.

Common issues:
- "Address already in use" on port 8501 -> change the port, e.g. --server.port 8502.
- Page loads but is blank -> check the terminal where Streamlit was started for Python errors.
- "model.pkl not found" -> make sure the pipeline ran successfully and the file exists in include/artifacts/.
""")


# =========================================================
# SLIDE 20 — RUN FLOWCHART (visual)
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "How to run — visual flowchart",
                "From a fresh machine to a working dashboard.")

# 5 nodes, horizontal
nodes = [
    ("Install\nprerequisites", "Docker, Astro,\nGit, Python"),
    ("astro dev start", "Launches Airflow\nin Docker"),
    ("Trigger\nthe DAG", "Open UI →\nclick 'Trigger'"),
    ("Open\nStreamlit", "streamlit run …\nport 8501"),
    ("Dashboard\nready", "View predictions\nin browser"),
]
checks = [
    "✓ docker --version works",
    "✓ http://localhost:8080 opens",
    "✓ All tasks turn green",
    "✓ http://localhost:8501 opens",
    None,
]

box_w = Inches(2.0); box_h = Inches(1.4); arrow_w = Inches(0.35)
total_w = box_w * len(nodes) + arrow_w * (len(nodes) - 1)
start_x = (prs.slide_width - total_w) / 2
top = Inches(2.3)

for i, (label, sub) in enumerate(nodes):
    x = start_x + (box_w + arrow_w) * i
    is_last = (i == len(nodes) - 1)
    shape = MSO_SHAPE.OVAL if is_last else MSO_SHAPE.ROUNDED_RECTANGLE
    box = s.shapes.add_shape(shape, x, top, box_w, box_h)
    box.line.color.rgb = PRIMARY if not is_last else GREEN
    box.fill.solid()
    box.fill.fore_color.rgb = WHITE if not is_last else RGBColor(0xE8, 0xF5, 0xEC)

    # number circle
    if not is_last:
        num = s.shapes.add_shape(MSO_SHAPE.OVAL,
                                 x + Inches(0.1), top + Inches(0.1),
                                 Inches(0.4), Inches(0.4))
        num.line.fill.background()
        num.fill.solid(); num.fill.fore_color.rgb = PRIMARY
        add_textbox(s, x + Inches(0.1), top + Inches(0.13),
                    Inches(0.4), Inches(0.4),
                    str(i + 1), font_size=14, bold=True, color=WHITE,
                    align=PP_ALIGN.CENTER)

    add_textbox(s, x + Inches(0.1), top + Inches(0.45),
                box_w - Inches(0.2), Inches(0.55),
                label, font_size=14, bold=True,
                color=DARK if not is_last else GREEN, align=PP_ALIGN.CENTER)
    add_textbox(s, x + Inches(0.1), top + Inches(0.95),
                box_w - Inches(0.2), Inches(0.45),
                sub, font_size=10, color=MUTED, align=PP_ALIGN.CENTER)

    if i < len(nodes) - 1:
        arrow = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW,
                                   x + box_w + Inches(0.02),
                                   top + Inches(0.55),
                                   arrow_w - Inches(0.04), Inches(0.3))
        arrow.line.fill.background()
        arrow.fill.solid(); arrow.fill.fore_color.rgb = PRIMARY

    # verification checkbox below
    if checks[i]:
        check_box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                                       x, top + Inches(1.7),
                                       box_w, Inches(0.55))
        check_box.line.color.rgb = GREEN
        check_box.fill.solid(); check_box.fill.fore_color.rgb = RGBColor(0xF0, 0xF9, 0xF3)
        add_textbox(s, x + Inches(0.05), top + Inches(1.78),
                    box_w - Inches(0.1), Inches(0.4),
                    checks[i], font_size=10, bold=True, color=GREEN,
                    align=PP_ALIGN.CENTER)
        # tiny vertical line connecting box to check
        line = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                                  x + box_w/2 - Inches(0.02),
                                  top + box_h, Inches(0.04), Inches(0.3))
        line.line.fill.background()
        line.fill.solid(); line.fill.fore_color.rgb = GREEN

# Decision diamond at the bottom: "Anything failed?"
diamond = s.shapes.add_shape(MSO_SHAPE.DIAMOND,
                             Inches(0.7), Inches(5.4),
                             Inches(2.8), Inches(1.3))
diamond.line.color.rgb = ORANGE
diamond.fill.solid(); diamond.fill.fore_color.rgb = RGBColor(0xFF, 0xF5, 0xE8)
add_textbox(s, Inches(0.7), Inches(5.85), Inches(2.8), Inches(0.5),
            "Step failed?", font_size=14, bold=True, color=ORANGE,
            align=PP_ALIGN.CENTER)

arrow_yes = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW,
                               Inches(3.55), Inches(5.95),
                               Inches(0.7), Inches(0.25))
arrow_yes.line.fill.background()
arrow_yes.fill.solid(); arrow_yes.fill.fore_color.rgb = ORANGE
add_textbox(s, Inches(3.55), Inches(5.55), Inches(0.7), Inches(0.4),
            "Yes", font_size=11, bold=True, color=ORANGE, align=PP_ALIGN.CENTER)

fix_box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                             Inches(4.3), Inches(5.4), Inches(8.4), Inches(1.3))
fix_box.line.color.rgb = ORANGE
fix_box.fill.solid(); fix_box.fill.fore_color.rgb = WHITE
add_textbox(s, Inches(4.5), Inches(5.5), Inches(8.0), Inches(0.4),
            "→ Debug guide", font_size=14, bold=True, color=ORANGE)
add_bullets(s, Inches(4.5), Inches(5.85), Inches(8.0), Inches(0.9), [
    "Read the error in the terminal or Airflow Logs tab.",
    "Try: astro dev kill → astro dev start, then re-trigger the DAG.",
    "Still stuck? See slides 24 (common issues) and 25 (where to look).",
], font_size=11, bullet_color=ORANGE)

add_footer(s, 20)

add_notes(s, """This is the happy-path flowchart.

Walk through each step:
1) Install prerequisites (Docker, Astro CLI, Git, Python).
2) Run "astro dev start" — Airflow launches inside Docker.
3) Open the Airflow UI and trigger the DAG.
4) Open Streamlit at http://localhost:8501.
5) Dashboard is ready.

Below each step is the verification check — a quick way to confirm that step succeeded before moving to the next.

The orange decision diamond at the bottom captures the failure path:
- If anything goes wrong, head to the debug guidance.
- The most common fix is to restart the environment with "astro dev kill" then "astro dev start".
- Detailed troubleshooting is on slides 24 and 25.
""")


# =========================================================
# SLIDE 21 — PREDICTION DAG VISUALISED
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "The Prediction DAG — visualised",
                "What actually runs every day inside Airflow.")

# Pipeline tasks from dags/prediction.py
tasks = [
    ("read_data", "Pull raw data\nfrom SQL Server", PRIMARY),
    ("snapshots", "Freeze member\nsnapshot in time", PRIMARY),
    ("ingest", "Clean + merge\nfeatures", PRIMARY),
    ("predict", "Run trained\nML model", ACCENT),
    ("final_merge", "Combine SQL +\npredictions", PRIMARY),
    ("upsert_to_sql", "Save results\nto SQL Server", GREEN),
    ("streamlit_app", "Launch the\ndashboard", GREEN),
]

box_w = Inches(1.6); box_h = Inches(1.5); arrow_w = Inches(0.22)
total_w = box_w * len(tasks) + arrow_w * (len(tasks) - 1)
start_x = (prs.slide_width - total_w) / 2
top = Inches(2.3)

for i, (name, desc, col) in enumerate(tasks):
    x = start_x + (box_w + arrow_w) * i
    box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, top, box_w, box_h)
    box.line.color.rgb = col
    box.fill.solid(); box.fill.fore_color.rgb = WHITE
    # colored header strip
    strip = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                               x, top, box_w, Inches(0.32))
    strip.line.fill.background()
    strip.fill.solid(); strip.fill.fore_color.rgb = col
    add_textbox(s, x, top + Inches(0.04), box_w, Inches(0.3),
                f"Task {i+1}", font_size=10, bold=True, color=WHITE,
                align=PP_ALIGN.CENTER)
    add_textbox(s, x + Inches(0.05), top + Inches(0.45),
                box_w - Inches(0.1), Inches(0.45),
                name, font_size=12, bold=True, color=DARK, align=PP_ALIGN.CENTER,
                font_name="Consolas")
    add_textbox(s, x + Inches(0.05), top + Inches(0.9),
                box_w - Inches(0.1), Inches(0.55),
                desc, font_size=10, color=TEXT, align=PP_ALIGN.CENTER)

    if i < len(tasks) - 1:
        arrow = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW,
                                   x + box_w + Inches(0.01),
                                   top + Inches(0.6),
                                   arrow_w - Inches(0.02), Inches(0.3))
        arrow.line.fill.background()
        arrow.fill.solid(); arrow.fill.fore_color.rgb = MUTED

# Source / Sink labels
src = s.shapes.add_shape(MSO_SHAPE.PARALLELOGRAM,
                         Inches(0.5), Inches(4.3), Inches(2.0), Inches(0.7))
src.line.color.rgb = DARK
src.fill.solid(); src.fill.fore_color.rgb = LIGHT
add_textbox(s, Inches(0.5), Inches(4.45), Inches(2.0), Inches(0.5),
            "SQL Server (in)", font_size=11, bold=True, color=DARK, align=PP_ALIGN.CENTER)

sink1 = s.shapes.add_shape(MSO_SHAPE.PARALLELOGRAM,
                           Inches(8.0), Inches(4.3), Inches(2.3), Inches(0.7))
sink1.line.color.rgb = DARK
sink1.fill.solid(); sink1.fill.fore_color.rgb = LIGHT
add_textbox(s, Inches(8.0), Inches(4.45), Inches(2.3), Inches(0.5),
            "SQL Server (out)", font_size=11, bold=True, color=DARK, align=PP_ALIGN.CENTER)

sink2 = s.shapes.add_shape(MSO_SHAPE.PARALLELOGRAM,
                           Inches(10.6), Inches(4.3), Inches(2.3), Inches(0.7))
sink2.line.color.rgb = DARK
sink2.fill.solid(); sink2.fill.fore_color.rgb = LIGHT
add_textbox(s, Inches(10.6), Inches(4.45), Inches(2.3), Inches(0.5),
            "Browser (8501)", font_size=11, bold=True, color=DARK, align=PP_ALIGN.CENTER)

# Legend
legend_top = Inches(5.3)
add_textbox(s, Inches(0.7), legend_top, Inches(4), Inches(0.4),
            "Legend:", font_size=12, bold=True, color=DARK)
legend_items = [
    ("Data prep", PRIMARY),
    ("ML inference", ACCENT),
    ("Persist + serve", GREEN),
]
for i, (label, col) in enumerate(legend_items):
    y = legend_top + Inches(0.45) + Inches(0.4) * i
    sw = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                            Inches(0.7), y + Inches(0.05),
                            Inches(0.3), Inches(0.25))
    sw.line.fill.background()
    sw.fill.solid(); sw.fill.fore_color.rgb = col
    add_textbox(s, Inches(1.1), y, Inches(3.5), Inches(0.4),
                label, font_size=12, color=TEXT)

# Triggered note
add_textbox(s, Inches(5.5), Inches(5.4), Inches(7.5), Inches(0.4),
            "Schedule:  @daily  (also manually triggerable)",
            font_size=12, bold=True, color=DARK)
add_textbox(s, Inches(5.5), Inches(5.85), Inches(7.5), Inches(0.4),
            "DAG ID:  Prediction      File:  dags/prediction.py",
            font_size=11, color=MUTED, font_name="Consolas")
add_textbox(s, Inches(5.5), Inches(6.25), Inches(7.5), Inches(0.4),
            "Output:  include/data/predict/final.csv",
            font_size=11, color=MUTED, font_name="Consolas")

add_footer(s, 21)

add_notes(s, """This is the actual Prediction DAG that runs in Airflow (file: dags/prediction.py).

Walk through the 7 tasks left to right:
1) read_data — pulls raw member data from SQL Server.
2) snapshots — freezes a snapshot of members at a point in time so the model sees a consistent picture.
3) ingest — cleans, merges and produces feature parquet files.
4) predict — loads model.pkl and produces a prediction (probability of churn) for each member.
5) final_merge — joins the SQL data with the predictions and adds the run date.
6) upsert_to_sql — writes results into the SQL Server table repo.MembershipRetentionPredictions (deletes existing rows for that RunDate first, then inserts).
7) streamlit_app — launches the dashboard process so business users can browse.

In the Airflow UI:
- Each box becomes a task node.
- Arrows show dependencies — a task only runs after the previous one succeeds.
- Click any task to see its logs.

The colour legend at the bottom-left helps stakeholders quickly recognise what each task category does.

Schedule:
- @daily means it runs automatically once per day.
- Can also be triggered manually from the Airflow UI.
""")


# =========================================================
# SLIDE 22 — TRAINING DAG VISUALISED
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "The Training DAG — visualised",
                "How the ML model gets (re-)trained.")

train_tasks = [
    ("read_data", "Pull historical\nmember data", PRIMARY),
    ("snapshots", "Freeze training\nsnapshot", PRIMARY),
    ("ingest", "Clean + merge\nfeatures", PRIMARY),
    ("train", "Fit Random Forest\n+ Gradient Boosting", ACCENT),
]

box_w = Inches(2.2); box_h = Inches(1.5); arrow_w = Inches(0.35)
total_w = box_w * len(train_tasks) + arrow_w * (len(train_tasks) - 1)
start_x = Inches(0.6)
top = Inches(2.3)

for i, (name, desc, col) in enumerate(train_tasks):
    x = start_x + (box_w + arrow_w) * i
    box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, top, box_w, box_h)
    box.line.color.rgb = col
    box.fill.solid(); box.fill.fore_color.rgb = WHITE
    strip = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                               x, top, box_w, Inches(0.32))
    strip.line.fill.background()
    strip.fill.solid(); strip.fill.fore_color.rgb = col
    add_textbox(s, x, top + Inches(0.04), box_w, Inches(0.3),
                f"Task {i+1}", font_size=10, bold=True, color=WHITE,
                align=PP_ALIGN.CENTER)
    add_textbox(s, x + Inches(0.05), top + Inches(0.45),
                box_w - Inches(0.1), Inches(0.45),
                name, font_size=14, bold=True, color=DARK, align=PP_ALIGN.CENTER,
                font_name="Consolas")
    add_textbox(s, x + Inches(0.05), top + Inches(0.95),
                box_w - Inches(0.1), Inches(0.5),
                desc, font_size=11, color=TEXT, align=PP_ALIGN.CENTER)

    if i < len(train_tasks) - 1:
        arrow = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW,
                                   x + box_w + Inches(0.02),
                                   top + Inches(0.6),
                                   arrow_w - Inches(0.04), Inches(0.3))
        arrow.line.fill.background()
        arrow.fill.solid(); arrow.fill.fore_color.rgb = MUTED

# Outputs branching from train task
last_x = start_x + (box_w + arrow_w) * (len(train_tasks) - 1)
train_right = last_x + box_w
artifacts = [
    ("model_*.pkl", "Trained ML\nmodel", GREEN),
    ("metrics_*.json", "Accuracy &\nrun stats", ACCENT),
    ("log_*.txt", "Run log\n(human read)", MUTED),
]
art_top = Inches(4.4)
art_w = Inches(2.2); art_h = Inches(1.3); art_gap = Inches(0.3)
art_total = art_w * len(artifacts) + art_gap * (len(artifacts) - 1)
art_start_x = train_right + Inches(0.7)

# vertical connector from train to artifacts row
vline = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                           train_right - Inches(1.1), top + box_h,
                           Inches(0.04), Inches(0.5))
vline.line.fill.background()
vline.fill.solid(); vline.fill.fore_color.rgb = ACCENT

# Artifacts label
add_textbox(s, Inches(0.7), Inches(4.3), Inches(4), Inches(0.4),
            "Outputs (saved to include/artifacts/):",
            font_size=14, bold=True, color=DARK)

for i, (name, desc, col) in enumerate(artifacts):
    x = Inches(0.7) + (art_w + art_gap) * i
    box = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, art_top, art_w, art_h)
    box.line.color.rgb = col
    box.fill.solid(); box.fill.fore_color.rgb = LIGHT
    add_textbox(s, x + Inches(0.1), art_top + Inches(0.15),
                art_w - Inches(0.2), Inches(0.45),
                name, font_size=13, bold=True, color=col,
                align=PP_ALIGN.CENTER, font_name="Consolas")
    add_textbox(s, x + Inches(0.1), art_top + Inches(0.65),
                art_w - Inches(0.2), Inches(0.6),
                desc, font_size=11, color=TEXT, align=PP_ALIGN.CENTER)

# Right side info panel
info_x = Inches(8.5); info_y = Inches(4.3)
info = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                          info_x, info_y, Inches(4.3), Inches(2.4))
info.line.color.rgb = RGBColor(0xCC, 0xCC, 0xCC)
info.fill.solid(); info.fill.fore_color.rgb = WHITE
add_textbox(s, info_x + Inches(0.2), info_y + Inches(0.15),
            Inches(4.0), Inches(0.4),
            "Training run details", font_size=14, bold=True, color=PRIMARY)
add_bullets(s, info_x + Inches(0.2), info_y + Inches(0.6),
            Inches(4.0), Inches(1.7), [
    "DAG ID: ModelTrain",
    "File: dags/train.py",
    "Schedule: @daily",
    "Algorithms: Random Forest, Gradient Boosting (with GridSearchCV)",
    "Each run timestamps its artifacts.",
], font_size=11, line_spacing=1.2)

add_footer(s, 22)

add_notes(s, """This is the Training DAG (file: dags/train.py).

Walk through the 4 tasks:
1) read_data — pulls historical, labelled member data (we know who churned).
2) snapshots — freezes the training snapshot.
3) ingest — feature engineering: cleans data, merges tables, produces parquet files.
4) train — fits Random Forest and Gradient Boosting models with GridSearchCV, picks the best, evaluates accuracy on a held-out test set.

Outputs (saved to include/artifacts/):
- model_<timestamp>.pkl  — the trained model. The Prediction DAG loads this.
- metrics_<timestamp>.json  — accuracy, row count, timestamp.
- log_<timestamp>.txt  — human-readable summary of the run.

Why have a separate training DAG?
- Training is heavy and only needs to run when we want to refresh the model.
- Prediction is cheap and runs daily.
- Separating them keeps daily runs fast and reliable.

When to re-train:
- Model accuracy drops over time (concept drift).
- New data sources are added.
- Member behaviour changes significantly (e.g. after a price change).
""")


# =========================================================
# SLIDE 23 — USEFUL COMMANDS CHEAT SHEET
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Useful commands — cheat sheet",
                "Copy, paste, run.")

add_textbox(s, Inches(0.7), Inches(1.9), Inches(6), Inches(0.4),
            "Astro / Airflow", font_size=16, bold=True, color=PRIMARY)
add_code_block(s, Inches(0.7), Inches(2.3), Inches(6), Inches(2.2), [
    "$ astro dev start          # start Airflow",
    "$ astro dev stop           # stop Airflow",
    "$ astro dev restart        # restart Airflow",
    "$ astro dev kill           # force stop + cleanup",
    "$ astro dev bash           # shell into container",
    "$ astro dev logs           # follow logs",
])

add_textbox(s, Inches(7.0), Inches(1.9), Inches(6), Inches(0.4),
            "Streamlit & Python", font_size=16, bold=True, color=PRIMARY)
add_code_block(s, Inches(7.0), Inches(2.3), Inches(6), Inches(2.2), [
    "$ streamlit run include/lesmills_project/streamlit.py",
    "$ pip install -r requirements.txt",
    "$ python -m src.les.train.run        # train model",
    "$ python -m src.les.prediction.run   # run prediction",
])

add_textbox(s, Inches(0.7), Inches(4.7), Inches(6), Inches(0.4),
            "Docker", font_size=16, bold=True, color=PRIMARY)
add_code_block(s, Inches(0.7), Inches(5.1), Inches(6), Inches(1.7), [
    "$ docker ps               # list running containers",
    "$ docker logs <name>      # view a container's logs",
    "$ docker system prune     # free up disk space",
])

add_textbox(s, Inches(7.0), Inches(4.7), Inches(6), Inches(0.4),
            "Git", font_size=16, bold=True, color=PRIMARY)
add_code_block(s, Inches(7.0), Inches(5.1), Inches(6), Inches(1.7), [
    "$ git status              # what changed",
    "$ git pull                # get latest code",
    "$ git checkout -b feature # new branch",
])
add_footer(s, 23)

add_notes(s, """A reference page to keep handy.

When stakeholders ask "is there documentation?" — point them here and to the README in the repo.

Tip:
- Most issues can be diagnosed by running "astro dev logs" and reading the most recent error.
""")


# =========================================================
# SLIDE 24 — DEBUGGING: COMMON ISSUES
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Debugging — common issues & fixes",
                "What to do when something doesn't work.")

issues = [
    ("Docker not running",
     "'Cannot connect to the Docker daemon.'",
     "Start Docker Desktop and wait for the whale icon."),
    ("Port already in use",
     "'Bind for 0.0.0.0:8080 failed: port is already allocated.'",
     "Stop the other app, or run: astro dev start --webserver-port 8081"),
    ("Airflow stuck / weird state",
     "DAG won't appear, or tasks hang in 'queued'.",
     "Run: astro dev kill   then   astro dev start"),
    ("Pipeline task fails",
     "A red box in the Airflow UI.",
     "Click the task -> open Logs -> read the last error -> Clear to retry."),
    ("Database connection error",
     "'Login failed for user' / timeout to SQL Server.",
     "Check VPN connection and credentials in your .env file."),
    ("Model file missing",
     "'FileNotFoundError: model.pkl'",
     "Re-train the model or copy model.pkl into include/artifacts/."),
    ("Streamlit shows blank page",
     "Browser loads but no UI appears.",
     "Check the terminal for Python errors; restart streamlit run."),
]

top = Inches(1.95); row_h = Inches(0.65)
for i, (title, symptom, fix) in enumerate(issues):
    y = top + row_h * i
    bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.7), y + Inches(0.05),
                             Inches(0.12), Inches(0.55))
    bar.line.fill.background(); bar.fill.solid(); bar.fill.fore_color.rgb = ORANGE
    add_textbox(s, Inches(0.95), y, Inches(3.2), Inches(0.4),
                title, font_size=13, bold=True, color=DARK)
    add_textbox(s, Inches(0.95), y + Inches(0.32), Inches(3.2), Inches(0.4),
                symptom, font_size=10, color=MUTED)
    add_textbox(s, Inches(4.3), y + Inches(0.1), Inches(8.5), Inches(0.5),
                "→  " + fix, font_size=12, color=TEXT)
add_footer(s, 24)

add_notes(s, """Walk through the table — these cover ~90% of the issues a new team member will hit.

General debugging mindset to share with stakeholders:
1) Read the last few lines of the error — they almost always tell you what's wrong.
2) Try the simplest fix first: stop and restart the affected piece.
3) If it still fails, escalate to engineering with:
   - The exact command you ran
   - The full error message (copy-paste)
   - A screenshot of the Airflow UI if relevant.
""")


# =========================================================
# SLIDE 25 — WHERE TO LOOK WHEN THINGS GO WRONG
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s)
add_title_block(s, "Where to look when things go wrong",
                "A quick map of logs and files for diagnostics.")

places = [
    ("Airflow UI logs",
     "http://localhost:8080  →  DAG  →  Task  →  Logs",
     "Best place to start. Shows exactly what each step did."),
    ("Astro / Docker logs",
     "Run: astro dev logs",
     "Shows the full output of the Airflow services."),
    ("Streamlit terminal",
     "The window where you ran 'streamlit run ...'",
     "Python errors from the dashboard appear here."),
    ("Project artifacts",
     "include/artifacts/*.pkl  +  include/artifacts/log_*.txt",
     "Confirms the trained model and run logs exist."),
    ("Output data",
     "include/data/predictions.csv",
     "Shows whether predictions actually got written."),
    (".env file (local)",
     "Project root, never committed",
     "Holds DB credentials and secrets — check for typos."),
]
top = Inches(1.95); row_h = Inches(0.78)
for i, (place, where, why) in enumerate(places):
    y = top + row_h * i
    bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.7), y + Inches(0.05),
                             Inches(0.12), Inches(0.65))
    bar.line.fill.background(); bar.fill.solid(); bar.fill.fore_color.rgb = ACCENT
    add_textbox(s, Inches(0.95), y, Inches(3.5), Inches(0.4),
                place, font_size=14, bold=True, color=DARK)
    add_textbox(s, Inches(0.95), y + Inches(0.35), Inches(3.5), Inches(0.4),
                where, font_size=11, color=MUTED, font_name="Consolas")
    add_textbox(s, Inches(4.6), y + Inches(0.15), Inches(8.2), Inches(0.6),
                why, font_size=12, color=TEXT)
add_footer(s, 25)

add_notes(s, """For non-technical stakeholders:
- You don't need to read the logs yourself — just know they exist.
- When raising a ticket with the engineering team, ask them to attach:
  1) The relevant Airflow task log.
  2) The .csv output (if any).
  3) The time the issue happened.
- This dramatically reduces the time-to-fix.

For engineers:
- Always check Airflow logs first.
- Then check container logs ("astro dev logs").
- Then check the Python files in src/les/ to reproduce locally.
""")


# =========================================================
# SLIDE 26 — THANK YOU
# =========================================================
s = prs.slides.add_slide(BLANK); add_background(s, DARK)
block = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, Inches(2.4), Inches(0.6), Inches(2.6))
block.line.fill.background(); block.fill.solid(); block.fill.fore_color.rgb = PRIMARY

add_textbox(s, Inches(1.0), Inches(2.4), Inches(11), Inches(1.0),
            "Thank you", font_size=64, bold=True, color=WHITE)
add_textbox(s, Inches(1.0), Inches(3.6), Inches(11), Inches(0.6),
            "Questions and discussion", font_size=24, color=LIGHT)
add_textbox(s, Inches(1.0), Inches(5.5), Inches(11), Inches(0.5),
            "Keyvan Salehi  •  Les Mills New Zealand  •  2025/11/12",
            font_size=16, color=MUTED)

add_notes(s, """Wrap-up:

- Recap the two halves: business value (slides 2-13) and operations (slides 15-25).
- Invite questions.
- Offer a follow-up demo of the live dashboard for anyone interested.
- Share the README and this deck for self-study.
""")


import os
import time

output = "lesmills_retention_overview.pptx"
try:
    prs.save(output)
except PermissionError:
    fallback = f"lesmills_retention_overview_{time.strftime('%Y%m%d_%H%M%S')}.pptx"
    prs.save(fallback)
    print(f"Original file was locked (open in PowerPoint?). Saved as: {fallback}")
    output = fallback

print(f"Saved: {output} with {len(prs.slides)} slides (each with speaker notes)")
