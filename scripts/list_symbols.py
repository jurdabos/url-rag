import ast
from pathlib import Path
import textwrap
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORT_NAME = "function_report.txt"
REPORT_PATH = PROJECT_ROOT / "scripts" / REPORT_NAME
py_files = PROJECT_ROOT.rglob("*.py")
REPORT_PATH.unlink(missing_ok=True)
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---- helpers ---------------------------------------------------------
def list_defs(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [
        f"{n.name}{'()' if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) else ''}"
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]


# ---- collect definitions -------------------------------------------
report_lines: list[str] = []
for file in sorted(PROJECT_ROOT.rglob("*.py")):
    if any(p in file.parts for p in (".venv", ".idea")):
        continue
    if defs := list_defs(file):
        report_lines += [str(file.relative_to(PROJECT_ROOT)),
                         textwrap.indent("\n".join(defs), "    "),
                         ""]

# ---- write fresh report --------------------------------------------
REPORT_PATH.write_text("\n".join(report_lines), encoding="utf-8")
print(f"Wrote {REPORT_PATH.relative_to(PROJECT_ROOT)}")
