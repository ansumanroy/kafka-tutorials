# 🎬 Export Kafka Tutorial Slides

## ⚠️ Issue: Mermaid Diagrams Not Rendering

Your slides use Mermaid diagrams which need special handling to render in PDF/PPTX.

## ✅ Quick Fix (Recommended)

### Option 1: Use HTML for Best Results

```bash
# Export to HTML (Mermaid diagrams work perfectly)
marp SLIDES.md --html -o kafka-tutorial-slides.html

# Open in browser for presentation
open kafka-tutorial-slides.html  # macOS
```

**Benefits:**
- ✅ Perfect Mermaid rendering
- ✅ Interactive diagrams
- ✅ Small file size
- ✅ Works immediately (no additional installs)

### Option 2: Install Mermaid CLI (for PDF/PPTX)

```bash
# Install Mermaid CLI
npm install -g @mermaid-js/mermaid-cli

# Re-export with proper rendering
./export-slides.sh
```

## 📊 Comparison

| Format | Command | Mermaid Quality |
|--------|---------|-----------------|
| **HTML** ⭐ | `marp SLIDES.md --html -o slides.html` | Perfect ✅✅✅ |
| **PDF** | Needs mermaid-cli | Good ⚠️ |
| **PPTX** | Needs mermaid-cli | Fair ⚠️ |

## 🚀 Quick Commands

### For Presentation (Best)
```bash
# Create HTML version
marp SLIDES.md --html -o presentation.html

# Open in fullscreen browser
open presentation.html

# Keyboard shortcuts:
# - Arrow keys: Navigate
# - F: Fullscreen
# - Esc: Exit
```

### For PDF (After Installing Mermaid CLI)
```bash
# Install mermaid-cli first
npm install -g @mermaid-js/mermaid-cli

# Then export
marp SLIDES.md --pdf --html --allow-local-files -o slides.pdf
```

### For PowerPoint (After Installing Mermaid CLI)
```bash
marp SLIDES.md --pptx --html --allow-local-files -o slides.pptx
```

## 🎯 Recommended: Present from HTML

**Why HTML is better:**
1. No installation needed (works now!)
2. Perfect diagram rendering
3. Interactive navigation
4. Professional appearance
5. Smaller file size

**How to present:**
```bash
# 1. Export to HTML
marp SLIDES.md --html -o presentation.html

# 2. Open in browser
open presentation.html

# 3. Press F for fullscreen
# 4. Use arrow keys to navigate
```

## 🐛 If You Still Want PDF/PPTX

### Step 1: Install Mermaid CLI
```bash
npm install -g @mermaid-js/mermaid-cli
```

### Step 2: Verify Installation
```bash
mmdc --version
# Should output: 10.x.x or similar
```

### Step 3: Export
```bash
# Use the export script
chmod +x export-slides.sh
./export-slides.sh

# Or manually:
marp SLIDES.md --pdf --html --allow-local-files -o slides.pdf
```

## 📝 Current Status

✅ Marp CLI installed (v4.2.3)
❌ Mermaid CLI not installed (needed for PDF/PPTX)

## 🎨 Alternative: Convert HTML to PDF

If Mermaid CLI installation fails, you can:

```bash
# 1. Export to HTML
marp SLIDES.md --html -o slides.html

# 2. Open in Chrome
open slides.html

# 3. Print to PDF
# File → Print → Save as PDF
# OR
# Cmd+P → Save as PDF
```

This gives you a PDF with perfect Mermaid rendering!

---

**Bottom Line:** Use HTML format for now - it works perfectly!
