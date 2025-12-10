# Kafka Tutorial Slides - Export Guide

This guide explains how to properly export the slides with Mermaid diagrams rendered.

## 🎯 Quick Start

```bash
# Run the export script (recommended)
./export-slides.sh
```

This will generate:
- ✅ `kafka-tutorial-slides.pdf`
- ✅ `kafka-tutorial-slides.pptx`
- ✅ `kafka-tutorial-slides.html`

## 📋 Prerequisites

### Install Required Tools

```bash
# 1. Install Marp CLI
npm install -g @marp-team/marp-cli

# 2. Install Mermaid CLI (for diagram rendering)
npm install -g @mermaid-js/mermaid-cli

# 3. Install Puppeteer (Chromium for rendering)
npm install -g puppeteer
```

## 🔧 Manual Export Methods

### Method 1: Using Export Script (Easiest)

```bash
chmod +x export-slides.sh
./export-slides.sh
```

### Method 2: Manual Marp Commands

```bash
# PDF with Mermaid support
marp SLIDES.md --pdf --allow-local-files --html -o kafka-tutorial-slides.pdf

# PowerPoint with Mermaid support
marp SLIDES.md --pptx --allow-local-files --html -o kafka-tutorial-slides.pptx

# HTML (works best for Mermaid)
marp SLIDES.md --html --allow-local-files -o kafka-tutorial-slides.html
```

### Method 3: Using Marp VS Code Extension

1. Open `SLIDES.md` in Cursor/VS Code
2. Install "Marp for VS Code" extension
3. Click the Marp icon in the toolbar
4. Select "Export Slide Deck"
5. Choose format (PDF, PPTX, HTML)

## 🎨 Best Export Format for Mermaid Diagrams

### Recommended Order:

1. **HTML** (Best) - Full Mermaid support, interactive
   ```bash
   marp SLIDES.md --html -o slides.html
   ```

2. **PDF via HTML** - Convert HTML to PDF for better rendering
   ```bash
   # Export to HTML first
   marp SLIDES.md --html -o slides.html
   
   # Then use Chromium to print to PDF
   google-chrome --headless --print-to-pdf=slides.pdf slides.html
   ```

3. **PDF via Marp** - Direct export (may have rendering issues)
   ```bash
   marp SLIDES.md --pdf --allow-local-files --html -o slides.pdf
   ```

4. **PPTX** - For editing in PowerPoint (diagrams may be images)
   ```bash
   marp SLIDES.md --pptx --allow-local-files --html -o slides.pptx
   ```

## 🐛 Troubleshooting

### Issue 1: Mermaid Diagrams Not Rendering

**Symptom**: Diagrams appear as text or are missing

**Solution**:
```bash
# Install mermaid-cli
npm install -g @mermaid-js/mermaid-cli

# Use --html flag
marp SLIDES.md --pdf --html --allow-local-files -o output.pdf
```

### Issue 2: "ENOENT: no such file or directory" Error

**Solution**:
```bash
# Install puppeteer
npm install -g puppeteer

# Or use system Chrome/Chromium
export PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
export PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome
```

### Issue 3: Diagrams Cut Off or Too Small

**Solution**: Edit font size in SLIDES.md:
```javascript
mermaid.initialize({ 
  startOnLoad: true,
  theme: 'default',
  themeVariables: {
    fontSize: '18px'  // Increase this
  }
});
```

### Issue 4: PDF Export Hangs

**Solution**:
```bash
# Use --no-sandbox flag
marp SLIDES.md --pdf --html --allow-local-files --no-sandbox -o output.pdf

# Or increase timeout
marp SLIDES.md --pdf --html --allow-local-files --timeout 300000 -o output.pdf
```

## 🌐 Alternative: Present from HTML

For the **best Mermaid rendering**, present directly from HTML:

```bash
# Export to HTML
marp SLIDES.md --html -o slides.html

# Open in browser
open slides.html  # macOS
xdg-open slides.html  # Linux
start slides.html  # Windows

# Or serve with Python
python3 -m http.server 8000
# Then open: http://localhost:8000/slides.html
```

**HTML Presentation Controls**:
- `→` or `Space`: Next slide
- `←` or `Backspace`: Previous slide
- `F`: Fullscreen
- `Esc`: Exit fullscreen

## 📊 Comparison of Export Formats

| Format | Mermaid Support | Editable | File Size | Presentation |
|--------|-----------------|----------|-----------|--------------|
| **HTML** | ✅✅✅ Perfect | ❌ No | Small | Best |
| **PDF** | ⚠️ Variable | ❌ No | Medium | Good |
| **PPTX** | ⚠️ As images | ✅ Yes | Large | Fair |

## 🎯 Recommended Workflow

**For Presentation:**
```bash
# Use HTML for live presentation
marp SLIDES.md --html -o presentation.html
open presentation.html
```

**For Distribution:**
```bash
# Create PDF via HTML for best quality
marp SLIDES.md --html -o temp.html
google-chrome --headless --print-to-pdf=kafka-slides.pdf temp.html
rm temp.html
```

**For Editing:**
```bash
# Export to PPTX if you need to customize
marp SLIDES.md --pptx -o kafka-slides.pptx
# Note: Mermaid diagrams become images in PPTX
```

## 🚀 Quick Commands Reference

```bash
# Best quality (HTML)
marp SLIDES.md --html -o slides.html

# Portable (PDF with Mermaid)
marp SLIDES.md --pdf --html --allow-local-files -o slides.pdf

# Editable (PowerPoint)
marp SLIDES.md --pptx --allow-local-files -o slides.pptx

# Watch mode (auto-reload)
marp -w SLIDES.md --html -o slides.html

# Custom theme
marp SLIDES.md --theme-set custom-theme.css --html -o slides.html

# With progress bar
marp SLIDES.md --html --progress -o slides.html
```

## 📝 Notes

1. **HTML format is recommended** for presentations with Mermaid diagrams
2. **PDF quality** depends on your Chromium/Puppeteer installation
3. **PPTX format** converts Mermaid diagrams to static images
4. Always use `--html` flag when exporting PDF/PPTX with Mermaid
5. For best results, present from HTML in a browser

## 🔗 Resources

- Marp Documentation: https://marp.app/
- Mermaid Documentation: https://mermaid.js.org/
- Marp CLI: https://github.com/marp-team/marp-cli
- Mermaid CLI: https://github.com/mermaid-js/mermaid-cli

---

**Need Help?** Check the troubleshooting section above or open an issue.
