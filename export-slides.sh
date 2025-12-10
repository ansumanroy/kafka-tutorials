#!/bin/bash

echo "🎬 Exporting Kafka Tutorial Slides..."
echo ""

# Check if marp-cli is installed
if ! command -v marp &> /dev/null; then
    echo "❌ Marp CLI not found. Installing..."
    npm install -g @marp-team/marp-cli
fi

# Check if mmdc (mermaid-cli) is installed
if ! command -v mmdc &> /dev/null; then
    echo "⚠️  Mermaid CLI not found. Installing..."
    npm install -g @mermaid-js/mermaid-cli
fi

echo ""
echo "📄 Exporting to PDF..."
marp SLIDES.md --pdf \
  --allow-local-files \
  --html \
  -o kafka-tutorial-slides.pdf

echo ""
echo "📊 Exporting to PowerPoint..."
marp SLIDES.md --pptx \
  --allow-local-files \
  --html \
  -o kafka-tutorial-slides.pptx

echo ""
echo "🌐 Exporting to HTML..."
marp SLIDES.md --html \
  --allow-local-files \
  -o kafka-tutorial-slides.html

echo ""
echo "✅ Export complete!"
echo ""
echo "Generated files:"
echo "  - kafka-tutorial-slides.pdf"
echo "  - kafka-tutorial-slides.pptx"
echo "  - kafka-tutorial-slides.html"
