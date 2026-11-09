SUMMARY = ../data/campaigns/reference-bounded-20261021/analysis/summary.json
FIGURES = $(addprefix figures/,$(addsuffix .pdf,ra_latency abd_latency throughput communication partition crash))

.PHONY: all clean

all: $(FIGURES)
	latexmk -pdf -interaction=nonstopmode -halt-on-error relatorio_projeto2.tex

$(FIGURES) &: figures.py $(SUMMARY)
	uv run --frozen --project ../analysis python figures.py

clean:
	latexmk -c relatorio_projeto2.tex
