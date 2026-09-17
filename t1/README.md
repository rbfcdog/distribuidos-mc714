# MC714 — Trabalho 1: Balanceador de Carga

Simulador de eventos discretos em Go para três servidores homogêneos. Cada servidor possui uma fila FCFS ilimitada e uma única thread. As chegadas seguem Poisson de taxa `lambda` e o serviço é exponencial com `mu = 1`.

O experimento obrigatório executa as políticas `random`, `round_robin` e `shortest_queue` em `lambda = 0.6, 1.2, 1.8, 2.4, 2.7`. Cada configuração tem 10 réplicas, horizonte de 5000 unidades e aquecimento de 500. O caso instável `lambda = 3.3` registra a dinâmica de filas.

## Requisitos

- Go compatível com a versão declarada em `simulator/go.mod`.
- Python e `uv` para regenerar os gráficos.
- LaTeX com `pdflatex` e `IEEEtran` para recompilar o relatório.

Não há dependências externas de Go. O código funciona em Linux e Windows.

## Compilar e testar

Linux:

```sh
cd simulator
go test ./...
go vet ./...
go build -o simulador ./cmd/simulador
./simulador
```

Windows PowerShell:

```powershell
cd simulator
go test ./...
go vet ./...
go build -o simulador.exe ./cmd/simulador
.\simulador.exe
```

## Selecionar política e observar filas

```sh
go run ./cmd/simulador -policy shortest_queue
go run ./cmd/simulador -trace -policy random
```

`-policy` aceita `all`, `random`, `round_robin` e `shortest_queue`. `-trace` imprime, após cada evento da primeira réplica, instante, número total no sistema, ocupação das três filas e conclusões por servidor. `-seed` troca a semente base preservando réplicas independentes. Para um mesmo `lambda` e índice de réplica, todas as políticas recebem a mesma sequência de chegadas.

## Regenerar dados e figuras

A partir de `simulator/`:

```sh
go run ./cmd/simulador \
  -results-csv ../analysis/data/results.csv \
  -trials-csv ../analysis/data/trials.csv \
  -trace-csv ../analysis/data/server_trace.csv
```

A partir de `analysis/`:

```sh
uv sync
uv run simulation-plots
```

Os arquivos `results.csv`, `trials.csv` e `server_trace.csv` preservam, respectivamente, médias e intervalos de confiança de 95%, as dez réplicas e a trajetória de filas. Os gráficos gerados são `response_comparison.png` e `unstable_queues.png`.

## Recompilar o relatório

A partir de `report/`:

```sh
pdflatex -interaction=nonstopmode -halt-on-error relatorio_projeto1.tex
pdflatex -interaction=nonstopmode -halt-on-error relatorio_projeto1.tex
```

O relatório final é `relatorio_projeto1_rodrigo_camargo.pdf`. O arquivo compactado de entrega é `codigo_projeto1_rodrigo_camargo.zip`.

## Estrutura

- `simulator/internal/balancer`: as três políticas de encaminhamento.
- `simulator/internal/stationary`: fila de eventos, servidores FCFS, métricas, modelo M/M/1 e testes.
- `simulator/cmd/simulador`: configuração por linha de comando e exportação CSV.
- `analysis`: gráficos da comparação e da instabilidade.
- `report`: fonte IEEE e figuras do relatório.
