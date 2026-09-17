# MC714 — Trabalho 1: Balanceador de Carga

Simulador de eventos discretos em Go para comparar políticas de balanceamento sob rajadas Bounded Pareto. O executável roda separadamente as rajadas de 30, 60, 90 e 120 requisições, com 10 repetições por política e horizonte de 200 unidades de tempo. Ele também executa um benchmark M/M/1 estacionário separado para validar distribuição geométrica, estabilidade, Lei de Little e o regime instável.

## Requisitos

- Go 1.26.4 ou compatível com o `go.mod`.
- Para regenerar gráficos: Python 3.14 e [uv](https://docs.astral.sh/uv/).
- Para recompilar o relatório: uma distribuição LaTeX com `pdflatex` e `IEEEtran`.

O simulador não usa dependências externas de Go e funciona em Linux e Windows.

## Compilar e testar

Linux:

```sh
cd simulator
go test ./...
go build -o simulador ./cmd/simulador
./simulador
```

Windows PowerShell:

```powershell
cd simulator
go test ./...
go build -o simulador.exe ./cmd/simulador
.\simulador.exe
```

Também é possível executar sem gerar um binário permanente:

```sh
go run ./cmd/simulador
```

A saída padrão apresenta as médias das 10 repetições, os valores analíticos, o benchmark M/M/1 e os cenários extras. Use `-extras=false` para suprimir extensões opcionais e `-stationary=false` para executar somente a matriz de rajadas.

## Monitorar a dinâmica dos servidores

A opção `-trace` imprime, após cada evento, o instante, servidor, ocupação, tamanho da fila, conclusões, capacidade e indicação de backup:

```sh
go run ./cmd/simulador -trace
```

Para alterar a semente reproduzível:

```sh
go run ./cmd/simulador -seed 20260831
```

Políticas de uma mesma combinação de rajada e repetição recebem o mesmo tráfego. Rajadas de tamanhos diferentes e as 10 repetições usam fluxos independentes.

## Cenários extras de arquitetura

Além de servidores heterogêneos e backup, `-extras` executa três arquiteturas de fila com a rajada 120 e três trabalhadores homogêneos de capacidade um: filas privadas com escolha Aleatória, filas privadas com Fila Mais Curta e uma fila FIFO compartilhada de onde trabalhadores ociosos retiram trabalho. Também compara consulta plana com hierarquia de dois pools em quatro trabalhadores. Os resultados ficam em `extras.csv` e a figura comparativa em `architecture_comparison.png`.

## Regenerar dados e gráficos

A partir de `simulator/`:

```sh
go run ./cmd/simulador \
  -results-csv ../analysis/data/results.csv \
  -trials-csv ../analysis/data/trials.csv \
  -trace-csv ../analysis/data/server_trace.csv \
  -traffic-csv ../analysis/data/traffic.csv \
  -extras-csv ../analysis/data/extras.csv \
  -stationary-csv ../analysis/data/stationary.csv
```

No Windows PowerShell, execute o mesmo comando em uma linha ou substitua `\` pelo acento grave `` ` `` de continuação.

Depois, a partir de `analysis/`:

```sh
uv sync
uv run simulation-plots
```

Os CSVs ficam em `analysis/data/` e as figuras em `analysis/figures/`. `stationary.csv` contém uma linha por política e carga, com valores analíticos, médias simuladas, termo direito de Little e tamanho final da fila.

## Recompilar o relatório

Copie `metrics_by_burst.png` e `stationary_validation.png` para `report/figures/` e execute, a partir de `report/`:

```sh
pdflatex -interaction=nonstopmode -halt-on-error relatorio_projeto1.tex
pdflatex -interaction=nonstopmode -halt-on-error relatorio_projeto1.tex
```

O PDF final deve ter no máximo quatro páginas. O arquivo de entrega gerado neste repositório é `relatorio_projeto1_rodrigo_camargo.pdf`.

## Entregáveis

- `relatorio_projeto1_rodrigo_camargo.pdf`: relatório IEEE final com quatro páginas.
- `codigo_projeto1_rodrigo_camargo.zip`: código Go, análise em Python, dados reproduzíveis, fonte LaTeX e este guia.

Esses nomes seguem o padrão definido no enunciado. São os dois arquivos que devem ser enviados no Classroom.

## Estrutura

- `simulator/cmd/simulador`: interface de linha de comando e exportação dos resultados.
- `simulator/internal/engine`: fila de eventos, servidores, filas e métricas.
- `simulator/internal/balancer`: políticas obrigatórias e adicionais.
- `simulator/internal/analytics`: modelo analítico de lote finito.
- `simulator/internal/stationary`: benchmark M/M/1 por réplica e aproximação fluida instável.
- `simulator/pkg/mathutil`: distribuição Bounded Pareto.
- `analysis`: dados, geração de gráficos e comparação quantitativa.
- `report`: fonte IEEE e figuras do relatório.
