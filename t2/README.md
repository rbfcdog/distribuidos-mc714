# MC714 — Trabalho 2

Projeto Go independente para Ricart–Agrawala e replicação por quórum ABD.

## Estado atual

- `cmd/node`: processo HTTP com configuração de membros validada, identidade do nó/execução, logs JSON, timeouts e encerramento por SIGINT/SIGTERM.
- `cmd/lab`: sonda de saúde, seção crítica RA, leitura/escrita ABD, incremento protegido e observador independente; retorna código não zero em falhas.
- `cmd/load`: gerador Poisson de malha aberta, concorrência/fila limitadas, sem retries de operações e evidência JSONL de todas as ofertas.
- `internal/config`: leitura estrita da configuração, normalização dos membros e identidade estável do cluster.
- `internal/wire`: contratos e decodificação validada de mensagens; resposta de saúde compartilhada.
- `internal/ra`: estado Ricart–Agrawala, permissões de todos os participantes e entrega HTTP com retransmissão limitada em memória.
- `internal/abd`: registro durável local e leituras/escritas ABD em duas fases de quórum.
- `internal/counter`: composição de RA e ABD para incremento fixo de +1, sem liberar a exclusão entre leitura e escrita.
- `compose.yaml`: três participantes independentes na rede bridge do projeto, com portas publicadas somente em localhost. Essa rede permite saída para a Internet.

RA, ABD e a aplicação de contador integrado estão implementados. Bônus: detector de falhas por heartbeat (`GET /fd/state`), reintegração opcional (`-allow-rejoin` / `ALLOW_REJOIN=true`), injeção de atraso/perda (`FAULT_DELAY_MS`, `FAULT_LOSS_RATE`) e overlay de nuvem (`compose.cloud.yaml`, `configs/cloud.json`). A campanha de referência e a análise estão concluídas: cem ensaios completos em [`data/campaigns/reference-bounded-20261021/analysis/REPORT.md`](data/campaigns/reference-bounded-20261021/analysis/REPORT.md). O [relatório IEEE](report/relatorio_projeto2.pdf) e o [roteiro do vídeo](roteiro_video.pdf) estão atualizados. `/healthz` indica apenas que o processo HTTP responde; não comprova correção nem disponibilidade distribuída.

## Requisitos

- Go 1.26.4 ou compatível com `go.mod` para desenvolvimento local.
- Docker Engine e plugin `docker compose` para os contêineres. O ambiente de referência do enunciado usa Compose v2.

Todos os comandos abaixo partem de `t2/`. O projeto Go usa somente a biblioteca padrão; não há dependências externas nem `go.sum`.

## Compilar e verificar

```sh
go build -o bin/ ./cmd/...
go test -race ./...
go vet ./...
```

## Executar localmente

No primeiro terminal:

```sh
./bin/node -id node1 -run-id local-001 -config configs/local.json -listen 127.0.0.1:18081
```

Para repetir a execução, escolha outro `run-id` e use-o em todos os participantes; a identidade já iniciada não pode ser reutilizada.

No segundo:

```sh
./bin/lab -url http://127.0.0.1:18081 -timeout 3s
```

A resposta identifica `status`, `node_id` e `run_id`. Use Ctrl+C para encerrar o nó. Consulte as opções com `./bin/node -h` e `./bin/lab -h`.

Para iniciar os demais participantes locais, use o mesmo `-config` e `-run-id`, trocando o ID para `node2`/`node3` e a porta para `18082`/`18083`. Não execute simultaneamente esse cluster e o Compose nas mesmas portas.

## Configuração de membros

`-config`, `-id` e `-run-id` são obrigatórios. O arquivo exige `mode` (`standalone` ou `integrated`) e `members`, sem modo implícito. `configs/local.json` e `configs/compose.json` usam `standalone`; `configs/integrated.json` usa `integrated`:

```json
{
  "mode": "standalone",
  "members": [
    {"id": "node1", "url": "http://node1:8080"},
    {"id": "node2", "url": "http://node2:8080"},
    {"id": "node3", "url": "http://node3:8080"}
  ]
}
```

- Configuração estática com 3, 5 ou 7 membros; o ID local deve pertencer à lista.
- IDs de nós e execução: 1–64 caracteres ASCII, letras, dígitos, `_` e `-`.
- URLs HTTP com porta explícita, sem credenciais, query, fragmento ou caminho além de `/`. IDs e endpoints normalizados não podem se repetir.
- Campos desconhecidos, JSON adicional e arquivos maiores que 64 KiB são rejeitados antes de abrir a porta HTTP.
- Os membros são ordenados por ID. `cluster_id` é SHA-256 do JSON canônico de `run_id`, `mode` e membros normalizados; independe da ordem do arquivo e do nó que carrega a configuração.
- Alterar um endpoint, modo ou `run_id` muda a identidade do cluster. Modos diferentes não trocam mensagens válidas entre si. Essa identidade evita mistura acidental; não autentica remetentes.
- A configuração é carregada uma vez. URLs devem alcançar as portas e nomes corretos no ambiente escolhido; a validação sintática não resolve aliases DNS nem verifica conectividade.

`GET /cluster` retorna `node_id`, `run_id`, `mode`, `cluster_id`, `members` e `quorum`. Para inspecionar um nó:

```sh
curl --fail http://127.0.0.1:18081/cluster
```

O `quorum` informa a maioria prevista para ABD. Ricart–Agrawala exige respostas de todos os outros participantes, não apenas dessa maioria.

## Contratos de mensagens

`wire.DecodeMessage` valida um único objeto JSON de até 16 KiB. A versão atual é `1`; campos desconhecidos e dados após o objeto são rejeitados.

Todo envelope contém `version`, `run_id`, `cluster_id`, `from`, `to`, `message_id`, `kind` e `request` (`owner`, `sequence`). IDs de mensagem e sequência são positivos. A execução e o cluster devem corresponder à configuração local; remetente e dono do pedido devem ser membros, e `to` deve identificar o nó receptor.

| `kind` | Dono do pedido | Payload |
| --- | --- | --- |
| `ra.request` | Remetente | `clock` positivo |
| `ra.reply` | Destinatário | `clock` positivo |
| `abd.query` | Remetente | Sem `clock` ou `register` |
| `abd.state` | Destinatário | `register` |
| `abd.update` | Remetente | `register` |
| `abd.ack` | Destinatário | `register` |

Mensagens RA não aceitam registro nem remetente igual ao destinatário. Mensagens ABD não aceitam `clock`; a réplica local pode usar o mesmo remetente/destinatário.

O registro contém `tag` (`counter`, `writer_id`) e `value` inteiro de 64 bits; todos esses campos devem estar presentes e não nulos. Tags são comparadas por contador e, no empate, ID de escritor em ordem lexicográfica. O estado inicial é exclusivamente contador zero, escritor vazio e valor zero; versões positivas exigem escritor membro do cluster.

Uma retransmissão preserva o pedido; `request` correlaciona fases/respostas com a operação original. IDs de frame são monotônicos por nó, execução e algoritmo. O decodificador não mantém estado. RA distingue pedidos ativos, antigos, duplicados e conflitantes; ABD aplica versões monotônicas e valida respostas pelo participante, contexto, pedido e fase.

## Ricart–Agrawala

Implementação baseada em [Ricart e Agrawala, 1981](https://doi.org/10.1145/358527.358537), com membros estáticos e modelo crash-stop:

- Pedidos ordenados por `(relógio_lógico, node_id)`; entrada somente após permissões de todos os outros nós.
- Estados `RELEASED`, `WANTED` e `HELD`. Uma tentativa local por vez; outra chamada concorrente ao mesmo nó recebe HTTP 429, sem fila ilimitada.
- A aquisição tem prazo operacional. Timeout retorna HTTP 504, abandona o pedido local e libera respostas adiadas; não remove participantes nem concede acesso por suspeita de falha.
- Mensagens usam tentativas HTTP de até 500 ms, com intervalo de retransmissão de 100 ms. Pedidos preservam sua identidade; respostas adiadas usam workers por participante e slots limitados, independentes do contexto do cliente.
- Depois da entrada, desconexão do cliente não interrompe o trabalho limitado nem pula a saída. Timeout ou falha ao receber a resposta do comando pode significar resultado indeterminado; não repetir automaticamente como nova operação.
- IDs, sequências e relógios nunca dão a volta por overflow. Reinicializar um participante na mesma execução não é suportado.

### API e cliente

Com os três nós iniciados:

```sh
./bin/lab -action ra -url http://127.0.0.1:18081 -hold-ms 50 -wait-ms 3000 -timeout 5s
curl --fail http://127.0.0.1:18081/ra/state
```

`POST /ra/execute` recebe `{"hold_ms":50,"wait_ms":3000}`. O trabalho deve durar de 0 a 500 ms; a espera deve ficar entre 1 e 3000 ms. O corpo tem limite de 1 KiB; campos desconhecidos, JSON adicional e valores inválidos retornam 400. A resposta `completed` só é produzida depois de liberar a seção crítica; contém identidade, pedido, `wait_ns` e `held_ns`, medidos localmente com relógio monotônico.

`POST /ra/messages` valida e processa apenas mensagens RA; 204 significa processamento/adiamento do frame, não permissão de entrada. Mensagens de outro algoritmo retornam 422. Conflitos de estado/protocolo retornam 409. Falha operacional ou de instrumentação retorna 503.

`GET /ra/state` expõe estado, relógio, pedido ativo, permissões, respostas adiadas, mensagens lógicas geradas e tentativas de transporte. Em uma aquisição normal são geradas `2(N-1)` mensagens de algoritmo; retransmissões incrementam tentativas, não criam novos pedidos. Observador, saúde e controle não entram na contagem algorítmica.

### Observador de correção

O observador recebe entrada/saída e conta sobreposições sem conceder lock, bloquear uma entrada até outra terminar ou fornecer permissões. Registra uma violação e confirma o evento mesmo quando há sobreposição. Não compara relógios de parede entre nós.

```sh
export RUN_ID=ra-001
export OBSERVER_URL=http://observer:8090
docker compose --profile verify up --build --detach --wait --wait-timeout 60
./bin/lab -action ra -url http://127.0.0.1:18081 -hold-ms 50 -wait-ms 3000
curl --fail http://127.0.0.1:18089/report
docker compose --profile verify down --remove-orphans
unset OBSERVER_URL
```

O perfil `verify` inicia o observador em outro processo. Sua API é `POST /events` e `GET /report`; os eventos são gravados como JSON no stdout, disponíveis em `docker compose logs observer`. `lab -action observe -listen 127.0.0.1:18089` permite executá-lo fora do Compose. A identidade de execução/cluster é fixada pelo primeiro evento válido.

A evidência só é válida com o número esperado de entradas e saídas, `active=0`, `violations=0` e `invalid_events=0`. Um relatório vazio não prova exclusão. Eventos repetidos do pedido atual/último concluído são idempotentes; eventos antigos ou inconsistentes não podem limpar uma seção ativa mais nova e invalidam a evidência.

Instrumentação é opcional, mas, quando configurada, falha de registro impede uma resposta de sucesso. Os callbacks têm prazo de 250 ms e aumentam o intervalo observado; não misturar essa medição com ensaios de desempenho sem observador.

### Smoke reproduzível: concorrência e crash

```sh
python3 experiments/ra_smoke.py
```

O script usa somente a biblioteca padrão de Python. Constrói e inicia três nós e o observador em um projeto Compose próprio, executa 31 seções críticas incluindo o cliente real, verifica os eventos e a contagem de mensagens, mata um participante e exige timeout sem novas entradas. Remove seus contêineres/rede mesmo em falha. As portas 18081–18083 e 18089 devem estar livres; encerre outros ambientes antes de executá-lo.

## ABD: registro atômico replicado

Implementação multiwriter/multireader de duas fases, sem depender de RA: escrita consulta uma maioria, gera uma tag maior e propaga o valor a uma maioria; leitura consulta uma maioria e faz write-back do maior par tag/valor antes de retornar. Quórum é `floor(N/2)+1`, com o voto local contado uma única vez.

Referências: [ABD, 1995](https://doi.org/10.1145/200836.200869), [extensão de Lynch e Shvartsman, 1997](https://doi.org/10.1109/FTCS.1997.614100) e [descrição do ABD multiwriter, seção 2.2](https://ar5iv.labs.arxiv.org/html/2304.09382#S2.SS2). Não implementa consenso, incremento atômico ou transação.

### Uso e API

```sh
./bin/lab -action write -url http://127.0.0.1:18081 -value 42 -wait-ms 3000
./bin/lab -action read -url http://127.0.0.1:18082 -wait-ms 3000
curl --fail http://127.0.0.1:18081/abd/state
```

- `POST /abd/write`: `{"timeout_ms":3000,"value":42}`. Valor inteiro de 64 bits obrigatório, inclusive zero. Disponível somente em `standalone`; em `integrated` retorna 403 com `not_applied`.
- `POST /abd/read`: `{"timeout_ms":3000}`. Não aceita `value`, nem nulo.
- As operações aceitam prazo de 1–3000 ms e corpo de até 1 KiB, sem campos desconhecidos, duplicados ou JSON adicional.
- `POST /abd/messages`: RPC de consulta/atualização; responde com `abd.state`/`abd.ack` após leitura ou atualização durável local. Respostas são validadas contra contexto, remetente, pedido e tag esperada.
- `GET /abd/state`: consulta diagnóstica da réplica local; não é leitura ABD nem tem garantia de refletir sozinha a última escrita global.
- Os estados RA/ABD expõem `logical_messages` (envelopes gerados) e `transport_attempts` (RPCs HTTP de saída), sem contar sondas. Não são pacotes físicos nem bytes transmitidos.

Uma operação coordenada local por vez; concorrência no mesmo nó retorna 429 sem fila ilimitada. Nós distintos coordenam operações concorrentemente e continuam atendendo RPCs de réplicas. Cada tentativa remota tem limite de 500 ms, com repetição em até 100 ms enquanto o prazo operacional permite. Ao obter maioria, as requisições restantes são canceladas; não há espera pela minoria indisponível.

Os transports RA e ABD também limitam DNS/TCP a 500 ms. Isso é distinto do timeout de cada chamada: `net/http.Transport` pode manter dials após cancelar o pedido; herdar o dial padrão de 30 s prolongava a indisponibilidade depois de reconectar a rede. A regressão foi observada no piloto e a retomada foi exercitada após a correção.

O resultado `completed` só é retornado após as duas fases; contém o registro, a identidade da operação e duração local. A escrita retorna seu par proposto, mesmo se um ACK já observar uma escrita concorrente maior.

Erros carregam `status`, `error` e `outcome`. Escrita com `not_applied` não iniciou propagação; `indeterminate` pode ter alterado réplicas apesar da falha. Uma leitura malsucedida retorna `failed`, sem valor. Timeout ou resposta inválida no cliente não prova que uma escrita foi desfeita: não repetir automaticamente. Sem maioria, não há sucesso.

### Persistência e execução

`node -data-dir .state` usa `.state/<run_id>/<node_id>/`. `.state/` é ignorado pelo Git. No Compose cada nó tem volume próprio em `/data`, com permissão para UID 65532; não compartilham armazenamento.

O arquivo `register.json`, limitado a 16 KiB na abertura, contém versão de formato, nó, execução, cluster e par tag/valor. Atualizações usam arquivo temporário no mesmo diretório, sincronização do arquivo, rename e sincronização do diretório antes de publicar o novo estado ou confirmar a atualização. Tag menor não regride a réplica; tag igual/valor igual é idempotente; tag igual/valor diferente é conflito.

Falha de I/O põe a réplica fora de serviço: leituras e atualizações falham, sem servir um valor aparentemente durável. Estado corrompido ou de outro contexto não é reinicializado silenciosamente. A durabilidade depende das garantias de fsync/rename do filesystem; o modelo é de crash de processo, não perda de disco.

O lock de inicialização impede dois processos de abrir o mesmo estado. O marcador `started` impede servir novamente a mesma identidade de execução, mesmo após uma parada limpa. Não há protocolo de reintegração após crash; comece todos os participantes com outro `RUN_ID`. Persistência não é promessa de recuperação distribuída.

### Históricos e verificação independente

```sh
python3 -m unittest discover -s analysis -p 'test_*.py'
python3 experiments/abd_smoke.py
```

O smoke já invoca o verificador. Para verificar outro arquivo, execute `python3 analysis/linearizability.py` passando o caminho do histórico como argumento.

O smoke grava dados reais em `data/abd/<run_id>/`: histórico com timestamps monotônicos de um único controlador e cópia do estado persistido após crash. Exercita escritores/leitores concorrentes, maioria após uma falha, recusa sem maioria e recusa de restart na mesma execução. Remove somente seus contêineres, rede e volumes; preserva os arquivos de evidência no host.

O verificador busca uma ordenação de leituras, escritas e incrementos compatível com os valores e com operações concluídas antes da invocação de outras. Mutação indeterminada pode ser omitida ou linearizada após o fim da espera do cliente: timeout não é conclusão da operação. Uma mutação `not_applied` não pode explicar um valor lido.

Formato mínimo: `{"initial_value":0,"operations":[{"id":"w1","operation":"write","value":42,"start_ns":1,"end_ns":2,"outcome":"completed"}]}`. Leituras concluídas também têm `value`; leituras malsucedidas não têm. Timestamps vêm do controlador, não dos relógios de parede dos nós. Metadados e respostas brutas podem acompanhar os registros.

Limites: até 18 operações modeladas e 200 mil estados de busca; excedê-los retorna inconclusivo, nunca aprova subconjunto. Saída JSON e código 0 indicam histórico linearizável; 1 indica não linearizável; 2 indica entrada inválida ou resultado inconclusivo. Um histórico vazio não demonstra a correção de um cenário real.

## Contador integrado: RA + ABD

O incremento é fixo de +1: adquirir RA → leitura ABD completa → verificar overflow → escrita ABD completa → liberar RA. Os serviços compartilham as mesmas implementações usadas pelos endpoints isolados; não há chamada HTTP de volta ao próprio nó nem um segundo coordenador.

```sh
export RUN_ID=counter-001
export MEMBERSHIP_CONFIG=./configs/integrated.json
export OBSERVER_URL=http://observer:8090
docker compose --profile verify up --build --detach --wait --wait-timeout 60
./bin/lab -action increment -url http://127.0.0.1:18081 -wait-ms 3000
./bin/lab -action read -url http://127.0.0.1:18082 -wait-ms 3000
curl --fail http://127.0.0.1:18089/report
docker compose --profile verify down --remove-orphans
unset MEMBERSHIP_CONFIG OBSERVER_URL
```

Use um `RUN_ID` novo a cada execução. Para processos locais, altere `mode` em uma configuração com endereços locais antes de iniciar todos os nós; não use nomes DNS internos do Compose no host.

### Contrato do incremento

- `POST /increment` recebe exclusivamente `{"timeout_ms":3000}`, com limite de 1 KiB e prazo de 1–3000 ms. Não aceita valor, delta, campos nulos/duplicados/desconhecidos ou JSON adicional.
- A resposta `completed` só aparece após a escrita confirmada e a liberação de RA; inclui `before`, registro posterior, duração monotônica, contexto e pedido RA.
- O prazo é único para aquisição e operações ABD. Depois da entrada, desconexão do cliente não aborta a sequência; o trabalho usa o orçamento original independente da conexão, sem ganhar um prazo novo por fase.
- Antes de iniciar escrita, falha retorna `not_applied`. Depois de propagação possível, ou se houve escrita confirmada seguida de falha ao finalizar a proteção/instrumentação, retorna `indeterminate`. Não há rollback nem retry automático.
- `MaxInt64 + 1` é recusado com 409 e `not_applied`; RA é liberado e o valor não é alterado.
- Em `standalone`, incremento retorna 403. Em `integrated`, escrita direta via `/abd/write` retorna 403, mas leitura ABD continua disponível.

Essa política assume participantes e RPCs de protocolo confiáveis na rede de laboratório, não segurança contra quem forja frames de atualização. Não representa autenticação nem tolerância bizantina.

Sem falhas, o valor final coincide exatamente com o número de incrementos concluídos desde zero. Havendo resultado indeterminado, a mutação pode já estar visível; não inferir o valor final contando apenas respostas de sucesso nem repetir o pedido como se não tivesse ocorrido.

Um crash permanente impede novas aquisições RA mesmo quando ABD ainda tem maioria. Assim, a aplicação integrada não herda a disponibilidade do registro isolado.

### Verificação reproduzível da integração

```sh
python3 experiments/increment_smoke.py
```

O cenário usa três nós e observador externo, exerce o cliente real e dez incrementos concorrentes, exige resultados distintos de 1 a 10 e valor final 10, bloqueia escrita direta e injeta crash. Depois do crash, novos incrementos expiram sem alteração, mas a leitura ABD ainda funciona. Históricos e relatório do observador ficam em `data/integrated/<run_id>/`; contêineres, rede e volumes do cenário são removidos.

O mesmo verificador independente aceita `operation: "increment"`: resultados concluídos exigem `before` e `value`, com exatamente +1 no estado atual e sem overflow. Incrementos indeterminados não carregam valores observados e podem ter ocorrido uma vez ou não; o limite de busca continua explícito, sem certificar subconjuntos.

## Executar três nós em contêineres

Escolha uma identidade diferente para cada execução e preserve a variável até remover o ambiente:

```sh
export RUN_ID=local-001
docker compose up --build --detach --wait --wait-timeout 60
docker compose ps
```

Consultar pela máquina hospedeira:

```sh
./bin/lab -url http://127.0.0.1:18081
./bin/lab -url http://127.0.0.1:18082
./bin/lab -url http://127.0.0.1:18083
```

Consultar pela rede interna, a partir de outro contêiner:

```sh
docker compose exec -T node1 /lab -url http://node2:8080
docker compose exec -T node2 /lab -url http://node3:8080
docker compose exec -T node3 /lab -url http://node1:8080
```

Inspecionar logs, interromper um participante e remover o ambiente:

```sh
docker compose logs --no-color
docker compose kill node3
# Deve retornar erro: o processo foi interrompido.
./bin/lab -url http://127.0.0.1:18083 -timeout 1s
docker compose down --remove-orphans
```

Os contêineres não reiniciam automaticamente, executam como usuário não root e têm filesystem somente leitura, exceto o volume de estado de cada nó. O estado RA é volátil; ABD persiste o registro, mas não reintegra participantes. Use outro `RUN_ID` para uma nova execução. `docker compose down --remove-orphans` preserva volumes; `docker compose down --volumes --remove-orphans` também apaga o estado persistido do projeto.

## Campanhas e análise

O controlador gera Compose JSON com 3, 5 ou 7 nós, imagem Docker imutável, volumes novos por ensaio e limites de 1 CPU/256 MiB/64 PIDs por nó. Não execute outros clusters nas portas 18081–18087 durante a coleta.

```sh
go build -o bin/ ./cmd/...
python3 -m unittest discover -s experiments -p test_campaign.py
python3 -m unittest discover -s analysis -p 'test_*.py'
uv sync --project analysis --locked

# Escolha um diretório novo para uma nova campanha.
python3 experiments/campaign.py pilot --output data/campaigns/minha-campanha
python3 experiments/campaign.py run --output data/campaigns/minha-campanha
uv run --project analysis --locked python analysis/campaign_analysis.py data/campaigns/minha-campanha
```

Python 3.11+ executa o controlador e a análise numérica; `uv` instala a versão travada de Matplotlib para os gráficos. Go continua usando somente a biblioteca padrão.

### Desenho experimental

- Piloto: taxas agregadas 2/5/10/20 pedidos/s em N=3 e N=7, aquecimento 2 s e medição 8 s; escolhe separadamente para RA/ABD a maior taxa com ≥95% de sucessos em ambos os extremos, ≥10 amostras, zero deadline/resposta inválida/fila cheia e p99 do atraso do gerador ≤10 ms. Crash e partição/cura curtos também precisam produzir evidência não vazia antes do congelamento.
- Principal: 60 ensaios, RA/ABD × N=3/5/7 × dez repetições; 10 s de aquecimento e 60 s de medição. RA mantém a seção crítica por 5 ms; ABD usa 50% de escritas. Taxa escolhida permanece fixa entre os tamanhos; prazo de operação 1 s, cliente 2 s, 32 workers e fila de 128.
- Falhas: 40 ensaios, RA/ABD × crash/partição com cura × dez repetições, N=3; aquecimento 10 s e medição 35 s. Injeta em 5 s; reconecta a partição em 20 s. Crash usa SIGKILL e não reinicia o nó.
- Configurações executam sequencialmente em ordem embaralhada registrada, sem sobrepor medições. Sementes independentes por repetição e pareadas entre tamanhos; não tornam o escalonamento do sistema determinístico.
- Duas redes: `client` mantém as APIs acessíveis ao host; `protocol` é interna e possui os aliases exclusivos `peer-nodeN` usados pelos pares. A partição remove apenas a interface de protocolo de node3. Exige HTTP 504 do nó isolado, não somente erro de conexão; a cura restaura o alias sem reiniciar o processo.

O gerador programa chegadas independentemente das respostas, registra atrasos/fila/rejeições e drena todo o aquecimento antes da linha de base. A coleta drena a medição sem transformar respostas tardias em conclusões dentro da janela. Não existe observador no caminho de desempenho; os smokes de correção anteriores são separados.

### Artefatos e retomada

`campaign.json` registra host/pressão de memória/carga, versões reais, revisão Git, digest dos fontes e imagem imutável. `source.tar.gz` preserva os bytes efetivos dos fontes Go/Dockerfile, controlador e binário de carga, inclusive alterações não commitadas. `pilot-report.json` preserva candidatos rejeitados; `frozen.json` fixa taxas, protocolo e a ordem dos cem ensaios antes da coleta principal.

Cada diretório em `trials/` contém manifesto, `operations.jsonl`, configuração, eventos/provas da injeção, logs, inspeção dos contêineres, comandos e SHA-256 de todos os arquivos. O controlador remove apenas os próprios contêineres/redes/volumes, inclusive em erro. A execução `run` reutiliza exclusivamente ensaios completos com hashes e protocolo idênticos; não substitui um ensaio parcial/falho. Nesse caso, preserve a evidência e inicie outra campanha. Não altere os fontes Go, controlador ou binário entre piloto e coleta.

### Estatística e limites

`analysis/REPORT.md` e `analysis/summary.json`, dentro do diretório da campanha, trazem métricas por repetição/operação, p50/p95/p99 condicionais ao sucesso, denominadores, rejeições, deadlines, escritas indeterminadas, censura de fronteira e cobertura dos contadores. O relatório usa médias, desvio-padrão amostral e IC95% Student-t entre dez repetições; não trata cada pedido como repetição independente. Os PNGs são regenerados a partir dos mesmos dados.

A análise final recusa subconjuntos: exige exatamente os cem ensaios completos e compatíveis. `--allow-partial --no-plots` é apenas diagnóstico explícito do piloto/coleta parcial; a saída fica marcada `INCOMPLETE`. Sem snapshot de um nó morto, os contadores totais ficam indisponíveis, nunca zero. Retomada após cura inclui o tempo até a próxima chegada; RA após crash permanente não recebe tempo finito de recuperação.

Campanha executada: [`reference-bounded-20261021`](data/campaigns/reference-bounded-20261021/), com RA a 10 pedidos/s e ABD a 20 pedidos/s. Foram 53.856 ofertas/52.898 conclusões nos ensaios normais, zero deadlines/respostas inválidas/fila local cheia; as 958 recusas normais foram `busy`. Os quarenta ensaios de falhas registraram 20.916 ofertas e 13.149 conclusões. O controlador validou recusa minoritária e retomada sem restart em todas as repetições de partição; a análise revalidou os hashes e os cem ensaios, e `run` foi reexecutado sem recoletar ensaios já completos.

O [relatório gerado](data/campaigns/reference-bounded-20261021/analysis/REPORT.md) relaciona os resultados à complexidade de mensagens, às duas fases ABD e aos limites do desenho. O [JSON completo](data/campaigns/reference-bounded-20261021/analysis/summary.json) inclui métricas por repetição, IC95%, latências de sucessos/deadlines/falhas separadas e contadores parciais após crash. Os gráficos de falha incluem todos os desfechos malsucedidos, inclusive erros de transporte imediatos do nó morto.

Contêineres compartilham máquina/disco/escalonador; taxas oferecidas não medem capacidade máxima e este desenho não isola causalmente o custo de `fsync`. Os traços longos de desempenho não são certificados pelo verificador limitado de linearizabilidade. Compatibilidade com Compose v2, relatório IEEE final e vídeo permanecem pendentes.

## Relatório IEEE

- [PDF para revisão](report/relatorio_projeto2.pdf). Seis páginas no total, incluindo referências, em duas colunas IEEE e com organização visual baseada no T1.
- [Fonte LaTeX](report/relatorio_projeto2.tex). Modelo do sistema, algoritmos, integração, correção, metodologia, duas tabelas compactas e seis figuras vetoriais. Os resultados se referem à campanha congelada, sem nova coleta.

```sh
make -C report
# Remove auxiliares LaTeX, preservando o PDF:
make -C report clean
```

Requer GNU Make 4.3 ou mais recente, `uv`, `latexmk`, `pdflatex`, a classe `IEEEtran` e os pacotes LaTeX usados no fonte. O Makefile executa `report/figures.py` com o ambiente travado de `analysis/` quando o script, o resumo ou as figuras precisam ser atualizados. As seis figuras em `report/figures/` são PDFs vetoriais gerados a partir de `data/campaigns/reference-bounded-20261021/analysis/summary.json`, sem modificar os dados da campanha. Elas mostram espera RA, latência ABD, vazão, comunicação, partição e crash. Auxiliares LaTeX são ignorados pelo Git. Fonte, script, figuras, Makefile e PDF não.

Pendências de entrega estão explícitas na primeira página: confirmar os nomes/RAs reaproveitados do T1 e informar o endereço do GitLab da disciplina e o link do vídeo. O GitHub disponível é identificado como tal, sem fingir que é o GitLab exigido. Não foi inventada divisão individual de trabalho do T2.

## Verificação executada

- Build de `node`, `lab` e `load`, `go test -race ./...` e `go vet ./...`.
- Nó local e três contêineres saudáveis; consultas pelo host e entre contêineres.
- Rejeição de argumentos inválidos, respostas HTTP inválidas e timeout de servidor sem resposta.
- HTTP 405 para POST em `/healthz` e 404 para rota inexistente.
- Após crash de um participante, a sonda retorna erro e os outros processos continuam respondendo.
- Encerramento por SIGTERM com código zero e log `node stopped`.
- Configuração carregada por um processo local e três contêineres, com o mesmo `cluster_id` entre participantes e maioria 2.
- Inicialização rejeitada para nó desconhecido, execução inválida e configuração ausente; `GET /cluster` inspecionado e POST rejeitado.
- Smoke executável dos contratos RA/ABD: mensagem válida aceita e mensagem de outra execução rejeitada.
- Regressão de registro incompleto reproduzida antes da correção e aprovada depois; demais limites e invariantes de configuração/mensagens cobertos pelos testes.
- `experiments/ra_smoke.py`: 31 seções críticas concluídas em três nós, 124 mensagens lógicas, 31 entradas/saídas e zero sobreposições ou eventos inválidos no observador.
- Após matar um participante nesse smoke, os sobreviventes retornam 504 sem novas entradas; os pedidos terminam em `RELEASED`, sem respostas adiadas retidas.
- Testes HTTP cobrem cancelamento antes da entrada, entrega de resposta adiada após falha transitória, concorrência e desconexão do cliente durante `HELD`.
- Testes do observador detectam sobreposição sem conceder permissões e impedem que saída antiga limpe seção ativa mais nova.
- Conclusões de execução com tempos ausentes/nulos foram reproduzidas como inválidas e corrigidas; o cliente não inventa latências zero para respostas incompletas.
- `experiments/abd_smoke.py`: histórico real de 12 tentativas, com 10 operações concluídas e duas recusadas sem maioria, validado pelo verificador independente.
- Escritores/leitores concorrentes e `-value 0` exercitados; escrita/leitura continuaram com um participante morto, mas retornaram 504 após perder a maioria.
- Valor confirmado 777 preservado no snapshot copiado após SIGKILL; restart na mesma execução recusado, sem reinicialização silenciosa.
- Evidência dessa execução em `data/abd/abd-smoke-1792591905064078768/`: `history.json` e `node1-register.json`.
- Os 32 testes do verificador incluem históricos conhecidos inválidos, escritas/incrementos indeterminados, atualizações perdidas, overflow, precedência e limites inconclusivos.
- Testes Go com detector de corridas cobrem write-back, escrita parcial indeterminada, quórum com minoria lenta, persistência, corrupção e envenenamento por I/O; snapshot excessivo reproduzido e rejeitado após correção.
- Smoke RA reexecutado com armazenamento e rotas integrados: 31 seções concluídas, 124 mensagens lógicas e zero violações; seus volumes também foram removidos.
- `experiments/increment_smoke.py`: dez incrementos com resultados distintos de 1 a 10 e valor final 10; observador com dez entradas/saídas, zero sobreposições e zero eventos inválidos.
- Histórico integrado de 18 tentativas verificado como linearizável; escrita direta recebeu 403, e dois incrementos após crash receberam 504/`not_applied`, enquanto a leitura ABD continuou retornando 10.
- Evidência da integração em `data/integrated/increment-smoke-1792593718127585912/`: `history.json` e `observer.json`.
- Testes de rede exercitam overflow sem mutação, liberação de RA em falhas, cancelamento antes da entrada, conclusão após desconexão e preservação de resultado indeterminado após escrita.
- Smokes isolados RA e ABD reexecutados após a integração; build, vet e suíte Go completa com detector de corridas aprovados.
- Campanha real: vinte ensaios de piloto válidos e cem ensaios principais/falhas completos; configuração e imagem congeladas antes da coleta, com hashes revalidados e retomada idempotente exercitada.
- Gerador Go exercitado em 3/5/7 contêineres e sob crash/partição; o piloto revelou a regressão de dials longos, reproduzida antes e corrigida com timeout DNS/TCP de 500 ms.
- Passaram quarenta testes Python de históricos/análise e nove do controlador; seis gráficos foram regenerados e inspecionados visualmente. O `REPORT.md` experimental não substitui o relatório IEEE de entrega.
- Relatório IEEE compilado via `make -C report` e inspecionado por páginas. São seis páginas no total, com seis figuras vetoriais, referências resolvidas, fontes incorporadas e colunas sem extravasamento. A extração do PDF confirmou ausência de travessões, dois-pontos e ponto e vírgula no texto apresentado, incluindo legendas e referências.

Ambiente exercitado: Go 1.26.4, Docker 29.8.2 e Compose 5.6.0 em Linux/amd64. Compatibilidade com Compose v2 ainda não foi exercitada.
