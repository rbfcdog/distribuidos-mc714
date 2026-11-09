# Campanha experimental RA/ABD

**INCOMPLETE — PILOT/SMOKE ONLY** — 20/100 repetições.

## Método e integridade

Somente ensaios completos com SHA-256 de todos os arquivos de evidência e identidade/fonte/imagem compatíveis. A campanha final exige exatamente os 100 ensaios do protocolo congelado; piloto é excluído do resultado final e só pode ser analisado explicitamente como incompleto. O arquivo-fonte e o binário arquivados são revalidados. Aquecimento é drenado e excluído. Falhas de integridade, relógio (>100 ms), contadores decrescentes ou intervalos inconclusivos abortam a análise.

A topologia separa rede client (bridge, portas locais preservadas) e protocol (interna); aliases peer-nodeN existem apenas na rede de protocolo e são os únicos endereços canônicos na associação. A partição desconecta somente protocol, mantendo node3 acessível ao gerador: recusa minoritária exige resposta HTTP504, não apenas falha de transporte. Limite: os pares são confiáveis e seguem os endereços configurados; não modela adversário usando caminhos alternativos.

Percentis interpolam a posição (n−1)p na amostra ordenada (tipo 7). Latências e espera RA são **condicionais ao sucesso**, inclusive respostas após a janela; n está no JSON. A vazão conta apenas respostas bem-sucedidas dentro da janela de oferta. Censura de fronteira (fim após a janela) é distinta de erro, e todas as respostas são preservadas após drenagem. Rejeições contam somente busy e fila local cheia; deadlines, transporte e demais falhas são desfechos separados. Escritas indeterminadas não são consideradas não aplicadas.

Cada ponto é a média de métricas calculadas por repetição independente; desvio-padrão amostral e IC95% Student-t (9 gl: 2,262157) usam repetições, não pedidos agrupados. O IC t pressupõe métricas aproximadamente normais entre repetições; dez repetições não garantem essa aproximação para caudas pesadas. IC ausente com n<2; métricas sem amostra = null/—, nunca zero artificial. A semente pareada entre N não cria repetições extras. Lag do gerador usa todas as ofertas; start−scheduled inclui também fila de clientes.

Host efetivamente registrado: `{"affinity": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], "cpu_model": "13th Gen Intel(R) Core(TM) i7-13620H", "dedicated_or_idle_host": false, "loadavg": [2.1044921875, 1.90673828125, 1.96337890625], "logical_cpus": 16, "memory": {"MemAvailable_bytes": 5099679744, "MemFree_bytes": 521486336, "MemTotal_bytes": 16452562944, "SwapFree_bytes": 98304, "SwapTotal_bytes": 4294963200}, "os": "Linux-7.0.0-38-generic-x86_64-with-glibc2.39", "unix_ns": 1792598159513947113}`.

## Ensaios normais

Médias entre repetições; IC95% de p99 exibido na tabela, demais ICs e desvios-padrão amostrais no JSON. Espera e latências em ms.

| Algoritmo | N | Operação | n rep. | Ofertas/s | Vazão/s | Sucesso | p50 cliente | p95 cliente | p99 cliente [IC95%] | p95 espera RA |
|---|---:|---|---:|---:|---:|---:|---:|---:|---|---:|
| ABD | 3 | all | 1 | 1.375 | 1.375 | 1.000 | 2.711 | 10.358 | 11.995 [—] | — |
| ABD | 3 | read | 1 | 0.750 | 0.750 | 1.000 | 2.327 | 2.646 | 2.698 [—] | — |
| ABD | 3 | write | 1 | 0.625 | 0.625 | 1.000 | 7.757 | 11.586 | 12.241 [—] | — |
| ABD | 3 | all | 1 | 5.000 | 5.000 | 1.000 | 3.331 | 43.189 | 47.527 [—] | — |
| ABD | 3 | read | 1 | 2.875 | 2.875 | 1.000 | 2.281 | 3.550 | 3.598 [—] | — |
| ABD | 3 | write | 1 | 2.125 | 2.125 | 1.000 | 7.641 | 47.009 | 47.818 [—] | — |
| ABD | 3 | all | 1 | 11.250 | 11.125 | 0.989 | 5.220 | 36.275 | 47.150 [—] | — |
| ABD | 3 | read | 1 | 5.250 | 5.125 | 0.976 | 2.186 | 4.123 | 4.570 [—] | — |
| ABD | 3 | write | 1 | 6.000 | 6.000 | 1.000 | 7.934 | 44.641 | 47.509 [—] | — |
| ABD | 3 | all | 1 | 18.875 | 18.500 | 0.980 | 4.086 | 7.710 | 8.711 [—] | — |
| ABD | 3 | read | 1 | 9.375 | 9.250 | 0.987 | 2.256 | 3.452 | 4.282 [—] | — |
| ABD | 3 | write | 1 | 9.500 | 9.250 | 0.974 | 6.338 | 8.293 | 9.603 [—] | — |
| ABD | 7 | all | 1 | 1.375 | 1.375 | 1.000 | 4.172 | 10.320 | 10.765 [—] | — |
| ABD | 7 | read | 1 | 0.750 | 0.750 | 1.000 | 2.512 | 3.892 | 4.116 [—] | — |
| ABD | 7 | write | 1 | 0.625 | 0.625 | 1.000 | 9.392 | 10.654 | 10.832 [—] | — |
| ABD | 7 | all | 1 | 5.000 | 5.000 | 1.000 | 4.631 | 10.891 | 11.770 [—] | — |
| ABD | 7 | read | 1 | 2.875 | 2.875 | 1.000 | 2.924 | 5.066 | 5.140 [—] | — |
| ABD | 7 | write | 1 | 2.125 | 2.125 | 1.000 | 9.336 | 11.305 | 12.030 [—] | — |
| ABD | 7 | all | 1 | 11.250 | 11.250 | 1.000 | 6.111 | 10.492 | 16.175 [—] | — |
| ABD | 7 | read | 1 | 5.250 | 5.250 | 1.000 | 3.244 | 5.546 | 5.941 [—] | — |
| ABD | 7 | write | 1 | 6.000 | 6.000 | 1.000 | 8.284 | 11.255 | 31.840 [—] | — |
| ABD | 7 | all | 1 | 18.875 | 18.875 | 1.000 | 5.541 | 10.135 | 11.074 [—] | — |
| ABD | 7 | read | 1 | 9.375 | 9.375 | 1.000 | 2.895 | 5.034 | 6.468 [—] | — |
| ABD | 7 | write | 1 | 9.500 | 9.500 | 1.000 | 7.578 | 10.509 | 11.811 [—] | — |
| RA | 3 | execute | 1 | 1.375 | 1.375 | 1.000 | 7.120 | 8.031 | 8.077 [—] | 1.521 |
| RA | 3 | execute | 1 | 5.000 | 4.875 | 0.975 | 6.974 | 7.705 | 7.930 [—] | 1.399 |
| RA | 3 | execute | 1 | 11.250 | 11.125 | 0.989 | 7.059 | 8.488 | 10.381 [—] | 1.722 |
| RA | 3 | execute | 1 | 18.875 | 17.875 | 0.947 | 7.087 | 8.801 | 12.984 [—] | 3.020 |
| RA | 7 | execute | 1 | 1.375 | 1.375 | 1.000 | 7.672 | 8.695 | 8.928 [—] | 2.758 |
| RA | 7 | execute | 1 | 5.000 | 5.000 | 1.000 | 7.237 | 8.439 | 10.764 [—] | 2.108 |
| RA | 7 | execute | 1 | 11.250 | 11.250 | 1.000 | 7.278 | 8.137 | 10.324 [—] | 1.984 |
| RA | 7 | execute | 1 | 18.875 | 18.375 | 0.974 | 7.514 | 10.773 | 13.282 [—] | 4.830 |

### Populações, falhas e geração de carga

Contagens médias por repetição (não somas de pedidos independentes). Lag p99 em ms inclui todas as ofertas.

| Algoritmo | N | Cenário | Ofertadas | Enviadas | Concluídas | Rejeitadas | Deadlines | Transporte | Escritas incertas | Censuradas | p99 lag gerador |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ABD | 3 | crash | 263.000 | 263.000 | 199.000 | 5.000 | 0.000 | 59.000 | 31.000 | 0.000 | 1.108 |
| ABD | 3 | normal | 11.000 | 11.000 | 11.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.028 |
| ABD | 3 | normal | 40.000 | 40.000 | 40.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.178 |
| ABD | 3 | normal | 90.000 | 90.000 | 89.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.099 |
| ABD | 3 | normal | 151.000 | 151.000 | 148.000 | 3.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.094 |
| ABD | 3 | partition | 263.000 | 263.000 | 224.000 | 36.000 | 3.000 | 0.000 | 0.000 | 0.000 | 1.103 |
| ABD | 7 | normal | 11.000 | 11.000 | 11.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.074 |
| ABD | 7 | normal | 40.000 | 40.000 | 40.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.021 |
| ABD | 7 | normal | 90.000 | 90.000 | 90.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.084 |
| ABD | 7 | normal | 151.000 | 151.000 | 151.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.090 |
| RA | 3 | crash | 134.000 | 134.000 | 27.000 | 58.000 | 15.000 | 34.000 | 0.000 | 1.000 | 1.127 |
| RA | 3 | normal | 11.000 | 11.000 | 11.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.301 |
| RA | 3 | normal | 40.000 | 40.000 | 39.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.030 |
| RA | 3 | normal | 90.000 | 90.000 | 89.000 | 1.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.224 |
| RA | 3 | normal | 151.000 | 151.000 | 143.000 | 8.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.121 |
| RA | 3 | partition | 134.000 | 134.000 | 80.000 | 45.000 | 9.000 | 0.000 | 0.000 | 0.000 | 1.098 |
| RA | 7 | normal | 11.000 | 11.000 | 11.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.171 |
| RA | 7 | normal | 40.000 | 40.000 | 40.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.077 |
| RA | 7 | normal | 90.000 | 90.000 | 90.000 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.098 |
| RA | 7 | normal | 151.000 | 151.000 | 147.000 | 4.000 | 0.000 | 0.000 | 0.000 | 0.000 | 1.093 |

## Contadores de comunicação

| Algoritmo | N | Cenário | Frames lógicos/conclusão | Tentativas HTTP/conclusão | n cobertura completa |
|---|---:|---|---:|---:|---:|
| ABD | 3 | crash | — | — | 0 |
| ABD | 3 | normal | 7.545 | 4.000 | 1 |
| ABD | 3 | normal | 7.350 | 4.000 | 1 |
| ABD | 3 | normal | 7.360 | 4.000 | 1 |
| ABD | 3 | normal | 7.291 | 4.000 | 1 |
| ABD | 3 | partition | 7.040 | 4.062 | 1 |
| ABD | 7 | normal | 21.909 | 12.000 | 1 |
| ABD | 7 | normal | 21.575 | 12.000 | 1 |
| ABD | 7 | normal | 21.711 | 11.978 | 1 |
| ABD | 7 | normal | 21.907 | 11.993 | 1 |
| RA | 3 | crash | — | — | 0 |
| RA | 3 | normal | 4.000 | 4.000 | 1 |
| RA | 3 | normal | 4.000 | 4.000 | 1 |
| RA | 3 | normal | 4.000 | 4.000 | 1 |
| RA | 3 | normal | 4.000 | 4.000 | 1 |
| RA | 3 | partition | 4.300 | 4.463 | 1 |
| RA | 7 | normal | 12.000 | 12.000 | 1 |
| RA | 7 | normal | 12.000 | 12.000 | 1 |
| RA | 7 | normal | 12.000 | 12.000 | 1 |
| RA | 7 | normal | 12.000 | 12.000 | 1 |

Frames são envelopes gerados, não pacotes de rede; tentativas contam RPCs HTTP de saída. Deltas incluem trabalho malsucedido e fanout cancelado/repetido. Divisor = conclusões de toda a coorte medida, inclusive drenagem. Sem snapshot final de qualquer nó, total do cluster e razão são indisponíveis; deltas parciais por nó permanecem no JSON. RA ideal sem falhas gera 2(N−1) frames por aquisição; ABD utiliza duas fases de quórum, mas envio/repetição/cancelamento impede identificar frames com apenas respostas necessárias.

## Falhas e recuperação observada

Guardas de 1 s excluem transições: pré termina antes do início da injeção−1 s; falha começa após aplicação+1 s e termina antes da cura−1 s; curado começa após fim da cura+1 s. Coortes usam instante de invocação (oferta em rejeições locais); vazão requer também conclusão dentro do intervalo. Pendências além da fronteira são censuradas. Maioria=node1/2; minoria=node3. Separadamente, recuperação observada é a primeira conclusão de invocação nova com início a partir do fim da cura, menos fim da cura, **sem** acrescentar a guarda de 1 s usada na prova de progresso. Inclui espera pela próxima chegada e conclusão sob carga, **não** tempo intrínseco de detecção/reintegração.

| Algoritmo | Cenário | Fase | Grupo | Ofertas/s | Vazão/s | Deadlines | Censuradas | p99 cliente ms |
|---|---|---|---|---:|---:|---:|---:|---:|
| ABD | crash | pre | majority | 14.912 | 14.912 | 0.000 | 0.000 | 9.429 |
| ABD | crash | pre | minority | 6.959 | 6.959 | 0.000 | 0.000 | 9.272 |
| ABD | crash | fault | majority | 14.825 | 14.175 | 0.000 | 0.000 | 10.935 |
| ABD | crash | fault | minority | 6.502 | 0.000 | 0.000 | 0.000 | — |
| ABD | partition | pre | majority | 14.911 | 14.911 | 0.000 | 0.000 | 7.879 |
| ABD | partition | pre | minority | 6.959 | 6.462 | 0.000 | 0.000 | 8.030 |
| ABD | partition | fault | majority | 12.714 | 11.697 | 0.000 | 0.000 | 8.400 |
| ABD | partition | fault | minority | 7.629 | 0.000 | 2.000 | 1.000 | — |
| ABD | partition | healed | majority | 16.398 | 15.373 | 0.000 | 0.000 | 47.328 |
| ABD | partition | healed | minority | 6.406 | 6.149 | 0.000 | 0.000 | 8.717 |
| RA | crash | pre | majority | 4.475 | 3.978 | 0.000 | 0.000 | 7.905 |
| RA | crash | pre | minority | 4.475 | 4.475 | 0.000 | 0.000 | 7.895 |
| RA | crash | fault | majority | 8.071 | 0.000 | 12.000 | 1.000 | — |
| RA | crash | fault | minority | 3.645 | 0.000 | 0.000 | 0.000 | — |
| RA | partition | pre | majority | 4.475 | 3.978 | 0.000 | 0.000 | 7.790 |
| RA | partition | pre | minority | 4.475 | 4.475 | 0.000 | 0.000 | 7.827 |
| RA | partition | fault | majority | 9.399 | 0.000 | 3.000 | 1.000 | — |
| RA | partition | fault | minority | 2.611 | 0.000 | 2.000 | 1.000 | — |
| RA | partition | healed | majority | 7.638 | 7.638 | 0.000 | 0.000 | 7.745 |
| RA | partition | healed | minority | 3.819 | 3.819 | 0.000 | 0.000 | 7.669 |

### Conclusões pós-cura e prova de disponibilidade

- ABD / crash permanente: não houve cura; recuperação finita não estimada (RA permanece censurado).
- ABD / partition: conclusão pós-cura observada média 0.058 s (n=1).
- RA / crash permanente: não houve cura; recuperação finita não estimada (RA permanece censurado).
- RA / partition: conclusão pós-cura observada média 0.120 s (n=1).

A prova de recusa usa invocações iniciadas no intervalo guardado: qualquer confirmação antes do início real da cura é relevante, inclusive na margem final de transição. No crash permanente, uma conclusão tardia durante a drenagem também violaria a recusa, pois não existe cura. Respostas após início da cura não provam sucesso durante a partição. Os resumos de fase mantêm limites guardados e censura separados dessa verificação. Retomada exige novas conclusões em maioria e minoria após a guarda de cura.

## Interpretação limitada aos dados

- ABD / crash: vazão guardada maioria 14.175/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- ABD, N=3: vazão média observada 1.375/s, fração de sucesso 1.000, sob oferta configurada 2/s. Não é medição de capacidade máxima.
- ABD, N=3: vazão média observada 5.000/s, fração de sucesso 1.000, sob oferta configurada 5/s. Não é medição de capacidade máxima.
- ABD, N=3: vazão média observada 11.125/s, fração de sucesso 0.989, sob oferta configurada 10/s. Não é medição de capacidade máxima.
- ABD, N=3: vazão média observada 18.500/s, fração de sucesso 0.980, sob oferta configurada 20/s. Não é medição de capacidade máxima.
- ABD / partition: vazão guardada maioria 11.697/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- ABD, N=7: vazão média observada 1.375/s, fração de sucesso 1.000, sob oferta configurada 2/s. Não é medição de capacidade máxima.
- ABD, N=7: vazão média observada 5.000/s, fração de sucesso 1.000, sob oferta configurada 5/s. Não é medição de capacidade máxima.
- ABD, N=7: vazão média observada 11.250/s, fração de sucesso 1.000, sob oferta configurada 10/s. Não é medição de capacidade máxima.
- ABD, N=7: vazão média observada 18.875/s, fração de sucesso 1.000, sob oferta configurada 20/s. Não é medição de capacidade máxima.
- RA / crash: vazão guardada maioria 0.000/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- RA, N=3: vazão média observada 1.375/s, fração de sucesso 1.000, sob oferta configurada 2/s. Não é medição de capacidade máxima.
- RA, N=3: vazão média observada 4.875/s, fração de sucesso 0.975, sob oferta configurada 5/s. Não é medição de capacidade máxima.
- RA, N=3: vazão média observada 11.125/s, fração de sucesso 0.989, sob oferta configurada 10/s. Não é medição de capacidade máxima.
- RA, N=3: vazão média observada 17.875/s, fração de sucesso 0.947, sob oferta configurada 20/s. Não é medição de capacidade máxima.
- RA / partition: vazão guardada maioria 0.000/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- RA, N=7: vazão média observada 1.375/s, fração de sucesso 1.000, sob oferta configurada 2/s. Não é medição de capacidade máxima.
- RA, N=7: vazão média observada 5.000/s, fração de sucesso 1.000, sob oferta configurada 5/s. Não é medição de capacidade máxima.
- RA, N=7: vazão média observada 11.250/s, fração de sucesso 1.000, sob oferta configurada 10/s. Não é medição de capacidade máxima.
- RA, N=7: vazão média observada 18.375/s, fração de sucesso 0.974, sob oferta configurada 20/s. Não é medição de capacidade máxima.

## Limitações

- Mesmo host físico: CPU, disco, escalonador e rede Docker compartilham recursos/domínio de falha. Repetições e ICs não capturam todos esses vieses sistemáticos; isolamento em contêiner não equivale a máquinas independentes.
- Não há observador na carga de desempenho; estes traços longos não estabelecem linearizabilidade. Ensaios limitados de correção são separados.
- Versões efetivamente registradas (sem presumir Docker Compose v2): `{"compose": "Docker Compose version v5.6.0", "docker": "Client: Docker Engine - Community\n Version:           29.8.2\n API version:       1.56\n Go version:        go1.26.8\n Git commit:        7fc2dff\n Built:             Wed Sep 30 19:32:28 2026\n OS/Arch:           linux/amd64\n Context:           default\n\nServer: Docker Engine - Community\n Engine:\n  Version:          29.8.2\n  API version:      1.56 (minimum version 1.40)\n  Go version:       go1.26.8\n  Git commit:       8af9fe3\n  Built:            Wed Sep 30 19:32:28 2026\n  OS/Arch:          linux/amd64\n  Experimental:     false\n containerd:\n  Version:          v2.3.6\n  GitCommit:        ee2735368117d2eb259779949d5e75cdafec9761\n runc:\n  Version:          1.5.1\n  GitCommit:        v1.5.1-0-g8f2685a4\n docker-init:\n  Version:          0.19.0\n  GitCommit:        de40ad0", "go": "go version go1.26.4 linux/amd64", "python": "3.12.3 (main, Aug 31 2026, 10:18:26) [GCC 13.3.0]"}`.
- Custos de fsync não podem ser causalmente isolados neste desenho; não atribuir diferenças exclusivamente à persistência. Gate busy do cliente/servidor e carga oferecida afetam os resultados.
- p99 condicional ao sucesso não descreve pedidos que falharam; consultar denominadores, deadlines, rejeições, escritas indeterminadas e censura. Contadores incluem tentativas e frames não entregues; não são bytes/pacotes físicos.
- Linhas temporais são binadas por repetição e depois agregadas; bandas são IC95% entre repetições, nunca entre pedidos. Sem amostra, lacunas não devem ser interpretadas como latência zero.

## Artefatos

`summary.json` contém valores por repetição/operação, contagens, n de percentis, cobertura dos contadores, estatísticas e linhas temporais. PNGs são gerados salvo `--no-plots`.

**INCOMPLETO: somente piloto/smoke; tabelas não representam os 100 ensaios congelados e não sustentam conclusões finais.**
