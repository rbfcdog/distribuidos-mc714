# Campanha experimental RA/ABD

**COMPLETE** — 100/100 repetições.

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
| ABD | 3 | all | 10 | 20.007 | 19.507 | 0.975 | 3.928 | 8.383 | 10.178 [9.375; 10.982] | — |
| ABD | 3 | read | 10 | 9.920 | 9.663 | 0.974 | 2.205 | 3.489 | 4.066 [3.945; 4.188] | — |
| ABD | 3 | write | 10 | 10.087 | 9.843 | 0.976 | 6.341 | 8.917 | 12.107 [8.785; 15.428] | — |
| ABD | 5 | all | 10 | 20.007 | 19.650 | 0.982 | 4.562 | 10.243 | 21.738 [5.645; 37.832] | — |
| ABD | 5 | read | 10 | 9.920 | 9.752 | 0.983 | 2.563 | 4.099 | 5.349 [4.516; 6.182] | — |
| ABD | 5 | write | 10 | 10.087 | 9.898 | 0.982 | 7.227 | 14.307 | 24.898 [6.354; 43.443] | — |
| ABD | 7 | all | 10 | 20.007 | 19.678 | 0.984 | 5.115 | 13.580 | 27.548 [9.490; 45.606] | — |
| ABD | 7 | read | 10 | 9.920 | 9.737 | 0.982 | 2.927 | 4.935 | 11.209 [3.081; 19.336] | — |
| ABD | 7 | write | 10 | 10.087 | 9.942 | 0.986 | 7.800 | 21.132 | 31.585 [10.799; 52.372] | — |
| RA | 3 | execute | 10 | 9.913 | 9.712 | 0.980 | 7.051 | 8.112 | 11.409 [11.151; 11.667] | 1.627 |
| RA | 5 | execute | 10 | 9.913 | 9.773 | 0.986 | 7.232 | 8.700 | 11.889 [11.407; 12.370] | 2.438 |
| RA | 7 | execute | 10 | 9.913 | 9.828 | 0.991 | 7.423 | 9.126 | 12.385 [12.061; 12.709] | 2.878 |

### Populações, falhas e geração de carga

Contagens médias por repetição (não somas de pedidos independentes). Lag p99 em ms inclui todas as ofertas.

| Algoritmo | N | Cenário | Ofertadas | Enviadas | Concluídas | Rejeitadas | Deadlines | Transporte | Escritas incertas | Censuradas | p99 lag gerador |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ABD | 3 | crash | 699.400 | 699.400 | 485.900 | 13.600 | 0.000 | 199.900 | 100.300 | 0.100 | 1.107 |
| ABD | 3 | normal | 1200.400 | 1200.400 | 1170.600 | 29.800 | 0.000 | 0.000 | 0.000 | 0.200 | 1.110 |
| ABD | 3 | partition | 699.400 | 699.400 | 585.200 | 101.200 | 13.000 | 0.000 | 0.000 | 0.100 | 1.107 |
| ABD | 5 | normal | 1200.400 | 1200.400 | 1179.200 | 21.200 | 0.000 | 0.000 | 0.000 | 0.200 | 1.104 |
| ABD | 7 | normal | 1200.400 | 1200.400 | 1180.900 | 19.500 | 0.000 | 0.000 | 0.000 | 0.200 | 1.104 |
| RA | 3 | crash | 346.400 | 346.400 | 50.300 | 153.800 | 47.500 | 94.800 | 0.000 | 1.800 | 1.113 |
| RA | 3 | normal | 594.800 | 594.800 | 582.800 | 12.000 | 0.000 | 0.000 | 0.000 | 0.100 | 1.107 |
| RA | 3 | partition | 346.400 | 346.400 | 193.500 | 118.700 | 34.200 | 0.000 | 0.000 | 0.200 | 1.109 |
| RA | 5 | normal | 594.800 | 594.800 | 586.500 | 8.300 | 0.000 | 0.000 | 0.000 | 0.100 | 1.104 |
| RA | 7 | normal | 594.800 | 594.800 | 589.800 | 5.000 | 0.000 | 0.000 | 0.000 | 0.100 | 1.106 |

## Contadores de comunicação

| Algoritmo | N | Cenário | Frames lógicos/conclusão | Tentativas HTTP/conclusão | n cobertura completa |
|---|---:|---|---:|---:|---:|
| ABD | 3 | crash | — | — | 0 |
| ABD | 3 | normal | 7.340 | 4.000 | 10 |
| ABD | 3 | partition | 6.934 | 4.092 | 10 |
| ABD | 5 | normal | 14.629 | 7.999 | 10 |
| ABD | 7 | normal | 21.850 | 11.992 | 10 |
| RA | 3 | crash | — | — | 0 |
| RA | 3 | normal | 4.000 | 4.000 | 10 |
| RA | 3 | partition | 4.472 | 4.717 | 10 |
| RA | 5 | normal | 8.000 | 8.000 | 10 |
| RA | 7 | normal | 12.000 | 12.000 | 10 |

Frames são envelopes gerados, não pacotes de rede; tentativas contam RPCs HTTP de saída. Deltas incluem trabalho malsucedido e fanout cancelado/repetido. Divisor = conclusões de toda a coorte medida, inclusive drenagem. Sem snapshot final de qualquer nó, total do cluster e razão são indisponíveis; deltas parciais por nó permanecem no JSON. RA ideal sem falhas gera 2(N−1) frames por aquisição; ABD utiliza duas fases de quórum, mas envio/repetição/cancelamento impede identificar frames com apenas respostas necessárias.

## Falhas e recuperação observada

Guardas de 1 s excluem transições: pré termina antes do início da injeção−1 s; falha começa após aplicação+1 s e termina antes da cura−1 s; curado começa após fim da cura+1 s. Coortes usam instante de invocação (oferta em rejeições locais); vazão requer também conclusão dentro do intervalo. Pendências além da fronteira são censuradas. Maioria=node1/2; minoria=node3. Separadamente, recuperação observada é a primeira conclusão de invocação nova com início a partir do fim da cura, menos fim da cura, **sem** acrescentar a guarda de 1 s usada na prova de progresso. Inclui espera pela próxima chegada e conclusão sob carga, **não** tempo intrínseco de detecção/reintegração.

| Algoritmo | Cenário | Fase | Grupo | Ofertas/s | Vazão/s | Deadlines | Censuradas | p99 cliente ms |
|---|---|---|---|---:|---:|---:|---:|---:|
| ABD | crash | pre | majority | 13.266 | 12.793 | 0.000 | 0.000 | 15.831 |
| ABD | crash | pre | minority | 6.222 | 5.924 | 0.000 | 0.100 | 11.499 |
| ABD | crash | fault | majority | 13.348 | 12.996 | 0.000 | 0.100 | 20.996 |
| ABD | crash | fault | minority | 6.686 | 0.000 | 0.000 | 0.000 | — |
| ABD | partition | pre | majority | 13.264 | 12.766 | 0.000 | 0.000 | 13.498 |
| ABD | partition | pre | minority | 6.221 | 5.948 | 0.000 | 0.100 | 12.905 |
| ABD | partition | fault | majority | 13.476 | 13.152 | 0.000 | 0.200 | 12.207 |
| ABD | partition | fault | minority | 6.410 | 0.000 | 11.600 | 1.000 | — |
| ABD | partition | healed | majority | 13.312 | 12.945 | 0.000 | 0.100 | 17.039 |
| ABD | partition | healed | minority | 6.843 | 6.641 | 0.000 | 0.000 | 20.571 |
| RA | crash | pre | majority | 7.068 | 6.893 | 0.000 | 0.200 | 9.228 |
| RA | crash | pre | minority | 3.235 | 3.161 | 0.000 | 0.000 | 9.561 |
| RA | crash | fault | majority | 6.698 | 0.000 | 45.000 | 1.700 | — |
| RA | crash | fault | minority | 3.166 | 0.000 | 0.000 | 0.000 | — |
| RA | partition | pre | majority | 7.067 | 6.893 | 0.000 | 0.200 | 9.444 |
| RA | partition | pre | minority | 3.235 | 3.160 | 0.000 | 0.000 | 9.683 |
| RA | partition | fault | majority | 6.737 | 0.000 | 20.400 | 1.800 | — |
| RA | partition | fault | minority | 3.113 | 0.000 | 10.100 | 0.900 | — |
| RA | partition | healed | majority | 6.654 | 6.524 | 0.000 | 0.100 | 9.954 |
| RA | partition | healed | minority | 3.229 | 3.179 | 0.000 | 0.000 | 9.006 |

### Conclusões pós-cura e prova de disponibilidade

- ABD / crash permanente: a maioria permaneceu disponível; não houve reintegração do nó morto nem tempo de cura a estimar.
- ABD / partição: conclusão pós-cura média 0.059 s; IC95% [0.025; 0.093] s (n=10).
- RA / crash permanente: novas aquisições permanecem bloqueadas; recuperação finita censurada.
- RA / partição: conclusão pós-cura média 0.309 s; IC95% [0.205; 0.413] s (n=10).

A prova de recusa usa invocações iniciadas no intervalo guardado: qualquer confirmação antes do início real da cura é relevante, inclusive na margem final de transição. No crash permanente, uma conclusão tardia durante a drenagem também violaria a recusa, pois não existe cura. Respostas após início da cura não provam sucesso durante a partição. Os resumos de fase mantêm limites guardados e censura separados dessa verificação. Retomada exige novas conclusões em maioria e minoria após a guarda de cura.

## Interpretação limitada aos dados

- ABD / crash: vazão guardada maioria 12.996/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- ABD, N=3: vazão média observada 19.507/s, fração de sucesso 0.975, sob oferta configurada 20/s. Não é medição de capacidade máxima.
- ABD / partition: vazão guardada maioria 13.152/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- ABD, N=5: vazão média observada 19.650/s, fração de sucesso 0.982, sob oferta configurada 20/s. Não é medição de capacidade máxima.
- ABD, N=7: vazão média observada 19.678/s, fração de sucesso 0.984, sob oferta configurada 20/s. Não é medição de capacidade máxima.
- RA / crash: vazão guardada maioria 0.000/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- RA, N=3: vazão média observada 9.712/s, fração de sucesso 0.980, sob oferta configurada 10/s. Não é medição de capacidade máxima.
- RA / partition: vazão guardada maioria 0.000/s e minoria 0.000/s; os valores, não causalidade adicional, fundamentam a comparação de disponibilidade.
- RA, N=5: vazão média observada 9.773/s, fração de sucesso 0.986, sob oferta configurada 10/s. Não é medição de capacidade máxima.
- RA, N=7: vazão média observada 9.828/s, fração de sucesso 0.991, sob oferta configurada 10/s. Não é medição de capacidade máxima.

### Escala e complexidade de mensagens

- RA gerou 4.000 → 8.000 → 12.000 frames por conclusão em N=3/5/7, comparáveis a 2(N−1)=4/8/12 sem falhas. A espera p95 foi 1.627 → 2.438 → 2.878 ms; a seção crítica tem duração configurada de 5 ms. Aumentar participantes acrescenta permissões necessárias e fanout O(N).
- ABD apresentou 4.000 → 7.999 → 11.992 tentativas HTTP e 7.340 → 14.629 → 21.850 frames por conclusão. Duas fases enviadas a N−1 pares explicam a escala de cerca de 2(N−1) RPCs; respostas geradas e fanout cancelado distinguem RPCs de frames e do mínimo necessário para confirmar o quórum.
- Medianas cliente ABD: leitura 2.205 → 2.563 → 2.927 ms e escrita 6.341 → 7.227 → 7.800 ms. Não há controle experimental que atribua a diferença exclusivamente a fsync.
- Diferenças nas médias das caudas ABD precisam ser lidas junto aos ICs p99 e à variabilidade entre repetições. A ordem das médias isolada não estabelece efeito estatisticamente significativo entre tamanhos; estes ICs não são um teste pareado de diferenças.
- A carga é agregada e fixa por algoritmo: aumentar N reduz pedidos oferecidos por coordenador e pode reduzir colisões no gate local. A melhora de fração concluída não demonstra aumento de capacidade máxima. RA e ABD usam taxas e trabalhos distintos; não interpretar a vazão maior de ABD como comparação direta de velocidade.

### Consequências das falhas

- A exigência de todos os pares de RA explica o bloqueio observado nos dois lados da partição e após crash. ABD confirma na maioria intersectante e recusa a minoria; a cura devolve progresso aos três processos sem restart.
- Os gráficos de falhas mostram todos os desfechos malsucedidos (busy, deadline, transporte e demais erros), não só timeouts. Um nó morto pode recusar conexão imediatamente; zero deadlines não significa zero falhas. Contagens e latências de deadlines/erros enviados ficam separadas no JSON.
- Tempos pós-cura incluem próxima chegada e execução sob taxas diferentes por algoritmo. Não são comparação isolada da velocidade intrínseca de recuperação. Escritas sem resposta permanecem indeterminadas, sem afirmar aplicação ou rollback.

## Limitações

- Mesmo host físico: CPU, disco, escalonador e rede Docker compartilham recursos/domínio de falha. Repetições e ICs não capturam todos esses vieses sistemáticos; isolamento em contêiner não equivale a máquinas independentes.
- Não há observador na carga de desempenho; estes traços longos não estabelecem linearizabilidade. Ensaios limitados de correção são separados.
- Versões efetivamente registradas (sem presumir Docker Compose v2): `{"compose": "Docker Compose version v5.6.0", "docker": "Client: Docker Engine - Community\n Version:           29.8.2\n API version:       1.56\n Go version:        go1.26.8\n Git commit:        7fc2dff\n Built:             Wed Sep 30 19:32:28 2026\n OS/Arch:           linux/amd64\n Context:           default\n\nServer: Docker Engine - Community\n Engine:\n  Version:          29.8.2\n  API version:      1.56 (minimum version 1.40)\n  Go version:       go1.26.8\n  Git commit:       8af9fe3\n  Built:            Wed Sep 30 19:32:28 2026\n  OS/Arch:          linux/amd64\n  Experimental:     false\n containerd:\n  Version:          v2.3.6\n  GitCommit:        ee2735368117d2eb259779949d5e75cdafec9761\n runc:\n  Version:          1.5.1\n  GitCommit:        v1.5.1-0-g8f2685a4\n docker-init:\n  Version:          0.19.0\n  GitCommit:        de40ad0", "go": "go version go1.26.4 linux/amd64", "python": "3.12.3 (main, Aug 31 2026, 10:18:26) [GCC 13.3.0]"}`.
- Custos de fsync não podem ser causalmente isolados neste desenho; não atribuir diferenças exclusivamente à persistência. Gate busy do cliente/servidor e carga oferecida afetam os resultados.
- p99 condicional ao sucesso não descreve pedidos que falharam; consultar denominadores, deadlines, rejeições, escritas indeterminadas e censura. Contadores incluem tentativas e frames não entregues; não são bytes/pacotes físicos.
- Linhas temporais são binadas por repetição e depois agregadas; bandas são IC95% entre repetições, nunca entre pedidos. Sem amostra, lacunas não devem ser interpretadas como latência zero.

## Artefatos

`summary.json` contém valores por repetição/operação, contagens, n de percentis, cobertura dos contadores, estatísticas e linhas temporais. PNGs são gerados salvo `--no-plots`.

Proveniência da análise: `{"dependency_lock_sha256": "971525465aedbcda419c4b9ce7da717e4c06ce7e564634b247bbae0acf7f6316", "python": "3.14.0 (main, Nov 19 2025, 22:48:15) [Clang 21.1.4 ]", "script_sha256": "8e824f1d62ddf5d699a98708499daf5195dd282f17c9c83ff251957b185d1811"}`.
