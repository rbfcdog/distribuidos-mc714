# Roteiro do vídeo do Trabalho 2

Duração aproximada de cinco minutos, com os dois integrantes falando. As indicações de integrante 1 e integrante 2 servem só para dividir a apresentação. Cada um deve dizer o próprio nome e RA na abertura.

A demonstração usa o contador integrado para mostrar os dois protocolos funcionando juntos. Depois de matar um nó, o incremento para, mas uma leitura ABD ainda funciona. Essa é a consequência esperada do crash, não uma recuperação do processo morto.

## Preparação antes de gravar

Abra um terminal em `t2/`, com fonte legível, e deixe `report/relatorio_projeto2.pdf` aberto nas figuras 4 e 5. As portas 18081, 18082, 18083 e 18089 precisam estar livres. Encerre antes qualquer outro cluster que esteja usando essas portas.

Execute no mesmo terminal que será usado durante o vídeo.

```sh
go build -o bin/ ./cmd/...
export COMPOSE_PROJECT_NAME="mc714-video-$(date +%s)"
export RUN_ID="$COMPOSE_PROJECT_NAME"
export MEMBERSHIP_CONFIG=./configs/integrated.json
export OBSERVER_URL=http://observer:8090
docker compose --profile verify up --build --detach --wait --wait-timeout 60
```

Use um projeto novo a cada ensaio ou gravação. Não execute incrementos extras nesse ambiente antes da demonstração, pois o roteiro parte de um contador zerado. Não reinicie o nó morto dentro da mesma execução.

Se um pedido falhar ou retornar resultado indeterminado, não o repita automaticamente nem apresente o valor previsto como se tivesse sido observado. Pare e confira a saída antes de continuar a gravação.

## 0m00 a 0m25 | Apresentação

**Tela**

Mostre o título do relatório e os dois integrantes. Cada um diz seu nome e RA.

**Integrante 1**

> Neste trabalho a gente implementou Ricart-Agrawala, para exclusão mútua, e ABD, para manter um registro replicado. Vamos mostrar os dois em um contador distribuído e depois interromper um dos nós para ver o que continua funcionando.

## 0m25 a 1m10 | Como o sistema funciona

**Tela**

Mostre os três processos do cluster.

```sh
docker compose ps node1 node2 node3
```

**Integrante 2**

> Cada nó está em um contêiner e conversa com os outros por HTTP. Para incrementar o contador, primeiro ele pede permissão a todos os pares usando Ricart-Agrawala. Só depois lê o valor por ABD, soma um, escreve por ABD e libera a seção crítica.
>
> Essa proteção é importante porque uma leitura seguida de escrita não é um incremento atômico. Sem ela, dois clientes poderiam ler o mesmo valor e uma atualização seria perdida. No ABD, uma operação precisa de maioria. Aqui, com três nós, essa maioria é de dois.

## 1m10 a 1m55 | Três incrementos concorrentes

**Tela**

Dispare um incremento por nó. Os três comandos rodam em segundo plano, sem esperar que o anterior termine.

```sh
./bin/lab -action increment -url http://127.0.0.1:18081 -wait-ms 3000 -timeout 5s &
p1=$!
./bin/lab -action increment -url http://127.0.0.1:18082 -wait-ms 3000 -timeout 5s &
p2=$!
./bin/lab -action increment -url http://127.0.0.1:18083 -wait-ms 3000 -timeout 5s &
p3=$!
wait "$p1" && wait "$p2" && wait "$p3"
```

**Integrante 1**

> Agora os três nós receberam pedidos de incremento ao mesmo tempo. As respostas precisam ter status completed e retornar os valores um, dois e três. A ordem dos nós pode mudar entre execuções. O que importa é que nenhum valor seja repetido e que a leitura final seja três.

```sh
./bin/lab -action read -url http://127.0.0.1:18082 -wait-ms 3000 -timeout 5s
```

Aponte `status`, `before` e `register.value` nas respostas dos incrementos e `register.value` na leitura. Não leia os identificadores longos de execução e configuração.

## 1m55 a 2m20 | Conferir a exclusão mútua

**Tela**

Consulte o observador externo.

```sh
curl --fail --silent --show-error http://127.0.0.1:18089/report
```

**Integrante 2**

> O observador registrou três entradas e três saídas. Não ficou nenhuma seção ativa, e os contadores de sobreposição e de eventos inválidos estão zerados. Ele só observa os eventos, não dá permissão para os nós entrarem. Isso confere o que aconteceu nesta execução.

Aponte `entries=3`, `exits=3`, `active=0`, `violations=0` e `invalid_events=0`. Um relatório vazio não serve como evidência de exclusão.

## 2m20 a 3m15 | Injetar um crash e observar a consequência

**Tela**

Mate o nó 3 sem reiniciá-lo.

```sh
docker compose kill -s SIGKILL node3
docker compose ps -a node3
```

**Integrante 1**

> Agora vamos derrubar o nó três. Como Ricart-Agrawala depende da resposta de todos, um novo incremento não pode entrar na seção crítica. O prazo vai terminar sem aplicar essa operação.

```sh
./bin/lab -action increment -url http://127.0.0.1:18081 -wait-ms 1000 -timeout 3s
```

Esse comando deve terminar com código diferente de zero. Mostre `HTTP 504` e `outcome=not_applied` na mensagem de erro. O restante do texto do erro pode variar. Não esconda o erro com `|| true`.

**Integrante 2**

> Mas ainda temos dois nós conectados, então ABD consegue formar maioria. Uma leitura direta continua funcionando, mesmo com o incremento bloqueado.

```sh
./bin/lab -action read -url http://127.0.0.1:18082 -wait-ms 3000 -timeout 5s
curl --fail --silent --show-error http://127.0.0.1:18089/report
```

Mostre que a leitura ainda retorna 3 e que o observador continua com três entradas e três saídas, sem nova seção crítica. A falha foi de um processo, não de uma máquina física.

## 3m15 a 4m25 | Resultados dos experimentos

**Tela**

Abra a figura 4 do relatório, de comunicação por conclusão. Depois mostre a figura 5, de partição e reconexão. Esses gráficos são da campanha arquivada, não dos três pedidos executados agora.

**Integrante 1**

> Além dessa demonstração, fizemos cem ensaios. Nos testes normais usamos três, cinco e sete nós, com dez repetições por configuração. Em Ricart-Agrawala, a contagem foi de quatro, oito e doze mensagens por aquisição. É o custo de dois vezes o número de pares, com um pedido e uma permissão por par.
>
> A espera para adquirir a seção também aumentou com o grupo. No ABD, as escritas tiveram latências maiores que as leituras e mais variação nas caudas. As barras dos gráficos mostram os intervalos de confiança entre as repetições.

**Integrante 2**

> Aqui está o teste de partição. Enquanto o nó três fica isolado, RA para de concluir dos dois lados. ABD continua na maioria, mas não no nó isolado. Depois da reconexão, os dois voltam a concluir operações nos três nós. Essa retomada foi testada na campanha. Na demonstração de hoje usamos um crash permanente, então não reiniciamos o processo.

Aponte o intervalo entre a desconexão próxima de 5 s e a reconexão próxima de 20 s. Na figura 5, conclusões estão na linha de cima e falhas na linha de baixo.

## 4m25 a 5m00 | Fechamento

**Tela**

Volte ao terminal com o último valor lido ou mostre a conclusão do relatório.

**Integrante 1**

> O contador mostra bem a diferença entre os protocolos. O registro pode continuar disponível por maioria, mas o incremento protegido precisa esperar todos os participantes de RA.

**Integrante 2**

> Todos os contêineres rodaram na mesma máquina, então esses resultados não representam uma rede entre computadores independentes. O código, os testes, os dados e os comandos de reprodução estão no repositório. Obrigado.

## Depois da gravação

Remova somente o projeto criado para o vídeo, incluindo seus volumes de teste.

```sh
docker compose --profile verify down --volumes --remove-orphans
unset RUN_ID MEMBERSHIP_CONFIG OBSERVER_URL COMPOSE_PROJECT_NAME
```

Confira o áudio dos dois integrantes e a leitura dos resultados no terminal. Depois de publicar o vídeo, coloque o link na primeira página do relatório, junto do endereço do GitLab da disciplina.
