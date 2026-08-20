# Aviso: Primeiro Trabalho de MC714

Prezados estudantes,

Informamos que já está disponível o enunciado do primeiro trabalho de nossa disciplina.

O objetivo deste projeto é a implementação, modelagem teórica e avaliação por simulação de um balanceador de carga para alocação de requisições. O trabalho deve ser realizado em duplas.

## I. Documento Anexo

* **MC714_2s2026.pdf:** Contém a especificação detalhada do trabalho, parâmetros de simulação e os critérios de avaliação. 

## II. Principais Orientações

* **Implementação:** Vocês têm liberdade para escolher a linguagem de programação (Python, C++, Java, Go, etc.), sendo permitido o uso de bibliotecas de simulação de eventos discretos (como SimPy). Devem ser implementadas as políticas Aleatória, Round Robin e Fila Mais Curta.
* **Simulação e Carga:** O sistema contará com 3 servidores homogêneos submetidos a um tráfego com distribuição Bounded Pareto sob diferentes intensidades de rajadas (30, 60, 90 e 120 requisições). Lembrem-se de que os experimentos devem ser repetidos 10 vezes para a coleta das médias de vazão e tempo de resposta.
* **Modelagem Analítica:** Além do código, é fundamental deduzir o modelo matemático teórico do balanceador e confrontar rigorosamente esses resultados analíticos com os dados obtidos nas simulações.
* **Relatório:** Documentação sucinta em formato IEEE de até 4 páginas, cobrindo a arquitetura, formulação teórica, análise comparativa dos resultados e divisão de tarefas da dupla. 

## III. Entrega e Prazos

* **Data limite:** 22 de setembro de 2026, via Google Classroom.
* **Formato:** Apenas um membro da dupla precisa realizar o envio, submetendo o relatório em PDF e o código-fonte compactado conforme o padrão de nomes especificado no PDF. 

Recomendamos que iniciem o desenvolvimento com antecedência e façam uma leitura cuidadosa do arquivo anexo. Utilizem o Classroom para esclarecer eventuais dúvidas conceituais ou de implementação.

Bom trabalho a todos!

Atenciosamente,

**Prof. Carlos Alberto Astudillo Trujillo**  
**Diogo Maciel da Cunha (PED)**  

*MC714 - Sistemas Distribuídos*  
*Instituto de Computação - UNICAMP*
