# Busca distribuída em arquivos de log

O programa procura um termo em vários arquivos de log usando C e MPI. Cada arquivo é uma tarefa. O processo coordenador distribui os arquivos entre os workers e apresenta os resultados reunidos.

**Integrantes**

- Guilherme Steiner Amabile
- Gabriel Mengato Chiarelli de Souza Scalon
- Margot Machado Baisch

O [relatório técnico](relatorio_tecnico_t2.docx) apresenta a implementação e os resultados da avaliação local.

## 1. Preparar o ambiente e compilar

Execute os comandos em um terminal Ubuntu, inclusive no WSL. Entre na pasta `t2-distribuida`, onde estão `script.c` e `gerador_logs_t2.c`. No WSL, o disco `C:` do Windows fica em `/mnt/c`.

```bash
cd "/mnt/c/Users/SEU_USUARIO/Desktop/distribuida/t2-distribuida"
```

Troque `SEU_USUARIO` pelo nome da sua pasta de usuário no Windows. Em outro ambiente Linux, use o caminho local da pasta do projeto. Confira se já tem as ferramentas com `command -v gcc mpicc mpirun`. Se alguma estiver faltando no Ubuntu, instale as dependências.

```bash
sudo apt update
sudo apt install build-essential mpich libmpich-dev
```

Compile o gerador e a aplicação de busca.

```bash
gcc -O2 -Wall -Wextra gerador_logs_t2.c -o gerador_logs_t2
mpicc -std=c11 -O2 -Wall -Wextra script.c -o busca_logs
```

## 2. Gerar os arquivos de entrada

O primeiro comando cria três arquivos de 20 linhas com oito palavras de mensagem por linha. O segundo cria 100 arquivos de 10.000 linhas em outra pasta.

```bash
./gerador_logs_t2 3 20 8 42 logs_teste
./gerador_logs_t2 100 10000 8 42 logs_maiores
```

Os argumentos, em ordem, são quantidade de arquivos, linhas por arquivo, palavras por linha, semente e diretório. A semente permite reproduzir os dados com a mesma versão do gerador e os mesmos parâmetros. Ao omitir os dois últimos argumentos, o gerador usa a semente `12345` e o diretório `logs`. Para escolher a pasta, informe também a semente.

O gerador usa nomes como `log_0001.log` e sobrescreve os arquivos com o mesmo nome. Use uma pasta nova para cada conjunto de dados. Para conferir a entrada pequena, rode `head -n 5 logs_teste/log_0001.log`.

## 3. Executar a busca

```bash
mpirun -np 3 ./busca_logs --dir logs_teste --search ERROR --mode static
mpirun -np 3 ./busca_logs --dir logs_teste --search ERROR --mode dynamic
```

`-np 3` inicia três processos MPI. O número de cada processo é seu *rank*. O rank 0 coordena, e os ranks 1 e 2 fazem a busca como workers. O programa precisa de pelo menos dois processos no total.

`--dir` indica a pasta que contém os logs. A aplicação processa os arquivos regulares diretamente nessa pasta. `--search` recebe um trecho literal com distinção entre maiúsculas e minúsculas. Por exemplo, `ERROR` e `error` são termos diferentes. Use aspas para procurar um trecho com espaços.

```bash
mpirun -np 3 ./busca_logs --dir logs_teste --search 'processo usuario' --mode dynamic
```

`--mode static` entrega os arquivos em blocos consecutivos de tamanho semelhante antes da busca. `--mode dynamic` distribui um arquivo inicial por worker, enquanto houver arquivos, e entrega o próximo ao worker que terminou.

## 4. Ler, guardar e comparar a saída

Ao rodar os comandos acima, a saída aparece no terminal. Cada correspondência tem o formato `arquivo:linha: conteúdo`. A numeração começa em 1 e cada linha conta uma vez, mesmo quando contém várias cópias do termo. As linhas aparecem em ordem de arquivo e número de linha. O resumo mostra o número de arquivos, as contagens por arquivo e total, e o tempo em segundos. A medição vai da listagem dos arquivos ao fim da coleta dos resultados.

No conjunto `logs_teste`, a busca por `ERROR` soma 11 linhas. Os arquivos `log_0001.log`, `log_0002.log` e `log_0003.log` têm 2, 6 e 3 ocorrências, respectivamente.

Use `>` para gravar a saída em um arquivo ou `tee` para ver no terminal e gravar ao mesmo tempo. Os dois sobrescrevem o arquivo de destino. Guarde os resultados fora da pasta de entrada, pois a busca lê todos os arquivos regulares da pasta escolhida.

```bash
mkdir -p resultados
mpirun -np 3 ./busca_logs --dir logs_teste --search ERROR --mode static > resultados/estatico.txt
mpirun -np 3 ./busca_logs --dir logs_teste --search ERROR --mode dynamic | tee resultados/dinamico.txt
```

Abra um resultado com `less resultados/estatico.txt` e pressione `q` para voltar ao terminal.

Para verificar se as duas estratégias encontraram as mesmas linhas e contagens, compare as saídas no Bash e retire a linha com a medição de tempo.

```bash
diff -u <(sed '/^Tempo de execucao:/d' resultados/estatico.txt) \
        <(sed '/^Tempo de execucao:/d' resultados/dinamico.txt)
```

Se o `diff` terminar sem imprimir diferenças, as saídas coincidem. Para comparar os tempos, use o mesmo diretório, termo e número de processos em cada par de execuções. Varie o número de processos entre pares.

### Criar uma entrada à mão

Em uma pasta nova, crie dois logs pequenos para saber exatamente o que a busca deve encontrar.

```bash
mkdir exemplo
printf 'INFO inicio\nERROR disco cheio\n' > exemplo/a.log
printf 'ERROR duas vezes ERROR\n' > exemplo/b.log
mpirun -np 3 ./busca_logs --dir exemplo --search ERROR --mode dynamic
```

A busca lista as linhas a seguir. O resumo registra dois arquivos, uma ocorrência por arquivo e duas no total, além do tempo.

```text
a.log:2: ERROR disco cheio
b.log:1: ERROR duas vezes ERROR
```

### Reproduzir os conjuntos da avaliação

O conjunto uniforme tem 24 arquivos com o mesmo número de linhas. O conjunto misto usa esse número como referência. Vinte arquivos têm metade do tamanho de um arquivo uniforme e quatro têm 3,5 vezes o tamanho. Com isso, os dois conjuntos têm o mesmo total de linhas em cada volume. Rode o bloco no Bash, na pasta do projeto.

```bash
for linhas in 4000 32000; do
    ./gerador_logs_t2 24 "$linhas" 8 42 "uniformes_$linhas"
    ./gerador_logs_t2 20 "$((linhas / 2))" 8 42 "mistos_$linhas"
    ./gerador_logs_t2 4 "$((7 * linhas / 2))" 8 43 "grandes_$linhas"
    for i in 1 2 3 4; do
        origem=$(printf 'grandes_%s/log_%04d.log' "$linhas" "$i")
        destino=$(printf 'mistos_%s/log_%04d.log' "$linhas" "$((20 + i))")
        cp "$origem" "$destino"
    done
done
```

Para repetir as configurações usadas na avaliação, busque `ERROR` nos diretórios `uniformes_4000`, `mistos_4000`, `uniformes_32000` e `mistos_32000`. Use `-np 2`, `-np 3` e `-np 5` para testar com um, dois e quatro workers respectivamente. Em cada combinação, rode cada modo uma vez para aquecer e mais sete vezes para medir. Alterne o modo inicial a cada repetição e use a mediana dos sete tempos de cada modo.

## Como o código funciona

1. **`main` e `listar_arquivos`.** O MPI inicia os processos e cada rank valida os argumentos. O coordenador lista os arquivos em ordem de nome. A posição na lista identifica cada tarefa.
2. **`executar_estatico`.** O coordenador divide a lista em blocos consecutivos. Os primeiros workers recebem um arquivo a mais quando a divisão tem resto. `MPI_Barrier` espera que todos recebam seus blocos antes de iniciar a busca.
3. **`executar_dinamico`.** O coordenador mantém a posição do próximo arquivo pendente. `receber_resultado` indica qual worker terminou. Esse rank recebe outro arquivo ou um sinal de fim.
4. **`processar_tarefa` e `apresentar_resultados`.** O worker lê seu arquivo com `getline`, testa cada linha com `strstr` e envia as correspondências ao coordenador. O coordenador armazena cada resultado na posição do arquivo e percorre a lista ordenada para imprimir as ocorrências e o resumo.
