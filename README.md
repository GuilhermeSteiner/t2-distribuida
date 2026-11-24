# Busca distribuída em arquivos de log

**Integrantes**

- Guilherme Steiner Amabile
- Gabriel Mengato Chiarelli de Souza Scalon
- Margot Machado Baisch

## Instalação do MPI

No Debian ou Ubuntu, instale o compilador C e o MPICH, que fornece `mpicc` e `mpirun`. Se esses comandos já estiverem disponíveis, siga para a compilação.

```bash
sudo apt update
sudo apt install build-essential mpich libmpich-dev
```

Confirme a instalação. Os comandos abaixo devem mostrar um caminho para cada programa.

```bash
command -v mpicc
command -v mpirun
```

## Compilação

Abra o terminal na pasta que contém `script.c` e compile.

```bash
mpicc -std=c11 -O2 -Wall -Wextra script.c -o busca_logs
```

## Entrada

Use uma pasta com os arquivos de log em texto.

```bash
mkdir logs
```

### Gerador de logs do enunciado

Para gerar logs sintéticos, baixe o [gerador_logs_t2.c](https://drive.google.com/file/d/1iWlRKR_4mHfbNhGBOrlP9FlXwlG6yCyW/view?usp=sharing) disponibilizado pelo professor na seção **Entrada** do enunciado do Trabalho 2 (p. 2). Na pasta onde salvou o arquivo, rode o exemplo do enunciado (p. 3).

```bash
gcc -O2 gerador_logs_t2.c -o gerador_logs_t2
./gerador_logs_t2 100 10000 8 42 logs
```

O comando gera na pasta `logs` 100 arquivos com 10.000 linhas cada, oito palavras de mensagem por linha e semente `42`. Para buscar nesses arquivos, substitua `--dir logs` por `--dir logs` ou informe o caminho até a pasta de logs gerada.

## Execução

Execute a busca primeiro com distribuição estática e depois com distribuição dinâmica.

```bash
mpirun -np 3 ./busca_logs --dir logs --search ERROR --mode static
mpirun -np 3 ./busca_logs --dir logs --search ERROR --mode dynamic
```

`-np 3` inicia um coordenador e dois workers. Para usar outros logs, informe o diretório em `--dir`. Troque `ERROR` pelo trecho desejado em `--search`. A busca diferencia maiúsculas de minúsculas. Use aspas se o trecho contiver espaços, como `--search "disco cheio"`.

Também é possível escrever as opções com `=`, por exemplo `--dir=logs --search="disco cheio" --mode=dynamic`. Use essa forma se um valor começar com `--`.

O modo estático divide a lista ordenada em blocos fixos e libera a busca após distribuir todos os blocos. No dinâmico, cada worker recebe outra tarefa à medida que devolve o resultado anterior.

A saída lista as correspondências no formato `arquivo:linha: conteúdo` e mostra as contagens por arquivo e o total, além do tempo de execução. Em `logs`, os dois modos encontram duas linhas, uma de cada arquivo.

O tempo exibido cobre a distribuição, a leitura dos logs, a busca e a coleta dos resultados em arquivos temporários. A listagem inicial e a impressão ficam fora dessa medição.

Para salvar as saídas em arquivos, use `>` e mantenha os arquivos de saída fora da pasta dos logs.

```bash
mpirun -np 3 ./busca_logs --dir logs --search ERROR --mode static > saida-estatica.txt
mpirun -np 3 ./busca_logs --dir logs --search ERROR --mode dynamic > saida-dinamica.txt
```
