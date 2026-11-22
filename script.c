/*
 * Busca distribuída em arquivos de log com C e MPI.
 *
 * Integrantes do Trabalho 2 de INE 5645
 * Guilherme Steiner Amabile
 * Gabriel Mengato Chiarelli de Souza Scalon
 * Margot Machado Baisch
 *
 * MPI permite que vários processos executem este mesmo programa e troquem
 * mensagens. O rank é o número de cada processo. Com -np 3, o rank 0
 * coordena e os ranks 1 e 2 trabalham. O mínimo é -np 2.
 *
 * Compile no terminal Linux ou Ubuntu no WSL.
 *   gcc -O2 gerador_logs_t2.c -o gerador_logs_t2
 *   mpicc -std=c11 -O2 -Wall -Wextra script.c -o busca_logs
 *
 * Gere uma entrada com três arquivos de 20 linhas, oito palavras de
 * mensagem por linha e semente 42.
 *   ./gerador_logs_t2 3 20 8 42 logs_teste
 *
 * --dir é a pasta de entrada, --search é o trecho literal a procurar e
 * --mode é a estratégia. O termo diferencia maiúsculas e minúsculas.
 * Coloque o termo entre aspas se contiver espaços.
 *   mpirun -np 3 ./busca_logs --dir logs_teste --search ERROR --mode static
 *   mpirun -np 3 ./busca_logs --dir logs_teste --search ERROR --mode dynamic > dinamico.txt
 *   mpirun -np 3 ./busca_logs --dir logs_teste --search 'processo usuario' --mode dynamic
 *
 * A saída vai para o terminal por padrão. O sinal > é do terminal e grava
 * a saída no arquivo indicado. Salve esse arquivo fora da pasta de logs.
 *
 * Também é possível criar logs pequenos à mão. Se a pasta exemplo tiver
 * só a.log, com as linhas "INFO inicio" e "ERROR disco cheio", e b.log,
 * com a linha "ERROR duas vezes ERROR", rode este comando.
 *   mpirun -np 3 ./busca_logs --dir exemplo --search ERROR --mode dynamic
 *
 * A saída lista "a.log:2: ERROR disco cheio" e "b.log:1: ERROR duas vezes ERROR".
 * São dois arquivos e duas ocorrências, uma por arquivo. Repetir ERROR
 * na mesma linha não altera a contagem. O resumo inclui o tempo em segundos.
 */

#define _POSIX_C_SOURCE 200809L

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Uma tag identifica o tipo de mensagem que o processo espera receber. */
enum { TAG_QUANTIDADE, TAG_TAREFA, TAG_RESULTADO, TAG_TEXTO };

/* O coordenador guarda os arquivos e seus resultados nesta lista. */
typedef struct {
    char *nome;
    unsigned long long ocorrencias;
    char *texto;
    size_t tamanho;
} Arquivo;

/* Uma tarefa identifica um arquivo inteiro pelo índice na lista e pelo nome. */
typedef struct {
    int indice;
    char *nome;
} Tarefa;

static void falhar(const char *contexto)
{
    /* Um erro de leitura ou memória encerra toda a execução MPI. */
    perror(contexto);
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    exit(EXIT_FAILURE);
}

static void *realocar(void *ponteiro, size_t tamanho)
{
    void *memoria = realloc(ponteiro, tamanho);
    if (memoria == NULL) {
        falhar("Memoria");
    }
    return memoria;
}

static int comparar_arquivos(const void *a, const void *b)
{
    return strcmp(((const Arquivo *)a)->nome, ((const Arquivo *)b)->nome);
}

static Arquivo *listar_arquivos(const char *diretorio, int *total)
{
    /* Apenas o rank 0 percorre a pasta. Cada arquivo regular vira uma tarefa. */
    DIR *dir = opendir(diretorio);
    if (dir == NULL) {
        falhar(diretorio);
    }

    Arquivo *arquivos = NULL;
    size_t capacidade = 0;
    *total = 0;

    for (;;) {
        errno = 0;
        struct dirent *entrada = readdir(dir);
        if (entrada == NULL) {
            if (errno != 0) {
                falhar(diretorio);
            }
            break;
        }

        struct stat dados;
        if (fstatat(dirfd(dir), entrada->d_name, &dados, AT_SYMLINK_NOFOLLOW) != 0) {
            falhar(entrada->d_name);
        }
        if (!S_ISREG(dados.st_mode)) {
            continue;
        }
        if (*total == INT_MAX) {
            errno = EOVERFLOW;
            falhar("Quantidade de arquivos");
        }
        if ((size_t)*total == capacidade) {
            capacidade = capacidade == 0 ? 16 : capacidade * 2;
            arquivos = realocar(arquivos, capacidade * sizeof(*arquivos));
        }

        char *nome = strdup(entrada->d_name);
        if (nome == NULL) {
            falhar("Nome do arquivo");
        }
        arquivos[*total] = (Arquivo){ .nome = nome };
        ++*total;
    }

    if (closedir(dir) != 0) {
        falhar(diretorio);
    }
    if (*total > 1) {
        /* A ordenação inicial também define a ordem da saída final. */
        qsort(arquivos, (size_t)*total, sizeof(*arquivos), comparar_arquivos);
    }
    return arquivos;
}

static void enviar_tarefa(int worker, int indice, const char *nome)
{
    /* O nome só segue quando há tarefa. O índice -1 sinaliza encerramento. */
    MPI_Send(&indice, 1, MPI_INT, worker, TAG_TAREFA, MPI_COMM_WORLD);
    if (indice >= 0) {
        MPI_Send(nome, (int)strlen(nome) + 1, MPI_CHAR, worker, TAG_TAREFA,
                 MPI_COMM_WORLD);
    }
}

static Tarefa receber_tarefa(void)
{
    Tarefa tarefa = {0};
    MPI_Recv(&tarefa.indice, 1, MPI_INT, 0, TAG_TAREFA, MPI_COMM_WORLD,
             MPI_STATUS_IGNORE);
    if (tarefa.indice >= 0) {
        /* O worker consulta o tamanho da próxima mensagem antes de alocar o nome. */
        MPI_Status status;
        int tamanho;
        MPI_Probe(0, TAG_TAREFA, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &tamanho);
        tarefa.nome = realocar(NULL, (size_t)tamanho);
        MPI_Recv(tarefa.nome, tamanho, MPI_CHAR, 0, TAG_TAREFA, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
    }
    return tarefa;
}

static void processar_tarefa(Tarefa tarefa, const char *diretorio, const char *termo)
{
    /* Cada worker abre o arquivo na pasta informada e o lê do início ao fim. */
    size_t tamanho_caminho = strlen(diretorio) + strlen(tarefa.nome) + 2;
    char *caminho = realocar(NULL, tamanho_caminho);
    snprintf(caminho, tamanho_caminho, "%s/%s", diretorio, tarefa.nome);
    FILE *log = fopen(caminho, "r");
    if (log == NULL) {
        falhar(caminho);
    }

    char *texto = NULL;
    size_t tamanho_texto = 0;
    /* O texto das correspondências cresce em memória até o fim do arquivo. */
    FILE *resultado = open_memstream(&texto, &tamanho_texto);
    if (resultado == NULL) {
        falhar("Buffer de resultados");
    }

    char *linha = NULL;
    size_t capacidade = 0;
    ssize_t tamanho_linha;
    unsigned long long numero = 0;
    unsigned long long ocorrencias = 0;
    while ((tamanho_linha = getline(&linha, &capacidade, log)) >= 0) {
        /* getline lê uma linha inteira. O número de linha começa em 1. */
        ++numero;
        if (tamanho_linha > 0 && linha[tamanho_linha - 1] == '\n') {
            linha[--tamanho_linha] = '\0';
            if (tamanho_linha > 0 && linha[tamanho_linha - 1] == '\r') {
                linha[tamanho_linha - 1] = '\0';
            }
        }
        if (strstr(linha, termo) != NULL) {
            /* Basta encontrar o trecho uma vez para incluir a linha. */
            if (fprintf(resultado, "%s:%llu: %s\n", tarefa.nome, numero, linha) < 0) {
                falhar("Buffer de resultados");
            }
            ++ocorrencias;
        }
    }

    if (!feof(log) || ferror(log)) {
        falhar(caminho);
    }
    if (fclose(log) != 0) {
        falhar(caminho);
    }
    if (fclose(resultado) != 0) {
        falhar("Buffer de resultados");
    }
    free(linha);
    free(caminho);
    free(tarefa.nome);

    unsigned long long dados[] = { (unsigned long long)tarefa.indice,
                                   ocorrencias, tamanho_texto };
    /* Primeiro vão o índice, a contagem e o tamanho em bytes. Depois vai o texto. */
    MPI_Send(dados, 3, MPI_UNSIGNED_LONG_LONG, 0, TAG_RESULTADO, MPI_COMM_WORLD);

    /* O tamanho de cada mensagem MPI deve caber em um int. */
    for (size_t inicio = 0; inicio < tamanho_texto;) {
        size_t restante = tamanho_texto - inicio;
        int bloco = restante > INT_MAX ? INT_MAX : (int)restante;
        MPI_Send(texto + inicio, bloco, MPI_CHAR, 0, TAG_TEXTO, MPI_COMM_WORLD);
        inicio += (size_t)bloco;
    }
    free(texto);
}

static int receber_resultado(Arquivo *arquivos)
{
    /* MPI_ANY_SOURCE permite receber de qualquer worker disponível. */
    unsigned long long dados[3];
    MPI_Status status;
    MPI_Recv(dados, 3, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, TAG_RESULTADO,
             MPI_COMM_WORLD, &status);

    int worker = status.MPI_SOURCE;
    /* O índice escolhe onde guardar o resultado, mesmo se chegar fora de ordem. */
    Arquivo *arquivo = &arquivos[dados[0]];
    arquivo->ocorrencias = dados[1];
    arquivo->tamanho = (size_t)dados[2];
    if (arquivo->tamanho > 0) {
        arquivo->texto = realocar(NULL, arquivo->tamanho);
    }
    for (size_t inicio = 0; inicio < arquivo->tamanho;) {
        size_t restante = arquivo->tamanho - inicio;
        int bloco = restante > INT_MAX ? INT_MAX : (int)restante;
        MPI_Recv(arquivo->texto + inicio, bloco, MPI_CHAR, worker, TAG_TEXTO,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        inicio += (size_t)bloco;
    }
    return worker;
}

static void executar_estatico(Arquivo *arquivos, int total, int rank, int processos,
                              const char *diretorio, const char *termo)
{
    Tarefa *tarefas = NULL;
    int quantidade = 0;
    if (rank == 0) {
        int workers = processos - 1;
        int indice = 0;
        /* Com três arquivos e dois workers, o rank 1 recebe os dois primeiros e
         * o rank 2 recebe o terceiro. Cada um termina o seu bloco fixo. */
        for (int worker = 1; worker < processos; ++worker) {
            quantidade = total / workers + (worker <= total % workers);
            MPI_Send(&quantidade, 1, MPI_INT, worker, TAG_QUANTIDADE, MPI_COMM_WORLD);
            for (int i = 0; i < quantidade; ++i, ++indice) {
                enviar_tarefa(worker, indice, arquivos[indice].nome);
            }
        }
    } else {
        MPI_Recv(&quantidade, 1, MPI_INT, 0, TAG_QUANTIDADE, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        if (quantidade > 0) {
            tarefas = realocar(NULL, (size_t)quantidade * sizeof(*tarefas));
        }
        for (int i = 0; i < quantidade; ++i) {
            tarefas[i] = receber_tarefa();
        }
    }

    /* Todos os blocos chegam aos workers antes que qualquer busca comece. */
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        for (int i = 0; i < total; ++i) {
            receber_resultado(arquivos);
        }
    } else {
        for (int i = 0; i < quantidade; ++i) {
            processar_tarefa(tarefas[i], diretorio, termo);
        }
        free(tarefas);
    }
}

static void executar_dinamico(Arquivo *arquivos, int total, int rank, int processos,
                              const char *diretorio, const char *termo)
{
    if (rank == 0) {
        /* A posição proximo marca o começo da fila de arquivos pendentes. */
        int proximo = 0;
        for (int worker = 1; worker < processos; ++worker) {
            if (proximo < total) {
                enviar_tarefa(worker, proximo, arquivos[proximo].nome);
                ++proximo;
            } else {
                enviar_tarefa(worker, -1, NULL);
            }
        }

        /* Com três arquivos e dois workers, cada um começa com um arquivo.
         * O primeiro que devolver o resultado completo recebe o terceiro. */
        for (int i = 0; i < total; ++i) {
            int worker = receber_resultado(arquivos);
            if (proximo < total) {
                enviar_tarefa(worker, proximo, arquivos[proximo].nome);
                ++proximo;
            } else {
                enviar_tarefa(worker, -1, NULL);
            }
        }
    } else {
        Tarefa tarefa = receber_tarefa();
        while (tarefa.indice >= 0) {
            processar_tarefa(tarefa, diretorio, termo);
            tarefa = receber_tarefa();
        }
    }
}

static void apresentar_resultados(Arquivo *arquivos, int total, double tempo)
{
    /* Só o rank 0 imprime. A lista já está em ordem e a leitura preserva
     * a ordem das linhas dentro de cada arquivo. */
    unsigned long long ocorrencias = 0;
    puts("=== RESULTADOS DA BUSCA ===");
    for (int i = 0; i < total; ++i) {
        if (arquivos[i].tamanho > 0) {
            fwrite(arquivos[i].texto, 1, arquivos[i].tamanho, stdout);
        }
        ocorrencias += arquivos[i].ocorrencias;
        free(arquivos[i].texto);
    }

    puts("\n=== RESUMO ===");
    printf("Total de arquivos: %d\n", total);
    printf("Total de ocorrencias: %llu\n", ocorrencias);
    for (int i = 0; i < total; ++i) {
        printf("%s: %llu ocorrencias\n", arquivos[i].nome, arquivos[i].ocorrencias);
        free(arquivos[i].nome);
    }
    printf("\nTempo de execucao: %.6f segundos\n", tempo);
    free(arquivos);
}

int main(int argc, char **argv)
{
    /* Todos executam main. O rank define o papel de cada processo. */
    MPI_Init(&argc, &argv);
    int rank, processos;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processos);

    const char *diretorio = NULL;
    const char *termo = NULL;
    const char *modo = NULL;
    /* São três pares de opção e valor, além do nome do programa. */
    if (argc == 7) {
        for (int i = 1; i < argc; i += 2) {
            if (strcmp(argv[i], "--dir") == 0) {
                diretorio = argv[i + 1];
            } else if (strcmp(argv[i], "--search") == 0) {
                termo = argv[i + 1];
            } else if (strcmp(argv[i], "--mode") == 0) {
                modo = argv[i + 1];
            }
        }
    }

    if (diretorio == NULL || diretorio[0] == '\0' || termo == NULL || termo[0] == '\0'
        || modo == NULL || (strcmp(modo, "static") != 0 && strcmp(modo, "dynamic") != 0)
        || processos < 2) {
        if (rank == 0) {
            fprintf(stderr,
                    "Uso: mpirun -np <2 ou mais> %s --dir <diretorio> "
                    "--search <termo> --mode <static|dynamic>\n", argv[0]);
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    Arquivo *arquivos = NULL;
    int total = 0;
    double inicio = 0.0;
    if (rank == 0) {
        /* A medição inclui a listagem, a distribuição, a busca e a coleta. */
        inicio = MPI_Wtime();
        arquivos = listar_arquivos(diretorio, &total);
    }
    if (strcmp(modo, "static") == 0) {
        executar_estatico(arquivos, total, rank, processos, diretorio, termo);
    } else {
        executar_dinamico(arquivos, total, rank, processos, diretorio, termo);
    }
    if (rank == 0) {
        /* A impressão vem depois da medição. */
        double tempo = MPI_Wtime() - inicio;
        apresentar_resultados(arquivos, total, tempo);
    }

    /* Todos os processos encerram o MPI antes de sair. */
    MPI_Finalize();
    return EXIT_SUCCESS;
}
