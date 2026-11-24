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

enum { TAG_QUANTIDADE, TAG_TAREFA, TAG_RESULTADO, TAG_TEXTO };

typedef struct {
    char *nome;
    unsigned long long ocorrencias;
    char *texto;
    size_t tamanho;
} Arquivo;

typedef struct {
    int indice;
    char *nome;
} Tarefa;

static void falhar(const char *contexto)
{
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
        qsort(arquivos, (size_t)*total, sizeof(*arquivos), comparar_arquivos);
    }
    return arquivos;
}

static void enviar_tarefa(int worker, int indice, const char *nome)
{
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
    size_t tamanho_caminho = strlen(diretorio) + strlen(tarefa.nome) + 2;
    char *caminho = realocar(NULL, tamanho_caminho);
    snprintf(caminho, tamanho_caminho, "%s/%s", diretorio, tarefa.nome);
    FILE *log = fopen(caminho, "r");
    if (log == NULL) {
        falhar(caminho);
    }

    char *texto = NULL;
    size_t tamanho_texto = 0;
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
        ++numero;
        if (tamanho_linha > 0 && linha[tamanho_linha - 1] == '\n') {
            linha[--tamanho_linha] = '\0';
            if (tamanho_linha > 0 && linha[tamanho_linha - 1] == '\r') {
                linha[tamanho_linha - 1] = '\0';
            }
        }
        if (strstr(linha, termo) != NULL) {
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
    MPI_Send(dados, 3, MPI_UNSIGNED_LONG_LONG, 0, TAG_RESULTADO, MPI_COMM_WORLD);

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
    unsigned long long dados[3];
    MPI_Status status;
    MPI_Recv(dados, 3, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, TAG_RESULTADO,
             MPI_COMM_WORLD, &status);

    int worker = status.MPI_SOURCE;
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
        int proximo = 0;
        for (int worker = 1; worker < processos; ++worker) {
            if (proximo < total) {
                enviar_tarefa(worker, proximo, arquivos[proximo].nome);
                ++proximo;
            } else {
                enviar_tarefa(worker, -1, NULL);
            }
        }

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
    MPI_Init(&argc, &argv);
    int rank, processos;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processos);

    const char *diretorio = NULL;
    const char *termo = NULL;
    const char *modo = NULL;
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
        inicio = MPI_Wtime();
        arquivos = listar_arquivos(diretorio, &total);
    }
    if (strcmp(modo, "static") == 0) {
        executar_estatico(arquivos, total, rank, processos, diretorio, termo);
    } else {
        executar_dinamico(arquivos, total, rank, processos, diretorio, termo);
    }
    if (rank == 0) {
        double tempo = MPI_Wtime() - inicio;
        apresentar_resultados(arquivos, total, tempo);
    }

    MPI_Finalize();
    return EXIT_SUCCESS;
}
