#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

enum { TAG_QUANTIDADE, TAG_TAREFA, TAG_INICIO, TAG_RESULTADO, TAG_TEXTO };
enum { BLOCO_TEXTO = 64 * 1024 };

typedef struct {
    char *nome;
    unsigned long long ocorrencias;
    unsigned long long tamanho;
    off_t posicao;
    int worker;
} Arquivo;

typedef struct {
    int indice;
    char *nome;
} Tarefa;

typedef struct {
    const char *diretorio;
    const char *termo;
    int estatico;
} Opcoes;

static _Noreturn void falhar(const char *contexto)
{
    perror(contexto);
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    _Exit(EXIT_FAILURE);
}

static _Noreturn void falhar_protocolo(const char *contexto)
{
    errno = EPROTO;
    falhar(contexto);
}

static FILE *abrir_temporario(void)
{
    FILE *arquivo = tmpfile();
    if (arquivo == NULL) {
        falhar("Arquivo temporario de resultados");
    }
    return arquivo;
}

static size_t tamanho_bloco(unsigned long long restante)
{
    return restante > BLOCO_TEXTO ? BLOCO_TEXTO : (size_t)restante;
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

static void enviar_tarefa(int worker, int indice, Arquivo *arquivo, int sincronizar_nome)
{
    if (indice < 0) {
        MPI_Send(&indice, 1, MPI_INT, worker, TAG_TAREFA, MPI_COMM_WORLD);
        return;
    }
    size_t comprimento = strlen(arquivo->nome);
    if (comprimento >= (size_t)INT_MAX) {
        errno = EOVERFLOW;
        falhar("Nome do arquivo excede o limite da mensagem MPI");
    }
    int tamanho = (int)(comprimento + 1);
    arquivo->worker = worker;
    MPI_Send(&indice, 1, MPI_INT, worker, TAG_TAREFA, MPI_COMM_WORLD);
    if (sincronizar_nome) {
        MPI_Ssend(arquivo->nome, tamanho, MPI_CHAR, worker, TAG_TAREFA, MPI_COMM_WORLD);
    } else {
        MPI_Send(arquivo->nome, tamanho, MPI_CHAR, worker, TAG_TAREFA, MPI_COMM_WORLD);
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
        if (tamanho <= 0) {
            falhar_protocolo("Tamanho invalido do nome da tarefa");
        }
        tarefa.nome = realocar(NULL, (size_t)tamanho);
        MPI_Recv(tarefa.nome, tamanho, MPI_CHAR, 0, TAG_TAREFA, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        if (tarefa.nome[0] == '\0' || tarefa.nome[tamanho - 1] != '\0') {
            falhar_protocolo("Nome invalido da tarefa");
        }
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

    FILE *resultado = NULL;
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
            if (resultado == NULL) {
                resultado = abrir_temporario();
            }
            if (fprintf(resultado, "%s:%llu: %s\n", tarefa.nome, numero, linha) < 0) {
                falhar("Gravacao do resultado do worker");
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
    free(linha);
    free(caminho);
    free(tarefa.nome);

    unsigned long long tamanho_texto = 0;
    if (resultado != NULL) {
        off_t tamanho = ftello(resultado);
        if (tamanho < 0 || fseeko(resultado, 0, SEEK_SET) != 0) {
            falhar("Posicionamento no resultado do worker");
        }
        tamanho_texto = (unsigned long long)tamanho;
    }
    unsigned long long dados[] = { (unsigned long long)tarefa.indice,
                                   ocorrencias, tamanho_texto };
    MPI_Send(dados, 3, MPI_UNSIGNED_LONG_LONG, 0, TAG_RESULTADO, MPI_COMM_WORLD);

    char texto[BLOCO_TEXTO];
    for (unsigned long long restante = tamanho_texto; restante > 0;) {
        size_t bloco = tamanho_bloco(restante);
        if (fread(texto, 1, bloco, resultado) != bloco) {
            falhar("Leitura do resultado do worker");
        }
        MPI_Send(texto, (int)bloco, MPI_CHAR, 0, TAG_TEXTO, MPI_COMM_WORLD);
        restante -= bloco;
    }
    if (resultado != NULL && fclose(resultado) != 0) {
        falhar("Fechamento do resultado do worker");
    }
}

static int receber_resultado(Arquivo *arquivos, int total, FILE **temporario)
{
    unsigned long long dados[3];
    MPI_Status status;
    MPI_Recv(dados, 3, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, TAG_RESULTADO,
             MPI_COMM_WORLD, &status);
    int quantidade;
    MPI_Get_count(&status, MPI_UNSIGNED_LONG_LONG, &quantidade);
    if (quantidade != 3 || dados[0] >= (unsigned long long)total
        || (dados[1] == 0) != (dados[2] == 0)) {
        falhar_protocolo("Cabecalho de resultado invalido");
    }
    int worker = status.MPI_SOURCE;
    Arquivo *arquivo = &arquivos[dados[0]];
    if (arquivo->worker != worker || worker == 0) {
        falhar_protocolo("Resultado de worker inesperado ou duplicado");
    }
    arquivo->ocorrencias = dados[1];
    arquivo->tamanho = dados[2];
    if (arquivo->tamanho > 0) {
        if (*temporario == NULL) {
            *temporario = abrir_temporario();
        }
        arquivo->posicao = ftello(*temporario);
        if (arquivo->posicao < 0) {
            falhar("Posicao do resultado no coordenador");
        }
    }
    char texto[BLOCO_TEXTO];
    for (unsigned long long restante = arquivo->tamanho; restante > 0;) {
        size_t bloco = tamanho_bloco(restante);
        MPI_Recv(texto, (int)bloco, MPI_CHAR, worker, TAG_TEXTO, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &quantidade);
        if (quantidade != (int)bloco) {
            falhar_protocolo("Tamanho invalido do bloco de resultado");
        }
        if (fwrite(texto, 1, bloco, *temporario) != bloco) {
            falhar("Gravacao do resultado no coordenador");
        }
        restante -= bloco;
    }
    arquivo->worker = 0;
    return worker;
}

static void executar_estatico(Arquivo *arquivos, int total, int rank, int processos,
                              const Opcoes *opcoes, FILE **temporario)
{
    if (rank == 0) {
        int workers = processos - 1;
        int indice = 0;
        for (int worker = 1; worker < processos; ++worker) {
            int quantidade = total / workers + (worker <= total % workers);
            MPI_Send(&quantidade, 1, MPI_INT, worker, TAG_QUANTIDADE, MPI_COMM_WORLD);
            for (int i = 0; i < quantidade; ++i, ++indice) {
                enviar_tarefa(worker, indice, &arquivos[indice], i == quantidade - 1);
            }
        }
        for (int worker = 1; worker < processos && worker <= total; ++worker) {
            MPI_Send(NULL, 0, MPI_BYTE, worker, TAG_INICIO, MPI_COMM_WORLD);
        }
        for (int i = 0; i < total; ++i) {
            receber_resultado(arquivos, total, temporario);
        }
        return;
    }

    int quantidade;
    MPI_Recv(&quantidade, 1, MPI_INT, 0, TAG_QUANTIDADE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (quantidade == 0) {
        return;
    }
    Tarefa *tarefas = realocar(NULL, (size_t)quantidade * sizeof(*tarefas));
    for (int i = 0; i < quantidade; ++i) {
        tarefas[i] = receber_tarefa();
        if (tarefas[i].indice < 0) {
            falhar_protocolo("Tarefa invalida no modo estatico");
        }
    }
    MPI_Recv(NULL, 0, MPI_BYTE, 0, TAG_INICIO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int i = 0; i < quantidade; ++i) {
        processar_tarefa(tarefas[i], opcoes->diretorio, opcoes->termo);
    }
    free(tarefas);
}

static void executar_dinamico(Arquivo *arquivos, int total, int rank, int processos,
                              const Opcoes *opcoes, FILE **temporario)
{
    if (rank == 0) {
        int proximo = 0;
        for (int worker = 1; worker < processos; ++worker) {
            if (proximo < total) {
                enviar_tarefa(worker, proximo, &arquivos[proximo], 0);
                ++proximo;
            } else {
                enviar_tarefa(worker, -1, NULL, 0);
            }
        }

        for (int i = 0; i < total; ++i) {
            int worker = receber_resultado(arquivos, total, temporario);
            if (proximo < total) {
                enviar_tarefa(worker, proximo, &arquivos[proximo], 0);
                ++proximo;
            } else {
                enviar_tarefa(worker, -1, NULL, 0);
            }
        }
    } else {
        Tarefa tarefa = receber_tarefa();
        while (tarefa.indice >= 0) {
            processar_tarefa(tarefa, opcoes->diretorio, opcoes->termo);
            tarefa = receber_tarefa();
        }
    }
}

static void apresentar_resultados(Arquivo *arquivos, int total, FILE *temporario,
                                  double tempo)
{
    unsigned long long ocorrencias = 0;
    char texto[BLOCO_TEXTO];
    puts("=== RESULTADOS DA BUSCA ===");
    for (int i = 0; i < total; ++i) {
        unsigned long long restante = arquivos[i].tamanho;
        if (restante > 0 && fseeko(temporario, arquivos[i].posicao, SEEK_SET) != 0) {
            falhar("Posicionamento no resultado do coordenador");
        }
        while (restante > 0) {
            size_t bloco = tamanho_bloco(restante);
            if (fread(texto, 1, bloco, temporario) != bloco) {
                falhar("Leitura do resultado no coordenador");
            }
            if (fwrite(texto, 1, bloco, stdout) != bloco) {
                falhar("Escrita da saida");
            }
            restante -= bloco;
        }
        ocorrencias += arquivos[i].ocorrencias;
    }

    puts("\n=== RESUMO ===");
    printf("Total de arquivos: %d\n", total);
    printf("Total de ocorrencias: %llu\n", ocorrencias);
    for (int i = 0; i < total; ++i) {
        printf("%s: %llu ocorrencias\n", arquivos[i].nome, arquivos[i].ocorrencias);
        free(arquivos[i].nome);
    }
    printf("\nTempo de execucao: %.6f segundos\n", tempo);
    if (fflush(stdout) != 0 || ferror(stdout)) {
        falhar("Escrita da saida");
    }
    if (temporario != NULL && fclose(temporario) != 0) {
        falhar("Fechamento do resultado no coordenador");
    }
    free(arquivos);
}

static int erro_opcao(int rank, const char *mensagem, const char *opcao)
{
    if (rank == 0) {
        fprintf(stderr, "%s: %s\n", mensagem, opcao);
    }
    return 0;
}

static int ler_opcoes(int argc, char **argv, int rank, Opcoes *opcoes)
{
    enum { OP_DIRETORIO, OP_TERMO, OP_MODO, TOTAL_OPCOES };
    const char *nomes[TOTAL_OPCOES] = { "--dir", "--search", "--mode" };
    const char *valores[TOTAL_OPCOES] = {0};

    for (int i = 1; i < argc; ++i) {
        const char *igual = strchr(argv[i], '=');
        size_t tamanho = igual == NULL ? strlen(argv[i]) : (size_t)(igual - argv[i]);
        int opcao = 0;
        while (opcao < TOTAL_OPCOES
               && (strlen(nomes[opcao]) != tamanho || strncmp(argv[i], nomes[opcao], tamanho) != 0)) {
            ++opcao;
        }
        if (opcao == TOTAL_OPCOES) {
            return erro_opcao(rank, "Opcao desconhecida", argv[i]);
        }
        if (valores[opcao] != NULL) {
            return erro_opcao(rank, "Opcao repetida", nomes[opcao]);
        }
        if (igual == NULL && (i + 1 == argc || strncmp(argv[i + 1], "--", 2) == 0)) {
            return erro_opcao(rank, "Falta valor para a opcao", nomes[opcao]);
        }
        valores[opcao] = igual == NULL ? argv[++i] : igual + 1;
        if (valores[opcao][0] == '\0') {
            return erro_opcao(rank, "Valor vazio para a opcao", nomes[opcao]);
        }
    }

    for (int i = 0; i < TOTAL_OPCOES; ++i) {
        if (valores[i] == NULL) {
            return erro_opcao(rank, "Falta a opcao obrigatoria", nomes[i]);
        }
    }
    opcoes->diretorio = valores[OP_DIRETORIO];
    opcoes->termo = valores[OP_TERMO];
    opcoes->estatico = strcmp(valores[OP_MODO], "static") == 0;
    if (!opcoes->estatico && strcmp(valores[OP_MODO], "dynamic") != 0) {
        return erro_opcao(rank, "Modo invalido", valores[OP_MODO]);
    }
    return 1;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank, processos;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processos);

    Opcoes opcoes;
    if (!ler_opcoes(argc, argv, rank, &opcoes) || processos < 2) {
        if (rank == 0) {
            fprintf(stderr,
                    "Uso: mpirun -np <2 ou mais> %s --dir <diretorio> "
                    "--search <termo> --mode <static|dynamic>\n"
                    "Tambem aceita --opcao=valor.\n", argv[0]);
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    Arquivo *arquivos = NULL;
    FILE *temporario = NULL;
    int total = 0;
    double inicio = 0.0;
    if (rank == 0) {
        arquivos = listar_arquivos(opcoes.diretorio, &total);
        inicio = MPI_Wtime();
    }
    if (opcoes.estatico) {
        executar_estatico(arquivos, total, rank, processos, &opcoes, &temporario);
    } else {
        executar_dinamico(arquivos, total, rank, processos, &opcoes, &temporario);
    }
    if (rank == 0) {
        if (temporario != NULL && fflush(temporario) != 0) {
            falhar("Gravacao do resultado consolidado");
        }
        double tempo = MPI_Wtime() - inicio;
        apresentar_resultados(arquivos, total, temporario, tempo);
    }

    MPI_Finalize();
    return EXIT_SUCCESS;
}
