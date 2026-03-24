# cdi-bonus-assignment

Pipeline local em **PySpark** (sem Pandas): leitura de Parquet em `data/raw/fake_transactions`, contagem e amostra das linhas, gravação em tabela **Delta Lake** em `data/bronze/bronze_transactions`.

Dependências com **Poetry**. Execução recomendada com **Spark em Linux via Docker** (evita problemas de bibliotecas nativas do Hadoop no Windows).

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e em execução (no Windows, com backend Linux/WSL2 conforme a instalação padrão).
- Opcional no host: [Poetry](https://python-poetry.org/docs/#installation) se quiser instalar dependências fora do container (Python 3.11+ alinhado ao `Dockerfile`).

## Estrutura de dados

| Caminho | Conteúdo |
|---------|----------|
| `data/raw/fake_transactions/` | Arquivos Parquet de entrada (layout Spark/Hadoop, ex.: `part-*.parquet`). |
| `data/bronze/bronze_transactions/` | Saída Delta (criada/atualizada em modo `append` pelo script). |

No container, o diretório do projeto é montado em `/workspace`; os caminhos acima são relativos à **raiz do repositório**.

## Passo a passo: Spark local com Docker

1. **Abra um terminal** na raiz do repositório (a pasta que contém `docker-compose.yml`, `pyproject.toml` e `data/`).

2. **Construa a imagem** (baixa Spark 3.5.3, JDK 17 e instala dependências Poetry a partir do lock):

   ```bash
   docker compose build
   ```

3. **Execute o job** (sobe o container, roda `poetry install` para sincronizar o `.venv` no volume e em seguida o script). O serviço usa `stdin_open` e `tty` para o script pedir **Enter** antes de encerrar e você conseguir ler o output.

   ```bash
   docker compose run --rm app
   ```

   Se o prompt “Pressione Enter…” não aparecer, use terminal interativo explícito: `docker compose run --rm -it app`.

   Comando equivalente explícito:

   ```bash
   docker compose run --rm app poetry run python pyspark_notebook.py
   ```

4. **Confira a saída**: no terminal aparecem o total de linhas (`count`), uma amostra (`show`) e a mensagem com o path Delta gravado. Os arquivos Delta ficam em `data/bronze/bronze_transactions/` no seu disco (via bind mount).

### Notas sobre Windows e caminhos

- No host, pastas podem aparecer como `data\raw\...`; dentro do container o separador é sempre `/`. O script usa `pathlib` a partir da raiz do projeto, compatível com os dois ambientes.
- Evite caminhos UNC ou cenários exóticos de permissão no volume montado; em caso de erro de permissão, verifique compartilhamento de unidade no Docker Desktop.

### Primeira execução e rede

- Na primeira vez, o Delta pode resolver JARs via Ivy/Maven (comportamento do `configure_spark_with_delta_pip`); é necessário acesso à internet nessa etapa.
- O `docker-entrypoint.sh` executa `poetry install` a cada `run` para manter o `.venv` alinhado ao `poetry.lock` quando o projeto está montado como volume.

### Solução de problemas

| Sintoma | O que verificar |
|---------|-----------------|
| `Cannot connect to the Docker daemon` | Inicie o Docker Desktop e aguarde até ficar pronto. |
| `poetry.lock` ausente | Rode `poetry lock` na raiz (com Poetry instalado no host) antes do `docker compose build`. |
| Falha ao ler Parquet (codec) | Confirme Spark 3.5.x na imagem; Parquet com compressão ZSTD costuma funcionar nesta versão. |
| Erro `NativeIO` / `UnsatisfiedLinkError` no **Windows** ao rodar `poetry run` **fora** do Docker | Use o fluxo Docker acima ou configure `winutils`/HADOOP_HOME para Spark no Windows; o caminho suportado pelo projeto é Linux no container. |
| `exec /docker-entrypoint.sh: no such file or directory` | Costuma ser **CRLF** no `docker-entrypoint.sh` no Windows. O repositório usa `.gitattributes` (`*.sh` → LF) e o `Dockerfile` normaliza o script no build; rode `docker compose build --no-cache` de novo. No editor, defina fim de linha **LF** para `docker-entrypoint.sh`. |

## Script principal

- Arquivo: `pyspark_notebook.py`
- No PySpark open-source não existe `display()` (função típica do Databricks); o script usa `show()` para exibir as primeiras linhas, sem Pandas.

## Dependências (Poetry)

Definidas em `pyproject.toml`:

- `pyspark` 3.5.3 (alinhado ao Spark instalado na imagem Docker)
- `delta-spark` 3.2.0

Sem `pandas` no projeto.
