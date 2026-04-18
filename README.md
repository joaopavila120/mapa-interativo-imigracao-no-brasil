# Cartografia da Imigracao Historica Brasileira

Aplicacao para extracao, tratamento, carga em PostgreSQL e exploracao visual da imigracao historica brasileira.

O projeto possui duas camadas principais:

- pipeline de dados para extrair PDFs, tratar os registros e carregar o banco;
- interface publicada em Streamlit, conectada a um PostgreSQL externo.

## Estrutura principal

```text
project/
|- app.py
|- streamlit_app.py
|- docker-compose.yml
|- requirements.txt
|- scripts/
|- src/
|- static/
|- templates/
|- tests/
`- README.md
```

## Requisitos locais

- Python 3.13
- Docker Desktop
- PostgreSQL local ou remoto

## Instalacao local

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Rodando localmente

### Banco local com Docker

```powershell
docker compose up -d postgres
```

### Carregar a base no banco local

Carga completa:

```powershell
python .\scripts\load_postgres_map_data.py
```

Carga enxuta para deploy gratuito:

```powershell
python .\scripts\load_postgres_map_data.py --skip-immigrant-records --light-indexes
```

### Subir a interface Streamlit local

```powershell
streamlit run .\streamlit_app.py
```

## O que e usado em runtime

Em runtime, a interface consulta apenas:

- `map_points`
- `map_view_stats`
- `map_build_meta`

O CSV tratado e usado apenas para alimentar o banco.

## Publicacao gratuita recomendada

Para manter o projeto 100% gratuito, o fluxo recomendado e:

- codigo no GitHub
- interface no Streamlit Community Cloud
- banco PostgreSQL gratuito externo

Neste projeto, o caminho mais direto e:

- Streamlit Community Cloud para a interface
- Supabase Free para o PostgreSQL

Para a base atual, a melhor estrategia pratica e carregar uma versao enxuta do banco:

```powershell
python .\scripts\load_postgres_map_data.py --skip-immigrant-records --light-indexes
```

Esse modo:

- nao importa `immigrant_records`
- cria apenas os indices principais do mapa
- reduz bastante o tamanho final do banco para publicacao gratuita

Na validacao local deste projeto, esse perfil enxuto ficou em cerca de `398 MB`, o que o torna compativel com um banco gratuito de `500 MB`.

## Passo a passo exato para publicar no Streamlit Community Cloud

### 1. Suba o codigo para o GitHub

No PowerShell, dentro da pasta do projeto:

```powershell
git init
git branch -M main
git remote add origin https://github.com/joaopavila120/mapa-interativo-imigracao-no-brasil.git
git add .
git commit -m "Prepare Streamlit deployment"
git push -u origin main
```

Se o repositorio ja existir localmente:

```powershell
git remote set-url origin https://github.com/joaopavila120/mapa-interativo-imigracao-no-brasil.git
git add .
git commit -m "Prepare Streamlit deployment"
git push -u origin main
```

### 2. Crie um banco PostgreSQL gratuito externo

O app Streamlit nao hospeda banco por conta propria. Voce precisa criar um PostgreSQL externo.

O projeto foi preparado para funcionar com qualquer PostgreSQL compativel com `DATABASE_URL`.

Para um fluxo 100% gratuito, use um projeto no Supabase Free.

No Supabase:

1. Crie uma conta
2. Clique em `New project`
3. Escolha nome, senha do banco e regiao
4. Aguarde o projeto ficar pronto
5. Abra o botao `Connect`
6. Copie a `Session pooler` connection string

Use a `Session pooler` connection string, porque ela e a opcao mais segura para um app hospedado e conectado por IPv4/IPv6 via pooler.

Depois de criar o banco, copie a string de conexao no formato:

```text
postgresql://usuario:senha@host:porta/database
```

No Supabase, use a connection string mostrada em `Connect > Session pooler`.

### 3. Carregue os dados no banco remoto

Na sua maquina local:

```powershell
$env:DATABASE_URL="COLE_AQUI_A_URL_DO_POSTGRES_REMOTO"
python .\scripts\load_postgres_map_data.py --skip-immigrant-records --light-indexes
```

Esse e o comando recomendado para publicacao gratuita.

### 4. Crie um arquivo local de secrets para testar antes do deploy

Crie o arquivo:

```text
.streamlit/secrets.toml
```

Com este conteudo:

```toml
DATABASE_URL = "postgresql://usuario:senha@host:porta/database"
```

Esse arquivo nao deve ser commitado. O `.gitignore` do projeto ja cobre isso.

### 5. Teste localmente com o banco remoto

```powershell
streamlit run .\streamlit_app.py
```

Se abrir normalmente e o mapa carregar, a conexao remota esta pronta para a nuvem.

### 6. Publique no Streamlit Community Cloud

No Streamlit Community Cloud:

1. Entre em `https://share.streamlit.io`
2. Clique em `Create app`
3. Escolha `From existing repo`
4. Selecione o repositorio `joaopavila120/mapa-interativo-imigracao-no-brasil`
5. Branch: `main`
6. Main file path: `streamlit_app.py`

### 7. Abra as configuracoes avancadas no deploy

Ainda na tela de deploy:

1. Clique em `Advanced settings`
2. Selecione a versao do Python mais proxima do seu ambiente local
3. Cole os secrets no campo `Secrets`

Use exatamente isto:

```toml
DATABASE_URL = "postgresql://usuario:senha@host:porta/database"
```

### 8. Clique em Deploy

Depois disso, o Streamlit Cloud vai:

- copiar os arquivos do repositorio
- instalar o `requirements.txt`
- executar `streamlit run` a partir da raiz do repositorio
- subir o `streamlit_app.py`

### 9. Verifique a aplicacao publicada

Teste estes pontos:

- troca de recorte do mapa
- linha do tempo
- filtro por sobrenome
- filtro por pais
- clique nos pontos
- painel de localidade

## Atualizando o projeto depois

### Quando mudar apenas o codigo

```powershell
git add .
git commit -m "Describe your change"
git push
```

O Streamlit Cloud faz redeploy automaticamente.

### Quando mudar os dados

```powershell
$env:DATABASE_URL="COLE_AQUI_A_URL_DO_POSTGRES_REMOTO"
python .\scripts\load_postgres_map_data.py --skip-immigrant-records --light-indexes
```

Se o schema do app nao mudou, normalmente nao e preciso tocar no deploy do Streamlit.

## Extracao dos PDFs

```powershell
python .\scripts\extract_immigrants.py
```

## Pre-processamento

```powershell
python -m src.run_preprocessing
```

Saidas locais:

- `data/interim/records_combined.csv`
- `data/processed/records_clean.csv`
- `data/processed/data_quality_report.json`

## Exploracao

Script:

```powershell
python .\scripts\explore_immigrants_csv.py
```

Notebook:

- `notebooks\explore_immigrants_csv.ipynb`

## Testes

```powershell
python -m unittest tests.test_preprocessing
```
