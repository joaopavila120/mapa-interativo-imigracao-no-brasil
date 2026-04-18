# Cartografia da Imigracao Historica Brasileira

Aplicacao web para extracao, tratamento, carga em PostgreSQL e visualizacao cartografica de registros historicos de imigracao no Brasil.

O projeto hoje roda assim:

- o CSV tratado alimenta o banco;
- a aplicacao Flask consulta apenas o PostgreSQL em runtime;
- o mapa usa filtros por recorte, ano, sobrenome e pais diretamente sobre a base indexada.

## Estrutura principal

```text
project/
|- app.py
|- docker-compose.yml
|- render.yaml
|- requirements.txt
|- scripts/
|- src/
|- static/
|- templates/
`- tests/
```

## Requisitos locais

- Python 3.13
- Docker Desktop

## Instalacao local

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Rodando localmente com Postgres

Suba o banco:

```powershell
docker compose up -d postgres
```

Carregue a base tratada no banco:

```powershell
python .\scripts\load_postgres_map_data.py
```

Suba a aplicacao:

```powershell
python app.py
```

Acesse:

```text
http://127.0.0.1:5000
```

## Fluxo local minimo

```powershell
docker compose up -d postgres
python .\scripts\load_postgres_map_data.py
python app.py
```

## Render e GitHub

O repositorio ja esta preparado para deploy no Render:

- `render.yaml` cria o Web Service e o Postgres;
- `.python-version` fixa a versao do Python usada no build;
- `.gitignore` evita subir dados locais pesados;
- `gunicorn` ja esta nas dependencias;
- `/healthz` ja existe para health check.

Importante:

- `data/` e `output/` ficam fora do GitHub;
- a carga do banco e feita a partir da sua maquina local, usando o CSV local;
- a aplicacao em producao le somente o Postgres.

### 1. Subir o codigo para o GitHub

No diretorio do projeto:

```powershell
git init
git branch -M main
git remote add origin https://github.com/joaopavila120/mapa-interativo-imigracao-no-brasil.git
git add .
git commit -m "Prepare project for Render deployment"
git push -u origin main
```

Se o repositorio ja estiver iniciado e o `origin` ja existir:

```powershell
git remote set-url origin https://github.com/joaopavila120/mapa-interativo-imigracao-no-brasil.git
git add .
git commit -m "Prepare project for Render deployment"
git push -u origin main
```

### 2. Criar a infraestrutura no Render

O caminho mais simples e usar Blueprint, porque o `render.yaml` ja esta pronto.

No Render:

1. Entre em `Dashboard > New > Blueprint`.
2. Conecte sua conta do GitHub ao Render, se ainda nao conectou.
3. Selecione o repositorio `mapa-interativo-imigracao-no-brasil`.
4. Escolha a branch `main`.
5. Confirme o arquivo `render.yaml`.
6. Revise os recursos que vao ser criados.

Configuracao atual do `render.yaml`:

- Web Service: `starter`
- Postgres: `basic-1gb`
- Regiao: `virginia`

Se quiser mudar custo ou capacidade, edite o `render.yaml` antes do deploy e faca novo push.

Depois clique em `Deploy Blueprint`.

### 3. Esperar o banco ficar disponivel

Depois que o Blueprint terminar:

1. Abra o banco `mapa-interativo-imigracao-db` no Render.
2. Abra `Connect`.
3. Copie a `External Database URL`.

Essa URL sera usada apenas na sua maquina local para popular o banco remoto.

### 4. Carregar os dados no banco remoto do Render

No seu PowerShell local:

```powershell
$env:DATABASE_URL="COLE_AQUI_A_EXTERNAL_DATABASE_URL_DO_RENDER"
python .\scripts\load_postgres_map_data.py
```

Esse script vai:

- importar `immigrant_records`;
- materializar `map_points`;
- materializar `map_view_stats`;
- criar indices para acelerar o mapa.

Observacao:

- para esse passo funcionar, o CSV tratado precisa existir localmente em `data\processed\records_clean.csv`;
- esse arquivo nao vai para o GitHub;
- ele so serve para alimentar o banco.

### 5. Validar se a aplicacao online ficou pronta

Depois da carga:

1. Abra o Web Service no Render.
2. Acesse a URL publica gerada pelo Render.
3. Teste:
   - troca de recorte do mapa;
   - linha do tempo;
   - busca por sobrenome;
   - clique em pontos;
   - filtro por pais.

Health check do servico:

```text
/healthz
```

### 6. Quando atualizar os dados no futuro

Se o CSV tratado mudar, nao precisa republicar o GitHub por causa dos dados.

Basta:

```powershell
$env:DATABASE_URL="COLE_AQUI_A_EXTERNAL_DATABASE_URL_DO_RENDER"
python .\scripts\load_postgres_map_data.py
```

Se fizer mudancas no codigo:

```powershell
git add .
git commit -m "Describe your change"
git push
```

O Render fara novo deploy automaticamente.

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
