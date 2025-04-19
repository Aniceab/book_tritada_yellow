# Usar uma imagem base do Python
FROM python:3.11-slim

# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar os arquivos do projeto para o container
COPY . /app

# Instalar as dependências do projeto
RUN pip install --no-cache-dir -r requirements.txt

# Garantir que o arquivo .env seja carregado (se necessário)
ENV PYTHONUNBUFFERED=1

# Comando para executar o componente
CMD ["python", "component-scripts/src/main.py"]
CMD ["python", "component-DQ/src/main.py"]
CMD ["airflow", "standalone"]