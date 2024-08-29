# Construcao da imagem do mkdocs

# Usar a imagem base Python oficial
FROM python:3.11.9-alpine

# Definir o diretório de trabalho
WORKDIR /docs

# Atualizar pip e instalar MkDocs com o tema Material e o plugin Minify
RUN pip install --upgrade pip \
    && pip install mkdocs mkdocs-material mkdocs-minify-plugin

# Expor a porta que o MkDocs usará
EXPOSE 8000

# Comando para rodar o MkDocs em modo de desenvolvimento na porta 8000
CMD ["mkdocs", "serve", "-a", "0.0.0.0:8000"]
