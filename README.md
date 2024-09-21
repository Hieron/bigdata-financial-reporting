# Relatório de Análise Financeira
#### Projeto final da disciplina de Big Data - MBA em Engenharia de Software, UFRJ
<br>

## Sobre o Projeto
Este projeto faz parte da disciplina de Big Data no MBA em Engenharia de Software da Universidade Federal do Rio de Janeiro. O objetivo é usar tecnologias de processamento de dados em larga escala e computação distribuída.

A aplicação permite solicitar relatórios financeiros através de uma interface web. O processamento dos dados é feito no backend, utilizando Apache Spark e HDFS, e o relatório gerado é enviado por e-mail ao solicitante.

O projeto mostra como essas tecnologias podem ser usadas para lidar com grandes volumes de dados de forma eficiente e organizada.
<br><br>

## Equipe do Projeto
- **André Kuniyoshi**
- **Hieron Giacomini**
- **Patrick Anjos**
<br><br>

## Principais Tecnologias
- **Docker** - Isolamento e gerenciamento de contêineres para ambientes consistentes e escaláveis.
- **Hadoop (HDFS)** - Armazenamento distribuído de dados para processamento eficiente em larga escala.
- **Apache Spark** - Processamento paralelo de grandes volumes de dados, acelerando análises complexas.
- **Python** - Linguagem utilizada para automação, processamento de dados e desenvolvimento do servidor web.
<br><br>

## Configuração do Ambiente
Para configurar o ambiente e executar o projeto, siga os passos abaixo:

1. **Download do Spark e Hadoop:** Baixe o Apache Spark e o Hadoop, e extraia-os nas pastas `./local/spark` e `./local/hadoop`, respectivamente.
2. **Configuração do Hadoop:** Copie os arquivos `core-site.xml` e `hdfs-site.xml` da pasta `./config` para `./local/hadoop/etc/hadoop/`.
3. **Variáveis de Ambiente:** Preencha o arquivo `.env` com as credenciais do provedor de e-mail e outras variáveis necessárias.
4. **Execução com Docker:** Com o Docker instalado, execute o comando `docker-compose up -d` para iniciar os serviços.
<br><br>

## Ambientes
O projeto oferece duas interfaces principais para interação e monitoramento:

- **Página do Formulário:** Acesse [http://localhost](http://localhost) para visualizar a interface de usuário principal com o formulário de requisição de relatório.
- **Página de Monitoramento de Jobs:** Acesse [http://localhost/jobs](http://localhost/jobs) para acompanhar os jobs agendados no sistema.
<br><br>

## Arquitetura do Sistema
O diagrama abaixo ilustra a arquitetura do projeto, detalhando como os componentes interagem para processar e gerar relatórios financeiros. Para configurar o ambiente e executar o projeto, siga os passos indicados na documentação.

![Arquitetura do Sistema de Relatórios Financeiros](https://raw.githubusercontent.com/Hieron/bigdata-financial-reporting/main/backend/static/images/architecture.svg#gh-light-mode-only)
![Arquitetura do Sistema de Relatórios Financeiros](https://raw.githubusercontent.com/Hieron/bigdata-financial-reporting/main/backend/static/images/architecture-dark.svg#gh-dark-mode-only)
<br><br>

## Fluxo Básico do Sistema
O diagrama abaixo demonstra o fluxo básico de operações do sistema, desde a solicitação do relatório até o envio dos resultados por e-mail. Cada etapa do processo é automatizada para garantir a eficiência e precisão no processamento dos dados financeiros.

![Fluxo Básico do Sistema de Relatórios Financeiros](https://raw.githubusercontent.com/Hieron/bigdata-financial-reporting/main/backend/static/images/flow.svg#gh-light-mode-only)
![Fluxo Básico do Sistema de Relatórios Financeiros](https://raw.githubusercontent.com/Hieron/bigdata-financial-reporting/main/backend/static/images/flow-dark.svg#gh-dark-mode-only)
<br><br>

## Contribuindo
Contribuições são sempre bem-vindas! Sinta-se à vontade para abrir um _pull request_ ou relatar qualquer problema encontrado.
<br><br>
<br><br>