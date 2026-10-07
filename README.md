# SampleRabbitMq

Exemplo básico de mensageria com RabbitMQ e .NET 10. O projeto foi pensado para quem está começando: mostra como publicar e consumir mensagens, configurar filas duráveis, tratar falhas, tentar novamente o processamento e encaminhar mensagens problemáticas para uma Dead Letter Queue (DLQ).

## O que o exemplo demonstra

- Publicação e consumo de mensagens JSON usando exchanges do tipo `direct`.
- Filas e exchanges duráveis, além de mensagens publicadas como persistentes.
- Publisher confirms para aguardar a confirmação do RabbitMQ a cada publicação; `mandatory` também detecta mensagens sem rota para uma fila.
- Confirmação manual do processamento com `ack` e rejeição sem requeue com `nack`.
- Retry com uma fila intermediária, TTL de 5 segundos e até 3 novas tentativas.
- Encaminhamento de mensagens inválidas ou que excederam as tentativas para a DLQ.

## Projetos

- `SampleRabbitMq.Producer`: declara a exchange e a fila principal e publica mensagens de exemplo.
- `SampleRabbitMq.Consumer`: consome e valida mensagens, aplica retry e configura a DLQ.
- `SampleRabbitMq.Models`: contém os modelos `Sample` e `SampleItem` compartilhados.

## Fluxo das mensagens

1. O produtor publica uma mensagem JSON persistente na exchange `sample.exchange`, usando a routing key `sample.routing.key`, e aguarda a confirmação do RabbitMQ.
2. A mensagem chega à fila principal `sample.queue`.
3. Se o processamento funcionar, o consumidor confirma a mensagem com `ack`.
4. Se ocorrer uma falha de processamento, o consumidor a publica na exchange `sample.retry`. A fila `sample.retry.queue` aguarda 5 segundos e então devolve a mensagem à fila principal.
5. São feitas até 3 novas tentativas após a entrega inicial. Se todas falharem, a mensagem é rejeitada sem requeue e segue para `sample.dlq`.
6. Mensagens com erro de desserialização vão diretamente para a DLQ, sem retry.

## Pré-requisitos

- .NET 10 SDK.
- RabbitMQ acessível em `localhost:5672`.
- Docker, caso opte por iniciar o RabbitMQ em um contêiner.

O exemplo usa as credenciais locais padrão `guest` / `guest`, definidas nos programas do produtor e do consumidor. Elas são apropriadas somente para desenvolvimento local; não use essas credenciais em um ambiente compartilhado ou de produção.

### Iniciar o RabbitMQ com Docker

O comando abaixo inicia o RabbitMQ com o plugin de gerenciamento e mantém as portas acessíveis apenas pela máquina local. A configuração permite que o exemplo use `guest` a partir do host; use-a somente para este ambiente de aprendizado.

```powershell
docker run -d --name sample-rabbitmq `
  -p 127.0.0.1:5672:5672 `
  -p 127.0.0.1:15672:15672 `
  -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbit loopback_users []" `
  rabbitmq:4-management
```

O painel de gerenciamento fica em [http://localhost:15672](http://localhost:15672), com usuário `guest` e senha `guest`. Para parar o contêiner, execute `docker stop sample-rabbitmq`; para removê-lo, execute `docker rm sample-rabbitmq`.

## Executar

Na pasta da solução, restaure e compile os projetos:

```powershell
dotnet restore SampleRabbitMq.slnx
dotnet build SampleRabbitMq.slnx
```

Abra um terminal para o consumidor e mantenha-o em execução:

```powershell
dotnet run --project SampleRabbitMq.Consumer/SampleRabbitMq.Consumer.csproj
```

Em outro terminal, inicie o produtor. Pressione Enter quando solicitado para publicar entre zero e três mensagens de exemplo:

```powershell
dotnet run --project SampleRabbitMq.Producer/SampleRabbitMq.Producer.csproj
```

O consumidor pode ser encerrado pressionando Enter no terminal dele. Para observar as filas e as mensagens pela interface de gerenciamento, acesse `http://localhost:15672`.

## Durabilidade e persistência

As filas e exchanges são declaradas como duráveis, e o produtor marca as mensagens como persistentes. O canal do produtor também usa publisher confirms, então a publicação só é considerada concluída depois que o RabbitMQ confirma que assumiu responsabilidade pela mensagem. Para uma mensagem persistente roteada a uma fila durável, essa confirmação ocorre após a persistência no broker.

Isso não representa uma garantia absoluta contra qualquer falha: o exemplo não configura replicação de filas. Se a conexão cair antes de a confirmação chegar, o produtor pode não saber se a mensagem foi aceita; uma nova tentativa pode gerar duplicatas. Aplicações de produção devem considerar idempotência, políticas de recuperação e monitoramento.

## Observação sobre as filas

O consumidor e o produtor declaram a topologia ao iniciar. Se já existir uma fila com o mesmo nome e argumentos diferentes, o RabbitMQ pode recusar a declaração. Em um ambiente de teste, remova a fila pelo painel de gerenciamento ou reinicie o broker de desenvolvimento antes de alterar esses argumentos.