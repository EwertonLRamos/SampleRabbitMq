using System.Text.Json;
using SampleRabbitMq.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string exchangeName = "sample.exchange";
const string queueName = "sample.queue";
const string routingKey = "sample.routing.key";

const string dlxName = "sample.dlx";
const string dlqName = "sample.dlq";
const string dlxroutingKey = "sample.dlx.routing.key";

const string retryName = "sample.retry";
const string retryQueueName = "sample.retry.queue";
const string retryRoutingKey = "sample.retry.routing.key";

const int maxRetryAttempts = 3;
const int retryDelayMs = 5000;

var factory = new ConnectionFactory() 
{ 
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
    Port = 5672,
    VirtualHost = "/",
    AutomaticRecoveryEnabled = true,
    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

#region DLX and DLQ Setup
await channel.ExchangeDeclareAsync(
    exchange: dlxName, 
    type: ExchangeType.Direct, 
    durable: true, 
    autoDelete: false
);

await channel.QueueDeclareAsync(
    queue: dlqName, 
    durable: true, 
    exclusive: false, 
    autoDelete: false,
    arguments: null
);

await channel.QueueBindAsync(
    queue: dlqName, 
    exchange: dlxName, 
    routingKey: dlxroutingKey
);
#endregion

#region Retry Exchange and Queue Setup
await channel.ExchangeDeclareAsync(
    exchange: retryName, 
    type: ExchangeType.Direct, 
    durable: true, 
    autoDelete: false
);

await channel.QueueDeclareAsync(
    queue: retryQueueName, 
    durable: true, 
    exclusive: false, 
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-message-ttl", retryDelayMs },
        { "x-dead-letter-exchange", exchangeName },
        { "x-dead-letter-routing-key", routingKey }
    }
);

await channel.QueueBindAsync(
    queue: retryQueueName, 
    exchange: retryName, 
    routingKey: retryRoutingKey
);
#endregion

#region Main Exchange and Queue Setup
await channel.ExchangeDeclareAsync(
    exchange: exchangeName, 
    type: ExchangeType.Direct, 
    durable: true, 
    autoDelete: false
);

await channel.QueueDeclareAsync(
    queue: queueName, 
    durable: true, 
    exclusive: false, 
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-dead-letter-exchange", dlxName },
        { "x-dead-letter-routing-key", dlxroutingKey }
    }
);

await channel.QueueBindAsync(
    queue: queueName, 
    exchange: exchangeName, 
    routingKey: routingKey
);
#endregion

await channel.BasicQosAsync(
    prefetchSize: 0, 
    prefetchCount: 1, 
    global: false
);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (model, ea) =>
{
    int retryCount = 0;
    
    if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.TryGetValue("x-retry-count", out var retryCountHeader))
    {
        if (retryCountHeader is byte[] bytes)
            retryCount = Convert.ToInt32(System.Text.Encoding.UTF8.GetString(bytes));
        else
            retryCount = Convert.ToInt32(retryCountHeader);
    }

    try
    {
        var body = ea.Body.ToArray();
        var json = System.Text.Encoding.UTF8.GetString(body);

        var message = JsonSerializer.Deserialize<Sample>(json) ?? throw new JsonException("A mensagem é nula.");
        
        Console.WriteLine($"\n[Processando] Mensagem recebida: {json} (Tentativa: {retryCount})");

        if (message?.Items == null || message.Items.Count < 1)
            throw new InvalidOperationException("A mensagem não possui itens.");

        await Task.Delay(1000);
        
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (JsonException jsonEx)
    {
        Console.WriteLine($"[Erro Fatal] Erro de desserialização: {jsonEx.Message}");
        
        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Erro] Falha ao processar: {ex.Message}");
        
        if (retryCount < maxRetryAttempts)
        {
            var properties = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/json",
                ContentEncoding = "utf-8",
                Headers = new Dictionary<string, object?>
                {
                    { "x-retry-count", (retryCount + 1).ToString() }
                }
            };

            await channel.BasicPublishAsync(
                exchange: retryName,
                routingKey: retryRoutingKey,
                mandatory: false,
                basicProperties: properties,
                body: ea.Body
            );

            Console.WriteLine($"[Retry] Reencaminhada para a fila de retry ({retryCount + 1}/{maxRetryAttempts}).");

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
        }
        else
        {
            Console.WriteLine("[DLQ] Número máximo de tentativas atingido. Enviando para a DLQ...");
            
            await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
        }
    }  
};

await channel.BasicConsumeAsync(
    queue: queueName, 
    autoAck: false, 
    consumer: consumer
);

Console.WriteLine("Consumer está rodando. Pressione [enter] para sair.");
Console.ReadLine();