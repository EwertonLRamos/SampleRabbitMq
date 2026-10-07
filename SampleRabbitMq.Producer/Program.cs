using System.Text.Json;
using SampleRabbitMq.Models;
using RabbitMQ.Client;

const string exchangeName = "sample.exchange";
const string queueName = "sample.queue";
const string routingKey = "sample.routing.key";

const string dlxName = "sample.dlx";
const string dlqName = "sample.dlq";
const string dlxroutingKey = "sample.dlx.routing.key";

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

var channelOptions = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true
);

await using var channel = await connection.CreateChannelAsync(channelOptions);

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
    arguments: new Dictionary<string, object>
    {
        { "x-dead-letter-exchange", dlxName },
        { "x-dead-letter-routing-key", dlxroutingKey }
    }!
);

await channel.QueueBindAsync(
    queue: queueName, 
    exchange: exchangeName, 
    routingKey: routingKey
);

Console.WriteLine("Producer está rodando. Pressione [enter] para enviar até 3 mensagens.");
Console.ReadLine();

static Sample CreateSample(int sampleIndex, int qtdItems)
{
    var items = new List<SampleItem>();

    for (int i = 0; i < qtdItems; i++)
        items.Add(new SampleItem { Id = i + 1, Name = $"Item {i + 1}" });

    return new Sample
    {
        Id = sampleIndex,
        Name = $"Sample {sampleIndex}",
        Items = items
    };
}

async Task PublishMessageAsync(Sample? sample)
{
    var json = sample is not null ? JsonSerializer.Serialize(sample) : null;
    var body = System.Text.Encoding.UTF8.GetBytes(json ?? "");
    var properties = new BasicProperties
    {
        Persistent = true,
        ContentType = "application/json",
        ContentEncoding = "utf-8"
    };
    try
    {
        await channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: properties,
            body: body
        );

        Console.WriteLine($"Mensagem enviada: {json}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Erro ao publicar mensagem: {ex.Message}");
        throw;
    }
}

var qtdSamples = Random.Shared.Next(0, 4);

if (qtdSamples == 0)
    await PublishMessageAsync(null);

for (int i = 0; i < qtdSamples; i++)
{
    var qtdItems = Random.Shared.Next(1, 4);
    
    var sample = CreateSample(i + 1, qtdItems);

    await PublishMessageAsync(sample);
}