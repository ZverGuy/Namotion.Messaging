# Namotion.Messaging

[Storage](https://github.com/RicoSuter/Namotion.Storage) | Messaging | [Reflection](https://github.com/RicoSuter/Namotion.Reflection)

[![Azure DevOps](https://img.shields.io/azure-devops/build/rsuter/9023bd0a-b641-4e30-9c0f-a7c15e1e080e/19/master.svg)](https://dev.azure.com/rsuter/Namotion/_build?definitionId=19)
[![Azure DevOps](https://img.shields.io/azure-devops/coverage/rsuter/9023bd0a-b641-4e30-9c0f-a7c15e1e080e/19/master.svg)](https://dev.azure.com/rsuter/Namotion/_build?definitionId=19)
[![Discord](https://img.shields.io/badge/Discord-join%20chat-1dce73.svg)](https://discord.gg/BxQNy25WF6)

<img align="left" src="https://raw.githubusercontent.com/RicoSuter/Namotion.Reflection/master/assets/Icon.png" width="48px" height="48px">

The Namotion.Messaging .NET libraries provide abstractions and implementations for message brokers, event queues and data ingestion services.

By programming against a messaging abstraction you enable the following scenarios: 

- Build **multi-cloud capable applications** by being able to change messaging technologies on demand. 
- Quickly **switch to different messaging technologies** to find the best technological fit for your applications. 
- Implement behavior driven **integration tests which can run in-memory** or against different technologies for better debugging experiences or local execution. 
- Provide **better local development experiences**, e.g. replace Service Bus with a locally running RabbitMQ docker container or an in-memory implementation. 

## Usage

To listen for messages, create a new message receiver for a specific implementation and call the `ListenWithRetryAsync()` method (this will newer return):

```csharp
IMessageReceiver receiver = ServiceBusMessageReceiver
	.Create("MyConnectionString", "myqueue");

await receiver.ListenAsync(async (messages, ct) =>
{
    ...
}, CancellationToken.None);
```

In another process or thread you can then publish messages to this listener: 

```csharp
IMessagePublisher publisher = ServiceBusMessagePublisher
    .Create(configuration["ServiceBusConnectionString"], "myqueue");

await publisher.PublishAsync(new Message(content: new byte[] { 1, 2, 3 }));
```

However, you should host the listener with a .NET generic host which provides dependency injection, configuration and logging. To use the `IMessageReceiver` in a simple command line application ([.NET Generic Host](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/host/generic-host?view=aspnetcore-2.2)), implement a new `BackgroundService` and start message processing in `ExecuteAsync`:

```CSharp
public class MyBackgroundService : BackgroundService
{
    private readonly IMessageReceiver _messageReceiver;
    private readonly ILogger _logger;

    public MyBackgroundService(IMessageReceiver messageReceiver, ILogger logger)
    {
        _messageReceiver = messageReceiver;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _messageReceiver.ListenWithRetryAsync(async (messages, ct) =>
        {
            foreach (var message in messages)
            {
                try
                {
                    // TODO: Process message

                    await _messageReceiver.ConfirmAsync(message, ct);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Error while processing {nameof(MyMessage)} message.");
                    await _messageReceiver.RejectAsync(message, ct);
                }
            }
        }, stoppingToken);
    }
}
```

In your program's `Main` method, create a new `HostBuilder` and add the background service as a hosted service:

```CSharp
public static async Task Main(string[] args)
{
    var host = new HostBuilder()
        .ConfigureServices(services => 
        {
            var receiver = ServiceBusMessageReceiver.Create("MyConnectionString", "myqueue");
            services.AddSingleton<IMessageReceiver>(receiver);
            services.AddHostedService<MyBackgroundService>();
        })
        .Build();

    await host.RunAsync();
}
```

## Extensions

Behavior extensions, for example custom dead letter queues or large message handling, is achieved with interceptors which wrap publisher and receiver methods with custom code. These interceptors are added with the `With*` extension methods. Custom interceptors can be implemented with the `MessagePublisher<T>` and `MessageReceiver<T>` classes.

## Core packages

### Namotion.Messaging.Abstractions

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Abstractions.svg)](https://www.nuget.org/packages/Namotion.Messaging.Abstractions/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Abstractions%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Abstractions/versions/latest?tab=types)

Contains the messaging abstractions, mainly interfaces with a very small footprint and extremely stable contracts:

- **IMessagePublisher\<T>**
- **IMessagePublisher**
    - `PublishAsync(messages, cancellationToken)`: Sends a batch of messages to the queue.
- **IMessageReceiver\<T>**
- **IMessageReceiver**
    - `GetMessageCountAsync(cancellationToken)`: Gets the count of messages waiting to be processed.
    - `ListenAsync(handleMessages, cancellationToken)`: (Fails when connection cannot be established)
    - `ListenWithRetryAsync(handleMessages, cancellationToken)`: Starts listening and processing messages with the `handleMessages` function until the `cancellationToken` signals a cancellation.
    - `KeepAliveAsync(messages, timeToLive, cancellationToken)`: Extends the message lock timeout on the given messages.
    - `ConfirmAsync(messages, cancellationToken)`: Confirms the processing of messages and removes them from the queue.
    - `RejectAsync(messages, cancellationToken)`: Rejects messages and requeues them for later reprocessing.
    - `DeadLetterAsync(messages, reason, errorDescription, cancellationToken)`: Removes the messages and moves them to the dead letter queue.
- **Message\<T>:**
- **Message:** A generic message implementation.

The idea behind the generic interfaces is to allow multiple instance registrations, read [Dependency Injection in .NET: A way to work around missing named registrations](https://blog.rsuter.com/dotnet-dependency-injection-way-to-work-around-missing-named-registrations/) for more information.

### Namotion.Messaging

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.svg)](https://www.nuget.org/packages/Namotion.Messaging/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging/versions/latest?tab=types)

Contains common helper methods and base implementations of the abstractions:

- **InMemoryMessagePublisherReceiver:** In-memory message publisher and receiver for integration tests and dependency free local development environments (i.e. use this implementation when no connection strings are defined).

Extension methods to enhance or modify instances: 

- **AsPublisher\<T>():**
- **AsReceiver\<T>():** Changes the type of the interface from `IMessagePublisher`/`IMessageReceiver` to `IMessagePublisher<T>`/`IMessageReceiver<T>`.
- **WithDeadLettering(messagePublisher):** Adds support for a custom dead letter queue, i.e. a call to `DeadLetterAsync()` will confirm the message and publish it to the specified `messagePublisher`.
- **WithGZipCompression(compressionLevel)** and **WithGZipCompression()**: Automatically compresses and decompresses the message body with GZip (must be registered on the publisher and receiver).

Other extension methods: 

- **ProcessByPartitionKeyAsync**: [Azure Event Hubs: Improve scalability with batch parallelization](https://blog.rsuter.com/azure-event-hubs-improve-scalability-with-batch-parallelization/)

### Namotion.Messaging.Storage

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Storage.svg)](https://www.nuget.org/packages/Namotion.Messaging.Storage/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Storage%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Storage/versions/latest?tab=types)

Extension methods to enhance or modify instances: 

- **WithLargeMessageStorage(blobContainer, maxMessageLength)** and **WithLargeMessageStorage(blobContainer)**: Write and read large message contents from a [Namotion.Storage](https://github.com/RicoSuter/Namotion.Storage) blob container (e.g. Azure Blob Storage).

Dependencies: 

- [Namotion.Storage](https://www.nuget.org/packages/Namotion.Storage/)

### Namotion.Messaging.Json

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Json.svg)](https://www.nuget.org/packages/Namotion.Messaging.Json/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Json%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Json/versions/latest?tab=types)

Provides extension methods on `IMessagePublisher<T>` and `IMessageReceiver<T>` to enable JSON serialization for messages: 

- **PublishAsJsonAsync(...):** Sends messages of type T which are serialized to JSON to the queue.
- **ListenAndDeserializeJsonAsync(...):** Receives messages and deserializes their content using the JSON serializer to the `Message<T>.Object` property. If the content could not be deserialized then `Object` is `null`.

Send a JSON encoded message: 

```CSharp
var publisher = ServiceBusMessagePublisher
    .Create("MyConnectionString", "myqueue")
    .AsPublisher<OrderCreatedMessage>();

await publisher.PublishAsJsonAsync(new OrderCreatedMessage { ... });
```

Receive JSON encoded messages:

```CSharp
var receiver = ServiceBusMessageReceiver
    .Create("MyConnectionString", "myqueue")
    .AsReceiver<OrderCreatedMessage>();

await receiver.ListenAndDeserializeJsonAsync(async (messages, ct) => 
{
    foreach (OrderCreatedMessage message in messages.Select(m => m.Object))
    {
        ...
    }

    await receiver.ConfirmAsync(messages, ct);
});
```

## Implementation packages

The following packages should only be used in the head project, i.e. directly in your application bootstrapping project where the dependency injection container is initialized.

|                      | Azure<br /> Service Bus | Azure<br /> Event Hub   | Azure<br /> Storage Queue | RabbitMQ           | Amazon SQS         | InMemory           | MQTT                  |
|----------------------|-------------------------|-------------------------|---------------------------|--------------------|--------------------|--------------------|-----------------------|
| PublishAsync         | :heavy_check_mark:      | :heavy_check_mark:      | :heavy_check_mark:        | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:    |
| ListenAsync          | :heavy_check_mark:      | :heavy_check_mark:      | :heavy_check_mark:        | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:    |
| GetMessageCountAsync | :x:                     | :x:                     | :heavy_check_mark:        | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :X:                   |
| KeepAliveAsync       | :heavy_check_mark:      | :heavy_minus_sign: (1.) | :heavy_check_mark:        | :x:                | :heavy_check_mark: | :heavy_minus_sign: | :X:                   |
| ConfirmAsync         | :heavy_check_mark:      | :heavy_minus_sign: (1.) | :heavy_check_mark:        | :heavy_check_mark: | :heavy_check_mark: | :heavy_minus_sign: | :heavy_check_mark:    |
| RejectAsync          | :heavy_check_mark:      | :heavy_minus_sign: (1.) | :heavy_check_mark:        | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :X:                   |
| DeadLetterAsync      | :heavy_check_mark:      | :x: (2.)                | :x: (2.)                  | :x: (2.)           | :x: (2.)           | :heavy_check_mark: | :X:                   |
| User properties      | :heavy_check_mark:      | :heavy_check_mark:      | :x: (3.)                  | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:(4) |

1) Because Event Hub is stream based and not transactional, these method calls are just ignored.
2) Use `receiver.WithDeadLettering(publisher)` to enable dead letter support.
3) Use `receiver.WithPropertiesInContent()` to enable user properties support (not implemented yet).
4) Set 'ProtocolVersion = MqttProtocolVersion.V500;` in `MQTTConfiguration` when create `MQTTPublisher`

:heavy_minus_sign: = Noop/Ignored

### Namotion.Messaging.Azure.ServiceBus

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Azure.ServiceBus.svg)](https://www.nuget.org/packages/Namotion.Messaging.Azure.ServiceBus/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Azure.ServiceBus%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Azure.ServiceBus/versions/latest?tab=types)

Implementations: 

- **ServiceBusMessagePublisher**
- **ServiceBusMessageReceiver**

Behavior: 

- Multiple queue receivers will process messages in parallel (competing consumers).
- When `handleMessages` throws an exception, then the messages are abandoned and later reprocessed until they are moved to the dead letter queue.

Dependencies: 

- [Microsoft.Azure.ServiceBus](https://www.nuget.org/packages/Microsoft.Azure.ServiceBus/)

### Namotion.Messaging.Azure.EventHub

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Azure.EventHub.svg)](https://www.nuget.org/packages/Namotion.Messaging.Azure.EventHub/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Azure.EventHub%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Azure.ServiceBus/versions/latest?tab=types)

Implementations: 

- **EventHubMessagePublisher**
- **EventHubMessageReceiver**

Behavior: 

- Messages are processed in sequence per partition and can only be retried immediately or be ignored.
- Exceptions from `handleMessages` are logged and then ignored, i.e. the processing moves forward in the partition.
- [More info on retries in the message publishing](https://docs.microsoft.com/en-us/azure/architecture/best-practices/retry-service-specific#event-hubs)

Dependencies: 

- [Microsoft.Azure.EventHubs.Processor](https://www.nuget.org/packages/Microsoft.Azure.EventHubs.Processor/)

### Namotion.Messaging.Azure.Storage.Queue

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Azure.Storage.Queue.svg)](https://www.nuget.org/packages/Namotion.Messaging.Azure.Storage.Queue/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Azure.Storage.Queue%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Azure.Storage.Queue/versions/latest?tab=types)

Implementations: 

- **AzureStorageQueuePublisher**
- **AzureStorageQueueReceiver**

Behavior: 

- When `handleMessages` throws an exception, then the messages are rejected and later reprocessed.

Dependencies: 

- [Microsoft.Azure.Storage.Queue](https://www.nuget.org/packages/Microsoft.Azure.Storage.Queue)

### Namotion.Messaging.RabbitMQ

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.RabbitMQ.svg)](https://www.nuget.org/packages/Namotion.Messaging.RabbitMQ/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.RabbitMQ%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.RabbitMQ/versions/latest?tab=types)

Implementations: 

- **RabbitMessagePublisher**
- **RabbitMessageReceiver**

Behavior: 

- When `handleMessages` throws an exception, then the messages are rejected and later reprocessed.

Dependencies: 

- [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client)

### Namotion.Messaging.Amazon.SQS

[![Nuget](https://img.shields.io/nuget/v/Namotion.Messaging.Amazon.SQS.svg)](https://www.nuget.org/packages/Namotion.Messaging.Amazon.SQS/)
[![Apimundo](https://img.shields.io/badge/Namotion.Messaging.Amazon.SQS%20API-Apimundo-728199.svg)](https://apimundo.com/organizations/nuget-org/nuget-feeds/public/packages/Namotion.Messaging.Amazon.SQS/versions/latest?tab=types)

Implementations: 

- **AmazonSqsMessagePublisher**
- **AmazonSqsMessageReceiver**

Behavior: 

- When `handleMessages` throws an exception, then the messages are rejected and later reprocessed.
- The message's `Content` bytes are serialized to Base64 because SQS can only handle string content.

Dependencies: 

- [AWSSDK.SQS](https://www.nuget.org/packages/AWSSDK.SQS)

### Namotion.Messaging.MQTT



Implementations:

- **MQTTMessagePublisher**
- **MQTTMessageReceiver**

Behavior:

- When `handleMessages` throws an exception, then the messages are ignored.

Dependencies:

- [AWSSDK.SQS](https://www.nuget.org/packages/AWSSDK.SQS)
