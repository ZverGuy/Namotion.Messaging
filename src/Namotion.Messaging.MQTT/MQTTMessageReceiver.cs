using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace Namotion.Messaging.MQTT;

public class MQTTMessageReceiver : IMessageReceiver
{
    private readonly MQTTConfiguration _configuration;

    private ConcurrentDictionary<Message, MqttApplicationMessageReceivedEventArgs> _handledMessages =
        new ConcurrentDictionary<Message, MqttApplicationMessageReceivedEventArgs>();


    public MQTTMessageReceiver(MQTTConfiguration configuration)
    {
        _configuration = configuration;
    }

    public void Dispose()
    {
        _handledMessages.Clear();
    }

    public ValueTask DisposeAsync()
    {
        _handledMessages.Clear();
        return new ValueTask(Task.CompletedTask);
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException" />
    public Task<long> GetMessageCountAsync(CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    public async Task ListenAsync(Func<IReadOnlyCollection<Message>, CancellationToken, Task> handleMessages,
        CancellationToken cancellationToken = default)
    {
        var mqttFactory = new MqttFactory();

        using (var mqttClient = mqttFactory.CreateMqttClient())
        {
            var mqttClientOptions = new MqttClientOptionsBuilder().WithTcpServer(_configuration.Address)
                .WithProtocolVersion(_configuration.ProtocolVersion).WithWillRetain(_configuration.RetainMessage).Build();

            mqttClient.ApplicationMessageReceivedAsync += async args =>
            {
                var message = new Message(
                    id: args.PacketIdentifier.ToString(),
                    content: args.ApplicationMessage.PayloadSegment.Array,
                    properties: args.ApplicationMessage.UserProperties != null
                        ? new ReadOnlyDictionary<string, object>(
                            args.ApplicationMessage.UserProperties.ToDictionary(property => property.Name,
                                property => (object)property.Value))
                        : null);

                // _handledMessages.TryAdd(message, args);
                var messages = new Message[] { message };
                try
                {
                    await handleMessages(messages, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    await RejectAsync(messages, cancellationToken).ConfigureAwait(false);
                }

                // _handledMessages.TryRemove(message, out var t);
            };

            await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

            // Create the subscribe options including several topics with different options.
            // It is also possible to all of these topics using a dedicated call of _SubscribeAsync_ per topic.
            var mqttSubscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
                .WithTopicFilter(t => t.WithTopic(_configuration.Topic))
                .Build();

            var subResult =  await mqttClient.SubscribeAsync(mqttSubscribeOptions, cancellationToken);
            await Task.Delay(Timeout.Infinite, cancellationToken);
            var unsubscribeResult = await mqttClient.UnsubscribeAsync(mqttFactory.CreateUnsubscribeOptionsBuilder()
                .WithTopicFilter(_configuration.Topic).Build(), CancellationToken.None);
            await mqttClient.DisconnectAsync(mqttFactory.CreateClientDisconnectOptionsBuilder().Build(),
                CancellationToken.None);
        }
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException" />
    public Task DeadLetterAsync(IEnumerable<Message> messages, string reason, string errorDescription,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException" />
    public Task KeepAliveAsync(IEnumerable<Message> messages, TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }


    public async Task ConfirmAsync(IEnumerable<Message> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            if (_handledMessages.TryGetValue(message, out var arg))
                await arg.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public Task RejectAsync(IEnumerable<Message> messages, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}