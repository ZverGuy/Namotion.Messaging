using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;

namespace Namotion.Messaging.MQTT;

public class MQTTMessagePublisher : IMessagePublisher
{
    private readonly MQTTConfiguration _configuration;
    private readonly IMqttClient _mqttClient;

    public MQTTMessagePublisher(MQTTConfiguration configuration)
    {
        _configuration = configuration;
        var mqttFactory = new MqttFactory();
        _mqttClient = mqttFactory.CreateMqttClient();
        var mqttClientOptions = new MqttClientOptionsBuilder().WithTcpServer(_configuration.Address, _configuration.Port)
            .WithProtocolVersion(_configuration.ProtocolVersion).Build();
        _mqttClient.ConnectAsync(mqttClientOptions).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        DisposeAsync().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        await _mqttClient.DisconnectAsync(new MqttClientDisconnectOptions()
        {
            Reason = MqttClientDisconnectOptionsReason.NormalDisconnection
        });
        _mqttClient.Dispose();
    }

    public async Task PublishAsync(IEnumerable<Message> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            if (cancellationToken.IsCancellationRequested)
                continue;
            var applicationMessage = new MqttApplicationMessageBuilder()
                .WithTopic(_configuration.Topic)
                .WithPayload(message.Content)
                .WithRetainFlag(_configuration.RetainMessage);

            if (_configuration.ProtocolVersion == MqttProtocolVersion.V500)
            {
                foreach (var messageProperty in message.Properties)
                {
                   
                    applicationMessage.WithUserProperty(messageProperty.Key, messageProperty.Value.ToString());
                }
            }
           
            await _mqttClient.PublishAsync(applicationMessage.Build(), cancellationToken);
        }
    }
}