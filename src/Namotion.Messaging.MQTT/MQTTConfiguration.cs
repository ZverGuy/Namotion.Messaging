using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace Namotion.Messaging.MQTT;

public class MQTTConfiguration
{
    public string Address { get; set; }
    
    public int? Port { get; set; }
    public string Topic { get; set; }

    public MqttQualityOfServiceLevel QoS { get; set; } = MqttQualityOfServiceLevel.AtMostOnce;

    public bool RetainMessage { get; set; } = false;

    public MqttProtocolVersion ProtocolVersion = MqttProtocolVersion.V311;
}