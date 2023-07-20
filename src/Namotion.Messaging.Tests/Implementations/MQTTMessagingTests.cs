using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using MQTTnet.Server;
using Namotion.Messaging.MQTT;
using Xunit;
using Xunit.Abstractions;

namespace Namotion.Messaging.Tests.Implementations
{
    public class MQTTMessagingTests : MessagingTestsBase
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private MqttServer _server;
        
        public MQTTMessagingTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }
        protected override IMessageReceiver<MyMessage> CreateMessageReceiver(IConfiguration configuration)
        {
            
            return new MQTTMessageReceiver(new MQTTConfiguration()
            {
                Address = "localhost",
                Topic = "/lol",
                ProtocolVersion = MqttProtocolVersion.V500,
                QoS = MqttQualityOfServiceLevel.AtLeastOnce,
                RetainMessage = false,
            }).AsReceiver<MyMessage>();
        }

        protected override IMessagePublisher<MyMessage> CreateMessagePublisher(IConfiguration configuration)
        {
            return new MQTTMessagePublisher(new MQTTConfiguration()
            {
                Address = "localhost",
                Topic = "/lol",
                ProtocolVersion = MqttProtocolVersion.V500,
                QoS = MqttQualityOfServiceLevel.AtLeastOnce,
                RetainMessage = false,
            }).AsPublisher<MyMessage>();
        }

        [Fact(Skip = "Not Supported")]
        public override Task WhenRetrievingMessageCount_ThenCountIsGreaterOrEqualZero()
        {
            return base.WhenRetrievingMessageCount_ThenCountIsGreaterOrEqualZero();
        }

        public override async Task<List<Message>> WhenSendingMessages_ThenMessagesWithPropertisShouldBeReceived()
        {
            // Arrange
            var config = GetConfiguration();

            int count = GetMessageCount();
            var content = Guid.NewGuid().ToByteArray();

            using var publisher = CreateMessagePublisher(config);
            using var receiver = CreateMessageReceiver(config);

            // Act
            var messages = new List<Message>();
            var listenCancellation = new CancellationTokenSource();
            var receiveCancellation = new CancellationTokenSource();
            var task = receiver.ListenWithRetryAsync(async (msgs, ct) =>
            {
                await receiver.KeepAliveAsync(msgs, TimeSpan.FromMinutes(1));
                _testOutputHelper.WriteLine($"msg all count: {msgs.Count}");
                _testOutputHelper.WriteLine($"equal msg all count: {msgs.Count(message => message.Content.SequenceEqual(content))}");

                foreach (var message in msgs
                             .Where(message => message.Content.SequenceEqual(content)))
                {
                    messages.Add(message);
                }

                if (messages.Count == count)
                {
                    receiveCancellation.Cancel();
                }

                await receiver.ConfirmAsync(msgs, ct);
            }, listenCancellation.Token);

            var stopwatch = Stopwatch.StartNew();
            
            //add delay before and after publish because mqtt dont save old messages
            var pubTask = Task.Delay(TimeSpan.FromSeconds(15)).ContinueWith(async t =>
            {
                await publisher.PublishAsync(Enumerable.Range(1, count)
                    .Select(i => CreateMessage(content))
                    .ToList());
            }).ContinueWith(async t => await Task.Delay(TimeSpan.FromSeconds(5)));

            await Task.WhenAny(task, pubTask, Task.Delay(TimeSpan.FromSeconds(120), receiveCancellation.Token));
            listenCancellation.Cancel();

            // Assert
            Assert.Equal(count, messages.Count);
            foreach (var message in messages)
            {
                Assert.Equal("hello", message.Properties["x-my-property"]);
            }

            return messages;
        }
    }
}