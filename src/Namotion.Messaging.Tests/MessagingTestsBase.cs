﻿using System.Collections.Generic;
using Xunit;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace Namotion.Messaging.Tests
{
    public abstract class MessagingTestsBase
    {
        [Fact]
        public virtual async Task<List<Message>> WhenSendingMessages_ThenMessagesWithPropertisShouldBeReceived()
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
            await publisher.PublishAsync(Enumerable.Range(1, count)
                .Select(i => CreateMessage(content))
                .ToList());

            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(120), receiveCancellation.Token));
            listenCancellation.Cancel();

            // Assert
            Assert.Equal(count, messages.Count);
            foreach (var message in messages)
            {
                Assert.Equal("hello", message.Properties["x-my-property"]);
            }

            return messages;
        }

        [Fact]
        public virtual async Task<List<Message<MyMessage>>> WhenSendingJsonMessages_ThenMessagesShouldBeReceived()
        {
            // Arrange
            var config = GetConfiguration();

            int count = GetMessageCount();
            var orderId = Guid.NewGuid().ToString();

            using var publisher = CreateMessagePublisher(config);
            using var receiver = CreateMessageReceiver(config);

            // Act
            var messages = new List<Message<MyMessage>>();

            var listenCancellation = new CancellationTokenSource();
            var receiveCancellation = new CancellationTokenSource();
            var task = receiver.ListenAndDeserializeJsonAsync(async (msgs, ct) =>
            {
                foreach (var message in msgs
                    .Where(message => message.Object?.Id == orderId))
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
            await publisher.PublishAsJsonAsync(Enumerable.Range(1, count)
                .Select(i => new MyMessage { Id = orderId })
                .ToList());

            await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(120), receiveCancellation.Token));
            listenCancellation.Cancel();

            // Assert
            Assert.Equal(count, messages.Count);
            return messages;
        }

        [Fact]
        public virtual async Task WhenRetrievingMessageCount_ThenCountIsGreaterOrEqualZero()
        {
            // Arrange
            var config = GetConfiguration();
            var receiver = CreateMessageReceiver(config);

            // Act
            var count = await receiver.GetMessageCountAsync();

            // Assert
            Assert.True(count >= 0);
        }

        protected virtual int GetMessageCount()
        {
            return 10;
        }

        protected virtual Message CreateMessage(byte[] content)
        {
            // Arrange
            return new Message(
                content: content,
                properties: new Dictionary<string, object>
                {
                    { "x-my-property", "hello" }
                });
        }

        protected abstract IMessageReceiver<MyMessage> CreateMessageReceiver(IConfiguration configuration);

        protected abstract IMessagePublisher<MyMessage> CreateMessagePublisher(IConfiguration configuration);

        protected static IConfigurationRoot GetConfiguration()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables()
                .Build();
        }
    }

    public class MyMessage
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
    }
}
