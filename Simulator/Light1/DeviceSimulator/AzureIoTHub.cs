using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Common.Exceptions;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DeviceSimulator
{
    public static class AzureIoTHub
    {
        private static int taskDelay = 10 * 1000;

        private static string hubName = "MeetingRoomIoTHub";
        private static string hubSharedAccessKey = "DEFLLFt4CAMMA8w0MlDL4Oe+rBG55JR2tAIoTHNssDw";

        private static string device1Name = "Light1";
        private static string device1SharedAccessKey = "B8Osgghiq8+xhdY0vYQG/fx3AY4HvinFXAIoTPZNHA4=";

        //these are composed from the above values
        private static string iotHubConnectionString = @$"HostName={hubName}.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey={hubSharedAccessKey}";

        private static string deviceConnectionString1 = $"HostName={hubName}.azure-devices.net;DeviceId={device1Name};SharedAccessKey={device1SharedAccessKey}";

        private static string deviceConnectionString = deviceConnectionString1;
        private static string deviceId = device1Name;

        public static async Task<string> CreateDeviceIdentityAsync(string deviceName)
        {
            var registryManager = RegistryManager.CreateFromConnectionString(iotHubConnectionString);
            var device = new Device(deviceName);
            try
            {
                device = await registryManager.AddDeviceAsync(device);
            }
            catch (DeviceAlreadyExistsException)
            {
                device = await registryManager.GetDeviceAsync(deviceName);
            }

            return device.Authentication.SymmetricKey.PrimaryKey;
        }

        public static async Task SendDeviceToCloudMessageAsync(CancellationToken cancelToken)
        {
            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString);

            double avgWatt = 10.0D;
            var rand = new Random();

            while (!cancelToken.IsCancellationRequested)
            {
                double currentWatt = avgWatt + rand.NextDouble() * 4;

                if (DateTime.Now.Millisecond % 2 == 0)
                {
                   if (currentWatt > 12.5D)
                    {
                        currentWatt = 0.0D;
                    }
                    else{
                         currentWatt = 12.0D;
                    }
                    
                }

                bool currentOn = new Random().NextDouble() < 0.8;


                if (currentOn == false)
                {
                    currentWatt = 0.0D;
                }

                var telemetryDataPoint = new TrailerTelemetry
                {
                    id = deviceId,
                    lightpower = currentWatt,
                    lightmaintenance = (currentOn == true && currentWatt == 0),
                    lighton = currentOn
                };
                var messageString = JsonSerializer.Serialize(telemetryDataPoint);

                var message = new Microsoft.Azure.Devices.Client.Message(Encoding.UTF8.GetBytes(messageString))
                {
                    ContentType = "application/json",
                    ContentEncoding = "utf-8"
                };
                await deviceClient.SendEventAsync(message);
                Console.WriteLine($"{DateTime.Now} > Sending message: {messageString}");
                
                //Keep this value above 1000 to keep a safe buffer above the ADT service limits
                //See https://aka.ms/adt-limits for more info
                await Task.Delay(taskDelay);
            }
        }

        public static async Task<string> ReceiveCloudToDeviceMessageAsync()
        {
            var oneSecond = TimeSpan.FromSeconds(1);
            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString);

            while (true)
            {
                var receivedMessage = await deviceClient.ReceiveAsync();
                if (receivedMessage == null)
                {
                    await Task.Delay(oneSecond);
                    continue;
                }

                var messageData = Encoding.ASCII.GetString(receivedMessage.GetBytes());
                await deviceClient.CompleteAsync(receivedMessage);
                return messageData;
            }
        }

        public static async Task ReceiveMessagesFromDeviceAsync(CancellationToken cancelToken)
        {
            try
            {
                string eventHubConnectionString = await IotHubConnection.GetEventHubsConnectionStringAsync(iotHubConnectionString);
                await using var consumerClient = new EventHubConsumerClient(
                    EventHubConsumerClient.DefaultConsumerGroupName,
                    eventHubConnectionString);

                await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsAsync(cancelToken))
                {
                    if (partitionEvent.Data == null) continue;

                    string data = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
                    Console.WriteLine($"Message received. Partition: {partitionEvent.Partition.PartitionId} Data: '{data}'");
                }
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading event: {ex}");
            }
        }
    }
}
