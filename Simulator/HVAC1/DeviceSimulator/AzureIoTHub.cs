using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Common.Exceptions;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Shared;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;
using System.Collections.Generic;

namespace DeviceSimulator
{
    public static class AzureIoTHub
    {
        private static int taskDelay = 10 * 1000;

        private static string modelPath1 = @"../../../Models/xgb_1.onnx";

        private static string hubName = "MeetingRoomIoTHub";
        private static string hubSharedAccessKey = "DEFLLFt4CAMMA8w0MlDL4Oe+rBG55JR2tAIoTHNssDw";

        private static string device1Name = "HVAC1";
        private static string device1SharedAccessKey = "dLywiA1GpmzVxw/fMB7jjfF9esHQ0Z7I4AIoTDEtBPo=";

         private static string targetDeviceId = "Outside1"; 

        //these are composed from the above values
        private static string iotHubConnectionString = @$"HostName={hubName}.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey={hubSharedAccessKey}";

        private static string deviceConnectionString1 = $"HostName={hubName}.azure-devices.net;DeviceId={device1Name};SharedAccessKey={device1SharedAccessKey}";

        private static string eventHubConnectionString = $"Endpoint=sb://ihsuprodsgres002dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=DEFLLFt4CAMMA8w0MlDL4Oe+rBG55JR2tAIoTHNssDw=;EntityPath=iothub-ehub-meetingroo-56987846-6f2ceff93f";

        private static string deviceConnectionString = deviceConnectionString1;
        private static string deviceId = device1Name;

        // Creates or retrieves a device identity in Azure IoT Hub.
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

        // Simulates a device sending telemetry data to Azure IoT Hub.
        public static async Task SendDeviceToCloudMessageAsync(CancellationToken cancelToken)
        {
            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString);

            double avgWatt = 600.0D;
            double avgAirSpeed = 5.0D;
            double avgTemperature = 19.0D;
            var rand = new Random();
            double intermediateValue = 20.0D;

            while (!cancelToken.IsCancellationRequested)
            {
                double currentWatt = avgWatt + (rand.NextDouble() - 0.5) * 20;
                double currentAirSpeed = avgAirSpeed + (rand.NextDouble()- 0.5) * 4;
                double currentTemperature = avgTemperature + (rand.NextDouble()- 0.5) * 4;
                
                if (DateTime.Now.Millisecond % 2 == 0)
                {
                    currentWatt = 600.0D;
                    currentAirSpeed = 4.5D;
                    currentTemperature = 19.3D;
                }

                try
                {
                    // Timeout duration
                    TimeSpan timeout = TimeSpan.FromSeconds(30);

                    // Task to retrieve telemetry for outside humidity
                    var humidityTask = IoTHubHelper.GetTelemetryFromDeviceAsync(
                        eventHubConnectionString,
                        targetDeviceId,
                        "outsidehumidity",
                        cancelToken
                    );

                    // Task to retrieve telemetry for outside temperature
                    var temperatureTask = IoTHubHelper.GetTelemetryFromDeviceAsync(
                        eventHubConnectionString,
                        targetDeviceId,
                        "outsidetemperature",
                        cancelToken
                    );

                    // Add timeout to telemetry retrieval
                    var completedHumidityTask = await Task.WhenAny(humidityTask, Task.Delay(timeout, cancelToken));
                    var completedTemperatureTask = await Task.WhenAny(temperatureTask, Task.Delay(timeout, cancelToken));

                    if (completedHumidityTask == humidityTask && completedTemperatureTask == temperatureTask)
                    {
                        // Both tasks completed successfully within timeout
                        double outsideHumidity = await humidityTask;
                        double outsideTemperature = await temperatureTask;

                        Console.WriteLine($"Telemetry Retrieved - Humidity: {outsideHumidity}, Temperature: {outsideTemperature}");

                        // Step 1: Predict intermediate value (Model1)
                        using var session1 = new InferenceSession(modelPath1);

                        // Prepare tensor for Model1 input
                        var inputTensor1 = new DenseTensor<float>(
                            new float[] { (float)outsideTemperature, (float)outsideHumidity },
                            new int[] { 1, 2 }
                        );

                        var input1 = NamedOnnxValue.CreateFromTensor("float_input", inputTensor1);
                        using var results1 = session1.Run(new List<NamedOnnxValue> { input1 });

                        // Extract the intermediate prediction value
                        intermediateValue = 0;
                        foreach (var result in results1)
                        {
                            intermediateValue = result.AsTensor<float>().GetValue(0);
                            break;
                        }

                        Console.WriteLine($"Intermediate Prediction: {intermediateValue}");
                    }
                    else
                    {
                        // Timeout occurred
                        Console.WriteLine("Telemetry retrieval timed out.");
                    }
                }
                catch (TaskCanceledException)
                {
                    Console.WriteLine("Telemetry retrieval was canceled.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error retrieving telemetry or processing: {ex.Message}");
                }

                var telemetryDataPoint = new TrailerTelemetry
                {
                    id = deviceId,
                    hvacpower = currentWatt,
                    hvacairspeed = currentAirSpeed,
                    hvactemperature = currentTemperature,
                    hvacmaintenance = (currentWatt > ((0.05 * Math.Pow(avgAirSpeed, 3) + 2 * avgAirSpeed + 5 * avgTemperature + 100) * 1.3)),
                    hvacidealtemperature = intermediateValue
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

        // Listens for messages sent from Azure IoT Hub to the device.
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

        // Reads messages sent by devices to Azure IoT Hub through the Event Hub endpoint.
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
                    //Console.WriteLine($"Message received. Partition: {partitionEvent.Partition.PartitionId} Data: '{data}'");
                }
            }
            catch (TaskCanceledException) { } // do nothing
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading event: {ex}");
            }
        }
    }
}
