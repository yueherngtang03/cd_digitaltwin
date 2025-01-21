using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using System.Text;
using System.Text.Json;
using System.Collections.Generic;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Shared
{
    public static class IoTHubHelper
    {
        public static async Task<double> GetTelemetryFromDeviceAsync(
            string eventHubConnectionString,
            string targetDeviceId,
            string telemetryField,
            CancellationToken cancellationToken)
        {
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create Event Hub consumer client
            await using var consumerClient = new EventHubConsumerClient(consumerGroup, eventHubConnectionString);

            Console.WriteLine($"Listening for telemetry from device: {targetDeviceId} using consumer group '{consumerGroup}'...");

            // Use startReadingAtEarliestEvent: false to skip older events and read only new ones
            await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsAsync(
                startReadingAtEarliestEvent: false, // start from the latest event
                cancellationToken: cancellationToken))
            {
                if (partitionEvent.Data == null)
                {
                    Console.WriteLine("No data in partition event.");
                    continue;
                }

                string data = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
                Console.WriteLine($"Raw message received: {data}");

                try
                {
                    // Deserialize the raw message into a Dictionary
                    var telemetry = JsonSerializer.Deserialize<Dictionary<string, object>>(data);

                    if (telemetry == null)
                    {
                        Console.WriteLine("Deserialization returned null.");
                        continue;
                    }

                    // Check if the message is from the target device
                    if (telemetry.TryGetValue("id", out var deviceId) && deviceId.ToString() == targetDeviceId)
                    {
                        Console.WriteLine($"Message is from the target device: {deviceId}");

                        // Check if the telemetryField exists
                        if (telemetry.TryGetValue(telemetryField, out var telemetryValue))
                        {
                            Console.WriteLine($"Telemetry field '{telemetryField}' value: {telemetryValue}");

                            // Attempt to parse as double
                            if (telemetryValue is JsonElement jsonElement && jsonElement.TryGetDouble(out double value))
                            {
                                return value;
                            }
                            else if (double.TryParse(telemetryValue.ToString(), out double convertedValue))
                            {
                                return convertedValue;
                            }
                            else
                            {
                                Console.WriteLine($"Unable to convert telemetry value '{telemetryValue}' to double.");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Telemetry field '{telemetryField}' not found in the message.");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Skipping message from device. Received DeviceId: {deviceId}. Expected: {targetDeviceId}");
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"Error parsing JSON: {ex.Message}");
                    Console.WriteLine($"Raw message: {data}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error: {ex.Message}");
                }
            }

            // If the loop ends, no matching telemetry was found
            throw new Exception($"No telemetry received from {targetDeviceId} for field '{telemetryField}'.");
        }
    }
}
