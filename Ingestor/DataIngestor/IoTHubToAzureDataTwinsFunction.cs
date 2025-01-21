// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using Azure.Identity;
using Azure.DigitalTwins.Core;
using Azure.Core.Pipeline;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Azure;
using System.Threading.Tasks;

namespace DataIngestor
{
    //https://docs.microsoft.com/en-us/azure/digital-twins/how-to-authenticate-client#write-application-code
    //https://docs.microsoft.com/en-us/azure/digital-twins/how-to-ingest-iot-hub-data?tabs=cli#create-a-function

    public static class IoTHubToAzureDataTwinsFunction
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("IoTHubToADTFunction")]
        public static async Task RunAsync([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation(eventGridEvent.Data.ToString());

            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");

            try
            {
                // Authenticate with Digital Twins
                var cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");
                var client = new DigitalTwinsClient(
                    new Uri(adtInstanceUrl),
                    cred,
                    new DigitalTwinsClientOptions { Transport = new HttpClientTransport(httpClient) });
                log.LogInformation($"ADT service client connection created.");

                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                    string deviceId = (string)deviceMessage["systemProperties"]?["iothub-connection-device-id"];
                    var sensorData = deviceMessage["body"];

                    if (deviceId == null || sensorData == null)
                    {
                        log.LogError("Missing deviceId or sensor data in event data.");
                        return;
                    }

                    log.LogInformation($"Device:{deviceId}, Sensor Data: {sensorData}");

                    var updateTwinData = new JsonPatchDocument();

                    JObject parsedSensorData = (JObject)sensorData;

                    // Iterate through all sensor data and update dynamically
                    foreach (var property in parsedSensorData.Properties())
                    {
                        string propertyName = property.Name;
                        JToken propertyValue = property.Value;

                        if (propertyName != "id" && propertyValue != null)
                        {
                            // Ensure that the property path exists in the Digital Twin model
                            string twinPath = $"/{propertyName}";

                            if (propertyValue.Type == JTokenType.Integer || propertyValue.Type == JTokenType.Float)
                            {
                                updateTwinData.AppendReplace(twinPath, propertyValue.Value<double>());
                            }
                            else if (propertyValue.Type == JTokenType.Boolean)
                            {
                                updateTwinData.AppendReplace(twinPath, propertyValue.Value<bool>());
                            }
                            else
                            {
                                log.LogWarning($"Unexpected type for {propertyName}: {propertyValue.Type}");
                            }
                        }
                    }

                    await client.UpdateDigitalTwinAsync(deviceId, updateTwinData);
                }
                // {
                //     log.LogInformation(eventGridEvent.Data.ToString());

                //     // <Find_device_ID_and_temperature>
                //     JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
                //     string deviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                //     var sensorData = deviceMessage["body"];
                //     var lightpower = deviceMessage["body"]["lightpower"];
                //     var lightmaintenance = deviceMessage["body"]["lightmaintenance"];

                //     log.LogInformation($"Device:{deviceId}, Sensor Data: {sensorData}");

                //     log.LogInformation($"Device:{deviceId} Power is:{lightpower}, Need Maintenance is: {lightmaintenance}");

                //     // <Update_twin_with_device_temperature>
                //     var updateTwinData = new JsonPatchDocument();
                //     updateTwinData.AppendReplace("/lightpower", lightpower.Value<double>());
                //     updateTwinData.AppendReplace("/lightmaintenance", lightmaintenance.Value<bool>());
                //     await client.UpdateDigitalTwinAsync(deviceId, updateTwinData);

                    
                // }
            }
            catch (Exception ex)
            {
                log.LogError($"Error in ingest function: {ex.Message}");
            }
        }
    }
}
