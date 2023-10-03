using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.ServiceBus;
using Newtonsoft.Json;

namespace Signature.FuelDisbursementConsumer
{
    public class FuelDisbursementConsumerData
    {
        [FunctionName("FuelDisbursementConsumerData")]
        public static async Task Run(
            [ServiceBusTrigger("%TopicName%", "%SubscriptionName%", Connection = "fuelhubDisbursementSB_ConnectionString")]ServiceBusReceivedMessage message,
            ServiceBusMessageActions messageActions, 
            [CosmosDB( databaseName: "%HubEventDatabaseName%", containerName: "%HubEventCollectionName%", Connection = "fuelhubDataDB_ConnectionString")]IAsyncCollector<dynamic> fuelHubDbOut,
            ILogger log,
            ExecutionContext executionContext)
        {
            string iid = Guid.NewGuid().ToString();
            string fuelEventString = message.Body?.ToString() ?? string.Empty;
            log.LogInformation($"{executionContext.FunctionName}: Process raw sb message: iid {iid}. raw body: {fuelEventString}");

            try {
                if (!string.IsNullOrEmpty(fuelEventString))
                {
                    dynamic data = JsonConvert.DeserializeObject(fuelEventString);
                    await fuelHubDbOut.AddAsync(data);
                    log.LogInformation($"{executionContext.FunctionName}: document saved: iid {iid}");
                }
            }
            catch (Exception e)
            {
                log.LogError($"{executionContext.FunctionName}: DB failed. request iid {iid}, Error: {e.Message}");
                await messageActions.DeadLetterMessageAsync(message, "DATABASE_WRITE_FAILED", e.Message);
            }

            await messageActions.CompleteMessageAsync(message);
        }
    }
}
