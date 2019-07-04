using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;

namespace VideoFunctions
{
    public static class ProcessVideoBlob
    {
        [FunctionName("ProcessVideoBlob")]
        public static void Run([BlobTrigger("media/{name}", Connection = "BlobConnectionString")]ICloudBlob videoBlob, string name, ILogger log, [ServiceBus("videosToProcess", Connection = "ServiceBusConnectionString", EntityType = Microsoft.Azure.WebJobs.ServiceBus.EntityType.Topic)]out Message message)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {videoBlob.Properties.Length} Bytes");
            message = new Message(Encoding.UTF8.GetBytes(name));
            foreach (KeyValuePair<string, string> metadata in videoBlob.Metadata)
            {
                message.UserProperties.Add(metadata.Key, metadata.Value);
            }
        }
    }
}
