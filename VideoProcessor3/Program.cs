using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NReco.VideoConverter;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VideoProcessor3
{
    class Program
    {
        private static NamespaceManager _nameSpaceManager = null;
        private static readonly string _connectionString = ConfigurationManager.AppSettings["ServiceBusConnectionString"];
        private static readonly string _topicName = ConfigurationManager.AppSettings["TopicName"];
        private static readonly string _subscriptionName = "Customer3Subscriber";
        private static bool _isStopped = false;

        static void Main(string[] args)
        {
            // Connect to the Service Bus namespace using a SAS Url with the Manage permission
            _nameSpaceManager = NamespaceManager.CreateFromConnectionString(_connectionString);

            // Create the topic if it does not already exist
            if (!_nameSpaceManager.TopicExists(_topicName)) _nameSpaceManager.CreateTopic(_topicName);

            // Create a new subscription with a message filter to only accept
            // messages from the ServiceBusSender - to be used by the ServiceBusSubscriber web application
            SqlFilter subscriptionFilter = new SqlFilter("Customer = 'Customer3'");
            if (!_nameSpaceManager.SubscriptionExists(_topicName, _subscriptionName)) _nameSpaceManager.CreateSubscription(_topicName, _subscriptionName, subscriptionFilter);

            Console.WriteLine($"**** {_subscriptionName} has started ***");
            Task.Run(() => ProcessMessages());
            Console.WriteLine($"*** {_subscriptionName} is Running. Press any key to stop. ***");
            Console.Read();
            Console.WriteLine($"*** {_subscriptionName} is Stopping ***");
            _isStopped = true;
        }

        private static void ProcessMessages()
        {
            SubscriptionClient subscriptionClient = SubscriptionClient.CreateFromConnectionString(_connectionString, _topicName, _subscriptionName);
            do
            {
                BrokeredMessage message = subscriptionClient.Receive(new TimeSpan(0, 1, 0));
                if (message != null)
                {
                    Stream stream = message.GetBody<Stream>();
                    StreamReader reader = new StreamReader(stream);
                    string blobName = reader.ReadToEnd();
                    // Process video here
                    Console.WriteLine($"*** Processing file {blobName} ***");
                    if (blobName.EndsWith(".mp4"))
                    {
                        ProcessVideo(blobName);
                    }
                    message.Complete();
                }
                Task.Delay(3000).Wait();
            } while (!_isStopped);
        }

        private static void ProcessVideo(string blobName)
        {
            string blobConnectionString = ConfigurationManager.AppSettings["BlobConnectionString"];
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(blobConnectionString);
            CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference("media");
            CloudBlob cloudBlob = cloudBlobContainer.GetBlobReference(blobName);
            string tempInputFileName = Path.Combine(Path.GetTempPath(), blobName);
            string tempOutputFileName = Path.Combine(Path.GetTempPath(), blobName.Replace(".mp4", ".gif"));
            Directory.CreateDirectory(Path.GetDirectoryName(tempOutputFileName));
            cloudBlob.DownloadToFile(tempInputFileName, FileMode.Create);
            var ffmpeg = new FFMpegConverter();
            ffmpeg.ConvertMedia(tempInputFileName, null, tempOutputFileName, null, new ConvertSettings());
            CloudBlockBlob convertedBlob = cloudBlobContainer.GetBlockBlobReference(blobName.Replace(".mp4", ".gif"));
            convertedBlob.Properties.ContentType = "image/gif";
            foreach (KeyValuePair<string, string> metadata in cloudBlob.Metadata)
            {
                convertedBlob.Metadata.Add(metadata.Key, metadata.Value);
            }
            convertedBlob.UploadFromFile(tempOutputFileName);
        }
    }
}
