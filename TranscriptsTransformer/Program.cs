using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Newtonsoft.Json;

namespace TranscriptsTransformer
{
    internal class Program
    {
        private static readonly string OUTPUT_PATH = "/Users/aliuspetraska/Documents/BlobStorage";

        private static List<BlobConfig> configs = new List<BlobConfig>()
        {
            new BlobConfig
            {
                AccountName = "usstorageaccountislucid",
                ConnectionString = "DefaultEndpointsProtocol=https;AccountName=usstorageaccountislucid;AccountKey=ZT1kQoFPm/trqyBO5GPcxuXj5b1pEo1HatRof0f89GKLQjityZJ1ix2+RNn8KIlFDrgrPIKc0hN+KCbNl7LUWg==;EndpointSuffix=core.windows.net",
            },
            new BlobConfig
            {
                AccountName = "eustorageaccountislucid",
                ConnectionString = "DefaultEndpointsProtocol=https;AccountName=eustorageaccountislucid;AccountKey=iR1XwJ6JZniHIvuMsNQB5SoGskcdTd+4KJmz57DwDUM/JsEyTeJslbT+9D64XEK47qlCn550OI3qGuZteuDnIA==;EndpointSuffix=core.windows.net",
            }
        };

        static void Main(string[] args)
        {
            // DataSources.CreateDataSources().GetAwaiter().GetResult();

            // DataSources.CreateIndex().GetAwaiter().GetResult();

            // DataSources.CreateIndexers().GetAwaiter().GetResult();

            // DataSources.ResetIndexers().GetAwaiter().GetResult();

            DataSources.RunIndexers().GetAwaiter().GetResult();

            /*

            // DOWNLOAD && TRANSFORM

            CreateMissingDirectories(new List<string>() { "original", "transformed" });

            List<TempObject> tempObjects = new List<TempObject>();

            foreach (var config in configs)
            {
                BlobServiceClient blobServiceClient = new BlobServiceClient(config.ConnectionString);

                foreach (var directory in new List<string>() { "original", "transformed" })
                {
                    if (!Directory.Exists(Path.Combine(OUTPUT_PATH, directory, config.AccountName)))
                    {
                        Directory.CreateDirectory(Path.Combine(OUTPUT_PATH, directory, config.AccountName));
                    }
                }

                var containers = ProcessContainers(blobServiceClient, config.AccountName).GetAwaiter().GetResult();

                tempObjects.Add(new TempObject
                {
                    AccountName = config.AccountName,
                    ConnectionString = "",
                    TenantIds = containers,
                });
            }

            Debug.WriteLine(JsonConvert.SerializeObject(tempObjects));

            // READ DIRECTORY && UPLOAD

            foreach (var config in configs)
            {
                var directories = Directory.GetDirectories(Path.Combine(OUTPUT_PATH, "transformed", config.AccountName), "*", SearchOption.TopDirectoryOnly);

                foreach (var directory in directories)
                {
                    var tenantId = new DirectoryInfo(directory).Name;

                    // Creating Blob Containers

                    BlobServiceClient blobServiceClient = new BlobServiceClient(config.ConnectionString);

                    var blobContainerClient = blobServiceClient.GetBlobContainerClient($"ts-{tenantId}");

                    blobContainerClient.CreateIfNotExists();

                    // Listing all files

                    var files = Directory.GetFiles(directory, "*.json", SearchOption.TopDirectoryOnly);

                    foreach (var file in files)
                    {
                        var blobName = Path.GetFileName(file);

                        BlobClient blobClient = blobContainerClient.GetBlobClient(blobName);

                        Debug.WriteLine(blobName);

                        using (FileStream content = File.OpenRead(file))
                        {
                            // https://docs.microsoft.com/en-us/azure/storage/blobs/access-tiers-overview#summary-of-access-tier-options

                            blobClient.UploadAsync(content, new BlobHttpHeaders { ContentType = "application/json" }, accessTier: AccessTier.Hot).GetAwaiter().GetResult();
                        }
                    }
                }
            }

            */

            Console.WriteLine("Done.");
        }

        private static void CreateMissingDirectories(List<string> directories)
        {
            foreach (var directory in directories)
            {
                if (!Directory.Exists(Path.Combine(OUTPUT_PATH, directory)))
                {
                    Directory.CreateDirectory(Path.Combine(OUTPUT_PATH, directory));
                }
            }
        }

        private static async Task<List<string>> ProcessContainers(BlobServiceClient blobServiceClient, string accountName)
        {
            var containers = new List<string>();

            foreach (var page in blobServiceClient.GetBlobContainers().AsPages(default, 1000))
            {
                foreach (BlobContainerItem blobContainerItem in page.Values)
                {
                    if (blobContainerItem.Name.Contains("-transcriptionlogs"))
                    {
                        var tenantId = blobContainerItem.Name.Replace("-transcriptionlogs", string.Empty);

                        containers.Add(tenantId);

                        foreach (var directory in new List<string>() { "original", "transformed" })
                        {
                            if (!Directory.Exists(Path.Combine(OUTPUT_PATH, directory, accountName, tenantId)))
                            {
                                Directory.CreateDirectory(Path.Combine(OUTPUT_PATH, directory, accountName, tenantId));
                            }
                        }

                        await ProcessBlobs(blobServiceClient, accountName, tenantId, blobContainerItem.Name);
                    }
                }
            }

            return containers;
        }

        private static async Task ProcessBlobs(BlobServiceClient blobServiceClient, string accountName, string tenantId, string blobContainerName)
        {
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

            foreach (var page in blobContainerClient.GetBlobs().AsPages(default, 1000))
            {
                foreach (BlobItem blobItem in page.Values)
                {
                    if (blobItem.Properties.ContentLength > 0)
                    {
                        await ProcessBlob(blobContainerClient, accountName, tenantId, blobItem.Name);
                    }
                }
            }
        }

        private static async Task ProcessBlob(BlobContainerClient blobContainerClient, string accountName, string tenantId, string blobItemName)
        {
            var originalFilePath = Path.Combine(OUTPUT_PATH, "original", accountName, tenantId, blobItemName.Replace("_transcription", string.Empty));

            if (!File.Exists(originalFilePath))
            {
                var blobClient = blobContainerClient.GetBlobClient(blobItemName);

                FileStream fileStream = File.OpenWrite(originalFilePath);

                await blobClient.DownloadToAsync(fileStream);

                fileStream.Close();
            }

            TransformBlob(originalFilePath, accountName, tenantId);
        }

        private static void TransformBlob(string filePath, string accountName, string tenantId)
        {
            string callChainId = Path.GetFileName(filePath).Replace(".txt", string.Empty);

            List<TranscriptRow> rows = new List<TranscriptRow>();

            string[] lines = File.ReadAllLines(filePath, Encoding.UTF8);

            Debug.WriteLine(callChainId);

            try
            {
                for (var i = 0; i < (int)Math.Round((double)(lines.Count() / 3)); i++)
                {
                    var line_1 = lines[i * 3 + 0].Trim();
                    var line_2 = lines[i * 3 + 1].Trim();

                    string[] separatingStrings = { "###", "%%%" };

                    var parts_1 = line_1.Split(separatingStrings, StringSplitOptions.RemoveEmptyEntries);
                    var parts_2 = line_2.Split(separatingStrings, StringSplitOptions.RemoveEmptyEntries);

                    // -------------------------------------------------------------------

                    TranscriptRow row = new TranscriptRow()
                    {
                        Timestamp = Int32.Parse(parts_1[0].Substring(0, 10)),
                        Author = parts_1[0].Substring(11).Trim(),
                        CallChainId = callChainId,
                        TenantId = tenantId,
                    };

                    // AadId

                    if (parts_1.Count() > 1)
                    {
                        row.AadId = parts_1[1].Trim();
                    }
                    else
                    {
                        row.AadId = "00000000-0000-0000-0000-000000000000";
                    }

                    // Text

                    if (parts_2.Count() > 0)
                    {
                        row.Text = parts_2[0].Trim();
                    }

                    // MessageId

                    if (parts_2.Count() > 1)
                    {
                        row.MessageId = parts_2[1].Trim();
                    }
                    else
                    {
                        row.MessageId = Guid.NewGuid().ToString();
                    }

                    // Final add

                    rows.Add(row);
                }

                File.WriteAllText(Path.Combine(OUTPUT_PATH, "transformed", accountName, tenantId, $"{callChainId}.json"), JsonConvert.SerializeObject(rows, Formatting.None));
            }
            catch (Exception exception)
            {
                Debug.WriteLine(exception.Message);
            }
        }
    }
}
