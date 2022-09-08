using System;
using Azure;
using System.Net;
using Azure.Search.Documents.Indexes;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Search.Documents.Indexes.Models;
using System.Diagnostics;
using System.Threading.Tasks;

namespace TranscriptsTransformer
{
    public class Indexers
    {
        public Indexers()
        {
        }

        public static async Task ListIndexers()
        {
            // https://[service name].search.windows.net/indexers?api-version=[api-version]
            // Content - Type: application / json
            // api - key: [admin key]

            string serviceName = "blob-storage-search";
            string apiKey = "41FcowTLHpcNKznlIwqtUJ5zD7hYlkBmRQI7SaEccrAzSeCltSIn";

            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

            await indexerClient.CreateDataSourceConnectionAsync(
                new SearchIndexerDataSourceConnection("ts-fb73857f-a518-433a-823c-499623be4fa0", SearchIndexerDataSourceType.AzureBlob, "DefaultEndpointsProtocol=https;AccountName=eustorageaccountislucid;AccountKey=iR1XwJ6JZniHIvuMsNQB5SoGskcdTd+4KJmz57DwDUM/JsEyTeJslbT+9D64XEK47qlCn550OI3qGuZteuDnIA==;EndpointSuffix=core.windows.net",
                new SearchIndexerDataContainer("ts-fb73857f-a518-433a-823c-499623be4fa0")));
        }

        private static async Task DeleteIndexer (SearchIndexerClient indexerClient, string indexerName)
        {
            try
            {
                await indexerClient.DeleteIndexerAsync(indexerName);
                await indexerClient.DeleteDataSourceConnectionAsync($"ts-{indexerName}");
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        private static async Task RunIndexer(SearchIndexerClient indexerClient, string indexerName)
        {
            try
            {
                await indexerClient.RunIndexerAsync(indexerName);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        private static async Task ResetIndexer(SearchIndexerClient indexerClient, string indexerName)
        {
            try
            {
                await indexerClient.GetIndexerAsync(indexerName);
                //Rest the indexer if it exsits.
                await indexerClient.ResetIndexerAsync(indexerName);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        /*
        SearchIndexClient indexClient = new SearchIndexClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

        foreach (var page in indexClient.GetIndexes().AsPages(default, 1000))
        {
            foreach (SearchIndex searchIndex in page.Values)
            {
                Debug.WriteLine(searchIndex.Name);
            }
        }

        SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

        foreach (var indexerName in indexerClient.GetIndexerNames().Value)
        {
            Debug.WriteLine(indexerName);

            // await ResetIndexer(indexerClient, indexerName);

            // await RunIndexer(indexerClient, indexerName);

            await DeleteIndexer(indexerClient, indexerName);
        }
        */
    }
}
