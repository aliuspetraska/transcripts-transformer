using Azure;
using Azure.Search;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace TranscriptsTransformer
{
    public class DataSources
    {
        public static List<TenantConfig> Configuration = new List<TenantConfig>()
        {
            new TenantConfig
            {
                AccountName = "usstorageaccountislucid",
                ConnectionString = "DefaultEndpointsProtocol=https;AccountName=usstorageaccountislucid;AccountKey=ZT1kQoFPm/trqyBO5GPcxuXj5b1pEo1HatRof0f89GKLQjityZJ1ix2+RNn8KIlFDrgrPIKc0hN+KCbNl7LUWg==;EndpointSuffix=core.windows.net",
                TenantIds = new List<string>()
                {
                    "03bd8a60-540d-4f89-b808-74ffcf021f53",
                    "0a4aa634-71bc-4d6e-b29c-d0ebd8402b32",
                    "0eaf69cd-6023-416a-aa3f-f22acdd2106d",
                    "1b2207af-b559-41ac-9987-28ab9c6adf73",
                    "25008fb2-22f1-4816-a817-345b629d6aa7",
                    "2aa10b25-24bd-4137-9691-c7c7d10a3a02",
                    "3fb223d7-d1d5-4c60-8af5-443cbccf2757",
                    "55a6f593-4942-46a8-b6fe-fa21752d3927",
                    "6683aa0e-0260-42ae-845f-ac00c6df7226",
                    "6fab3c2a-175f-4164-8e9a-21cb9aed5e6e",
                    "7586f192-c2dd-4895-82cd-acc0a5036c7e",
                    "77520799-9a7d-4713-a612-f88ba6da6649",
                    "82aa3d6b-e336-4ccc-8805-988950fc19e0",
                    "8a3bb26b-c20f-4547-b75f-b854cf6fffbd",
                    "9fe9419f-a606-483a-a6e1-e8c63de575bb",
                    "a478c779-b5c0-47ff-8d18-ff1bf0e57b41",
                    "a946a841-29a5-4acb-a310-7336a7134218",
                    "b7ab2de5-e8a9-4ce8-8f43-c85a85840de4",
                    "e7ea46e4-ebec-4d13-80fe-a65931354a10",
                    "ee1ba185-5b8f-4a1a-988c-50d9d57663da",
                    "fa70e402-3d6c-419c-97ca-4ea295cd3414"
                },
            },
            new TenantConfig
            {
                AccountName = "eustorageaccountislucid",
                ConnectionString = "DefaultEndpointsProtocol=https;AccountName=eustorageaccountislucid;AccountKey=iR1XwJ6JZniHIvuMsNQB5SoGskcdTd+4KJmz57DwDUM/JsEyTeJslbT+9D64XEK47qlCn550OI3qGuZteuDnIA==;EndpointSuffix=core.windows.net",
                TenantIds = new List<string>()
                {
                    "0a571e8a-d9d1-40e0-8361-b4fe88bd3a0b",
                    "0a9d4768-8cb5-4b1c-bbc8-db8d22d576da",
                    "177d1db3-1b27-4c4a-a672-61e1238cb48f",
                    "1eea10e4-0c4e-413e-abf9-a62e58e73253",
                    "425fcf34-7378-4e37-9e29-90dce4002a9d",
                    "546f0c5b-4839-4027-b345-0944ebeee546",
                    "56264744-b965-431b-9ea2-f67f7fc42de4",
                    "6bec6ec3-53d8-4450-9b65-ee10269d1dba",
                    "8645fdac-d72a-4953-b051-1a3027923d62",
                    "9188040d-6c67-4c5b-b112-36a304b66dad",
                    "9784bf45-4b98-4a7e-a537-ccd3b9867982",
                    "9ad8e586-ef09-4504-a3db-4f7ea34a4883",
                    "9c4ddf91-1f98-41c0-91f2-ebaa4afbda4c",
                    "a1e97c4d-7067-41cb-bc1b-9026da1d1e85",
                    "b613de1d-004e-4fd2-a4a5-94c3632922f9",
                    "b624c3c4-6bf0-43b4-9b83-991206a29932",
                    "c64a4e50-c84d-49a5-ae3f-3ad2224351aa",
                    "d456b1a9-ef40-4aae-a765-3e195bf31eae",
                    "db795ab6-4fb0-4937-b873-2eb8f2c26313",
                    "fa7a82b0-f9a9-4308-a4fb-4f68370c082c",
                    "fb73857f-a518-433a-823c-499623be4fa0"
                }
            }
        };

        public static string serviceName = "blob-storage-search";
        public static string apiKey = "41FcowTLHpcNKznlIwqtUJ5zD7hYlkBmRQI7SaEccrAzSeCltSIn";

        public DataSources()
        {
        }

        public static async Task CreateIndexers()
        {
            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

            foreach (var config in Configuration)
            {
                foreach (var tenantId in config.TenantIds)
                {
                    Debug.WriteLine(tenantId);

                    await indexerClient.CreateOrUpdateIndexerAsync(new SearchIndexer(tenantId, $"ts-{tenantId}", "default-index")
                    {
                        Parameters = new IndexingParameters
                        {
                            IndexingParametersConfiguration = new IndexingParametersConfiguration
                            {
                                DataToExtract = BlobIndexerDataToExtract.ContentAndMetadata,
                                ParsingMode = BlobIndexerParsingMode.JsonArray
                            }
                        },
                        FieldMappings =
                        {
                            new FieldMapping("AzureSearch_DocumentKey") { TargetFieldName = "AzureSearch_DocumentKey", MappingFunction = new FieldMappingFunction("base64Encode") }
                        }
                    });
                }
            }
        }

        public static async Task CreateDataSources()
        {
            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

            foreach (var config in Configuration)
            {
                foreach (var tenantId in config.TenantIds)
                {
                    Debug.WriteLine(tenantId);

                    await indexerClient.CreateOrUpdateDataSourceConnectionAsync(new SearchIndexerDataSourceConnection($"ts-{tenantId}", SearchIndexerDataSourceType.AzureBlob, config.ConnectionString, new SearchIndexerDataContainer($"ts-{tenantId}")));
                }
            }
        }

        public static async Task CreateIndex()
        {
            SearchIndexClient indexClient = new SearchIndexClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

            SearchIndex index = new SearchIndex("default-index")
            {
                Fields =
                {
                    new SimpleField("Timestamp", SearchFieldDataType.Int64) { IsFilterable = true, IsSortable = true },
                    new SimpleField("Author", SearchFieldDataType.String),
                    new SimpleField("AadId", SearchFieldDataType.String),
                    new SearchableField("Text") { AnalyzerName = LexicalAnalyzerName.StandardLucene },
                    new SimpleField("MessageId", SearchFieldDataType.String),
                    new SimpleField("CallChainId", SearchFieldDataType.String),
                    new SimpleField("TenantId", SearchFieldDataType.String) { IsFilterable = true },
                    new SimpleField("AzureSearch_DocumentKey", SearchFieldDataType.String) { IsKey = true },
                    new SimpleField("metadata_storage_content_type", SearchFieldDataType.String),
                    new SimpleField("metadata_storage_size", SearchFieldDataType.Int64),
                    new SimpleField("metadata_storage_last_modified", SearchFieldDataType.DateTimeOffset),
                    new SimpleField("metadata_storage_content_md5", SearchFieldDataType.String),
                    new SimpleField("metadata_storage_name", SearchFieldDataType.String),
                    new SimpleField("metadata_storage_path", SearchFieldDataType.String),
                    new SimpleField("metadata_storage_file_extension", SearchFieldDataType.String),
                }
            };

            Debug.WriteLine("default-index");

            await indexClient.CreateIndexAsync(index);
        }

        public static async Task ResetIndexers()
        {
            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

            foreach (var indexerName in indexerClient.GetIndexerNames().Value)
            {
                Debug.WriteLine(indexerName);

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
        }

        public static async Task RunIndexers()
        {
            SearchIndexerClient indexerClient = new SearchIndexerClient(new Uri($"https://{serviceName}.search.windows.net"), new AzureKeyCredential(apiKey));

            foreach (var indexerName in indexerClient.GetIndexerNames().Value)
            {
                Debug.WriteLine(indexerName);

                try
                {
                    await indexerClient.RunIndexerAsync(indexerName);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex.Message);
                }
            }
        }
    }
}
