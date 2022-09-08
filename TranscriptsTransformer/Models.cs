
using System;
using System.Collections.Generic;

namespace TranscriptsTransformer
{
    public class TranscriptRow
    {
        public int Timestamp { get; set; }
        public string Author { get; set; }
        public string AadId { get; set; }
        public string Text { get; set; }
        public string MessageId { get; set; }
        public string CallChainId { get; set; }
        public string TenantId { get; set; }
    }

    public class BlobConfig
    {
        public string AccountName { get; set; }
        public string ConnectionString { get; set; }
    }

    public class TenantConfig
    {
        public string AccountName { get; set; }
        public string ConnectionString { get; set; }
        public List<string> TenantIds { get; set; }
    }
}
