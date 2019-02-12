using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Documents.Client;
using Microsoft.DataTransfer.Extensibility;

namespace Microsoft.DataTransfer.DocumentDb.Client.PartitionResolvers
{
    sealed class CollectionNamePartitionResolver : IPartitionResolver
    {
        private readonly string partitionKeyProperty;
        private readonly IList<string> collectionNames;

        private readonly Dictionary<string, string> collectionLinks = new Dictionary<string, string>();

        public CollectionNamePartitionResolver(string partitionKeyProperty, IReadOnlyList<string> collectionNames)
        {
            this.partitionKeyProperty = partitionKeyProperty;
            this.collectionNames = collectionNames.ToList();
        }

        public object GetPartitionKey(object document)
        {
            if (!(document is IDataItem dataItem))
                throw Errors.FailedToExtractPartitionKey(Resources.DocumentIsNotDataItem);

            string partitionKey = null;
            try
            {
                // extract the collection id from the _self property
                partitionKey = dataItem.GetValue(partitionKeyProperty).ToString();
                partitionKey = partitionKey.Substring(0, partitionKey.IndexOf("docs/"));
            }
            catch (Exception error)
            {
                throw Errors.FailedToExtractPartitionKey(error.Message);
            }

            return partitionKey;
        }

        public string ResolveForCreate(object partitionKey)
        {
            string key = partitionKey.ToString();

            // if the collection id is not yet mapped to a collection name, we associate it with the next one
            if (!collectionLinks.ContainsKey(key))
            {
                if (collectionNames.Count > 0)
                {
                    collectionLinks.Add(key, RemoveAndGet(collectionNames, 0));
                }
                else
                {
                    throw Errors.NotEnoughCollectionNameToMap(key);
                }
            }

            return collectionLinks[key];
        }

        private T RemoveAndGet<T>(IList<T> list, int index)
        {
            lock (list)
            {
                T value = list[index];
                list.RemoveAt(index);
                return value;
            }
        }

        public IEnumerable<string> ResolveForRead(object partitionKey)
        {
            throw new NotImplementedException();
        }
    }
}
