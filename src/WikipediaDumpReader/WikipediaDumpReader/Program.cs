using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using ICSharpCode.SharpZipLib.BZip2;
using Newtonsoft.Json;

namespace WikipediaDumpReader
{
    class Program
    {
        static void Main(string[] args)
        {
            var action = args[0].ToLower();

            if (action == "unpack")
            {
                var compressedFileName = args[1];
                var targetFileName = args[2];

                using (var compressedStream = File.OpenRead(compressedFileName))
                using (var decompressedStream = File.Create(targetFileName))
                {
                    BZip2.Decompress(compressedStream, decompressedStream, true);
                }
            }
            else if (action == "copy")
            {
                var fileName = args[1];
                var target = args[2];
                var count = int.Parse(args[3]);
                var read = 0;

                using (var targetStream = File.Create(target))
                using (var writer = new StreamWriter(targetStream))
                using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096))
                using (var stream = new BufferedStream(fs, 4096))
                using (var reader = new StreamReader(stream))
                {
                    Console.WriteLine("attempting to read first line");

                    var line = reader.ReadLine();

                    Console.WriteLine("read first line");

                    while (!string.IsNullOrWhiteSpace(line))
                    {
                        line = reader.ReadLine();

                        read++;

                        Console.WriteLine("read line " + read);

                        if (line.StartsWith("]") || read == count)
                        {
                            break;
                        }
                        else if (line.StartsWith("["))
                        {
                            continue;
                        }

                        var jsonObj = JsonConvert.DeserializeObject(line);
                        var formatted = JsonConvert.SerializeObject(jsonObj, Formatting.None);

                        writer.Write(formatted);
                        writer.WriteLine();
                    }
                }
            }
            else if (action == "submit")
            {
                var fileName = args[1];
                var url = args[2];
                var count = int.Parse(args[3]);
                var batchSize = int.Parse(args[4]);
                var batchCount = 1;
                var fullTime = Stopwatch.StartNew();

                foreach (var batch in ReadFile(fileName, count).Batch(batchSize))
                {
                    var time = Stopwatch.StartNew();

                    Submit(
                        batch.Where(x => x.Contains("title")).Select(x => new Dictionary<string, object>
                            {
                                { "_language", x["language"].ToString() },
                                { "__url", string.Format("www.wikipedia.org/search-redirect.php?family=wikipedia&language={0}&search={1}", x["language"], x["title"]) },
                                { "title", x["title"] },
                                { "body", x["text"] }
                            }), 
                        url);

                    Console.WriteLine("submit {0} took {1}", batchCount++, time.Elapsed);
                }

                Console.WriteLine("write took {0}", fullTime.Elapsed);
            }
        }

        private static IEnumerable<IDictionary> ReadFile(string fileName, int count)
        {
            var read = 0;

            using (var stream = File.OpenRead(fileName))
            using (var reader = new StreamReader(stream))
            {
                var line = reader.ReadLine();

                while (!string.IsNullOrWhiteSpace(line))
                {
                    line = reader.ReadLine();

                    read++;

                    if (line.StartsWith("]") || read == count)
                    {
                        break;
                    }
                    else if (line.StartsWith("["))
                    {
                        continue;
                    }

                    yield return JsonConvert.DeserializeObject<IDictionary>(line);
                }
            }
        }

        private static void Submit(IEnumerable<object> documents, string url)
        {
            var httpWebRequest = (HttpWebRequest)WebRequest.Create(url);
            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "POST";

            var json = JsonConvert.SerializeObject(documents);

            using (var stream = httpWebRequest.GetRequestStream())
            {
                Serialize(documents, stream);
            }

            var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                var result = streamReader.ReadToEnd();
            }
        }

        private static void Serialize(IEnumerable<object> docs, Stream stream)
        {
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, docs);
                jsonWriter.Flush();
            }
        }
    }

    public static class ListHelper
    {
        public static IEnumerable<IEnumerable<T>> Batch<T>(
        this IEnumerable<T> source, int size)
        {
            T[] bucket = null;
            var count = 0;

            foreach (var item in source)
            {
                if (bucket == null)
                    bucket = new T[size];

                bucket[count++] = item;

                if (count != size)
                    continue;

                yield return bucket;

                bucket = null;
                count = 0;
            }

            // Return the last bucket with all remaining elements
            if (bucket != null && count > 0)
                yield return bucket.Take(count);
        }
    }
}
