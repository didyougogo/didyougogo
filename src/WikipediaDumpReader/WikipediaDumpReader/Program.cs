using System.IO;
using System.Net;
using System.Text;
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
                using (var stream = File.OpenRead(fileName))
                using (var reader = new StreamReader(stream, Encoding.Unicode))
                {
                    var line = reader.ReadLine();

                    while (!string.IsNullOrWhiteSpace(line))
                    {
                        line = reader.ReadLine();

                        if (line.StartsWith("]") || read++ == count)
                        {
                            break;
                        }
                        else if (line.StartsWith("["))
                        {
                            continue;
                        }

                        var jsonObj = JsonConvert.DeserializeObject(line);
                        var formatted = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                        writer.Write(formatted);
                    }
                }
            }
            else if (action == "submit")
            {
                var fileName = args[1];
                var url = args[2];
                var count = int.Parse(args[3]);
                var read = 0;

                using (var stream = File.OpenRead(fileName))
                using (var reader = new StreamReader(stream, Encoding.Unicode))
                {
                    var line = reader.ReadLine();

                    while (!string.IsNullOrWhiteSpace(line))
                    {
                        line = reader.ReadLine();

                        if (line.StartsWith("]") || read++ == count)
                        {
                            break;
                        }
                        else if (line.StartsWith("["))
                        {
                            continue;
                        }

                        Submit(JsonConvert.DeserializeObject(line), url);
                    }
                }
            }
        }

        private static void Submit(object document, string url)
        {
            var httpWebRequest = (HttpWebRequest)WebRequest.Create(url);
            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "POST";

            var json = JsonConvert.SerializeObject(document);

            using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
            {
                streamWriter.Write(json);
            }

            var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                var result = streamReader.ReadToEnd();
            }
        }
    }
}
