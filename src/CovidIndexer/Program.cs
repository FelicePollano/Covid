using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace CovidIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            HttpClient client = new HttpClient();
            var stream = await client.GetStreamAsync("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv");
            string line;
            
            using(var sr = new StreamReader(stream))
            {
                line = await sr.ReadLineAsync();
                var dates = ExtractTimeStamps(line);
                while(null!=(line = await sr.ReadLineAsync()))
                {
                    Console.WriteLine(line);
                }
            }
            
        }

        private static DataDoc[]  ExtractTimeStamps(string line)
        {
           List<DataDoc> docs = new List<DataDoc>();
           var chunks = line.Split(',');
           for(int i=4;i<chunks.Length;++i)
           {
               var dt = DateTime.ParseExact(chunks[i],"M/d/yy",null);
               var k = new DataDoc();
               k.TimeStamp = dt;
               docs.Add(k);
           }   
           return docs.ToArray();
        }
    }
}
